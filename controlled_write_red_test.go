package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// gatedConn wraps accepted connections: the first Write (the HTTP 101
// handshake response) passes; every later Write blocks until release or Close.
// This gates the bridge's real transport Write path, mirroring the accepted
// SSE controlled-backpressure proof.
type gatedConn struct {
	net.Conn
	first   sync.Once
	release chan struct{}
	closed  chan struct{}
	closeIt sync.Once
	entered chan struct{}
	enterIt sync.Once
}

func (g *gatedConn) Write(p []byte) (int, error) {
	fresh := false
	g.first.Do(func() { fresh = true })
	if fresh {
		return g.Conn.Write(p) // handshake response passes
	}
	g.enterIt.Do(func() { close(g.entered) })
	select {
	case <-g.release:
		return g.Conn.Write(p)
	case <-g.closed:
		return 0, net.ErrClosed
	}
}

func (g *gatedConn) Close() error {
	g.closeIt.Do(func() { close(g.closed) })
	return g.Conn.Close()
}

type gatedListener struct {
	net.Listener
	conns []*gatedConn
	mu    sync.Mutex
}

func (l *gatedListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	g := &gatedConn{Conn: c, release: make(chan struct{}), closed: make(chan struct{}), entered: make(chan struct{})}
	l.mu.Lock()
	l.conns = append(l.conns, g)
	l.mu.Unlock()
	return g, nil
}

func (l *gatedListener) releaseAll() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, g := range l.conns {
		select {
		case <-g.release:
		default:
			close(g.release)
		}
	}
}

func mustListen(t *testing.T) net.Listener {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	return l
}

// TestTransparentTurnWriteOutlives30sBudget proves the accepted example for
// the bridge: a turn whose client stops reading is NOT terminated by the
// bridge's own 30-second sendText deadline while the upstream stays
// connected. Regression on the unfixed bridge: the socket is closed at ~30s
// (client read fails before the gate releases) — a local established-request
// timeout decision. After the fix, the write parks past 30s, resumes on
// release, and the same connection completes the turn.
func TestTransparentTurnWriteOutlives30sBudget(t *testing.T) {
	upstreamConnected := make(chan struct{}, 1)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		select {
		case upstreamConnected <- struct{}{}:
		default:
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_a\",\"status\":\"in_progress\"}}\n\n")
		fmt.Fprint(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_a\",\"status\":\"completed\",\"output\":[]}}\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer up.Close()
	uURL, _ := url.Parse(up.URL)

	cfg := config{
		upstream: uURL, listen: "127.0.0.1:0",
		maxMsgBytes: 1 << 20, maxBodyBytes: 1 << 20,
	}
	br := newBridge(cfg)
	gl := &gatedListener{Listener: mustListen(t)}
	bs := &http.Server{Handler: br}
	serveErr := make(chan error, 1)
	go func() { serveErr <- bs.Serve(gl) }()
	t.Cleanup(func() {
		gl.releaseAll()
		_ = bs.Close()
		select {
		case <-serveErr:
		default:
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	dctx, dcancel := context.WithTimeout(ctx, 5*time.Second)
	conn, _, err := websocket.Dial(dctx, "ws://"+gl.Addr().String()+"/v1/responses", nil)
	dcancel()
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.CloseNow() })
	conn.SetReadLimit(1 << 20)

	wctx, wcancel := context.WithTimeout(ctx, 5*time.Second)
	if err := conn.Write(wctx, websocket.MessageText, []byte(`{"type":"response.create","model":"m","input":[]}`)); err != nil {
		wcancel()
		t.Fatalf("write create: %v", err)
	}
	wcancel()

	select {
	case <-upstreamConnected:
	case <-ctx.Done():
		t.Fatal("upstream never received the turn")
	}

	gl.mu.Lock()
	gated := gl.conns[0]
	gl.mu.Unlock()
	select {
	case <-gated.entered:
	case <-ctx.Done():
		t.Fatal("proxy never entered the gated application transport Write")
	}

	// Hold the actual application transport write past the old 30-second budget.
	type readRes struct {
		typ  websocket.MessageType
		data []byte
		err  error
	}
	readCh := make(chan readRes, 1)
	go func() {
		rctx, rcancel := context.WithTimeout(context.Background(), 36*time.Second)
		defer rcancel()
		typ, data, err := conn.Read(rctx)
		readCh <- readRes{typ, data, err}
	}()

	holdDone := make(chan struct{})
	go func() {
		time.Sleep(32 * time.Second)
		close(holdDone)
	}()
	select {
	case res := <-readCh:
		t.Fatalf("contract violation: bridge terminated the stalled-client turn by itself (read returned type=%v err=%v) while the upstream stayed connected", res.typ, res.err)
	case <-holdDone:
	}

	// Release the gate: both events must arrive in order on the SAME
	// connection, then the connection stays usable.
	gl.releaseAll()
	gotCompleted := false
	for events := 0; events < 2; events++ {
		var typ websocket.MessageType
		var data []byte
		var err error
		if events == 0 {
			select {
			case result := <-readCh:
				typ, data, err = result.typ, result.data, result.err
			case <-ctx.Done():
				t.Fatal("first reader did not return after transport gate release")
			}
		} else {
			typ, data, err = conn.Read(ctx)
		}
		if err != nil {
			t.Fatalf("read after release %d: %v", events, err)
		}
		if typ != websocket.MessageText {
			t.Fatalf("frame type %v", typ)
		}
		var ev map[string]any
		if err := json.Unmarshal(data, &ev); err != nil {
			t.Fatalf("event JSON: %v", err)
		}
		if ev["type"] == "response.completed" {
			gotCompleted = true
		}
	}
	if !gotCompleted {
		t.Fatal("turn never completed after release")
	}
	conn.CloseRead(ctx)
	pctx, pcancel := context.WithTimeout(ctx, 5*time.Second)
	err = conn.Ping(pctx)
	pcancel()
	if err != nil {
		t.Fatalf("connection not usable after resumed turn: %v", err)
	}
}
