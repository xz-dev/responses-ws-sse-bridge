package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"strings"
	"testing"

	"github.com/coder/websocket"
)

// readerDraining reports whether a socket reader goroutine has entered
// drainSocket, i.e. it refused a message and handed the refusal to the worker.
func readerDraining() bool {
	buf := make([]byte, 1<<20)
	return strings.Contains(string(buf[:runtime.Stack(buf, true)]), ".drainSocket(")
}

// Review B2: a reader-side refusal while a turn is in flight (the worker is
// blocked in a real transport Write), then a client TCP close. The reader
// must still observe the close, so the turn's upstream request, spools and
// leases are released. Every wait is on a condition; a leak hangs until the
// go test timeout.
func TestReaderRefusalThenClientCloseReleasesTurn(t *testing.T) {
	upDone := make(chan struct{})
	up := newSSEUpstream(t)
	up.sse = func(w http.ResponseWriter, r *http.Request) {
		defer close(upDone)
		fmt.Fprint(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_a\"}}\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}
	uSrv := httptest.NewServer(up.handler())
	t.Cleanup(uSrv.Close)
	u, _ := url.Parse(uSrv.URL)
	b := newBridge(config{upstream: u, maxMsgBytes: 4096, maxBodyBytes: 4096, spoolDir: t.TempDir()})
	gl := &gatedListener{Listener: mustListen(t)}
	handlerDone := make(chan struct{})
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(handlerDone)
		b.ServeHTTP(w, r)
	})}
	joined := make(chan struct{})
	go func() { _ = server.Serve(gl); close(joined) }()
	t.Cleanup(func() {
		gl.releaseAll()
		_ = server.Close()
		<-joined
	})
	ctx := context.Background()
	conn, _, err := websocket.Dial(ctx, "ws://"+gl.Addr().String()+"/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.CloseNow() })
	if err := conn.Write(ctx, websocket.MessageText, []byte(`{"type":"response.create","model":"m","input":[]}`)); err != nil {
		t.Fatal(err)
	}
	gl.mu.Lock()
	gate := gl.conns[0]
	gl.mu.Unlock()
	<-gate.entered // the worker relays response.created into a blocked Write
	over := `{"type":"response.create","model":"m","input":"` + strings.Repeat("x", 8192) + `"}`
	if err := conn.Write(ctx, websocket.MessageText, []byte(over)); err != nil {
		t.Fatal(err)
	}
	for !readerDraining() {
		runtime.Gosched()
	}
	if err := conn.CloseNow(); err != nil {
		t.Fatal(err)
	}
	<-handlerDone
	<-upDone
	if b.work.Load() != 0 || b.disk.Load() != 0 || len(openSpools(t, b.cfg.spoolDir)) != 0 {
		t.Fatalf("close left memory=%d disk=%d spools=%d", b.work.Load(), b.disk.Load(), len(openSpools(t, b.cfg.spoolDir)))
	}
}
