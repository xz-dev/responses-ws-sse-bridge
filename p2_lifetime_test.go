package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime/pprof"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/coder/websocket"
)

type observedUploadTransport struct {
	http.RoundTripper
	bodies chan *ownedRequestBody
}

func (t observedUploadTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if body, ok := r.Body.(*ownedRequestBody); ok {
		t.bodies <- body
	}
	return t.RoundTripper.RoundTrip(r)
}

type uploadWindowGate struct {
	net.Conn
	writes  int
	entered chan int
	closed  chan struct{}
	once    sync.Once
}

func (g *uploadWindowGate) Write(p []byte) (int, error) {
	g.writes++
	if g.writes > 1 {
		g.entered <- len(p)
		<-g.closed
		return 0, net.ErrClosed
	}
	return g.Conn.Write(p)
}
func (g *uploadWindowGate) Close() error {
	g.once.Do(func() { close(g.closed) })
	return g.Conn.Close()
}

func TestHTTPUploadLeaseLivesUntilBodyClose(t *testing.T) {
	b := requestTestBridge(t, 2*httpUploadWindowBytes)
	release := b.acquire(httpUploadWindowBytes)
	if release == nil {
		t.Fatal("positive upload lease refused")
	}
	owned := &ownedRequestBody{Reader: bytes.NewBufferString("complete"), done: make(chan struct{}), releaseUpload: release}
	if _, e := io.ReadAll(owned); e != nil {
		t.Fatal(e)
	}
	if got := b.work.Load(); got != httpUploadWindowBytes {
		t.Fatalf("EOF prematurely released upload credit: %d", got)
	}
	select {
	case <-owned.done:
		t.Fatal("EOF acknowledged body close")
	default:
	}
	_ = owned.Close()
	_ = owned.Close()
	select {
	case <-owned.done:
	default:
		t.Fatal("close not acknowledged")
	}
	assertRequestResourcesZero(t, b)
}

func TestHTTPUploadReserveAndConstructionFailuresRelease(t *testing.T) {
	b := requestTestBridge(t, 2*httpUploadWindowBytes)
	b.upstreamBase = "%"
	if e := b.runUpstream(context.Background(), nil, bytes.NewBufferString("x"), 1, http.Header{}, "/v1/responses"); e == nil {
		t.Fatal("invalid URL accepted")
	}
	assertRequestResourcesZero(t, b)
	b.cfg.workBytes = httpUploadWindowBytes - 1
	if e := b.runUpstream(context.Background(), nil, bytes.NewBufferString("x"), 1, http.Header{}, "/v1/responses"); !errors.Is(e, errWorkBudget) {
		t.Fatalf("expected true upload capacity failure: %v", e)
	}
	assertRequestResourcesZero(t, b)
}

func TestSmallBlockedHTTPUploadWindowsCharged(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.Copy(io.Discard, r.Body) }))
	t.Cleanup(up.Close)
	u, _ := url.Parse(up.URL)
	b := newBridge(config{upstream: u, maxMsgBytes: 4096, maxBodyBytes: 4096, workBytes: 1 << 20, spoolDir: t.TempDir()})
	entered := make(chan int, 1)
	transport := b.client.Transport.(*http.Transport)
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, e := (&net.Dialer{}).DialContext(ctx, network, address)
		if e != nil {
			return nil, e
		}
		return &uploadWindowGate{Conn: conn, entered: entered, closed: make(chan struct{})}, nil
	}
	bodies := make(chan *ownedRequestBody, 1)
	b.client.Transport = observedUploadTransport{RoundTripper: transport, bodies: bodies}
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { defer close(done); b.ServeHTTP(w, r) }))
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, e := websocket.Dial(ctx, wsURL(server.URL)+"/v1/responses", nil)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = conn.CloseNow() })
	head := []byte(`{"type":"response.create","a":"`)
	frame := append(append(head, bytes.Repeat([]byte("x"), 4080-len(head)-2)...), '"', '}')
	if e := conn.Write(ctx, websocket.MessageText, frame); e != nil {
		t.Fatal(e)
	}
	var tail int
	select {
	case tail = <-entered:
	case <-ctx.Done():
		t.Fatal("no real second upload Write")
	}
	owned := <-bodies
	select {
	case <-owned.done:
		t.Fatal("transport credit released while upload parked")
	default:
	}
	var stack bytes.Buffer
	_ = pprof.Lookup("goroutine").WriteTo(&stack, 2)
	if !bytes.Contains(stack.Bytes(), []byte("net/http.(*transferWriter).doBodyCopy")) {
		t.Fatal("fixture did not reach actual HTTP body copy")
	}
	if dir := os.Getenv("BRIDGE_REPAIR_ARTIFACTS"); dir != "" {
		if e := os.WriteFile(filepath.Join(dir, "upload-repair-small-stack.txt"), stack.Bytes(), 0600); e != nil {
			t.Fatal(e)
		}
	}
	var fs syscall.Statfs_t
	if e := syscall.Statfs(b.cfg.spoolDir, &fs); e != nil {
		t.Fatal(e)
	}
	block, record := int64(fs.Bsize), int64(fs.Bsize)
	if fs.Type == 0x2fc12fc1 {
		block = 4096
	}
	var physical int64
	files, e := os.ReadDir(b.cfg.spoolDir)
	if e != nil {
		t.Fatal(e)
	}
	for _, file := range files {
		stat, e := file.Info()
		if e != nil {
			t.Fatal(e)
		}
		data := block
		for data < stat.Size() {
			data *= 2
		}
		physical += spoolPhysicalBase + data + ((data+record-1)/record)*block
	}
	charged := b.work.Load()
	minimum := int64((8+8+32+8)<<10) + int64(tail)
	t.Logf("frame=%d parkedTail=%d totalCharge=%d physicalReservation=%d RAMCharge=%d observedMinRAM=%d separateUploadLease=%d", len(frame), tail, charged, physical, charged-physical, minimum, httpUploadWindowBytes)
	if charged-physical < minimum {
		t.Fatalf("live HTTP windows/stacks undercharged by %d", minimum-(charged-physical))
	}
	if charged-physical < sessionBaseBytes+httpUploadWindowBytes+requestMetadataBytes {
		t.Fatal("upload windows borrowed session or spool-metadata credit")
	}
	_ = conn.CloseNow()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("upload cancellation did not join handler")
	}
	select {
	case <-owned.done:
	default:
		t.Fatal("handler exited before transport body close")
	}
	assertRequestResourcesZero(t, b)
}

func TestSpoolRandomPhysicalAndMetadataAreDistinct(t *testing.T) {
	for _, size := range []int{64 << 10, 128 << 10, 256 << 10, 512 << 10} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			b := requestTestBridge(t, 8<<20)
			want := make([]byte, size)
			_, _ = rand.New(rand.NewSource(27)).Read(want)
			spool, e := b.newChargedSpool("bridge-physical-*.json")
			if e != nil {
				t.Fatal(e)
			}
			defer spool.release()
			for at := 0; at < size; {
				end := at + (32 << 10)
				if end > size {
					end = size
				}
				if _, e := spool.Write(want[at:end]); e != nil {
					t.Fatal(e)
				}
				stat, e := spool.file.Stat()
				if e != nil {
					t.Fatal(e)
				}
				actual := stat.Sys().(*syscall.Stat_t).Blocks * 512
				if actual+requestMetadataBytes > b.work.Load() {
					t.Fatalf("physical=%d + independent Go/FIFO=%d exceeds quota=%d at partial=%d", actual, requestMetadataBytes, b.work.Load(), end)
				}
				at = end
			}
			got, e := os.ReadFile(spool.msg.name)
			if e != nil || !bytes.Equal(got, want) {
				t.Fatal("incompressible bytes changed", e)
			}
			spool.release()
			assertRequestResourcesZero(t, b)
		})
	}
}

func TestSpooledEventJSONValidationAndDuplicateSemantics(t *testing.T) {
	cases := []string{
		`{"type":"ignored","type":"response.completed","response":{"id":"a","id":"last"},"sequence_number":9}`,
		`{"t\u0079pe":"response.completed","response":{"\u0069d":"resp_\ud83d\udc08"},"sequence_number":11}`,
		`{"type":"response.completed","response":{"id":1,"id":"later"}}`,
		`{"type":"response.completed","response":{"id":"earlier","id":null}}`,
		`{"type":"response.completed","payload":[1,]}`,
		`{"type":"response.completed","payload":{"x" 1}}`,
		`{"type":"response.completed","payload":"bad\q"}`,
		`{"type":"response.completed","payload":01}`,
		`null`,
	}
	for _, raw := range cases {
		t.Run(raw, func(t *testing.T) {
			b := requestTestBridge(t, 8<<20)
			spool, e := b.newChargedSpool("bridge-json-*.json")
			if e != nil {
				t.Fatal(e)
			}
			defer spool.release()
			if _, e := spool.Write([]byte(raw)); e != nil {
				t.Fatal(e)
			}
			var original map[string]json.RawMessage
			valid := json.Unmarshal([]byte(raw), &original) == nil && original != nil
			got, e := b.inspectEvent(spool)
			if (e == nil) != valid {
				t.Fatalf("validation differs from encoding/json: valid=%v got=%v", valid, e)
			}
			if e == nil {
				defer got.releaseID()
				var typ string
				var seq int64
				var response struct {
					ID string `json:"id"`
				}
				_ = json.Unmarshal(original["type"], &typ)
				_ = json.Unmarshal(original["sequence_number"], &seq)
				id := ""
				if json.Unmarshal(original["response"], &response) == nil {
					id = response.ID
				}
				if got.typ != typ || got.seq != seq || got.id != id {
					t.Fatalf("metadata differs: got=%+v expected type=%q seq=%d id=%q", got, typ, seq, id)
				}
			}
		})
	}
}

func TestProperCloseReleasesBlockedWriterWithinProtocolGrace(t *testing.T) {
	upDone := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(upDone)
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_close\"}}\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	t.Cleanup(up.Close)
	u, _ := url.Parse(up.URL)
	b := newBridge(config{upstream: u, maxMsgBytes: 4096, maxBodyBytes: 4096, workBytes: 2 << 20, spoolDir: t.TempDir()})
	gl := &gatedListener{Listener: mustListen(t)}
	handlerDone := make(chan struct{})
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { defer close(handlerDone); b.ServeHTTP(w, r) })}
	go server.Serve(gl)
	t.Cleanup(func() { gl.releaseAll(); _ = server.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _, e := websocket.Dial(ctx, "ws://"+gl.Addr().String()+"/v1/responses", nil)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = conn.CloseNow() })
	if e := conn.Write(ctx, websocket.MessageText, []byte(`{"type":"response.create","model":"m","input":[]}`)); e != nil {
		t.Fatal(e)
	}
	gl.mu.Lock()
	gate := gl.conns[0]
	gl.mu.Unlock()
	select {
	case <-gate.entered:
	case <-ctx.Done():
		t.Fatal("writer never parked")
	}
	started := time.Now()
	closed := make(chan error, 1)
	go func() { closed <- conn.Close(websocket.StatusNormalClosure, "done") }()
	timer := time.NewTimer(7 * time.Second)
	defer timer.Stop()
	select {
	case <-handlerDone:
	case <-timer.C:
		t.Fatal("proper Close exceeded existing five-second protocol grace plus padding")
	}
	select {
	case <-upDone:
	case <-ctx.Done():
		t.Fatal("proper Close leaked upstream")
	}
	select {
	case e := <-closed:
		t.Logf("proper Close completed after %s; client result=%v", time.Since(started), e)
	case <-ctx.Done():
		t.Fatal("client close goroutine leaked")
	}
	assertRequestResourcesZero(t, b)
}
