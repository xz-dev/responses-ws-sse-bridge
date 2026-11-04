package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// This tests actual client TCP close, not a Ping without a client reader.
// The reader's queue-full application error must never prevent observing
// cancellation when another application writer owns the transport mutex.
func TestTransparentReaderErrorCancelsBlockedWriter(t *testing.T) {
	upDone := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(upDone)
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_a\"}}\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	t.Cleanup(up.Close)
	u, _ := url.Parse(up.URL)
	b := newBridge(config{upstream: u, maxMsgBytes: 4096, maxBodyBytes: 4096, workBytes: 1 << 20, diskBytes: 1 << 20, spoolDir: t.TempDir()})
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
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	conn, _, err := websocket.Dial(ctx, "ws://"+gl.Addr().String()+"/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.CloseNow() })
	frame := []byte(`{"type":"response.create","model":"m","input":[]}`)
	if err := conn.Write(ctx, websocket.MessageText, frame); err != nil {
		t.Fatal(err)
	}
	gl.mu.Lock()
	gate := gl.conns[0]
	gl.mu.Unlock()
	select {
	case <-gate.entered:
	case <-ctx.Done():
		t.Fatal("worker did not enter real blocked transport Write")
	}
	for i := 0; i < 2; i++ {
		if err := conn.Write(ctx, websocket.MessageText, frame); err != nil {
			t.Fatal(err)
		}
	}
	if err := conn.CloseNow(); err != nil {
		t.Fatal(err)
	}
	<-handlerDone // a write parked behind the blocked worker hangs here
	select {
	case <-upDone:
	case <-ctx.Done():
		t.Fatal("handler returned without releasing upstream")
	}
	if b.work.Load() != 0 {
		t.Fatalf("cancel left %d working bytes charged", b.work.Load())
	}
}
