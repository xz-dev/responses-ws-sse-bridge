package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// Input spooling is deliberately stalled before frame EOF. Charging only
// after a completed spool cannot bound concurrent partial-frame materialization.
func TestTransparentPartialSpoolsChargedBeforeEOF(t *testing.T) {
	const size = 32 << 10
	u, _ := url.Parse("http://127.0.0.1:9")
	b := newBridge(config{upstream: u, maxMsgBytes: 1 << 20, workBytes: 64 << 10, spoolDir: t.TempDir()})
	var wg sync.WaitGroup
	var writers []*io.PipeWriter
	var results []chan wsMsgLite
	var received []bool
	var messages []wsMsgLite
	t.Cleanup(func() {
		for _, writer := range writers {
			_ = writer.Close()
		}
		wg.Wait()
		for i, ch := range results {
			if !received[i] {
				messages = append(messages, <-ch)
			}
		}
		for i := range messages {
			messages[i].release()
		}
		if got := b.work.Load(); got != 0 {
			t.Errorf("after all readers joined/released: quota=%d", got)
		}
		if open := openSpools(t, b.cfg.spoolDir); len(open) != 0 {
			t.Errorf("partial spools leaked: %d", len(open))
		}
	})
	for i := 0; i < 8; i++ {
		reader, writer := io.Pipe()
		writers = append(writers, writer)
		result := make(chan wsMsgLite, 1)
		results = append(results, result)
		received = append(received, false)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer reader.Close()
			result <- b.spoolFrame(reader)
		}()
		if _, err := writer.Write(bytes.Repeat([]byte{'x'}, size)); err != nil {
			select {
			case msg := <-result:
				received[i] = true
				messages = append(messages, msg)
				if !errors.Is(msg.err, errWorkBudget) {
					t.Fatalf("writer failure without matched genuine quota result: writer=%v spool=%v", err, msg.err)
				}
			case <-time.After(time.Second):
				t.Fatal("writer failed without a joined spool result")
			}
		}
	}
	var materialized int64
	for _, stat := range openSpools(t, b.cfg.spoolDir) {
		materialized += stat.Size()
	}
	charged := b.work.Load()
	if materialized > charged || materialized > b.cfg.workBytes {
		t.Fatalf("partial spools materialized=%d charged=%d hard_budget=%d before any frame EOF", materialized, charged, b.cfg.workBytes)
	}
}

// A real upstream event cannot bypass the retained hard-byte budget merely
// because its request spool was tiny. The transport gate makes retention
// observable before any payload is delivered to the downstream client.
func TestTransparentUpstreamEventChargedBeforeWrite(t *testing.T) {
	t.Run("pre_upload_pressure_64KiB", func(t *testing.T) { testUpstreamEventCapacity(t, 64<<10) })
	t.Run("actual_SSE_pressure_256KiB", func(t *testing.T) { testUpstreamEventCapacity(t, 256<<10) })
}

func testUpstreamEventCapacity(t *testing.T, budget int64) {
	upDone := make(chan struct{})
	upStarted := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(upStarted)
		defer close(upDone)
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"%s\"}\n\n", strings.Repeat("x", 1<<20))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	t.Cleanup(up.Close)
	u, _ := url.Parse(up.URL)
	b := newBridge(config{upstream: u, maxMsgBytes: 4096, maxBodyBytes: 2 << 20, workBytes: budget, spoolDir: t.TempDir()})
	gl := &gatedListener{Listener: mustListen(t)}
	handlerDone := make(chan struct{})
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { defer close(handlerDone); b.ServeHTTP(w, r) })}
	joined := make(chan struct{})
	go func() { _ = server.Serve(gl); close(joined) }()
	t.Cleanup(func() { gl.releaseAll(); _ = server.Close(); <-joined })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
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
	select {
	case <-gate.entered:
		if charged := b.work.Load(); charged >= 1<<20 {
			break // the real event is held fully charged
		}
		// M1 (D9): a capacity refusal is now an explicit frame, so the first
		// gated write must be that refusal, never an uncharged 1MiB event.
		gl.releaseAll()
		_, data, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("refusal frame not delivered: %v", err)
		}
		if !strings.Contains(string(data), `"code":"working_set_unavailable"`) || !strings.Contains(string(data), bridgeHop) {
			t.Fatalf("gated write was neither a charged event nor an explicit refusal: %.200s", data)
		}
		if budget == 64<<10 {
			select {
			case <-upStarted:
				t.Fatal("64KiB pressure must fail before handing off a 64KiB upload lease")
			default:
			}
		}
		// Complete the coded close handshake the refusal started.
		if _, _, err := conn.Read(ctx); websocket.CloseStatus(err) != websocket.StatusTryAgainLater {
			t.Fatalf("refusal close: %v", err)
		}
		select {
		case <-handlerDone:
		case <-ctx.Done():
			t.Fatal("capacity refusal did not join handler")
		}
		if b.work.Load() != 0 {
			t.Fatalf("capacity refusal leaked %d bytes", b.work.Load())
		}
	case <-gate.closed:
		if budget == 64<<10 {
			select {
			case <-upStarted:
				t.Fatal("64KiB pressure must fail before handing off a 64KiB upload lease")
			default:
			}
			t.Log("genuine pre-upload capacity refusal; not SSE accounting evidence")
		} else {
			select {
			case <-upStarted:
			default:
				t.Fatal("adequately budgeted SSE case never reached upstream")
			}
			select {
			case <-upDone:
			case <-ctx.Done():
				t.Fatal("SSE capacity refusal did not release upstream")
			}
		}
		select {
		case <-handlerDone:
		case <-ctx.Done():
			t.Fatal("capacity refusal did not join handler")
		}
		if b.work.Load() != 0 {
			t.Fatalf("capacity refusal leaked %d bytes", b.work.Load())
		}
	case <-ctx.Done():
		t.Fatal("neither bounded resource refusal nor observable downstream Write")
	}
}
