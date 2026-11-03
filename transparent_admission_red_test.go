package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// TestTransparentAdmission65LightweightSessions completes each mocked turn
// BEFORE dialing the next session. All earlier sockets remain open. On the
// baseline, the causal failure is therefore dial 65 after 64 completed turns.
func TestTransparentAdmission65LightweightSessions(t *testing.T) {
	var handled atomic.Int64
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		handled.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_a\",\"status\":\"in_progress\"}}\n\n")
		fmt.Fprint(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_a\",\"status\":\"completed\",\"output\":[]}}\n\n")
	}))
	t.Cleanup(up.Close)
	u, _ := url.Parse(up.URL)
	b := newBridge(config{upstream: u, maxMsgBytes: 1 << 20, maxBodyBytes: 1 << 20})
	bs := httptest.NewServer(b)
	t.Cleanup(bs.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	conns := make([]*websocket.Conn, 0, 65)
	for i := 0; i < 65; i++ {
		conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(bs.URL, "http")+"/v1/responses", nil)
		if err != nil {
			t.Fatalf("session %d rejected after %d completed turns: %v", i+1, handled.Load(), err)
		}
		t.Cleanup(func() { _ = conn.CloseNow() })
		conn.SetReadLimit(1 << 20)
		conns = append(conns, conn)
		if err := conn.Write(ctx, websocket.MessageText, []byte(`{"type":"response.create","model":"m","input":[]}`)); err != nil {
			t.Fatal(err)
		}
		for _, want := range []string{"response.created", "response.completed"} {
			typ, data, err := conn.Read(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var event struct {
				Type string `json:"type"`
			}
			if typ != websocket.MessageText || json.Unmarshal(data, &event) != nil || event.Type != want {
				t.Fatalf("session %d type/order mismatch: got %s want %s", i+1, data, want)
			}
		}
	}
	if handled.Load() != 65 {
		t.Fatalf("completed %d turns, want 65", handled.Load())
	}
	// CloseRead continuously services control frames; Ping alone cannot consume
	// its own pong. No application output remains after the completed event.
	for i, conn := range conns {
		conn.CloseRead(ctx)
		pctx, stop := context.WithTimeout(ctx, time.Second)
		err := conn.Ping(pctx)
		stop()
		if err != nil {
			t.Fatalf("session %d no longer usable: %v", i+1, err)
		}
	}
}
