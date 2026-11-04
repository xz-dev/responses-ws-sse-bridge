package main

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// readCloseAfter reads one more message and requires a close frame that
// carries a code (never a bare close: no -1, no 1006, no 1009).
func readCloseAfter(t *testing.T, conn *websocket.Conn, want websocket.StatusCode) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, data, err := conn.Read(ctx)
	if err == nil {
		t.Fatalf("want close %d, got another message %s", want, data)
	}
	if got := websocket.CloseStatus(err); got != want {
		t.Fatalf("close status = %d (%v), want %d", got, err, want)
	}
}

func requireErrorEvent(t *testing.T, ev map[string]any, code string) map[string]any {
	t.Helper()
	if ev["type"] != "error" {
		t.Fatalf("want error event, got %v", ev)
	}
	errObj, _ := ev["error"].(map[string]any)
	if errObj["code"] != code || errObj["hop"] != bridgeHop {
		t.Fatalf("error event code/hop = %v/%v, want %s/%s: %v", errObj["code"], errObj["hop"], code, bridgeHop, ev)
	}
	return errObj
}

// D9: an over-limit message gets request_too_large naming the limit, then a
// coded close; never a library 1009.
func TestM1OversizeFrameErrorEventThenClose(t *testing.T) {
	up := newSSEUpstream(t)
	p := startPair(t, up, func(c *config) { c.maxMsgBytes = 64; c.workBytes = 64 << 10 })
	defer p.close()
	conn := dial(t, p, "/v1/responses", nil)
	defer conn.CloseNow()
	writeFrame(t, conn, `{"type":"response.create","model":"m","input":"`+strings.Repeat("x", 4096)+`"}`)
	errObj := requireErrorEvent(t, readEvent(t, conn), "request_too_large")
	if msg, _ := errObj["message"].(string); !strings.Contains(msg, "64-byte") {
		t.Fatalf("message does not name the limit: %q", msg)
	}
	readCloseAfter(t, conn, websocket.StatusNormalClosure)
	up.mu.Lock()
	defer up.mu.Unlock()
	if len(up.bodies) != 0 {
		t.Fatal("oversize frame reached upstream")
	}
}

// Per-socket ordering: a turn queued before the over-limit message completes
// first; the error event follows it.
func TestM1OversizeAfterQueuedTurnKeepsOrder(t *testing.T) {
	up := newSSEUpstream(t)
	p := startPair(t, up, func(c *config) { c.maxMsgBytes = 256; c.workBytes = 1 << 20 })
	defer p.close()
	conn := dial(t, p, "/v1/responses", nil)
	defer conn.CloseNow()
	writeFrame(t, conn, `{"type":"response.create","model":"m","input":[]}`)
	writeFrame(t, conn, `{"type":"response.create","model":"m","input":"`+strings.Repeat("y", 1024)+`"}`)
	for _, want := range []string{"response.created", "response.output_text.delta", "response.completed"} {
		if got := readEvent(t, conn)["type"]; got != want {
			t.Fatalf("got %v want %s", got, want)
		}
	}
	requireErrorEvent(t, readEvent(t, conn), "request_too_large")
	readCloseAfter(t, conn, websocket.StatusNormalClosure)
}

// A working-budget refusal while spooling is explicit (503 code), then a
// coded close; previously the session was dropped with a bare close.
func TestM1WorkBudgetRefusalIsExplicit(t *testing.T) {
	up := newSSEUpstream(t)
	p := startPair(t, up, func(c *config) { c.maxMsgBytes = 1 << 20; c.workBytes = 48 << 10 })
	defer p.close()
	conn := dial(t, p, "/v1/responses", nil)
	defer conn.CloseNow()
	writeFrame(t, conn, `{"type":"response.create","model":"m","input":"`+strings.Repeat("z", 200<<10)+`"}`)
	requireErrorEvent(t, readEvent(t, conn), "working_set_unavailable")
	readCloseAfter(t, conn, websocket.StatusTryAgainLater)
}

// A refusal after response.created becomes response.failed with the code
// and hop, then a coded close.
func TestM1MidStreamRefusalIsResponseFailed(t *testing.T) {
	up := newSSEUpstream(t)
	up.sse = func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_a\",\"status\":\"in_progress\"}}\n\n")
		w.(http.Flusher).Flush()
		fmt.Fprintf(w, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"%s\"}\n\n", strings.Repeat("d", 2<<20))
	}
	p := startPair(t, up, func(c *config) { c.maxMsgBytes = 4096; c.maxBodyBytes = 4 << 20; c.workBytes = 1 << 20 })
	defer p.close()
	conn := dial(t, p, "/v1/responses", nil)
	defer conn.CloseNow()
	writeFrame(t, conn, `{"type":"response.create","model":"m","input":[]}`)
	if got := readEvent(t, conn)["type"]; got != "response.created" {
		t.Fatalf("first event %v", got)
	}
	ev := readEvent(t, conn)
	if ev["type"] != "response.failed" {
		t.Fatalf("want response.failed, got %v", ev)
	}
	resp, _ := ev["response"].(map[string]any)
	errObj, _ := resp["error"].(map[string]any)
	if errObj["code"] != "working_set_unavailable" || errObj["hop"] != bridgeHop || resp["id"] != "resp_a" {
		t.Fatalf("response.failed fields: %v", resp)
	}
	readCloseAfter(t, conn, websocket.StatusTryAgainLater)
}
