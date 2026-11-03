package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestSpooledLargeEventAndLongTurnReleaseEachEvent(t *testing.T) {
	const count = 20
	chunk := strings.Repeat("payload_", 32768) // 256KiB/event, cumulative >4MiB WORK budget
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		for i := 0; i < count; i++ {
			fmt.Fprintf(w, "data: {\"type\":\"response.output_text.delta\",\"sequence_number\":%d,\"delta\":%q}\n\n", i, chunk)
			w.(http.Flusher).Flush()
		}
		fmt.Fprint(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_large\",\"output\":[]}}\n\n")
	}))
	t.Cleanup(up.Close)
	u, _ := url.Parse(up.URL)
	b := newBridge(config{upstream: u, maxMsgBytes: 1 << 20, maxBodyBytes: 1 << 20, workBytes: 4 << 20, spoolDir: t.TempDir()})
	server := httptest.NewServer(b)
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, _, e := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", nil)
	if e != nil {
		t.Fatal(e)
	}
	defer conn.CloseNow()
	conn.SetReadLimit(1 << 20)
	if e := conn.Write(ctx, websocket.MessageText, []byte(`{"type":"response.create","model":"m","input":[]}`)); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < count; i++ {
		typ, data, e := conn.Read(ctx)
		if e != nil {
			t.Fatal(e)
		}
		want := []byte(fmt.Sprintf(`{"type":"response.output_text.delta","sequence_number":%d,"delta":%q}`, i, chunk))
		if typ != websocket.MessageText || !bytes.Equal(data, want) {
			t.Fatalf("large message type/content/order mismatch at %d: len=%d err=%v", i, len(data), e)
		}
	}
	_, data, e := conn.Read(ctx)
	if e != nil || !bytes.Contains(data, []byte(`"response.completed"`)) {
		t.Fatalf("long turn ended prematurely: %v %s", e, data)
	}
	_ = conn.CloseNow()
	until := time.Now().Add(time.Second)
	for b.work.Load() != 0 && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	assertRequestResourcesZero(t, b)
}
