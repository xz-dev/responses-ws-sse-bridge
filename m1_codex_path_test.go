package main

import (
	"bytes"
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

const codexPath = "/backend-api/codex/responses"

// Exit gate 7.5: a WS turn on the Codex direct path is converted to an HTTP
// POST on the same path, with the same header, ticket and spool handling as
// /v1/responses, instead of being tunnelled to the upstream's WS ingress.
func TestM1CodexPathWSConvertedToPOST(t *testing.T) {
	ledger := ledgerWithTicket(t)
	var mu sync.Mutex
	var method, path string
	var clen int64
	up := newSSEUpstream(t)
	up.sse = func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		method, path, clen = r.Method, r.URL.Path, r.ContentLength
		mu.Unlock()
		defaultSSE(w, r)
	}
	p := startPair(t, up, func(c *config) { c.ledgerDir = ledger })
	defer p.close()

	conn := dial(t, p, codexPath, http.Header{"Authorization": {"Bearer userkey"}})
	defer conn.CloseNow()
	writeFrame(t, conn, `{"`+ticketMember+`":"`+testTicket+`","type":"response.create","model":"m","input":[]}`)
	for _, want := range []string{"response.created", "response.output_text.delta", "response.completed"} {
		if got := readEvent(t, conn)["type"]; got != want {
			t.Fatalf("got %v want %s", got, want)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	up.mu.Lock()
	defer up.mu.Unlock()
	if method != http.MethodPost || path != codexPath {
		t.Fatalf("upstream saw %s %s, want POST %s", method, path, codexPath)
	}
	body, hdr := up.bodies[0], up.headers[0]
	if clen != int64(len(body)) {
		t.Fatalf("upstream Content-Length %d, body %d bytes", clen, len(body))
	}
	if bytes.Contains(body, []byte(ticketMember)) || bytes.Contains(body, []byte(`"type"`)) {
		t.Fatalf("ticket member or type reached the upstream body: %s", body)
	}
	if !bytes.Contains(body, []byte(`"stream":true`)) {
		t.Fatalf("stream not forced: %s", body)
	}
	if hdr.Get("Authorization") != "Bearer userkey" {
		t.Fatalf("Authorization %q not forwarded", hdr.Get("Authorization"))
	}
	if hdr.Get(ticketHeader) != testTicket {
		t.Fatalf("ticket header %q", hdr.Get(ticketHeader))
	}
	if hdr.Get("Upgrade") != "" || hdr.Get("Sec-Websocket-Key") != "" {
		t.Fatal("WS handshake headers leaked to the POST")
	}
}

// The converted paths share the size refusal: an over-limit message gets the
// same request_too_large error frame and never reaches the upstream.
func TestM1ConvertedPathsShareSizeRefusal(t *testing.T) {
	for _, path := range []string{"/v1/responses", "/responses", codexPath} {
		t.Run(path, func(t *testing.T) {
			up := newSSEUpstream(t)
			p := startPair(t, up, func(c *config) {
				c.maxMsgBytes = 64
				c.workBytes = 64 << 10
			})
			defer p.close()
			conn := dial(t, p, path, nil)
			defer conn.CloseNow()
			writeFrame(t, conn, `{"type":"response.create","model":"m","input":"`+strings.Repeat("x", 256)+`"}`)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, data, err := conn.Read(ctx)
			if err != nil || !strings.Contains(string(data), `"code":"request_too_large"`) {
				t.Fatalf("want request_too_large event: data=%s err=%v", data, err)
			}
			up.mu.Lock()
			defer up.mu.Unlock()
			if len(up.bodies) != 0 {
				t.Fatal("oversize frame reached upstream")
			}
		})
	}
}

// Every other upgrade path, including subpaths of the Codex path, is still
// tunnelled natively: the upstream sees the WS handshake on the same path.
func TestM1OtherWSPathsTunnelledNatively(t *testing.T) {
	for _, path := range []string{"/v1/realtime", codexPath + "/compact", "/backend-api/codex/realtime/calls"} {
		t.Run(path, func(t *testing.T) {
			seen := make(chan string, 1)
			up := newSSEUpstream(t)
			up.body = func(w http.ResponseWriter, r *http.Request) {
				seen <- r.Method + " " + r.URL.Path
				c, err := websocket.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer c.CloseNow()
				ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
				defer cancel()
				typ, msg, err := c.Read(ctx)
				if err != nil {
					return
				}
				_ = c.Write(ctx, typ, append([]byte("echo:"), msg...))
				_ = c.Close(websocket.StatusNormalClosure, "")
			}
			p := startPair(t, up, nil)
			defer p.close()
			conn := dial(t, p, path, nil)
			defer conn.CloseNow()
			writeFrame(t, conn, `{"type":"response.create"}`)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, data, err := conn.Read(ctx)
			if err != nil || string(data) != `echo:{"type":"response.create"}` {
				t.Fatalf("tunnel echo: data=%s err=%v", data, err)
			}
			if got := <-seen; got != http.MethodGet+" "+path {
				t.Fatalf("upstream saw %q, want the native upgrade on %s", got, path)
			}
		})
	}
}
