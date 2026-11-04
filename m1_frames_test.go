package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/coder/websocket"
)

// A fragmented message whose fragments are each under the cap but whose sum
// is over it gets our request_too_large event, then a coded close (not 1009).
func TestM1FragmentedOverLimitGetsErrorEvent(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(regressSSE))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	bs := httptest.NewServer(newBridge(config{upstream: u, maxMsgBytes: 4096, maxBodyBytes: 4096, workBytes: 1 << 20}))
	defer bs.Close()
	a, _ := url.Parse(bs.URL)
	c, err := net.Dial("tcp", a.Host)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	fmt.Fprintf(c, "GET /v1/responses HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n", a.Host)
	br := bufio.NewReader(c)
	if resp, err := http.ReadResponse(br, nil); err != nil || resp.StatusCode != 101 {
		t.Fatalf("handshake: %v %v", resp, err)
	}
	if err := regressMaskedFrame(c, false, 1, []byte(`{"type":"response.create","input":"`)); err != nil {
		t.Fatal(err)
	}
	part := bytes.Repeat([]byte("x"), 120) // the raw helper sends 7-bit lengths
	for i := 0; i < 40; i++ {
		if err := regressMaskedFrame(c, false, 0, part); err != nil {
			t.Fatal(err)
		}
	}
	op, p, err := regressRawRead(br)
	if err != nil || op != 1 || !bytes.Contains(p, []byte(`"code":"request_too_large"`)) || !bytes.Contains(p, []byte("4096-byte")) {
		t.Fatalf("want request_too_large event: op=%d payload=%q err=%v", op, p, err)
	}
	op, p, err = regressRawRead(br)
	if err != nil || op != 8 || len(p) < 2 {
		t.Fatalf("want close frame: op=%d err=%v", op, err)
	}
	if code := binary.BigEndian.Uint16(p); code == 1009 || code != 1000 {
		t.Fatalf("close code %d", code)
	}
}

func dialDeflate(t *testing.T, p *pair) *websocket.Conn {
	t.Helper()
	c, resp, err := websocket.Dial(context.Background(), wsURL(p.bSrv.URL)+"/v1/responses", &websocket.DialOptions{CompressionMode: websocket.CompressionNoContextTakeover, CompressionThreshold: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(resp.Header.Get("Sec-WebSocket-Extensions"), "permessage-deflate") {
		t.Fatal("permessage-deflate not negotiated")
	}
	return c
}

// An inflation bomb: a few KiB on the wire inflating past the cap gets our
// error event; the counted size is the inflated size.
func TestM1DeflatedOverLimitGetsErrorEvent(t *testing.T) {
	p := startPair(t, newSSEUpstream(t), func(c *config) { c.maxMsgBytes = 64 << 10; c.workBytes = 4 << 20 })
	defer p.close()
	conn := dialDeflate(t, p)
	defer conn.CloseNow()
	writeFrame(t, conn, `{"type":"response.create","model":"m","input":"`+strings.Repeat("x", 1<<20)+`"}`)
	requireErrorEvent(t, readEvent(t, conn), "request_too_large")
	readCloseAfter(t, conn, websocket.StatusNormalClosure)
}

// Under the cap, a deflated turn relays normally and outbound stays plain.
func TestM1DeflatedUnderLimitPasses(t *testing.T) {
	up := newSSEUpstream(t)
	p := startPair(t, up, func(c *config) { c.maxMsgBytes = 64 << 10; c.workBytes = 4 << 20 })
	defer p.close()
	conn := dialDeflate(t, p)
	defer conn.CloseNow()
	input := strings.Repeat("y", 32<<10)
	writeFrame(t, conn, `{"type":"response.create","model":"m","input":"`+input+`"}`)
	for _, want := range []string{"response.created", "response.output_text.delta", "response.completed"} {
		if got := readEvent(t, conn)["type"]; got != want {
			t.Fatalf("got %v want %s", got, want)
		}
	}
	up.mu.Lock()
	defer up.mu.Unlock()
	if !bytes.Contains(up.bodies[0], []byte(input)) {
		t.Fatal("inflated input did not reach upstream intact")
	}
}
