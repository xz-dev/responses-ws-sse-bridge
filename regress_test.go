package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/coder/websocket"
)

func regressPair(t *testing.T, handler http.HandlerFunc, override func(*config)) (*httptest.Server, *websocket.Conn) {
	t.Helper()
	up := httptest.NewServer(handler)
	u, _ := url.Parse(up.URL)
	cfg := config{upstream: u, maxMsgBytes: 16 << 20, maxBodyBytes: 16 << 20}
	if override != nil {
		override(&cfg)
	}
	bs := httptest.NewServer(newBridge(cfg))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(bs.URL, "http")+"/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.CloseNow(); bs.Close(); up.Close() })
	return bs, conn
}
func regressSend(t *testing.T, c *websocket.Conn, b []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Write(ctx, websocket.MessageText, b); err != nil {
		t.Fatal(err)
	}
}
func regressRead(t *testing.T, c *websocket.Conn) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, b, err := c.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func regressSSE(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	fmt.Fprint(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_ok\",\"output\":[]}}\n\n")
}

func regressMaskedFrame(c net.Conn, fin bool, opcode byte, payload []byte) error {
	h := []byte{opcode, 0x80 | byte(len(payload))}
	if fin {
		h[0] |= 0x80
	}
	key := []byte{1, 2, 3, 4}
	h = append(h, key...)
	for i, b := range payload {
		h = append(h, b^key[i%4])
	}
	_, err := c.Write(h)
	return err
}
func regressRawRead(br *bufio.Reader) (byte, []byte, error) {
	var message []byte
	var opcode byte
	for {
		var h [2]byte
		if _, e := io.ReadFull(br, h[:]); e != nil {
			return 0, nil, e
		}
		n := int(h[1] & 127)
		if n == 126 {
			var extended [2]byte
			if _, e := io.ReadFull(br, extended[:]); e != nil {
				return 0, nil, e
			}
			n = int(binary.BigEndian.Uint16(extended[:]))
		}
		if n == 127 || h[1]&128 != 0 {
			return 0, nil, fmt.Errorf("unsupported/masked server fixture frame")
		}
		part := make([]byte, n)
		if _, e := io.ReadFull(br, part); e != nil {
			return 0, nil, e
		}
		op := h[0] & 15
		if opcode == 0 {
			opcode = op
		} else if op != 0 {
			return 0, nil, fmt.Errorf("expected continuation, got opcode %d", op)
		}
		message = append(message, part...)
		if h[0]&128 != 0 {
			return opcode, message, nil
		}
		if opcode != 1 && opcode != 2 {
			return 0, nil, fmt.Errorf("fragmented control frame")
		}
	}
}
func TestRegressionRawControlInsideFragment(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(regressSSE))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	bs := httptest.NewServer(newBridge(config{upstream: u, maxMsgBytes: 4096, maxBodyBytes: 4096, workBytes: 1 << 20}))
	defer bs.Close()
	a, _ := url.Parse(bs.URL)
	c, e := net.Dial("tcp", a.Host)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(time.Second))
	fmt.Fprintf(c, "GET /v1/responses HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n", a.Host)
	br := bufio.NewReader(c)
	resp, e := http.ReadResponse(br, nil)
	if e != nil || resp.StatusCode != 101 {
		t.Fatalf("handshake: %v %v", resp, e)
	}
	if e = regressMaskedFrame(c, false, 1, []byte(`{"type":"response.create",`)); e != nil {
		t.Fatal(e)
	}
	if e = regressMaskedFrame(c, true, 9, []byte("probe")); e != nil {
		t.Fatal(e)
	}
	op, p, e := regressRawRead(br)
	if e != nil || op != 10 || string(p) != "probe" {
		t.Fatalf("interleaved ping not answered: op=%d payload=%q err=%v", op, p, e)
	}
	if e = regressMaskedFrame(c, true, 0, []byte(`"model":"m","input":[]}`)); e != nil {
		t.Fatal(e)
	}
	op, p, e = regressRawRead(br)
	if e != nil || op != 1 || !bytes.Contains(p, []byte("response.completed")) {
		t.Fatalf("fragmented create failed: op=%d payload=%q err=%v", op, p, e)
	}
	if e = regressMaskedFrame(c, true, 8, []byte{3, 232}); e != nil {
		t.Fatal(e)
	}
	op, p, e = regressRawRead(br)
	if e != nil || op != 8 || len(p) < 2 || binary.BigEndian.Uint16(p) != 1000 {
		t.Fatalf("close handshake failed: op=%d payload=%q err=%v", op, p, e)
	}
}

func TestRegressionNestedAndEscapedKeysStayCorrect(t *testing.T) {
	raw := []byte(`{"ty\u0070e":"response.create","model":"m","str\u0065am":"false","input":[{"type":"nested","stream":false}]}`)
	info, err := inspectFrame(raw)
	if err != nil || !info.isCreate {
		t.Fatalf("inspect: %+v %v", info, err)
	}
	var b bytes.Buffer
	if err := rewriteBody(&b, raw); err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(b.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["type"] != nil || got["stream"] != true {
		t.Fatalf("bad rewrite: %s", b.Bytes())
	}
	nested := got["input"].([]any)[0].(map[string]any)
	if nested["type"] != "nested" || nested["stream"] != false {
		t.Fatalf("nested touched: %s", b.Bytes())
	}
}

func TestRegressionUnknownEventBytesAndSequence(t *testing.T) {
	want := `{"type":"future.event","sequence_number":43,"z":[{"x":1}],"response":{"id":"resp_foreign"}}`
	_, c := regressPair(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "data: %s\n\n", want)
		regressSSE(w, r)
	}, nil)
	regressSend(t, c, []byte(`{"type":"response.create","model":"m","input":[]}`))
	if got := regressRead(t, c); string(got) != want {
		t.Fatalf("changed future event: %s", got)
	}
}

func TestRegressionOutboundUTF8Valid(t *testing.T) {
	_, c := regressPair(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"")
		w.Write([]byte{0xff})
		fmt.Fprint(w, "\"}\n\n")
		regressSSE(w, r)
	}, nil)
	regressSend(t, c, []byte(`{"type":"response.create","model":"m","input":[]}`))
	got := regressRead(t, c)
	if !utf8.Valid(got) {
		t.Fatalf("invalid UTF-8 sent as WS text: %q", got)
	}
}

func TestRegressionHTTPChatDONEByteForByte(t *testing.T) {
	want := "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\r\n\r\ndata: [DONE]\r\n\r\n"
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(201)
		fmt.Fprint(w, want)
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	bs := httptest.NewServer(newBridge(config{upstream: u, maxMsgBytes: 1024, maxBodyBytes: 1024}))
	defer bs.Close()
	resp, err := http.Post(bs.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"model":"m","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if string(b) != want || resp.StatusCode != 201 {
		t.Fatalf("HTTP mutated: status=%d body=%q", resp.StatusCode, b)
	}
}

func TestRegressionUTF8Requires1007(t *testing.T) {
	_, c := regressPair(t, regressSSE, nil)
	regressSend(t, c, []byte{'{', '"', 'x', '"', ':', '"', 0xff, '"', '}'})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	// M1 (D9): an explicit error event precedes the 1007 close.
	_, b, err := c.Read(ctx)
	if err != nil || !strings.Contains(string(b), `"code":"invalid_utf8"`) {
		t.Fatalf("want invalid_utf8 error event first; got data=%s err=%v", b, err)
	}
	_, b, err = c.Read(ctx)
	if websocket.CloseStatus(err) != websocket.StatusInvalidFramePayloadData {
		t.Fatalf("want close 1007; got data=%s err=%v close=%v", b, err, websocket.CloseStatus(err))
	}
}

func TestRegressionDuplicateParentMustNotReachUpstream(t *testing.T) {
	var calls atomic.Int32
	_, c := regressPair(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); regressSSE(w, r) }, nil)
	regressSend(t, c, []byte(`{"type":"response.create","model":"m","previous_response_id":"resp_foreign","previous_response_id":"","input":[]}`))
	got := regressRead(t, c)
	if calls.Load() != 0 || !bytes.Contains(got, []byte("previous_response_not_found")) {
		t.Fatalf("foreign parent bypassed: upstream=%d event=%s", calls.Load(), got)
	}
}

func TestRegressionNonStringParentMustNotReachUpstream(t *testing.T) {
	var calls atomic.Int32
	_, c := regressPair(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); regressSSE(w, r) }, nil)
	regressSend(t, c, []byte(`{"type":"response.create","model":"m","previous_response_id":{"id":"resp_foreign"},"input":[]}`))
	got := regressRead(t, c)
	if calls.Load() != 0 {
		t.Fatalf("invalid parent accepted and forwarded: calls=%d event=%s", calls.Load(), got)
	}
}

func TestRegressionRejectMalformedJSON(t *testing.T) {
	for _, raw := range []string{
		`{"type":"response.create" "model":"m","input":[]}`,
		`{"type":"response.create","model":"m","input":[1,]}`,
		`{"type":"response.create","model":"m","input":truue}`,
		`{"type":"response.create","model":"m","input":[]}trailing`,
	} {
		t.Run(raw, func(t *testing.T) {
			var calls atomic.Int32
			_, c := regressPair(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); regressSSE(w, r) }, nil)
			regressSend(t, c, []byte(raw))
			got := regressRead(t, c)
			if calls.Load() != 0 {
				t.Fatalf("invalid JSON accepted: calls=%d response=%s", calls.Load(), got)
			}
		})
	}
}

func TestRegressionRewriteDuplicateAndEscapedStream(t *testing.T) {
	frame := []byte(`{"type":"response.create","model":"m","stream":false,"str\u0065am":"false","input":[{"type":"nested","stream":false}]}`)
	var out bytes.Buffer
	if err := rewriteBody(&out, frame); err != nil {
		t.Fatal(err)
	}
	dec := json.NewDecoder(bytes.NewReader(out.Bytes()))
	_, _ = dec.Token()
	n := 0
	for dec.More() {
		key, _ := dec.Token()
		var v json.RawMessage
		if err := dec.Decode(&v); err != nil {
			t.Fatal(err)
		}
		if key == "stream" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("want one stream member, got %d: %s", n, out.Bytes())
	}
}

func TestRegressionGenerateTrueIsWSONLY(t *testing.T) {
	seen := make(chan []byte, 1)
	_, c := regressPair(t, func(w http.ResponseWriter, r *http.Request) { b, _ := io.ReadAll(r.Body); seen <- b; regressSSE(w, r) }, nil)
	regressSend(t, c, []byte(`{"type":"response.create","model":"m","generate":true,"input":[]}`))
	_ = regressRead(t, c)
	b := <-seen
	if bytes.Contains(b, []byte(`"generate"`)) {
		t.Fatalf("WS-only generate sent to HTTP: %s", b)
	}
}

func TestRegressionInfiniteStreamNoCumulativeCap(t *testing.T) {
	_, c := regressPair(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_ok\"}}\n\n")
		for i := 0; i < 6; i++ {
			fmt.Fprintf(w, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"%s\"}\n\n", strings.Repeat("x", 70))
		}
		regressSSE(w, r)
	}, func(cfg *config) { cfg.maxBodyBytes = 350 })
	regressSend(t, c, []byte(`{"type":"response.create","model":"m","input":[]}`))
	for i := 0; i < 10; i++ {
		got := regressRead(t, c)
		if bytes.Contains(got, []byte(`"response.failed"`)) {
			t.Fatalf("valid bounded events rejected cumulatively: %s", got)
		}
		if bytes.Contains(got, []byte(`"response.completed"`)) {
			return
		}
	}
	t.Fatal("no completion")
}

func TestRegressionWSErrorTerminatesTurn(t *testing.T) {
	_, c := regressPair(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"type\":\"error\",\"code\":\"bad_request\",\"message\":\"bad\",\"sequence_number\":0}\n\n")
	}, nil)
	regressSend(t, c, []byte(`{"type":"response.create","model":"m","input":[]}`))
	got := regressRead(t, c)
	if !bytes.Contains(got, []byte(`"type":"error"`)) {
		t.Fatal(string(got))
	}
	// Next queued turn should be the next response, not a second synthetic error.
	regressSend(t, c, []byte(`{"type":"response.create","model":"m","input":[]}`))
	got = regressRead(t, c)
	if bytes.Contains(got, []byte("without a terminal")) {
		t.Fatalf("upstream error doubled with synthetic error: %s", got)
	}
}

func TestRegressionMalformedSSEPayloadIsNotRelayed(t *testing.T) {
	_, c := regressPair(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: this-is-not-json\n\n")
		regressSSE(w, r)
	}, nil)
	regressSend(t, c, []byte(`{"type":"response.create","model":"m","input":[]}`))
	got := regressRead(t, c)
	if !json.Valid(got) {
		t.Fatalf("invalid upstream JSON emitted on WS: %q", got)
	}
}

func TestRegressionIdleTurnWaitsForBackend(t *testing.T) {
	_, c := regressPair(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.(http.Flusher).Flush()
		select {
		case <-time.After(120 * time.Millisecond):
			regressSSE(w, r)
		case <-r.Context().Done():
		}
	}, nil)
	regressSend(t, c, []byte(`{"type":"response.create","model":"m","input":[]}`))
	if got := regressRead(t, c); !bytes.Contains(got, []byte(`"response.completed"`)) {
		t.Fatalf("pipe expired idle backend turn: %s", got)
	}
}

func TestRegressionConnectionNominatedHeadersStripped(t *testing.T) {
	r := httptest.NewRequest("GET", "http://bridge/v1/responses", nil)
	r.Header.Set("Connection", "Upgrade, X-Hop, Authorization")
	r.Header.Set("X-Hop", "hop-secret")
	r.Header.Set("Authorization", "Bearer nominated-hop")
	got := endToEndHeaders(r)
	if got.Get("X-Hop") != "" || got.Get("Authorization") != "" {
		t.Fatalf("Connection-nominated hop headers forwarded: %v", got)
	}
}

func TestRegressionPrewarmSequences(t *testing.T) {
	events := prewarmEvents("resp_local", "m")
	for i, ev := range events {
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(ev, &obj); err != nil {
			t.Fatal(err)
		}
		if len(obj["sequence_number"]) == 0 {
			t.Fatalf("prewarm event %d missing required sequence_number: %s", i, ev)
		}
	}
}

func TestRegressionNoRedirectFollowing(t *testing.T) {
	var follow atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { follow.Add(1); regressSSE(w, r) }))
	defer target.Close()
	_, c := regressPair(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", target.URL)
		w.WriteHeader(302)
		fmt.Fprint(w, `{"error":{"message":"redirect not allowed"}}`)
	}, nil)
	regressSend(t, c, []byte(`{"type":"response.create","model":"m","input":[]}`))
	got := regressRead(t, c)
	if follow.Load() != 0 || !bytes.Contains(got, []byte(`"status":302`)) {
		t.Fatalf("redirect followed, calls=%d response=%s", follow.Load(), got)
	}
}

func TestRegressionQueuedCreatesCompleteFIFO(t *testing.T) {
	gate, release := context.WithCancel(context.Background())
	defer release()
	started := make(chan struct{}, 1)
	var calls atomic.Int32
	_, c := regressPair(t, func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Model string `json:"model"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			return
		}
		calls.Add(1)
		select {
		case started <- struct{}{}:
		default:
		}
		select {
		case <-gate.Done():
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":%q,\"output\":[]}}\n\n", body.Model)
	}, nil)
	regressSend(t, c, []byte(`{"type":"response.create","model":"turn_0","input":[]}`))
	<-started
	for i := 1; i < 6; i++ {
		regressSend(t, c, []byte(fmt.Sprintf(`{"type":"response.create","model":"turn_%d","input":[]}`, i)))
	}
	release()
	for i := 0; i < 6; i++ {
		got := regressRead(t, c)
		var event struct {
			Type     string `json:"type"`
			Response struct {
				ID string `json:"id"`
			} `json:"response"`
		}
		if err := json.Unmarshal(got, &event); err != nil || event.Type != "response.completed" || event.Response.ID != fmt.Sprintf("turn_%d", i) {
			t.Fatalf("queued turn %d was rejected or reordered: %s (%v)", i, got, err)
		}
	}
	if calls.Load() != 6 {
		t.Fatalf("upstream completed %d turns, want six", calls.Load())
	}
}

func TestRegressionTerminalDoesNotWaitEOF(t *testing.T) {
	_, c := regressPair(t, func(w http.ResponseWriter, r *http.Request) {
		regressSSE(w, r)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}, nil)
	regressSend(t, c, []byte(`{"type":"response.create","model":"m","input":[]}`))
	_ = regressRead(t, c)
	regressSend(t, c, []byte(`{"type":"response.create","model":"m","input":[]}`))
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, b, err := c.Read(ctx)
	if ctx.Err() != nil {
		t.Fatalf("next turn blocked waiting EOF after terminal: %v %s", err, b)
	}
}

// --- REVIEW2 regression tests -------------------------------------------------
// Ported from /var/tmp/ws-sse-bridge-review2-20261022/probes_test.go.

func TestRegressionFlatErrorSingleEnvelope(t *testing.T) {
	_, c := regressPair(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"type\":\"error\",\"code\":\"rate_limit_exceeded\",\"message\":\"try later\",\"param\":null,\"sequence_number\":0}\n\n")
	}, nil)
	regressSend(t, c, []byte(`{"type":"response.create","model":"m","input":[]}`))
	first := regressRead(t, c)
	var obj map[string]json.RawMessage
	json.Unmarshal(first, &obj)
	if len(obj["error"]) == 0 || len(obj["status"]) == 0 {
		t.Errorf("first error is not WS status/error envelope: %s", first)
	}
	// A following turn must receive its own result, not an extra error from turn 1.
	regressSend(t, c, []byte(`{"type":"response.create","model":"m","generate":false,"input":[]}`))
	next := regressRead(t, c)
	if !bytes.Contains(next, []byte(`"response.created"`)) {
		t.Errorf("duplicate error before next turn: %s", next)
	}
}

func TestRegressionIdleHeartbeatIsData(t *testing.T) {
	_, c := regressPair(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		f := w.(http.Flusher)
		for i := 0; i < 6; i++ {
			fmt.Fprint(w, ": keepalive\n\n")
			f.Flush()
			time.Sleep(15 * time.Millisecond)
		}
		regressSSE(w, r)
	}, nil)
	regressSend(t, c, []byte(`{"type":"response.create","model":"m","input":[]}`))
	got := regressRead(t, c)
	if !bytes.Contains(got, []byte(`"response.completed"`)) {
		t.Fatalf("active heartbeat stream killed: %s", got)
	}
}

func TestRegressionHeaderWaitHasNoPipeExpiry(t *testing.T) {
	_, c := regressPair(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-time.After(120 * time.Millisecond):
			regressSSE(w, r)
		case <-r.Context().Done():
		}
	}, nil)
	regressSend(t, c, []byte(`{"type":"response.create","model":"m","input":[]}`))
	if got := regressRead(t, c); !bytes.Contains(got, []byte(`"response.completed"`)) {
		t.Fatalf("pipe expired before upstream headers: %s", got)
	}
}

func TestRegressionQueuedSpoolRemovedOnDisconnect(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)
	started := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		close(started)
		<-r.Context().Done()
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	b := newBridge(config{upstream: u, maxMsgBytes: 1 << 20, maxBodyBytes: 1 << 20})
	bs := httptest.NewServer(b)
	defer bs.Close()
	c, _, e := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(bs.URL, "http")+"/v1/responses", nil)
	if e != nil {
		t.Fatal(e)
	}
	defer c.CloseNow()
	raw := []byte(`{"type":"response.create","model":"m","input":[]}`)
	regressSend(t, c, raw)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("upstream not started")
	}
	regressSend(t, c, raw)
	until := time.Now().Add(time.Second)
	for time.Now().Before(until) {
		if len(openSpools(t, dir)) >= 2 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	c.CloseNow()
	for time.Now().Before(until) {
		if b.work.Load() == 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	if open := openSpools(t, dir); len(open) != 0 {
		t.Fatalf("after socket handler exited, leaked %d spool files", len(open))
	}
}

func TestRegressionNullSSEPayloadRejected(t *testing.T) {
	_, c := regressPair(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: null\n\n")
		regressSSE(w, r)
	}, nil)
	regressSend(t, c, []byte(`{"type":"response.create","model":"m","input":[]}`))
	got := regressRead(t, c)
	if string(got) == "null" {
		t.Fatal("non-object null relayed as a Responses WS message")
	}
}

func TestRegressionSSENoDataEventsIgnored(t *testing.T) {
	for _, body := range []string{"id: marker\n\n", "event: message\n\n"} {
		t.Run(strings.TrimSpace(body), func(t *testing.T) {
			_, c := regressPair(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprint(w, body)
				regressSSE(w, r)
			}, nil)
			regressSend(t, c, []byte(`{"type":"response.create","model":"m","input":[]}`))
			got := regressRead(t, c)
			if !bytes.Contains(got, []byte(`"response.completed"`)) {
				t.Fatalf("WHATWG no-data block was dispatched instead of ignored: %s", got)
			}
		})
	}
}

func TestRegressionSSEEOFWithoutBlankLineDiscarded(t *testing.T) {
	for _, end := range []string{"", "\n", "\r", "\r\n"} {
		t.Run(fmt.Sprintf("ending-%q", end), func(t *testing.T) {
			_, c := regressPair(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprint(w, `data: {"type":"response.completed","sequence_number":0,"response":{"id":"resp_partial","status":"completed","output":[]}}`+end)
			}, nil)
			regressSend(t, c, []byte(`{"type":"response.create","model":"m","input":[]}`))
			got := regressRead(t, c)
			if bytes.Contains(got, []byte(`"response.completed"`)) {
				t.Fatalf("WHATWG incomplete EOF event was dispatched as success: %s", got)
			}
		})
	}
}

func TestRegressionGeneratedResponseRequiredFields(t *testing.T) {
	for _, raw := range prewarmEvents("resp_test", "m") {
		var e map[string]json.RawMessage
		json.Unmarshal(raw, &e)
		var r map[string]json.RawMessage
		json.Unmarshal(e["response"], &r)
		for _, key := range []string{"id", "created_at", "object", "model", "output", "parallel_tool_calls", "tool_choice", "tools"} {
			if _, ok := r[key]; !ok {
				t.Errorf("%s: synthetic response missing required Response.%s", e["type"], key)
			}
		}
	}
}

func TestRegressionInvalidUTF8DoesNotAccumulate(t *testing.T) {
	v := &utf8Validator{}
	p := bytes.Repeat([]byte{0xff}, 32768)
	for i := 0; i < 32; i++ {
		v.Write(p)
	}
	if len(v.tail) > 4 {
		t.Fatalf("UTF8 validator keeps entire invalid message: tail=%d bytes", len(v.tail))
	}
}
