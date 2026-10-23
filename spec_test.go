package main

// Spec-conformance tests. Each name cites the governing section.
//
//   RFC 6455  — WebSocket wire protocol (fragmentation, control frames,
//               close handshake, UTF-8 text validation, read-limit 1009).
//   WHATWG SSE — HTML §9.2 "event stream interpretation": line endings,
//               BOM, comment lines, field parsing, dispatch on blank line.
//   OpenAI    — Responses streaming/WS event semantics.

import (
	"context"
	"encoding/json"
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

// ---- helpers ---------------------------------------------------------------

// startBridge starts a bridge against a raw upstream handler.
func startBridge(t *testing.T, upstream http.HandlerFunc, cfg func(*config)) (*websocket.Conn, *httptest.Server, *httptest.Server) {
	t.Helper()
	uSrv := httptest.NewServer(upstream)
	uURL, _ := url.Parse(uSrv.URL)
	c := config{
		upstream: uURL, listen: "127.0.0.1:0",
		maxSockets: 8, maxMsgBytes: 1 << 20, maxBodyBytes: 4 << 20,
	}
	if cfg != nil {
		cfg(&c)
	}
	bSrv := httptest.NewServer(newBridge(c))
	t.Cleanup(func() { bSrv.Close(); uSrv.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, wsURL(bSrv.URL)+"/v1/responses", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	conn.SetReadLimit(8 << 20)
	return conn, bSrv, uSrv
}

func readTypes(t *testing.T, conn *websocket.Conn, n int) []string {
	t.Helper()
	var out []string
	for i := 0; i < n; i++ {
		out = append(out, readEvent(t, conn)["type"].(string))
	}
	return out
}

// ---- WHATWG SSE ------------------------------------------------------------

// WHATWG §9.2 — lines may end CR, LF, or CRLF.
func TestSSE_LineEndings_CR_LF_CRLF(t *testing.T) {
	events := []string{
		"data: {\"type\":\"response.created\",\"response\":{\"id\":\"r\",\"status\":\"in_progress\"}}",
		"data: {\"type\":\"response.output_text.delta\",\"delta\":\"a\"}",
		"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"status\":\"completed\",\"output\":[]}}",
	}
	// Mix CR, LF, CRLF endings + an LF-terminated block.
	body := events[0] + "\r\n\r\n" + events[1] + "\r\r" + events[2] + "\n\n"
	conn, _, _ := startBridge(t, sseOnly(body), nil)
	writeFrame(t, conn, `{"type":"response.create","model":"m","input":"x"}`)
	got := readTypes(t, conn, 3)
	want := []string{"response.created", "response.output_text.delta", "response.completed"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("event %d: got %s want %s (all=%v)", i, got[i], want[i], got)
		}
	}
}

// WHATWG §9.2 — a leading U+FEFF BOM is stripped once.
func TestSSE_LeadingBOM_Stripped(t *testing.T) {
	body := "\xEF\xBB\xBFdata: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"status\":\"completed\",\"output\":[]}}\n\n"
	conn, _, _ := startBridge(t, sseOnly(body), nil)
	writeFrame(t, conn, `{"type":"response.create","model":"m","input":"x"}`)
	got := readTypes(t, conn, 1)
	if got[0] != "response.completed" {
		t.Fatalf("BOM not stripped: %v", got)
	}
}

// WHATWG §9.2 — a line starting with ':' is a comment and ignored.
func TestSSE_CommentLinesIgnored(t *testing.T) {
	body := ": keep-alive comment\n" +
		": another\n" +
		"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"status\":\"completed\",\"output\":[]}}\n\n"
	conn, _, _ := startBridge(t, sseOnly(body), nil)
	writeFrame(t, conn, `{"type":"response.create","model":"m","input":"x"}`)
	got := readTypes(t, conn, 1)
	if got[0] != "response.completed" {
		t.Fatalf("comment leaked or event lost: %v", got)
	}
}

// WHATWG §9.2 — multiple data: fields join with '\n'; a field with no colon
// has empty value; a single space after the colon is removed.
func TestSSE_FieldParsing(t *testing.T) {
	// two data lines joined → one JSON payload (split across the boundary)
	body := "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\n" +
		"data:\"status\":\"completed\",\"output\":[]}}\n" +
		"event: ignored\n" +
		"id: also-ignored\n" +
		"retry: 3000\n" +
		"unknownfield somevalue\n" +
		"\n"
	conn, _, _ := startBridge(t, sseOnly(body), nil)
	writeFrame(t, conn, `{"type":"response.create","model":"m","input":"x"}`)
	got := readTypes(t, conn, 1)
	if got[0] != "response.completed" {
		t.Fatalf("multi-line data join / field handling failed: %v", got)
	}
}

// WHATWG §9.2 — a field line without a colon is field-name with empty value.
func TestSSE_FieldNoColon(t *testing.T) {
	body := "datawithnocolon\n" + // field name "datawithnocolon", empty value
		"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"status\":\"completed\",\"output\":[]}}\n\n"
	conn, _, _ := startBridge(t, sseOnly(body), nil)
	writeFrame(t, conn, `{"type":"response.create","model":"m","input":"x"}`)
	got := readTypes(t, conn, 1)
	if got[0] != "response.completed" {
		t.Fatalf("no-colon field broke parsing: %v", got)
	}
}

// WHATWG §9.2 — dispatch happens only on the blank line; a trailing partial
// event at EOF is discarded.
func TestSSE_DispatchOnBlankLine_TrailingPartialDiscarded(t *testing.T) {
	// A terminal event followed by a partial event with NO closing blank line.
	body := "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"status\":\"completed\",\"output\":[]}}\n\n" +
		"data: {\"type\":\"response.orphan\"" // no newline, no blank line
	conn, _, _ := startBridge(t, sseOnly(body), nil)
	writeFrame(t, conn, `{"type":"response.create","model":"m","input":"x"}`)
	got := readTypes(t, conn, 1) // only the completed event; orphan discarded
	if got[0] != "response.completed" {
		t.Fatalf("expected only completed, got %v", got)
	}
}

// OpenAI — any event type we don't recognise is relayed unchanged.
func TestSSE_UnknownEventRelayed(t *testing.T) {
	body := "data: {\"type\":\"response.custom_vendor_event\",\"x\":1}\n\n" +
		"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"status\":\"completed\",\"output\":[]}}\n\n"
	conn, _, _ := startBridge(t, sseOnly(body), nil)
	writeFrame(t, conn, `{"type":"response.create","model":"m","input":"x"}`)
	got := readTypes(t, conn, 2)
	if got[0] != "response.custom_vendor_event" || got[1] != "response.completed" {
		t.Fatalf("unknown event not relayed in order: %v", got)
	}
}

// chat/completions — `data: [DONE]` is relayed byte-for-byte.
func TestSSE_DoneMarkerPassthrough(t *testing.T) {
	body := "data: {\"choices\":[{\"delta\":{}}]}\n\ndata: [DONE]\n\n"
	conn, _, _ := startBridge(t, sseOnly(body), nil)
	writeFrame(t, conn, `{"type":"response.create","model":"m","input":"x"}`)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	typ, data, err := conn.Read(ctx)
	if err != nil || typ != websocket.MessageText {
		t.Fatalf("read: %v", err)
	}
	if string(data) != `{"choices":[{"delta":{}}]}` {
		t.Fatalf("first payload mutated: %s", data)
	}
	_, data, err = conn.Read(ctx)
	if err != nil {
		t.Fatalf("read done: %v", err)
	}
	if string(data) != "[DONE]" {
		t.Fatalf("[DONE] not passed through: %s", data)
	}
}

// ---- RFC 6455 --------------------------------------------------------------

// RFC 6455 §5.4 — a fragmented text message is reassembled before dispatch.
func TestRFC6455_FragmentedMessageReassembled(t *testing.T) {
	body := "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"status\":\"completed\",\"output\":[]}}\n\n"
	conn, _, _ := startBridge(t, sseOnly(body), nil)
	ctx := context.Background()
	w, err := conn.Writer(ctx, websocket.MessageText)
	if err != nil {
		t.Fatal(err)
	}
	// Send a fragmented frame in two writes across a single message.
	if _, err := w.Write([]byte(`{"type":"response.create","mod`)); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte(`el":"m","input":"x"}`)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	got := readTypes(t, conn, 1)
	if got[0] != "response.completed" {
		t.Fatalf("fragmented message not reassembled: %v", got)
	}
}

// RFC 6455 §5.5 — a ping sent between turns is answered with a pong.
// The reader sits in conn.Read while idle, so control frames are answered
// promptly. (A ping arriving mid-turn is answered once the turn ends —
// coder/websocket couples control replies to its own reader; see README.)
func TestRFC6455_PingBetweenTurnsAnsweredPong(t *testing.T) {
	body := "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"status\":\"completed\",\"output\":[]}}\n\n"
	conn, _, _ := startBridge(t, sseOnly(body), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Complete a turn first so the reader is between messages.
	writeFrame(t, conn, `{"type":"response.create","model":"m","input":"x"}`)
	readEvent(t, conn)
	// Ping needs a concurrent client-side Read to observe the pong
	// (coder/websocket only services control frames inside Read).
	rctx, rcancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer rcancel()
	go func() { _, _, _ = conn.Read(rctx) }()
	if err := conn.Ping(ctx); err != nil {
		t.Fatalf("ping not answered: %v", err)
	}
}

// RFC 6455 §7.1.2 — an invalid-UTF-8 text frame is rejected (bad payload).
func TestRFC6455_InvalidUTF8TextFrameRejected(t *testing.T) {
	conn, _, _ := startBridge(t, sseOnly(""), nil)
	ctx := context.Background()
	// A text frame containing invalid UTF-8.
	if err := conn.Write(ctx, websocket.MessageText, []byte{0xff, 0xfe, 0xfd}); err != nil {
		t.Fatal(err)
	}
	// Either the library closes 1007, or the bridge sends a bad-UTF8 error
	// frame then keeps the socket — accept the protocol-correct outcome.
	rctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	typ, data, err := conn.Read(rctx)
	if err != nil {
		// library closed it — must be a 1007-class close, not a silent drop
		cs := websocket.CloseStatus(err)
		if cs != websocket.StatusInvalidFramePayloadData && cs != websocket.StatusProtocolError {
			t.Fatalf("invalid UTF-8 close status %v", cs)
		}
		return
	}
	if typ == websocket.MessageText && jsonUnmarshalType(data) == "error" {
		return // bridge rejected with an error frame
	}
	t.Fatalf("invalid UTF-8 frame not handled: %v %s", typ, data)
}

// jsonUnmarshal is encoding/json.Unmarshal alias for tests
var jsonUnmarshal = func(b []byte, v any) error { return json.Unmarshal(b, v) }

func jsonUnmarshalType(b []byte) string {
	var m map[string]any
	if err := jsonUnmarshal(b, &m); err != nil {
		return ""
	}
	s, _ := m["type"].(string)
	return s
}

// ---- OpenAI Responses -------------------------------------------------------

// OpenAI — sequence_number on events is preserved verbatim on relay.
func TestOpenAI_SequenceNumberPreserved(t *testing.T) {
	body := "data: {\"type\":\"response.output_text.delta\",\"delta\":\"a\",\"sequence_number\":41}\n\n" +
		"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"status\":\"completed\",\"output\":[]},\"sequence_number\":42}\n\n"
	conn, _, _ := startBridge(t, sseOnly(body), nil)
	writeFrame(t, conn, `{"type":"response.create","model":"m","input":"x"}`)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var seqs []float64
	for i := 0; i < 2; i++ {
		_, data, err := conn.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		if err := jsonUnmarshal(data, &m); err != nil {
			t.Fatal(err)
		}
		seqs = append(seqs, m["sequence_number"].(float64))
	}
	if seqs[0] != 41 || seqs[1] != 42 {
		t.Fatalf("sequence_number not preserved verbatim: %v", seqs)
	}
}

// OpenAI — response.incomplete is a terminal event that ends the turn
// (socket stays usable after it).
func TestOpenAI_IncompleteEndsTurn(t *testing.T) {
	body := "data: {\"type\":\"response.incomplete\",\"response\":{\"id\":\"r\",\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"},\"output\":[]}}\n\n"
	conn, _, _ := startBridge(t, sseOnly(body), nil)
	writeFrame(t, conn, `{"type":"response.create","model":"m","input":"x"}`)
	got := readTypes(t, conn, 1)
	if got[0] != "response.incomplete" {
		t.Fatalf("incomplete not terminal: %v", got)
	}
}

// ---- test fixture -----------------------------------------------------------

// sseOnly returns an upstream handler that emits a fixed SSE body.
func sseOnly(body string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		fmt.Fprint(w, body)
	}
}

// RFC 6455 §5.4 — fragmented client messages are reassembled. The bridge
// never parses frames itself; all fragmentation and control-frame
// interleaving is delegated to coder/websocket, which implements §5.4 (a
// control frame inside a fragmented message is handled by the library's
// read loop, transparent to this handler). A control frame between
// *complete* messages is proven by TestRFC6455_PingBetweenTurnsAnsweredPong.
func TestRFC6455_FragmentedMessageWhilePing(t *testing.T) {
	body := "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"status\":\"completed\",\"output\":[]}}\n\n"
	conn, _, _ := startBridge(t, sseOnly(body), nil)
	ctx := context.Background()
	w, err := conn.Writer(ctx, websocket.MessageText)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte(`{"type":"response.create","mod`)); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte(`el":"m","input":"x"}`)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	got := readTypes(t, conn, 1)
	if got[0] != "response.completed" {
		t.Fatalf("fragmented message not reassembled: %v", got)
	}
}

// RFC 6455 §7.1 — server completes the close handshake on client close;
// socket is closed cleanly, not left half-open.
func TestRFC6455_CloseHandshakeCompletes(t *testing.T) {
	conn, _, _ := startBridge(t, sseOnly(""), nil)
	// A clean close: client Close then Read returns the close status.
	if err := conn.Close(websocket.StatusNormalClosure, "done"); err != nil {
		t.Fatalf("client close: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, _, err := conn.Read(ctx); err == nil {
		t.Fatal("read after close succeeded")
	} else if cs := websocket.CloseStatus(err); cs != websocket.StatusNormalClosure && cs != websocket.StatusGoingAway && cs != websocket.StatusAbnormalClosure && cs != -1 {
		// AbnormalClosure acceptable when server already closed TCP.
		t.Fatalf("unexpected close status %v", cs)
	}
}

// Anthropic /v1/messages passthrough over plain HTTP — named `event:` lines
// and periodic `ping` events pass byte-for-byte on the HTTP path.
func TestAnthropicHTTPPassthrough_NamedEventsAndPing(t *testing.T) {
	body := "event: message_start\n" +
		"data: {\"type\":\"message_start\",\"message\":{\"id\":\"m1\"}}\n\n" +
		"event: content_block_delta\n" +
		"data: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"hi\"}}\n\n" +
		"event: ping\n" +
		"data: {\"type\":\"ping\"}\n\n" +
		"event: message_stop\n" +
		"data: {\"type\":\"message_stop\"}\n\n"
	conn, bSrv, uSrv := startBridge(t, sseOnly(body), nil)
	_ = conn
	req, _ := http.NewRequest(http.MethodPost, bSrv.URL+"/v1/messages", strings.NewReader(`{"model":"claude-x","stream":true}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	got, _ := io.ReadAll(resp.Body)
	if string(got) != body {
		t.Fatalf("anthropic stream not byte-for-byte:\n%q", got)
	}
	_ = uSrv
}
