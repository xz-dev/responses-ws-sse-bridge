package main

import (
	"bufio"
	"context"
	"encoding/json"
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

// sseUpstream emulates the inner APISIX: records requests, serves canned SSE.
type sseUpstream struct {
	t *testing.T

	mu           sync.Mutex
	bodies       [][]byte
	headers      []http.Header
	requestsDone chan struct{}
	sse          func(w http.ResponseWriter, r *http.Request)
	body         func(w http.ResponseWriter, r *http.Request)
}

func newSSEUpstream(t *testing.T) *sseUpstream {
	u := &sseUpstream{t: t, requestsDone: make(chan struct{}, 64)}
	return u
}

func (u *sseUpstream) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		u.mu.Lock()
		u.bodies = append(u.bodies, body)
		u.headers = append(u.headers, r.Header.Clone())
		u.mu.Unlock()
		select {
		case u.requestsDone <- struct{}{}:
		default:
		}
		if u.body != nil {
			u.body(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		write := u.sse
		if write == nil {
			write = defaultSSE
		}
		write(w, r)
		fl.Flush()
	})
}

func defaultSSE(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_a\",\"status\":\"in_progress\"}}\n\n")
	fmt.Fprint(w, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n")
	fmt.Fprint(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_a\",\"status\":\"completed\",\"output\":[]}}\n\n")
}

// startPair wires upstream → bridge → WS client.
type pair struct {
	upstream *sseUpstream
	bridge   *bridge
	bSrv     *httptest.Server
	uSrv     *httptest.Server
}

func startPair(t *testing.T, upstream *sseUpstream, cfgOverride func(*config)) *pair {
	t.Helper()
	uSrv := httptest.NewServer(upstream.handler())
	uURL, _ := url.Parse(uSrv.URL)
	cfg := config{
		upstream: uURL, listen: "127.0.0.1:0",
		maxSockets: 8, maxMsgBytes: 1 << 20, maxBodyBytes: 1 << 20,
	}
	if cfgOverride != nil {
		cfgOverride(&cfg)
	}
	br := newBridge(cfg)
	bSrv := httptest.NewServer(br)
	return &pair{upstream: upstream, bridge: br, bSrv: bSrv, uSrv: uSrv}
}

func (p *pair) close() {
	p.bSrv.Close()
	p.uSrv.Close()
}

func wsURL(h string) string { return "ws" + strings.TrimPrefix(h, "http") }

func dial(t *testing.T, p *pair, path string, hdr http.Header) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, wsURL(p.bSrv.URL)+path, &websocket.DialOptions{HTTPHeader: hdr})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	conn.SetReadLimit(4 << 20)
	return conn
}

func writeFrame(t *testing.T, conn *websocket.Conn, payload string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Write(ctx, websocket.MessageText, []byte(payload)); err != nil {
		t.Fatalf("write frame: %v", err)
	}
}

// readEvent reads one text frame and unmarshals it.
func readEvent(t *testing.T, conn *websocket.Conn) map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	typ, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	if typ != websocket.MessageText {
		t.Fatalf("frame type %v", typ)
	}
	var ev map[string]any
	if err := json.Unmarshal(data, &ev); err != nil {
		t.Fatalf("event JSON: %v", err)
	}
	return ev
}

func TestSingleTurn(t *testing.T) {
	up := newSSEUpstream(t)
	p := startPair(t, up, nil)
	defer p.close()

	conn := dial(t, p, "/v1/responses", http.Header{"Authorization": {"Bearer userkey"}, "X-Custom": {"v"}})
	defer conn.CloseNow()

	writeFrame(t, conn, `{"type":"response.create","model":"claude-opus-5-5","input":"hello"}`)

	var types []string
	for i := 0; i < 3; i++ {
		ev := readEvent(t, conn)
		types = append(types, ev["type"].(string))
	}
	want := []string{"response.created", "response.output_text.delta", "response.completed"}
	for i, w := range want {
		if types[i] != w {
			t.Fatalf("event %d: got %s want %s", i, types[i], w)
		}
	}

	up.mu.Lock()
	defer up.mu.Unlock()
	if len(up.bodies) != 1 {
		t.Fatalf("upstream bodies: %d", len(up.bodies))
	}
	var sent map[string]any
	json.Unmarshal(up.bodies[0], &sent)
	if sent["type"] != nil {
		t.Fatal("type field leaked to upstream")
	}
	if sent["stream"] != true {
		t.Fatal("stream not forced true")
	}
	if sent["model"] != "claude-opus-5-5" {
		t.Fatal("model not forwarded")
	}
	if up.headers[0].Get("Authorization") != "Bearer userkey" {
		t.Fatal("Authorization not forwarded")
	}
	if up.headers[0].Get("X-Custom") != "v" {
		t.Fatal("end-to-end header missing")
	}
	if up.headers[0].Get("Connection") != "" {
		t.Fatal("hop-by-hop Connection leaked")
	}
}

func TestMultiTurnOrder(t *testing.T) {
	up := newSSEUpstream(t)
	p := startPair(t, up, nil)
	defer p.close()
	conn := dial(t, p, "/v1/responses", nil)
	defer conn.CloseNow()

	for turn := 1; turn <= 3; turn++ {
		writeFrame(t, conn, fmt.Sprintf(`{"type":"response.create","model":"m","input":"turn %d"}`, turn))
		var last string
		for i := 0; i < 3; i++ {
			last = readEvent(t, conn)["type"].(string)
		}
		if last != "response.completed" {
			t.Fatalf("turn %d ended with %s", turn, last)
		}
	}
	up.mu.Lock()
	defer up.mu.Unlock()
	if len(up.bodies) != 3 {
		t.Fatalf("expected 3 upstream posts, got %d", len(up.bodies))
	}
	var lastBody map[string]any
	json.Unmarshal(up.bodies[2], &lastBody)
	if lastBody["input"] != "turn 3" {
		t.Fatalf("turn ordering broken: %v", lastBody["input"])
	}
}

func TestPreviousResponseIDRejectedNoUpstreamCall(t *testing.T) {
	up := newSSEUpstream(t)
	p := startPair(t, up, nil)
	defer p.close()
	conn := dial(t, p, "/v1/responses", nil)
	defer conn.CloseNow()

	writeFrame(t, conn, `{"type":"response.create","model":"m","previous_response_id":"resp_x","input":"delta"}`)
	ev := readEvent(t, conn)
	if ev["type"] != "error" {
		t.Fatalf("expected error frame, got %v", ev["type"])
	}
	if ev["status"].(float64) != 400 {
		t.Fatalf("status %v", ev["status"])
	}
	errObj := ev["error"].(map[string]any)
	if errObj["code"] != "previous_response_not_found" {
		t.Fatalf("code %v", errObj["code"])
	}
	if errObj["param"] != "previous_response_id" {
		t.Fatalf("param %v", errObj["param"])
	}
	// Socket stays usable: a plain turn still works.
	writeFrame(t, conn, `{"type":"response.create","model":"m","input":"ok"}`)
	var last string
	for i := 0; i < 3; i++ {
		last = readEvent(t, conn)["type"].(string)
	}
	if last != "response.completed" {
		t.Fatalf("socket not usable after rejection: %s", last)
	}
	up.mu.Lock()
	defer up.mu.Unlock()
	if len(up.bodies) != 1 { // only the plain turn
		t.Fatalf("delta turn reached upstream: %d posts", len(up.bodies))
	}
}

func TestGenerateFalseAnsweredLocally(t *testing.T) {
	up := newSSEUpstream(t)
	p := startPair(t, up, nil)
	defer p.close()
	conn := dial(t, p, "/v1/responses", nil)
	defer conn.CloseNow()

	writeFrame(t, conn, `{"type":"response.create","model":"gpt-6.1-sol","generate":false,"input":[]}`)
	created := readEvent(t, conn)
	completed := readEvent(t, conn)
	if created["type"] != "response.created" || completed["type"] != "response.completed" {
		t.Fatalf("prewarm events: %v %v", created["type"], completed["type"])
	}
	resp := completed["response"].(map[string]any)
	if !strings.HasPrefix(resp["id"].(string), "resp_") {
		t.Fatalf("id %v", resp["id"])
	}
	if resp["model"] != "gpt-6.1-sol" {
		t.Fatalf("model echo %v", resp["model"])
	}
	if resp["status"] != "completed" {
		t.Fatalf("status %v", resp["status"])
	}
	up.mu.Lock()
	defer up.mu.Unlock()
	if len(up.bodies) != 0 {
		t.Fatalf("prewarm reached upstream: %d posts", len(up.bodies))
	}
}

func TestUpstreamErrorBecomesErrorFrameSocketSurvives(t *testing.T) {
	up := newSSEUpstream(t)
	up.body = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		fmt.Fprint(w, `{"error":{"type":"rate_limit_error","code":"rate_limited","message":"slow down"}}`)
	}
	p := startPair(t, up, nil)
	defer p.close()
	conn := dial(t, p, "/v1/responses", nil)
	defer conn.CloseNow()

	writeFrame(t, conn, `{"type":"response.create","model":"m","input":"x"}`)
	ev := readEvent(t, conn)
	if ev["type"] != "error" || ev["status"].(float64) != 429 {
		t.Fatalf("error frame: %v", ev)
	}
	errObj := ev["error"].(map[string]any)
	if errObj["type"] != "rate_limit_error" || errObj["code"] != "rate_limited" {
		t.Fatalf("upstream error not preserved: %v", errObj)
	}
	// Socket survives.
	up.body = nil
	writeFrame(t, conn, `{"type":"response.create","model":"m","input":"again"}`)
	var last string
	for i := 0; i < 3; i++ {
		last = readEvent(t, conn)["type"].(string)
	}
	if last != "response.completed" {
		t.Fatalf("socket unusable after upstream error: %s", last)
	}
}

func TestTruncatedSSEEndsFailed(t *testing.T) {
	up := newSSEUpstream(t)
	up.sse = func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_t\",\"status\":\"in_progress\"}}\n\n")
		fmt.Fprint(w, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n")
		// connection closes without a terminal event
	}
	p := startPair(t, up, nil)
	defer p.close()
	conn := dial(t, p, "/v1/responses", nil)
	defer conn.CloseNow()

	writeFrame(t, conn, `{"type":"response.create","model":"m","input":"x"}`)
	var types []string
	var failed map[string]any
	for i := 0; i < 3; i++ {
		ev := readEvent(t, conn)
		types = append(types, ev["type"].(string))
		if ev["type"] == "response.failed" {
			failed = ev
		}
	}
	if failed == nil {
		t.Fatalf("no response.failed after truncation: %v", types)
	}
	resp := failed["response"].(map[string]any)
	if resp["id"] != "resp_t" || resp["status"] != "failed" {
		t.Fatalf("failed response shape: %v", resp)
	}
	if resp["error"].(map[string]any)["code"] != "upstream_stream_failed" {
		t.Fatalf("error code: %v", resp["error"])
	}
}

func TestClientCloseCancelsUpstream(t *testing.T) {
	sawCancel := make(chan struct{}, 1)
	up := newSSEUpstream(t)
	up.sse = func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_c\",\"status\":\"in_progress\"}}\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		select {
		case sawCancel <- struct{}{}:
		default:
		}
	}
	p := startPair(t, up, nil)
	defer p.close()
	conn := dial(t, p, "/v1/responses", nil)

	writeFrame(t, conn, `{"type":"response.create","model":"m","input":"x"}`)
	readEvent(t, conn) // response.created
	conn.Close(websocket.StatusNormalClosure, "")
	// The upstream request context must have been cancelled promptly.
	select {
	case <-sawCancel:
	case <-time.After(3 * time.Second):
		t.Fatal("upstream request not cancelled on client close")
	}
}

// RFC 6455 §7.4.1 + §5.2: an over-limit message must close the socket with
// status 1009. The library enforces the read limit itself; verify the close
// code reaches the client.
func TestOversizeFrame(t *testing.T) {
	up := newSSEUpstream(t)
	p := startPair(t, up, func(c *config) { c.maxMsgBytes = 64 })
	defer p.close()
	conn := dial(t, p, "/v1/responses", nil)
	defer conn.CloseNow()

	big := `{"type":"response.create","model":"m","input":"` + strings.Repeat("x", 256) + `"}`
	writeFrame(t, conn, big)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _, err := conn.Read(ctx)
	if err == nil {
		t.Fatal("expected read limit error")
	}
	if websocket.CloseStatus(err) != websocket.StatusMessageTooBig {
		t.Fatalf("close status: %v", err)
	}
	up.mu.Lock()
	defer up.mu.Unlock()
	if len(up.bodies) != 0 {
		t.Fatal("oversize frame reached upstream")
	}
}

func TestHTTPPassthroughSSE(t *testing.T) {
	up := newSSEUpstream(t)
	p := startPair(t, up, nil)
	defer p.close()

	req, _ := http.NewRequest(http.MethodPost, p.bSrv.URL+"/v1/responses", strings.NewReader(`{"model":"m","stream":true}`))
	req.Header.Set("Authorization", "Bearer k")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	var events []string
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		if strings.HasPrefix(sc.Text(), "data: ") {
			events = append(events, sc.Text())
		}
	}
	if len(events) != 3 {
		t.Fatalf("events %v", events)
	}
	up.mu.Lock()
	defer up.mu.Unlock()
	if len(up.bodies) != 1 || string(up.bodies[0]) != `{"model":"m","stream":true}` {
		t.Fatalf("body mutated: %s", up.bodies)
	}
}

func TestHTTPPassthroughGET(t *testing.T) {
	up := newSSEUpstream(t)
	up.body = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Upstream", "yes")
		w.WriteHeader(200)
		fmt.Fprint(w, `{"ok":true}`)
	}
	p := startPair(t, up, nil)
	defer p.close()

	resp, err := http.Get(p.bSrv.URL + "/v1/models")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.Header.Get("X-Upstream") != "yes" {
		t.Fatal("upstream headers not preserved")
	}
}

func TestUnsupportedFrameType(t *testing.T) {
	up := newSSEUpstream(t)
	p := startPair(t, up, nil)
	defer p.close()
	conn := dial(t, p, "/v1/responses", nil)
	defer conn.CloseNow()

	writeFrame(t, conn, `{"type":"session.update","foo":1}`)
	ev := readEvent(t, conn)
	if ev["type"] != "error" {
		t.Fatalf("expected error frame, got %v", ev)
	}
	msg := ev["error"].(map[string]any)["message"].(string)
	if !strings.Contains(msg, "session.update") {
		t.Fatalf("message should name the type: %v", msg)
	}
}

// RFC 6455 §7.1: the socket limit must reject the upgrade with an HTTP
// status, not complete the handshake then close. Uses a real net.Listener —
// httptest.Server's connection tracking can serialize the third client.
func TestSocketLimit(t *testing.T) {
	up := newSSEUpstream(t)
	release := make(chan struct{})
	up.sse = func(w http.ResponseWriter, r *http.Request) { <-release }
	p := startPair(t, up, func(c *config) { c.maxSockets = 2 })
	defer p.close()
	defer close(release)

	var conns []*websocket.Conn
	for i := 0; i < 2; i++ {
		conns = append(conns, dial(t, p, "/v1/responses", nil))
		writeFrame(t, conns[i], `{"type":"response.create","model":"m","input":"x"}`)
	}
	defer func() {
		for _, c := range conns {
			c.CloseNow()
		}
	}()
	// Wait until both sockets are counted busy before the third dial.
	deadline := time.Now().Add(2 * time.Second)
	for len(p.bridge.sockets) < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, resp, err := websocket.Dial(ctx, wsURL(p.bSrv.URL)+"/v1/responses", nil)
	if err == nil {
		t.Fatal("expected dial failure")
	}
	if resp.StatusCode != 503 {
		t.Fatalf("status %d", resp.StatusCode)
	}
}

func TestHealthz(t *testing.T) {
	up := newSSEUpstream(t)
	p := startPair(t, up, nil)
	defer p.close()
	resp, err := http.Get(p.bSrv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("healthz %d", resp.StatusCode)
	}
}
