package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/coder/websocket"
)

// bridge serves plain HTTP transparently and converts Responses WebSocket
// turns to streaming HTTP posts. Data is treated as an endless stream: a
// client frame is spooled to bounded disk as it is read, validated in one
// streaming pass, then streamed to the upstream POST member by member; each
// upstream SSE event is written out as soon as its blank line arrives.
type bridge struct {
	cfg          config
	client       *http.Client
	proxy        *httputil.ReverseProxy
	sockets      chan struct{}
	work         atomic.Int64 // bytes of resident frames currently charged
	upstreamBase string
}

func newBridge(cfg config) *bridge {
	if cfg.workBytes <= 0 {
		// Default: four max-size frames resident — a budget, not a leak.
		cfg.workBytes = 4 * cfg.maxMsgBytes
	}
	tr := &http.Transport{
		Proxy:               nil, // no proxy env: deterministic egress to the inner APISIX
		MaxIdleConns:        64,
		MaxIdleConnsPerHost: 64,
		DisableCompression:  true, // upstream SSE must pass through uncompressed
	}
	return &bridge{
		cfg: cfg,
		client: &http.Client{
			Transport: tr,
			// Forwarding boundary: a redirect must surface as the upstream's
			// own status, never re-route the POST.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		sockets:      make(chan struct{}, cfg.maxSockets),
		upstreamBase: strings.TrimSuffix(cfg.upstream.String(), "/"),
		proxy: &httputil.ReverseProxy{
			Rewrite: func(pr *httputil.ProxyRequest) {
				pr.SetURL(cfg.upstream)
				pr.Out.Host = cfg.upstream.Host
			},
			Transport:     tr,
			FlushInterval: -1,
			ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
				log.Printf("native proxy: upstream request failed: %v", err)
				writeJSONError(w, http.StatusBadGateway, "upstream_error", "", "upstream request failed")
			},
		},
	}
}

func (b *bridge) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if isUpgradeRequest(r) {
		if r.URL.Path != "/v1/responses" && r.URL.Path != "/responses" {
			// Outside the Responses paths the selector handles upgrades itself;
			// tunnel them through unchanged.
			b.serveNative(w, r)
			return
		}
		b.serveWS(w, r)
		return
	}
	b.serveNative(w, r)
}

func (b *bridge) serveNative(w http.ResponseWriter, r *http.Request) {
	b.proxy.ServeHTTP(w, r)
}

func isUpgradeRequest(r *http.Request) bool {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		return false
	}
	for _, proto := range r.Header.Values("Connection") {
		for _, item := range strings.Split(proto, ",") {
			if strings.EqualFold(strings.TrimSpace(item), "upgrade") {
				return true
			}
		}
	}
	return false
}

// --- errors -----------------------------------------------------------------

type apiError struct {
	Status  int    `json:"-"`
	Type    string `json:"type"`
	Code    string `json:"code,omitempty"`
	Param   string `json:"param,omitempty"`
	Message string `json:"message"`
}

func (e *apiError) Error() string { return e.Message }

var (
	errSocketLimit  = &apiError{Status: http.StatusServiceUnavailable, Type: "server_error", Code: "working_set_unavailable", Message: "Working capacity unavailable; retry later."}
	errTurnQueued   = &apiError{Status: http.StatusServiceUnavailable, Type: "server_error", Code: "working_set_unavailable", Message: "A turn is already in progress on this connection and the queue is full."}
	errTooLarge     = &apiError{Status: http.StatusRequestEntityTooLarge, Type: "invalid_request_error", Code: "working_set_too_large", Message: "Request exceeds the configured size limit."}
	errPrevNotFound = &apiError{Status: http.StatusBadRequest, Type: "invalid_request_error", Code: "previous_response_not_found", Param: "previous_response_id", Message: "The provided previous_response_id is not available for this user."}
)

func writeAPIError(w http.ResponseWriter, e *apiError) {
	writeJSONError(w, e.Status, e.Type, e.Code, e.Message)
}

func writeJSONError(w http.ResponseWriter, status int, typ, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	errObj := map[string]any{"type": typ, "message": msg}
	if code != "" {
		errObj["code"] = code
	}
	enc, _ := json.Marshal(map[string]any{"error": errObj})
	_, _ = w.Write(enc)
}

// --- websocket session ------------------------------------------------------

// serveWS terminates a Responses WS. Turns run strictly in order on a
// socket. Two goroutines per socket: this turn worker (drains each message's
// streaming reader and relays turns) and the reader goroutine that services
// conn.Reader — the only path that observes a client close or TCP drop and
// answers pings.
func (b *bridge) serveWS(w http.ResponseWriter, r *http.Request) {
	select {
	case b.sockets <- struct{}{}:
	default:
		writeAPIError(w, errSocketLimit)
		return
	}
	defer func() { <-b.sockets }()

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns:  []string{"*"},
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		log.Printf("ws accept failed: %v", err)
		return
	}
	defer conn.CloseNow()
	conn.SetReadLimit(b.cfg.maxMsgBytes)

	headers := endToEndHeaders(r)
	path := r.URL.Path
	if r.URL.RawQuery != "" {
		path += "?" + r.URL.RawQuery
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	var socketOnce sync.Once
	dead := func() { socketOnce.Do(cancel) }
	defer dead()

	// Goroutine 1 of 2 — the reader. It stays inside conn.Reader so control
	// frames are always serviced (pings are answered even mid-message). Each
	// message is spooled to a bounded temp file while UTF-8 is checked; only
	// the file handle is handed to the worker, so the next conn.Reader call
	// happens immediately and the worker never touches conn state. On queue
	// overflow the reader drains the rejected message and answers it with an
	// explicit overload error — one terminal reply per request, never
	// coalesced.
	type wsMsg = wsMsgLite
	incoming := make(chan wsMsg, 1) // one frame may queue behind the in-flight turn
	go func() {
		defer close(incoming)
		defer dead()
		for {
			// Background ctx: the library force-closes the conn when a read
			// context ends; teardown is done by dead()/CloseNow.
			typ, reader, err := conn.Reader(context.Background())
			if err != nil {
				return
			}
			msg := b.spoolFrame(reader)
			msg.typ = typ
			select {
			case incoming <- msg:
			case <-ctx.Done():
				msg.release()
				return
			default:
				msg.release()
				b.sendErrorFrame(ctx, conn, errTurnQueued)
			}
		}
	}()

	// Worker exit: cancel the reader so it cannot enqueue again, then drain
	// and release every queued spool. cancel() both unblocks the reader's
	// <-ctx.Done() arm and fails conn.Reader; CloseNow() makes that prompt.
	defer func() {
		cancel()
		conn.CloseNow()
		for msg := range incoming {
			msg.release()
		}
	}()

	// Goroutine 2 of 2 — this turn worker.
	for {
		var msg wsMsg
		var have bool
		select {
		case <-ctx.Done():
			return
		case msg, have = <-incoming:
			if !have {
				conn.Close(websocket.StatusNormalClosure, "bye")
				return
			}
		}

		turnCtx, stopTurn := context.WithCancel(ctx)

		if msg.typ != websocket.MessageText {
			msg.release()
			b.sendErrorFrame(ctx, conn, &apiError{Status: http.StatusBadRequest, Type: "invalid_request_error", Message: "unsupported WebSocket frame type; send text frames containing response.create events"})
			stopTurn()
			continue
		}
		if msg.badUTF {
			msg.release()
			// RFC 6455 §8.1/§7.4.1: invalid UTF-8 in a text message fails the
			// connection with 1007.
			stopTurn()
			conn.Close(websocket.StatusInvalidFramePayloadData, "invalid utf-8")
			return
		}

		if err := b.handleTurnStream(turnCtx, conn, msg.spool, msg.size, headers, path); err != nil {
			stopTurn()
			conn.Close(websocket.StatusInternalError, "turn failed")
			return
		}
		stopTurn()
	}
}

func sendText(ctx context.Context, conn *websocket.Conn, data []byte) error {
	wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return conn.Write(wctx, websocket.MessageText, data)
}

func (b *bridge) sendErrorFrame(ctx context.Context, conn *websocket.Conn, e *apiError) {
	errObj := map[string]any{
		"type":    e.Type,
		"message": e.Message,
	}
	if e.Code != "" {
		errObj["code"] = e.Code
	}
	if e.Param != "" {
		errObj["param"] = e.Param
	}
	frame := map[string]any{"type": "error", "error": errObj}
	if e.Status != 0 {
		frame["status"] = e.Status
	}
	data, _ := json.Marshal(frame)
	if err := sendText(ctx, conn, data); err != nil {
		log.Printf("ws write error frame: %v", err)
	}
}

// acquire reserves n bytes of the global working budget; all-or-nothing.
// Returns a release func, or nil when the budget is full.
func (b *bridge) acquire(n int64) func() {
	for {
		cur := b.work.Load()
		if cur+n > b.cfg.workBytes {
			return nil
		}
		if b.work.CompareAndSwap(cur, cur+n) {
			return func() { b.work.Add(-n) }
		}
	}
}

// release frees the queued message's spool exactly once.
func (m *wsMsgLite) release() {
	if m.spool != nil {
		m.spool.Close()
		os.Remove(m.spool.Name())
		m.spool = nil
	}
}

// spoolFrame copies one message to a bounded temp file while checking
// UTF-8. Called by the reader goroutine; returns the file (caller removes).
func (b *bridge) spoolFrame(frame io.Reader) (m wsMsgLite) {
	spool, err := os.CreateTemp(b.cfg.spoolDir, "bridge-turn-*.json")
	if err != nil {
		_, _ = io.Copy(io.Discard, frame)
		return wsMsgLite{}
	}
	uv := &utf8Validator{}
	size, err := io.Copy(spool, io.TeeReader(io.LimitReader(frame, b.cfg.maxMsgBytes+1), uv))
	if err != nil || size > b.cfg.maxMsgBytes {
		_, _ = io.Copy(io.Discard, frame)
	}
	if err == nil {
		_, err = spool.Seek(0, io.SeekStart)
	}
	if err != nil {
		spool.Close()
		os.Remove(spool.Name())
		return wsMsgLite{badUTF: false, size: -1}
	}
	return wsMsgLite{spool: spool, size: size, badUTF: !uv.valid()}
}

// wsMsgLite is the reader→worker handoff: a spooled frame plus validity.
type wsMsgLite struct {
	typ    websocket.MessageType
	spool  *os.File
	size   int64
	badUTF bool
}

// handleTurnStream validates the spooled frame's JSON in one streaming
// token pass that records each top-level member's value byte span, then
// streams the rewrite to the upstream POST. The frame is never held in
// RAM: validation reads it once from disk, and the POST body is an
// io.MultiReader of "key": literals plus SectionReader value spans — a
// known Content-Length with no second copy. The working budget admits the
// turn before validation, on the spool's nominal size.
func (b *bridge) handleTurnStream(ctx context.Context, conn *websocket.Conn, spool *os.File, size int64, headers http.Header, path string) error {
	if spool == nil {
		b.sendErrorFrame(ctx, conn, &apiError{Status: http.StatusBadRequest, Type: "invalid_request_error", Message: "could not read frame"})
		return nil
	}
	defer func() {
		spool.Close()
		os.Remove(spool.Name())
	}()
	if size > b.cfg.maxMsgBytes {
		b.sendErrorFrame(ctx, conn, errTooLarge)
		return nil
	}

	// Admit the turn against the global working budget before any
	// allocation on its behalf, using the spool's bounded nominal size.
	release := b.acquire(size)
	if release == nil {
		b.sendErrorFrame(ctx, conn, errTooLarge)
		return nil
	}
	defer release()

	// Streaming validation + routing fields + member spans, one pass.
	if _, err := spool.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("turn spool: %w", err)
	}
	info, err := inspectFrameStream(spool)
	if err != nil {
		msg := "invalid JSON in WebSocket frame"
		if errors.Is(err, errTooDeep) {
			msg = fmt.Sprintf("JSON nesting exceeds the %d-level depth limit", maxJSONDepth)
		}
		b.sendErrorFrame(ctx, conn, &apiError{Status: http.StatusBadRequest, Type: "invalid_request_error", Message: msg})
		return nil
	}
	if !info.isCreate {
		quoted, _ := json.Marshal(info.typeVal)
		b.sendErrorFrame(ctx, conn, &apiError{Status: http.StatusBadRequest, Type: "invalid_request_error", Message: fmt.Sprintf("unsupported frame type %s; expected response.create", quoted)})
		return nil
	}
	if info.havePrevID {
		b.sendErrorFrame(ctx, conn, errPrevNotFound)
		return nil
	}
	if info.genFalse {
		id := "resp_" + randomHex(24)
		for _, ev := range prewarmEvents(id, info.model) {
			if err := sendText(ctx, conn, ev); err != nil {
				return fmt.Errorf("ws write prewarm: %w", err)
			}
		}
		return nil
	}

	// The POST body is a MultiReader of literal segments plus spool spans:
	// Content-Length is known, nothing is copied into RAM.
	body, bodyLen := rewrittenBody(spool, info)
	return b.runUpstream(ctx, conn, body, bodyLen, headers, path)
}

// utf8Validator is an io.Writer that validates UTF-8 across chunk
// boundaries while the frame is spooled (TeeReader sink). It keeps at most
// one partial trailing rune; once invalid, it stops accumulating.
type utf8Validator struct {
	tail [utf8.UTFMax]byte
	tn   int
	bad  bool
}

func (v *utf8Validator) Write(p []byte) (int, error) {
	if v.bad {
		return len(p), nil // already invalid: drain, accumulate nothing
	}
	// Prepend any partial rune from the previous chunk.
	data := p
	if v.tn > 0 {
		buf := make([]byte, 0, v.tn+len(p))
		buf = append(buf, v.tail[:v.tn]...)
		data = append(buf, p...)
	}
	i := 0
	for i < len(data) {
		r, sz := utf8.DecodeRune(data[i:])
		if r == utf8.RuneError && sz <= 1 {
			if !utf8.FullRune(data[i:]) {
				// incomplete trailing sequence: keep at most UTFMax bytes
				if n := len(data) - i; n <= utf8.UTFMax {
					copy(v.tail[:], data[i:])
					v.tn = n
					return len(p), nil
				}
			}
			v.bad = true
			v.tn = 0
			return len(p), nil
		}
		i += sz
	}
	v.tn = 0
	return len(p), nil
}

func (v *utf8Validator) valid() bool {
	// EOF: a leftover partial rune was an incomplete sequence — invalid.
	return !v.bad && v.tn == 0
}

func randomHex(n int) string {
	buf := make([]byte, n/2)
	_, _ = rand.Read(buf)
	return hex.EncodeToString(buf)
}

// responseObject builds a schema-complete OpenAI Response object for a
// locally generated (prewarm or failed) response. Required fields per the
// OpenAI Responses schema (openai-python types/responses/response.py):
// id, created_at, model, object:"response", output, parallel_tool_calls,
// tool_choice, tools. The rest are emitted for strict consumers.
//
// A non-null `usage` must match ResponseUsage
// (openai-python types/responses/response_usage.py): input_tokens,
// output_tokens, total_tokens, plus required nested
// input_tokens_details {cached_tokens} and
// output_tokens_details {reasoning_tokens}.
func responseObject(id, model, status string, errObj any, completedAt float64) map[string]any {
	now := completedAt
	if now == 0 {
		now = float64(time.Now().Unix())
	}
	return map[string]any{
		"id":                 id,
		"object":             "response",
		"created_at":         now,
		"status":             status,
		"model":              model,
		"output":             []any{},
		"parallel_tool_calls": true,
		"tool_choice":        "auto",
		"tools":              []any{},
		"error":              errObj,
		"incomplete_details": nil,
		"usage": map[string]any{
			"input_tokens":          0,
			"input_tokens_details":  map[string]any{"cached_tokens": 0},
			"output_tokens":         0,
			"output_tokens_details": map[string]any{"reasoning_tokens": 0},
			"total_tokens":          0,
		},
		"metadata":   map[string]any{},
		"background": false,
		"store":      false,
	}
}

// prewarmEvents answers generate:false without an upstream call. Synthetic
// events carry sequence_number like a native stream and a full Response
// schema object (OpenAI streaming-event schema requires it on
// response.created/response.completed).
func prewarmEvents(id, model string) [][]byte {
	created := map[string]any{
		"type":            "response.created",
		"sequence_number": 0,
		"response":        responseObject(id, model, "in_progress", nil, 0),
	}
	completed := map[string]any{
		"type":            "response.completed",
		"sequence_number": 1,
		"response":        responseObject(id, model, "completed", nil, 0),
	}
	a, _ := json.Marshal(created)
	c, _ := json.Marshal(completed)
	return [][]byte{a, c}
}

// runUpstream POSTs the frame body to the upstream — the body is an
// io.Reader of member spans with a known Content-Length — and relays each
// complete SSE event's payload as one WS text frame.
//
// The idle timeout bounds upstream inactivity: it starts when the request
// is issued (covering the wait for response headers) and resets on every
// byte the body reader yields, including comment/heartbeat lines that the
// SSE parser does not surface as events.
func (b *bridge) runUpstream(ctx context.Context, conn *websocket.Conn, body io.Reader, bodyLen int64, headers http.Header, path string) error {
	u := b.upstreamBase + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, body)
	if err != nil {
		return err
	}
	req.Header = headers.Clone()
	req.Header.Set("Content-Type", "application/json")
	req.Header.Del("Accept-Encoding")
	req.ContentLength = bodyLen

	// Run client.Do in a goroutine so the idle timer bounds the header wait
	// too. The timer cancels the request context on fire, unblocking Do.
	var iw *idleWatch
	if b.cfg.turnIdle > 0 {
		iw = newIdleWatch(b.cfg.turnIdle, nil)
		defer iw.stop()
	}
	type doRes struct {
		resp *http.Response
		err  error
	}
	doCh := make(chan doRes, 1)
	reqCtx, cancelReq := context.WithCancel(ctx)
	defer cancelReq()
	req = req.WithContext(reqCtx)
	go func() {
		resp, err := b.client.Do(req)
		doCh <- doRes{resp, err}
	}()

	var resp *http.Response
	if iw != nil {
		select {
		case r := <-doCh:
			resp, err = r.resp, r.err
		case <-iw.fired():
			cancelReq()
			b.sendErrorFrame(ctx, conn, &apiError{Status: http.StatusGatewayTimeout, Type: "upstream_error", Message: "upstream produced no data within the idle timeout"})
			return nil
		case <-ctx.Done():
			cancelReq()
			return ctx.Err()
		}
		iw.stop() // headers received; body stage re-arms its own watch
	} else {
		r := <-doCh
		resp, err = r.resp, r.err
	}
	if err != nil {
		if ctx.Err() != nil {
			return err
		}
		b.sendErrorFrame(ctx, conn, &apiError{Status: http.StatusBadGateway, Type: "upstream_error", Message: "upstream request failed"})
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		limit := b.cfg.maxBodyBytes
		ebody, rerr := io.ReadAll(io.LimitReader(resp.Body, limit+1))
		if rerr != nil || int64(len(ebody)) > limit {
			b.sendErrorFrame(ctx, conn, errTooLarge)
			return nil
		}
		b.sendUpstreamError(ctx, conn, resp.StatusCode, ebody)
		return nil
	}

	// Feed the body through a pipe that signals the idle watch on every
	// read, so comments and partial-event bytes reset the timer.
	var bodyReader io.Reader = resp.Body
	var bw *idleWatch
	if b.cfg.turnIdle > 0 {
		bw = newIdleWatch(b.cfg.turnIdle, nil)
		defer bw.stop()
		pr, pw := io.Pipe()
		go func() {
			buf := make([]byte, 32<<10)
			for {
				n, rerr := resp.Body.Read(buf)
				if n > 0 {
					bw.activity()
					if _, werr := pw.Write(buf[:n]); werr != nil {
						return
					}
				}
				if rerr != nil {
					pw.CloseWithError(rerr)
					return
				}
			}
		}()
		bodyReader = pr
		defer pr.Close()
	}

	return relaySSE(ctx, conn, bodyReader, b.cfg.maxBodyBytes, bw, b.sendErrorFrame)
}

// sendUpstreamError forwards a non-2xx upstream reply as an error frame that
// carries the status and the upstream error object verbatim when it parses.
func (b *bridge) sendUpstreamError(ctx context.Context, conn *websocket.Conn, status int, body []byte) {
	var errVal any
	if raw := findTopLevelMember(body, "error"); len(raw) > 0 {
		if err := json.Unmarshal(raw, &errVal); err != nil {
			errVal = nil
		}
	}
	if errVal == nil {
		msg := string(bytes.TrimSpace(body))
		if len(msg) > 400 {
			msg = msg[:400]
		}
		errVal = map[string]any{"type": "upstream_error", "message": msg}
	}
	frame, _ := json.Marshal(map[string]any{
		"type": "error", "status": status, "error": errVal,
	})
	if err := sendText(ctx, conn, frame); err != nil {
		log.Printf("ws write upstream error: %v", err)
	}
}

// findTopLevelMember returns the raw value bytes of a top-level object
// member, decoding only the small error body. The error body is bounded by
// maxBodyBytes, so a plain Unmarshal is fine here.
func findTopLevelMember(b []byte, name string) []byte {
	var obj map[string]json.RawMessage
	if json.Unmarshal(b, &obj) != nil {
		return nil
	}
	return obj[name]
}

var _ = errors.Is
