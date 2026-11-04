package main

import (
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
	Hop     string `json:"hop,omitempty"`
	Message string `json:"message"`
}

func (e *apiError) Error() string { return e.Message }

var (
	errWorkingCapacity = &apiError{Status: http.StatusServiceUnavailable, Type: "server_error", Code: "working_set_unavailable", Message: "Working capacity unavailable; retry later."}
	errTooLarge        = &apiError{Status: http.StatusRequestEntityTooLarge, Type: "invalid_request_error", Code: "working_set_too_large", Message: "Request exceeds the configured size limit."}
	errPrevNotFound    = &apiError{Status: http.StatusBadRequest, Type: "invalid_request_error", Code: "previous_response_not_found", Param: "previous_response_id", Message: "The provided previous_response_id is not available for this user."}
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
	sessionRelease := b.acquire(sessionWorkingBytes(r))
	if sessionRelease == nil {
		writeAPIError(w, errWorkingCapacity)
		return
	}
	defer sessionRelease()

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns:  []string{"*"},
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		log.Printf("ws accept failed: %v", err)
		return
	}
	defer conn.CloseNow()
	// Library limit sits above the cap: spoolFrame reads at most cap+1 bytes
	// and answers over-limit messages itself with an error event (D9).
	conn.SetReadLimit(b.cfg.maxMsgBytes + 1)

	headers := endToEndHeaders(r)
	path := r.URL.Path
	if r.URL.RawQuery != "" {
		path += "?" + r.URL.RawQuery
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	var socketOnce sync.Once
	dead := func() { socketOnce.Do(func() { cancel(); _ = conn.CloseNow() }) }
	defer dead()

	// The reader never writes application frames or waits for queue slots.
	// Each closed spool owns its hard-byte lease until the worker finishes.
	incoming := newRequestFIFO()
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		handedOff := false
		defer func() {
			if !handedOff {
				dead()
			}
		}()
		for {
			typ, reader, err := conn.Reader(context.Background())
			if err != nil {
				return
			}
			msg := b.spoolFrame(reader)
			msg.typ = typ
			if msg.err != nil {
				msg.release()
				if msg.remote {
					return // the socket itself broke mid-message
				}
				// A local refusal (over-limit, budget, disk) is answered by the
				// worker in FIFO order with an error event, then a close. The
				// reader never writes application frames and stops reading.
				failure := &wsMsgLite{err: msg.err}
				handedOff = incoming.push(failure)
				return
			}
			if !incoming.push(&msg) {
				msg.release()
				return
			}
		}
	}()
	defer func() {
		dead()
		incoming.close()
		<-readerDone
	}()

	// Only this worker emits application frames, in FIFO order.
	for {
		msg, have := incoming.pop(ctx)
		if !have {
			return
		}
		if msg.err != nil {
			b.failTurn(ctx, conn, msg.err)
			return
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
			stopTurn()
			b.sendErrorFrame(ctx, conn, &apiError{Status: http.StatusBadRequest, Type: "invalid_request_error", Code: "invalid_utf8", Hop: bridgeHop, Message: "WebSocket text frame is not valid UTF-8"})
			_ = conn.Close(websocket.StatusInvalidFramePayloadData, "invalid utf-8")
			return
		}
		spool, err := os.Open(msg.name)
		if err == nil {
			err = b.handleTurnStream(turnCtx, conn, spool, msg.size, headers, path)
			_ = spool.Close()
		}
		msg.release()
		stopTurn()
		if err != nil {
			if ctx.Err() != nil {
				return // the client socket is gone; nothing can be delivered
			}
			b.failTurn(ctx, conn, err)
			return
		}
	}
}

func sendText(ctx context.Context, conn *websocket.Conn, data []byte) error {
	return conn.Write(ctx, websocket.MessageText, data)
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
	if e.Hop != "" {
		errObj["hop"] = e.Hop
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
func (b *bridge) reserve(n int64) bool {
	for {
		cur := b.work.Load()
		if n < 0 || n > b.cfg.workBytes-cur {
			return false
		}
		if b.work.CompareAndSwap(cur, cur+n) {
			return true
		}
	}
}

func (b *bridge) acquire(n int64) func() {
	if !b.reserve(n) {
		return nil
	}
	return func() { b.work.Add(-n) }
}

// release keeps file bytes charged until its descriptor is closed and its
// pathname removed. An unlink failure retains the lease rather than lying
// about freed disk capacity.
func (m *wsMsgLite) release() {
	if m.name != "" {
		if err := os.Remove(m.name); err != nil && !os.IsNotExist(err) {
			log.Printf("request spool cleanup failed: %v", err)
			return
		}
		m.name = ""
	}
	if m.lease != nil {
		m.lease.release()
	}
}

// spoolFrame charges fixed read storage before allocating it and allocated
// file blocks before writing. Admission is nonblocking: no lease waits for
// more capacity while retaining prior spool data.
func (b *bridge) spoolFrame(frame io.Reader) (m wsMsgLite) {
	readBytes := int64(requestReadBytes)
	if b.cfg.maxMsgBytes+1 < readBytes {
		readBytes = b.cfg.maxMsgBytes + 1
	}
	release := b.acquire(readBytes)
	if release == nil {
		return wsMsgLite{err: errWorkBudget}
	}
	defer release()
	spool, err := b.newChargedSpool("bridge-turn-*.json")
	if err != nil {
		return wsMsgLite{err: err}
	}
	defer func() {
		if err := spool.close(); m.err == nil && err != nil {
			m.err = err
		}
		if m.err != nil {
			spool.release()
		}
	}()
	buf := make([]byte, int(readBytes))
	var uv utf8Validator
	for {
		limit := b.cfg.maxMsgBytes + 1 - spool.msg.size
		if limit <= 0 {
			m.err = errTooLarge
			return
		}
		if limit > int64(len(buf)) {
			limit = int64(len(buf))
		}
		n, rerr := frame.Read(buf[:limit])
		if n > 0 {
			if spool.msg.size+int64(n) > b.cfg.maxMsgBytes {
				m.err = errTooLarge
				return
			}
			_, _ = uv.Write(buf[:n])
			if _, err := spool.Write(buf[:n]); err != nil {
				m.err = err
				return
			}
		}
		if rerr != nil {
			m = spool.msg
			if rerr != io.EOF {
				m.err, m.remote = rerr, true
			}
			m.badUTF = !uv.valid()
			return
		}
	}
}

// wsMsgLite is the reader→worker handoff. Closed files avoid a file
// descriptor per queued create; the worker reopens only its active spool.
type wsMsgLite struct {
	typ    websocket.MessageType
	name   string
	size   int64
	badUTF bool
	remote bool // err came from the socket, not a local refusal
	err    error
	lease  *requestLease
	next   *wsMsgLite
}

// handleTurnStream validates the spooled frame's JSON in one streaming
// token pass that records each top-level member's value byte span, then
// streams the rewrite to the upstream POST. The frame is never held in
// RAM: validation reads it once from disk, and the POST body is an
// io.MultiReader of "key": literals plus SectionReader value spans — a
// turn's reader-owned spool lease remains charged through validation and POST.
func (b *bridge) handleTurnStream(ctx context.Context, conn *websocket.Conn, spool *os.File, size int64, headers http.Header, path string) error {
	if spool == nil {
		b.sendErrorFrame(ctx, conn, &apiError{Status: http.StatusBadRequest, Type: "invalid_request_error", Message: "could not read frame"})
		return nil
	}
	if size > b.cfg.maxMsgBytes {
		b.sendErrorFrame(ctx, conn, errTooLarge)
		return nil
	}

	// Streaming validation + routing fields + member spans, one pass.
	if _, err := spool.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("turn spool: %w", err)
	}
	parseLease := &requestLease{bridge: b}
	if !parseLease.grow(scanWorkingBytes(size)) {
		return errWorkBudget
	}
	defer parseLease.release()
	info, err := inspectFrameStream(spool, parseLease)
	if errors.Is(err, errWorkBudget) {
		return err
	}
	if err != nil {
		msg := "invalid JSON in WebSocket frame"
		if errors.Is(err, errTooDeep) {
			msg = fmt.Sprintf("JSON nesting exceeds the %d-level depth limit", maxJSONDepth)
		}
		b.sendErrorFrame(ctx, conn, &apiError{Status: http.StatusBadRequest, Type: "invalid_request_error", Message: msg})
		return nil
	}
	if !info.isCreate {
		release := b.acquire(32*int64(len(info.typeVal)) + 8192)
		if release == nil {
			return errWorkBudget
		}
		defer release()
		quoted, _ := json.Marshal(info.typeVal)
		b.sendErrorFrame(ctx, conn, &apiError{Status: http.StatusBadRequest, Type: "invalid_request_error", Message: fmt.Sprintf("unsupported frame type %s; expected response.create", quoted)})
		return nil
	}
	if info.havePrevID {
		b.sendErrorFrame(ctx, conn, errPrevNotFound)
		return nil
	}
	if info.genFalse {
		release := b.acquire(32*int64(len(info.model)) + 8192)
		if release == nil {
			return errWorkBudget
		}
		defer release()
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
	for _, member := range info.members {
		if !parseLease.grow(512 + 24*int64(len(member.key))) {
			return errWorkBudget
		}
	}
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
	n := len(p)
	if v.bad {
		return n, nil
	}
	if v.tn > 0 {
		for !utf8.FullRune(v.tail[:v.tn]) && len(p) > 0 && v.tn < utf8.UTFMax {
			v.tail[v.tn] = p[0]
			v.tn++
			p = p[1:]
		}
		if !utf8.FullRune(v.tail[:v.tn]) {
			return n, nil
		}
		_, size := utf8.DecodeRune(v.tail[:v.tn])
		if size == 1 {
			v.bad = true
			return n, nil
		}
		v.tn = 0
	}
	for len(p) > 0 {
		_, size := utf8.DecodeRune(p)
		if size == 1 && p[0] >= utf8.RuneSelf {
			if !utf8.FullRune(p) {
				v.tn = copy(v.tail[:], p)
				return n, nil
			}
			v.bad = true
			return n, nil
		}
		p = p[size:]
	}
	return n, nil
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
		"id":                  id,
		"object":              "response",
		"created_at":          now,
		"status":              status,
		"model":               model,
		"output":              []any{},
		"parallel_tool_calls": true,
		"tool_choice":         "auto",
		"tools":               []any{},
		"error":               errObj,
		"incomplete_details":  nil,
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
// complete SSE event's payload as one WS text frame. Only the actual request
// context and transport termination govern its established lifetime.
func (b *bridge) runUpstream(ctx context.Context, conn *websocket.Conn, body io.Reader, bodyLen int64, headers http.Header, path string) error {
	u := b.upstreamBase + path
	reqCtx, cancelReq := context.WithCancel(ctx)
	defer cancelReq()
	releaseUpload := b.acquire(httpUploadWindowBytes)
	if releaseUpload == nil {
		return errWorkBudget
	}
	owned := &ownedRequestBody{Reader: body, done: make(chan struct{}), releaseUpload: releaseUpload}
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, u, owned)
	if err != nil {
		_ = owned.Close()
		return err
	}
	req.Header = headers.Clone()
	req.Header.Set("Content-Type", "application/json")
	req.Header.Del("Accept-Encoding")
	req.ContentLength = bodyLen

	resp, err := b.client.Do(req)
	defer func() { cancelReq(); <-owned.done }()
	if err != nil {
		if ctx.Err() != nil {
			return err
		}
		b.sendErrorFrame(ctx, conn, &apiError{Status: http.StatusBadGateway, Type: "upstream_error", Message: "upstream request failed"})
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return b.relayHTTPError(ctx, conn, resp)
	}

	return b.relaySpooledSSE(ctx, conn, resp.Body)
}
