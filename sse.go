package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/coder/websocket"
	sse "github.com/tmaxmax/go-sse"
)

// idleWatch bounds upstream inactivity. A timer fires after `d` without
// an activity() call. createdFired reports the fired channel; stop releases
// the timer. The header-wait stage passes an onFire that cancels the
// request; the body stage uses fired() in relaySSE's select.
type idleWatch struct {
	t   *time.Timer
	d   time.Duration
	ch  chan struct{}
	once sync.Once
}

func newIdleWatch(d time.Duration, onFire func()) *idleWatch {
	iw := &idleWatch{ch: make(chan struct{}), d: d}
	iw.t = time.AfterFunc(d, func() {
		iw.once.Do(func() {
			close(iw.ch)
			if onFire != nil {
				onFire()
			}
		})
	})
	return iw
}

func (w *idleWatch) activity() {
	select {
	case <-w.ch:
	default:
		w.t.Reset(w.d)
	}
}

func (w *idleWatch) fired() <-chan struct{} { return w.ch }

func (w *idleWatch) stop() { w.t.Stop() }

// eofUnexpectedEOF marks the body reached EOF so go-sse's internal parser
// yields ErrUnexpectedEOF for a pending (unterminated) event instead of
// dispatching it — WHATWG discards pending data at EOF. We wrap the body so
// a clean io.EOF becomes this distinct error only when a partial event is
// in flight; a fully-delimited stream still ends in clean io.EOF.
type eofMarkingReader struct {
	r io.Reader
}

var errStreamEOF = errors.New("sse body ended")

func (e eofMarkingReader) Read(p []byte) (int, error) {
	n, err := e.r.Read(p)
	if err == io.EOF {
		return n, errStreamEOF
	}
	return n, err
}

// relaySSE streams upstream Server-Sent Events to the client. Parsing is
// delegated to github.com/tmaxmax/go-sse, a streaming WHATWG-§9.2 decoder
// (CR/LF/CRLF, BOM, comments, multi-line data), with two corrections the
// library does not enforce: a block whose accumulated data buffer is empty
// is not dispatched (metadata-only blocks), and a pending event is
// discarded at EOF rather than delivered (data line + single newline + EOF
// has no blank-line dispatch).
//
// Each event is written out as soon as its blank line arrives — the turn
// is never collected, and no cumulative byte cap applies to the already-
// forwarded stream: only per-event size (MaxEventSize) is bounded.
//
// The turn ends as soon as a recognised terminal response event or a
// request-level error event has been forwarded; the upstream body is then
// abandoned instead of waiting for HTTP EOF.
//
// The idle watch (started by runUpstream at request issue and reset by
// body reads) bounds dead time between upstream bytes, never total turn
// length; heartbeats/comments reset it.
//
// Each relayed payload is validated as a complete JSON object and as UTF-8
// (WS text frames must carry valid UTF-8, RFC 6455 §8.1); malformed data
// is replaced by the declared upstream-failure terminal.
func relaySSE(ctx context.Context, conn *websocket.Conn, body io.Reader, maxBody int64, iw *idleWatch, sendErr func(context.Context, *websocket.Conn, *apiError)) error {
	var sawStart bool
	var terminal bool
	var respID string
	var lastSeq int64

	// peek records, per dispatched SSE block, whether the block carried a
	// `data` field — WHATWG §9.2.6 dispatches `data:\n\n` as an event
	// with empty data, while an id:/event:-only block is not dispatched.
	// go-sse yields an indistinguishable empty-Data Event for both, so
	// the distinction must come from the raw stream.
	peek := newBlockPeek(eofMarkingReader{r: body})
	iter, stop := pullIter(peek, maxBody, ctx.Done())
	defer stop()

	var idleC <-chan struct{}
	if iw != nil {
		idleC = iw.fired()
	}

	for {
		var ev sse.Event
		var err error
		var idleFired bool
		select {
		case <-idleC:
			idleFired = true
		case r, isOpen := <-iter:
			if !isOpen {
				if terminal {
					return nil
				}
				return finishErr(ctx, conn, sawStart, false, respID, &lastSeq, errTruncated, sendErr)
			}
			ev, err = r.ev, r.err
		}
		if idleFired {
			return finishErr(ctx, conn, sawStart, terminal, respID, &lastSeq, errIdle, sendErr)
		}

		if err != nil {
			// A pending event at EOF is discarded per WHATWG §9.2: treat it
			// like a clean stream end (no terminal seen → truncated turn).
			if errors.Is(err, sse.ErrUnexpectedEOF) || errors.Is(err, errStreamEOF) {
				if !terminal {
					return finishErr(ctx, conn, sawStart, false, respID, &lastSeq, errTruncated, sendErr)
				}
				return nil
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return finishErr(ctx, conn, sawStart, terminal, respID, &lastSeq, errUpstreamStream, sendErr)
		}

		payload := []byte(ev.Data)
		if len(payload) == 0 {
			flag, ok := peek.next()
			if ok && flag.hasData {
				// WHATWG §9.2.6: `data:\n\n` dispatches an event whose
				// data is the empty string. For a Responses-only relay an
				// empty-data event is malformed JSON — apply the declared
				// malformed-event rule, never a silent skip.
				return finishErr(ctx, conn, sawStart, terminal, respID, &lastSeq, errMalformedEvent, sendErr)
			}
			// Metadata-only block (id:/event:/retry: only, or a comment).
			// WHATWG does not dispatch a message for it; it does not reset
			// the inactivity timer either — the raw byte read already did.
			continue
		}

		typ, id, seq, valid := eventHead(payload)
		if seq > 0 {
			lastSeq = seq
		}
		if id != "" {
			respID = id
		}

		// Malformed upstream payload: a WS text frame must be UTF-8 and the
		// relay contract promises a JSON object. The Chat Completions
		// [DONE] marker is a defined non-JSON sentinel and passes verbatim.
		if !valid || !utf8.Valid(payload) {
			if !bytes.Equal(payload, []byte("[DONE]")) {
				return finishErr(ctx, conn, sawStart, terminal, respID, &lastSeq, errMalformedEvent, sendErr)
			}
		}

		// Inspect the event type before sending: a flat upstream error is
		// converted to exactly one WS error envelope, never relayed raw
		// plus an adapted second message.
		if typ == "error" {
			if env := adaptFlatError(payload); env != nil {
				if err := sendText(ctx, conn, env); err != nil {
					return fmt.Errorf("ws write error envelope: %w", err)
				}
				return nil
			}
			// Already a native {type:error,error:{...}} envelope: relay once.
			if err := sendText(ctx, conn, payload); err != nil {
				return finishErr(ctx, conn, sawStart, terminal, respID, &lastSeq, fmt.Errorf("ws write: %w", err), sendErr)
			}
			return nil
		}

		if err := sendText(ctx, conn, payload); err != nil {
			return finishErr(ctx, conn, sawStart, terminal, respID, &lastSeq, fmt.Errorf("ws write: %w", err), sendErr)
		}

		// [DONE] is the terminal sentinel of the transparent stream dialects.
		if bytes.Equal(payload, []byte("[DONE]")) {
			return nil
		}

		switch typ {
		case "response.created", "response.in_progress":
			sawStart = true
		case "response.completed", "response.incomplete", "response.failed":
			return nil // terminal forwarded: turn over, no EOF wait
		}
	}
}

type iterRes struct {
	ev  sse.Event
	err error
}

// pullIter adapts the sse.Read range-pull to a channel so an idle deadline
// or cancellation can abandon a stalled read. One extra goroutine per turn;
// the caller's defer stop() or ctx.Done() releases it.
func pullIter(body io.Reader, maxBody int64, done <-chan struct{}) (chan iterRes, func()) {
	out := make(chan iterRes, 1)
	stop := make(chan struct{})
	go func() {
		defer close(out)
		for ev, err := range sse.Read(body, &sse.ReadConfig{MaxEventSize: int(maxBody)}) {
			select {
			case out <- iterRes{ev: ev, err: err}:
			case <-stop:
				return
			case <-done:
				return
			}
		}
	}()
	return out, func() { close(stop) }
}

var (
	errTruncated      = errors.New("upstream stream ended without a terminal response event")
	errUpstreamStream = errors.New("upstream stream read failed")
	errMalformedEvent = errors.New("upstream event was not a valid JSON object")
	errIdle           = errors.New("upstream turn produced no data within the idle timeout")
)

// eventHead extracts just "type", response.id and sequence_number from one
// SSE data payload, and reports whether the payload parsed as a non-null
// JSON object (JSON "null"/array/scalar are not Responses events).
func eventHead(payload []byte) (typ, id string, seq int64, valid bool) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(payload, &obj); err != nil || obj == nil {
		return "", "", 0, false
	}
	if raw := obj["type"]; len(raw) > 0 {
		_ = json.Unmarshal(raw, &typ)
	}
	if raw := obj["sequence_number"]; len(raw) > 0 {
		_ = json.Unmarshal(raw, &seq)
	}
	if raw := obj["response"]; len(raw) > 0 {
		var resp struct {
			ID string `json:"id"`
		}
		if json.Unmarshal(raw, &resp) == nil {
			id = resp.ID
		}
	}
	return typ, id, seq, true
}

// adaptFlatError converts a flat HTTP-streaming error event
// ({"type":"error","code":...,"message":...}) into the WS error envelope
// ({"type":"error","status":400,"error":{...}}) the client contract
// expects — exactly one WS message. Returns nil when the payload already
// carries an "error" member (native envelope).
func adaptFlatError(payload []byte) []byte {
	var obj map[string]json.RawMessage
	if json.Unmarshal(payload, &obj) != nil || obj == nil {
		return nil
	}
	if _, has := obj["error"]; has {
		return nil
	}
	inner := map[string]json.RawMessage{}
	for k, v := range obj {
		switch k {
		case "type", "sequence_number":
			continue
		default:
			inner[k] = v
		}
	}
	if _, ok := inner["type"]; !ok {
		inner["type"], _ = json.Marshal("invalid_request_error")
	}
	frame, _ := json.Marshal(map[string]any{
		"type": "error", "status": http.StatusBadRequest, "error": inner,
	})
	return frame
}

// finishErr ends a broken turn in protocol terms: response.failed if the
// response had started, else a request-level error frame.
func finishErr(ctx context.Context, conn *websocket.Conn, started, terminal bool, respID string, lastSeq *int64, cause error, sendErr func(context.Context, *websocket.Conn, *apiError)) error {
	if cause == nil || terminal {
		return nil
	}
	if started {
		if respID == "" {
			respID = "resp_unknown"
		}
		seq := int64(0)
		if lastSeq != nil {
			seq = *lastSeq + 1
		}
		frame, _ := json.Marshal(map[string]any{
			"type":            "response.failed",
			"sequence_number": seq,
			"response": responseObject(respID, "", "failed", map[string]any{
				"type": "server_error", "code": "upstream_stream_failed",
				"message": cause.Error(),
			}, 0),
		})
		_ = sendText(ctx, conn, frame)
		return nil
	}
	sendErr(ctx, conn, &apiError{Status: http.StatusBadGateway, Type: "upstream_error", Message: cause.Error()})
	return nil
}

// endToEndHeaders copies client headers minus hop-by-hop and WS handshake
// headers, for reuse on the upstream POST. Per RFC 9110 §7.6.1, fields
// nominated by the incoming Connection header are also removed — a client
// that marks Authorization hop-by-hop must not see it forwarded.
func endToEndHeaders(r *http.Request) http.Header {
	skip := map[string]bool{
		"connection": true, "proxy-connection": true, "keep-alive": true,
		"proxy-authenticate": true, "proxy-authorization": true,
		"te": true, "trailer": true, "transfer-encoding": true,
		"upgrade": true, "host": true, "content-length": true,
	}
	for _, conn := range r.Header.Values("Connection") {
		for _, name := range strings.Split(conn, ",") {
			if n := strings.ToLower(strings.TrimSpace(name)); n != "" {
				skip[n] = true
			}
		}
	}
	h := make(http.Header, len(r.Header))
	for name, vals := range r.Header {
		if skip[strings.ToLower(name)] || strings.HasPrefix(strings.ToLower(name), "sec-websocket-") {
			continue
		}
		for _, v := range vals {
			h.Add(name, v)
		}
	}
	return h
}
