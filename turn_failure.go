package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"

	"github.com/coder/websocket"
)

// bridgeHop names this hop in every locally generated error (design D9).
const bridgeHop = "responses-ws-sse-bridge"

var (
	// errInsufficientStorage: a spool write would cross the disk reserve (D3).
	errInsufficientStorage = errors.New("spool write refused: free disk would fall below the reserve")
	// errInsufficientMemory: this hop's cgroup headroom cannot cover the step.
	errInsufficientMemory = errors.New("cgroup memory headroom unavailable")
	// errCommunicated wraps a failure whose terminal event already reached
	// the client (response.failed or an error frame); only the close remains.
	errCommunicated = errors.New("failure already communicated")
	// errMidMessage: a WS message was partially written; no further
	// application frame can follow on this socket, only a close.
	errMidMessage = errors.New("websocket message write interrupted")
)

// isLocalRefusal: a resource refusal by this hop, which the client must see
// as an explicit error rather than a transport failure.
func isLocalRefusal(err error) bool {
	return errors.Is(err, errWorkBudget) || errors.Is(err, errInsufficientStorage) || errors.Is(err, errInsufficientMemory)
}

func requestTooLarge(limit int64) *apiError {
	return &apiError{Status: http.StatusRequestEntityTooLarge, Type: "invalid_request_error", Code: "request_too_large", Hop: bridgeHop,
		Message: fmt.Sprintf("Request exceeds the %d-byte WebSocket message limit of %s.", limit, bridgeHop)}
}

// failureAPIError maps a local turn/reader failure to the explicit error the
// client receives before the close. Every branch names this hop.
func (b *bridge) failureAPIError(err error) *apiError {
	switch {
	case errors.Is(err, errTooLarge):
		return requestTooLarge(b.cfg.maxMsgBytes)
	case errors.Is(err, errWorkBudget):
		return &apiError{Status: http.StatusServiceUnavailable, Type: "server_error", Code: "working_set_unavailable", Hop: bridgeHop,
			Message: fmt.Sprintf("Working capacity of %s (%d bytes) is exhausted; retry later.", bridgeHop, b.cfg.workBytes)}
	case errors.Is(err, errInsufficientStorage):
		return &apiError{Status: http.StatusInsufficientStorage, Type: "server_error", Code: "insufficient_storage", Hop: bridgeHop,
			Message: fmt.Sprintf("%s spool refused: free disk would fall below the %d-byte reserve.", bridgeHop, b.cfg.diskReserveBytes)}
	case errors.Is(err, errInsufficientMemory):
		return &apiError{Status: http.StatusServiceUnavailable, Type: "server_error", Code: "insufficient_memory", Hop: bridgeHop,
			Message: fmt.Sprintf("%s cgroup memory headroom is below the required step plus the 32 MiB margin.", bridgeHop)}
	default:
		var ticket *ticketError
		if errors.As(err, &ticket) {
			return ticket.apiError()
		}
		return &apiError{Status: http.StatusInternalServerError, Type: "server_error", Code: "bridge_turn_failed", Hop: bridgeHop,
			Message: fmt.Sprintf("%s failed the turn: %v", bridgeHop, err)}
	}
}

func closeCodeFor(err error) (websocket.StatusCode, string) {
	switch {
	case errors.Is(err, errTooLarge):
		return websocket.StatusNormalClosure, "request too large"
	case errors.Is(err, errWorkBudget), errors.Is(err, errInsufficientStorage), errors.Is(err, errInsufficientMemory):
		return websocket.StatusTryAgainLater, "capacity unavailable"
	default:
		return websocket.StatusInternalError, "turn failed"
	}
}

// failStream reports a local failure inside a relayed turn: response.failed
// once the response started, a request-level error frame before. The id is
// re-encoded only if its expansion can be charged; otherwise the frame uses
// a fixed placeholder id so the explicit terminal event is never skipped.
func (b *bridge) failStream(ctx context.Context, conn *websocket.Conn, started bool, id string, sequence *int64, cause error) error {
	e := b.failureAPIError(cause)
	if !started {
		b.sendErrorFrame(ctx, conn, e)
		return fmt.Errorf("%w: %w", errCommunicated, cause)
	}
	release := b.acquire(20*int64(len(id)) + 8192)
	if release == nil {
		id = "resp_unknown"
	} else {
		defer release()
	}
	if id == "" {
		id = "resp_unknown"
	}
	seq := int64(0)
	if sequence != nil {
		seq = *sequence + 1
	}
	frame, _ := json.Marshal(map[string]any{
		"type": "response.failed", "sequence_number": seq,
		"response": responseObject(id, "", "failed", map[string]any{
			"type": e.Type, "code": e.Code, "hop": e.Hop, "message": e.Message,
		}, 0),
	})
	_ = sendText(ctx, conn, frame)
	return fmt.Errorf("%w: %w", errCommunicated, cause)
}

// failTurn ends the session in protocol terms: an explicit error frame
// (unless one was already sent or a message is half-written), then a close
// frame carrying a code and reason. It never leaves a bare close.
func (b *bridge) failTurn(ctx context.Context, conn *websocket.Conn, err error) {
	if !errors.Is(err, errCommunicated) && !errors.Is(err, errMidMessage) {
		b.sendErrorFrame(ctx, conn, b.failureAPIError(err))
	}
	code, reason := closeCodeFor(err)
	log.Printf("ws turn failed (hop=%s close=%d): %v", bridgeHop, code, err)
	_ = conn.Close(code, reason)
}
