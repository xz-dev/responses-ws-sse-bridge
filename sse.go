package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/coder/websocket"
)

var (
	errStreamEOF      = errors.New("sse body ended") // legacy blockPeek compatibility; forwarding uses eventSpoolReader.
	errTruncated      = errors.New("upstream stream ended without a terminal response event")
	errUpstreamStream = errors.New("upstream stream read failed")
	errMalformedEvent = errors.New("upstream event was not a valid JSON object")
)

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
			"type": "response.failed", "sequence_number": seq,
			"response": responseObject(respID, "", "failed", map[string]any{
				"type": "server_error", "code": "upstream_stream_failed", "message": cause.Error(),
			}, 0),
		})
		_ = sendText(ctx, conn, frame)
		return nil
	}
	sendErr(ctx, conn, &apiError{Status: http.StatusBadGateway, Type: "upstream_error", Message: cause.Error()})
	return nil
}

// endToEndHeaders copies client headers minus hop-by-hop and WS handshake
// headers, including fields nominated by Connection.
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
