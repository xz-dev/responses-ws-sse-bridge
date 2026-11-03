package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
)

func TestSpooledFlatErrorDeduplicatesEscapedFields(t *testing.T) {
	_, conn := regressPair(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"type\":\"error\",\"code\":\"first\",\"co\\u0064e\":\"last\",\"message\":\"denied\"}\n\n")
	}, nil)
	regressSend(t, conn, []byte(`{"type":"response.create","model":"m","input":[]}`))
	payload := regressRead(t, conn)
	var event struct {
		Type   string `json:"type"`
		Status int    `json:"status"`
		Error  struct {
			Code    string `json:"code"`
			Message string `json:"message"`
			Type    string `json:"type"`
		} `json:"error"`
	}
	if e := json.Unmarshal(payload, &event); e != nil || event.Type != "error" || event.Status != 400 || event.Error.Code != "last" || event.Error.Message != "denied" || event.Error.Type != "invalid_request_error" {
		t.Fatalf("flat error semantics changed: %s %v", payload, e)
	}
	if bytes.Contains(payload, []byte(`"first"`)) {
		t.Fatalf("superseded duplicate field emitted: %s", payload)
	}
}
