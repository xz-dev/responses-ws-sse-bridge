package main

// Round-4 regression tests ported unmodified from the round-3 reviewer's
// probes (/var/tmp/ws-sse-bridge-review3-20261022/extra_test.go). Helper
// names are adapted to this repo's regress* test helpers; assertions are
// unchanged.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
)

// REVIEW3-1 / B6: custom validation must not turn small admitted input into
// an unbounded recursion stack. A fixed nesting limit rejects deep input.
func TestReview4NestedInputMemoryBound(t *testing.T) {
	const depth = 100000
	raw := []byte(`{"type":"response.create","model":"m","input":` + strings.Repeat("[", depth) + `0` + strings.Repeat("]", depth) + `}`)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err := inspectFrameStream(bytes.NewReader(raw))
	runtime.ReadMemStats(&after)
	t.Logf("frame=%d bytes accepted=%v stack_growth=%d total_alloc=%d", len(raw), err == nil, after.StackInuse-before.StackInuse, after.TotalAlloc-before.TotalAlloc)
	if err == nil && after.StackInuse > before.StackInuse+8<<20 {
		t.Fatal("small input accepted with >8 MiB recursion stack outside request-byte budget")
	}
}

// REVIEW3-1 depth limit boundary: input inside the documented limit still
// validates; over the limit is rejected (and never reaches upstream).
func TestReview4DepthLimitBoundary(t *testing.T) {
	const limit = 128 // documented parser nesting limit
	build := func(d int) []byte {
		return []byte(`{"type":"response.create","model":"m","input":` + strings.Repeat("[", d) + `0` + strings.Repeat("]", d) + `}`)
	}
	if _, err := inspectFrameStream(bytes.NewReader(build(limit - 2))); err != nil {
		t.Fatalf("input at depth limit-2 rejected: %v", err)
	}
	if _, err := inspectFrameStream(bytes.NewReader(build(limit + 64))); err == nil {
		t.Fatal("input over the nesting depth limit accepted")
	}
	// Over-depth input is rejected before any upstream side effect.
	var calls atomic.Int32
	_, c := regressPair(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); regressSSE(w, r) }, nil)
	regressSend(t, c, build(limit+64))
	reply := regressRead(t, c)
	if calls.Load() != 0 {
		t.Fatalf("over-depth input forwarded upstream: calls=%d", calls.Load())
	}
	if !bytes.Contains(reply, []byte("invalid_request_error")) {
		t.Fatalf("over-depth input must answer invalid_request_error, got %s", reply)
	}
}

// REVIEW3-2 / WHATWG §9.2.6: `data:\n\n` dispatches an event whose data is
// the empty string; it is not a metadata-only block. For this Responses
// converter an empty-data event is malformed JSON — the declared
// malformed-event rule applies, not a silent skip.
func TestReview4EmptyDataBlockIsNotMetadata(t *testing.T) {
	_, c := regressPair(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data:\n\n")
		regressSSE(w, r)
	}, nil)
	regressSend(t, c, []byte(`{"type":"response.create","model":"m","input":[]}`))
	reply := regressRead(t, c)
	if bytes.Contains(reply, []byte(`"response.completed"`)) {
		t.Fatal("empty data message was silently discarded as metadata instead of invalid JSON failure")
	}
}

// A genuinely metadata-only block (id:/event: only, no data field) must NOT
// dispatch — unchanged behaviour from round 2.
func TestReview4MetadataOnlyStillNotDispatched(t *testing.T) {
	_, c := regressPair(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "id: 7\nevent: keepalive\n\n")
		regressSSE(w, r)
	}, nil)
	regressSend(t, c, []byte(`{"type":"response.create","model":"m","input":[]}`))
	reply := regressRead(t, c)
	if !bytes.Contains(reply, []byte(`"response.completed"`)) {
		t.Fatalf("metadata-only block must not disturb the turn: %s", reply)
	}
}

// REVIEW3-3 / B2,B5: non-null usage must carry the full ResponseUsage
// schema — input_tokens_details {cached_tokens} and
// output_tokens_details {reasoning_tokens} are required members.
func TestReview4SyntheticUsageSchema(t *testing.T) {
	for i, raw := range prewarmEvents("resp_schema", "m") {
		var ev struct {
			Response struct {
				Usage map[string]json.RawMessage `json:"usage"`
			} `json:"response"`
		}
		if err := json.Unmarshal(raw, &ev); err != nil {
			t.Fatal(err)
		}
		for _, k := range []string{"input_tokens_details", "output_tokens_details"} {
			if _, ok := ev.Response.Usage[k]; !ok {
				t.Errorf("prewarm event %d missing ResponseUsage.%s", i, k)
			}
		}
		var itd, otd struct {
			N int `json:"cached_tokens"`
		}
		if err := json.Unmarshal(ev.Response.Usage["input_tokens_details"], &itd); err != nil {
			t.Errorf("input_tokens_details malformed: %v", err)
		}
		var otdV struct {
			N int `json:"reasoning_tokens"`
		}
		if err := json.Unmarshal(ev.Response.Usage["output_tokens_details"], &otdV); err != nil || otd != itd {
			t.Errorf("output_tokens_details malformed: %v", err)
		}
	}
}
