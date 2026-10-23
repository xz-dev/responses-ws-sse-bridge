package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/coder/websocket"
	"github.com/tidwall/gjson"
)

// sampleFrame builds a representative response.create frame (~2 KiB, nested).
func sampleFrame() []byte {
	items := make([]map[string]any, 3)
	for i := range items {
		items[i] = map[string]any{
			"type": "message", "role": "user",
			"content": []map[string]any{{"type": "input_text", "text": strings.Repeat("x", 500)}},
		}
	}
	f := map[string]any{
		"type": "response.create", "model": "claude-opus-5-5",
		"input": items, "stream": true, "store": false,
		"reasoning": map[string]any{"effort": "medium"},
	}
	b, _ := json.Marshal(f)
	return b
}

func BenchmarkInspectFrame(b *testing.B) {
	frame := sampleFrame()
	b.ReportAllocs()
	b.SetBytes(int64(len(frame)))
	for i := 0; i < b.N; i++ {
		if _, err := inspectFrame(frame); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInspectFrameUnmarshal(b *testing.B) { // baseline: full decode
	frame := sampleFrame()
	b.ReportAllocs()
	b.SetBytes(int64(len(frame)))
	for i := 0; i < b.N; i++ {
		var m map[string]any
		if err := json.Unmarshal(frame, &m); err != nil {
			b.Fatal(err)
		}
		_ = m["type"]
	}
}

func BenchmarkRewriteBody(b *testing.B) {
	frame := sampleFrame()
	buf := new(bytes.Buffer)
	b.ReportAllocs()
	b.SetBytes(int64(len(frame)))
	for i := 0; i < b.N; i++ {
		if err := rewriteBody(buf, frame); err != nil {
			b.Fatal(err)
		}
	}
}

// makeSSE builds an SSE body of n events (~totalBytes payload).
func makeSSE(n int, payloadBytes int) string {
	var sb strings.Builder
	per := payloadBytes / n
	if per < 40 {
		per = 40
	}
	for i := 0; i < n-1; i++ {
		fmt.Fprintf(&sb, "data: {\"type\":\"response.output_text.delta\",\"delta\":%q}\n\n",
			strings.Repeat("y", per-60))
	}
	fmt.Fprintf(&sb, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_b\",\"status\":\"completed\",\"output\":[]}}\n\n")
	return sb.String()
}

// benchUpstream serves a fixed SSE stream per request.
func benchUpstream(body string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		fmt.Fprint(w, body)
	})
}

// runRelayBench drives a full WS turn through the bridge.
func runRelayBench(b *testing.B, sseBody string) {
	uSrv := httptest.NewServer(benchUpstream(sseBody))
	b.Cleanup(uSrv.Close)
	uURL, _ := url.Parse(uSrv.URL)
	br := newBridge(config{
		upstream: uURL, listen: "127.0.0.1:0",
		maxSockets: 8, maxMsgBytes: 1 << 20, maxBodyBytes: 64 << 20,
	})
	bSrv := httptest.NewServer(br)
	b.Cleanup(bSrv.Close)

	ctx := context.Background()
	conn, _, err := websocket.Dial(ctx, wsURL(bSrv.URL)+"/v1/responses", nil)
	if err != nil {
		b.Fatalf("dial: %v", err)
	}
	b.Cleanup(func() { conn.CloseNow() })
	conn.SetReadLimit(64 << 20)

	frame := []byte(`{"type":"response.create","model":"m","input":"x"}`)
	nEvents := strings.Count(sseBody, "data: ")

	b.ReportAllocs()
	b.SetBytes(int64(len(sseBody)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := conn.Write(ctx, websocket.MessageText, frame); err != nil {
			b.Fatal(err)
		}
		for j := 0; j < nEvents; j++ {
			if _, _, err := conn.Read(ctx); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkRelayTurn1MiB(b *testing.B)      { runRelayBench(b, makeSSE(50, 1<<20)) }
func BenchmarkRelayTurn100Events(b *testing.B) { runRelayBench(b, makeSSE(100, 40*1024)) }

func BenchmarkHTTPPassthrough(b *testing.B) {
	uSrv := httptest.NewServer(benchUpstream(makeSSE(50, 1<<20)))
	b.Cleanup(uSrv.Close)
	uURL, _ := url.Parse(uSrv.URL)
	br := newBridge(config{
		upstream: uURL, listen: "127.0.0.1",
		maxSockets: 8, maxMsgBytes: 1 << 20, maxBodyBytes: 64 << 20,
	})
	bSrv := httptest.NewServer(br)
	b.Cleanup(bSrv.Close)

	b.ReportAllocs()
	b.SetBytes(int64(1 << 20))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := http.Post(bSrv.URL+"/v1/responses", "application/json", strings.NewReader(`{"model":"m"}`))
		if err != nil {
			b.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}

// Library comparison for the reuse decision (step 3). gjson peeks top-level
// keys without decoding; stdlib Decoder.Token walks the whole doc.
func BenchmarkInspectGjson(b *testing.B) {
	frame := sampleFrame()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = gjsonType(frame)
	}
}
func gjsonType(frame []byte) string {
	_ = gjson.GetBytes(frame, "type").String()
	_ = gjson.GetBytes(frame, "previous_response_id").String()
	_ = gjson.GetBytes(frame, "generate").Bool()
	_ = gjson.GetBytes(frame, "model").String()
	return ""
}
func BenchmarkInspectDecoderToken(b *testing.B) {
	frame := sampleFrame()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		d := json.NewDecoder(bytes.NewReader(frame))
		for {
			tok, err := d.Token()
			if err != nil {
				break
			}
			_ = tok
		}
	}
}

// Actual-path 2 MiB benches: the production inspect validates and records
// member spans without materialising values; the rewrite emits member
// spans via an io.MultiReader, so the work is O(spans), not O(bytes).
func BenchmarkInspectFrameStream2MiB(b *testing.B) {
	raw := []byte(`{"type":"response.create","model":"m","input":[{"content":"` + strings.Repeat("x", 2<<20) + `"}]}`)
	b.SetBytes(int64(len(raw)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := inspectFrameStream(bytes.NewReader(raw)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRewrittenBody2MiB(b *testing.B) {
	raw := []byte(`{"type":"response.create","model":"m","input":[{"content":"` + strings.Repeat("x", 2<<20) + `"}]}`)
	info, err := inspectFrameStream(bytes.NewReader(raw))
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(raw)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, _ := rewrittenBody(bytes.NewReader(raw), info)
		if _, err := io.Copy(io.Discard, r); err != nil {
			b.Fatal(err)
		}
	}
}
