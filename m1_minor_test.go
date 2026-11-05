package main

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

// Review m3: a chunked native body without a ticket is refused as it
// crosses 64 MiB, and the HTTP error body names the hop.
func TestM1NativeChunkedUnticketedRefusedAt64MiB(t *testing.T) {
	up := newSSEUpstream(t)
	up.body = func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }
	p := startPair(t, up, func(c *config) { c.maxMsgBytes = 256 << 20 })
	defer p.close()
	body := io.MultiReader(strings.NewReader(`{"input":"`), io.LimitReader(neverEnding('x'), 80<<20))
	req, _ := http.NewRequest(http.MethodPost, p.bSrv.URL+"/v1/responses", body)
	req.ContentLength = -1 // chunked
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got struct{ Error apiError }
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable || got.Error.Code != "capacity_ticket_missing" || got.Error.Hop != bridgeHop {
		t.Fatalf("status %d error %+v", resp.StatusCode, got.Error)
	}
}

// Review m4/m5: older local events carry a code and the hop, and a size
// refusal carries the limit as a field.
func TestM1LocalEventsCarryCodeHopLimit(t *testing.T) {
	p := startPair(t, newSSEUpstream(t), func(c *config) { c.maxMsgBytes = 64 })
	defer p.close()
	conn := dial(t, p, "/v1/responses", nil)
	defer conn.CloseNow()
	writeFrame(t, conn, `{"type":"x",`)
	requireErrorEvent(t, readEvent(t, conn), "invalid_json")
	writeFrame(t, conn, `{"type":"session.update"}`)
	requireErrorEvent(t, readEvent(t, conn), "unsupported_event_type")
	writeFrame(t, conn, `{"type":"response.create","input":"`+strings.Repeat("x", 64)+`"}`)
	if e := requireErrorEvent(t, readEvent(t, conn), "request_too_large"); e["limit_bytes"] != float64(64) || e["limit"] != nil {
		t.Fatalf("limit_bytes = %v, legacy limit = %v", e["limit_bytes"], e["limit"])
	}
}

// Review m8: a zero disk reserve would disable the D3 floor.
func TestM1ZeroDiskReserveRejected(t *testing.T) {
	t.Setenv("UPSTREAM_URL", "http://127.0.0.1:9")
	t.Setenv("DISK_RESERVE_BYTES", "0")
	if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), "DISK_RESERVE_BYTES") {
		t.Fatalf("want DISK_RESERVE_BYTES refusal, got %v", err)
	}
}
