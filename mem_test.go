// memload is a standalone load harness: a real bridge + real WS clients.
// Reports runtime.MemStats and /proc/self/status RSS at three checkpoints:
// idle, 64 idle sockets, 16 concurrent streaming turns.
package main

import (
	"context"
	"fmt"
	"net/http/httptest"
	"net/url"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func rssKB() int {
	b, _ := os.ReadFile("/proc/self/status")
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, "VmRSS:") {
			var kb int
			fmt.Sscanf(l, "VmRSS:\t%d kB", &kb)
			return kb
		}
	}
	return -1
}

func TestMemLoad(t *testing.T) {
	if os.Getenv("MEMLOAD") != "1" {
		t.Skip("set MEMLOAD=1")
	}
	sse := makeSSE(200, 4*1024*1024) // 200 events, ~4 MiB per turn
	uSrv := httptest.NewServer(benchUpstream(sse))
	defer uSrv.Close()
	uURL, _ := url.Parse(uSrv.URL)
	br := newBridge(config{
		upstream: uURL, listen: "127.0.0.1:0",
		maxSockets: 64, maxMsgBytes: 16 << 20, maxBodyBytes: 64 << 20,
	})
	bSrv := httptest.NewServer(br)
	defer bSrv.Close()

	report := func(tag string) {
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		t.Logf("%-28s RSS=%d kB  HeapAlloc=%d kB  HeapIdle=%d kB  Goroutines=%d",
			tag, rssKB(), m.HeapAlloc/1024, m.HeapIdle/1024, runtime.NumGoroutine())
	}

	report("idle")

	// 64 idle sockets
	idle := make([]*websocket.Conn, 0, 64)
	ctx := context.Background()
	for i := 0; i < 64; i++ {
		c, _, err := websocket.Dial(ctx, wsURL(bSrv.URL)+"/v1/responses", nil)
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		idle = append(idle, c)
	}
	report("64 idle sockets")

	// CPU profile: 16 concurrent streaming turns, ~30s
	f, _ := os.Create("/var/tmp/ws-sse-bridge-20261022/cpu.pprof")
	pprof.StartCPUProfile(f)
	start := time.Now()
	var wg sync.WaitGroup
	turns := 0
	var mu sync.Mutex
	for time.Since(start) < 20*time.Second {
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func(c *websocket.Conn) {
				defer wg.Done()
				frame := `{"type":"response.create","model":"m","input":"x"}`
				if err := c.Write(ctx, websocket.MessageText, []byte(frame)); err != nil {
					return
				}
				for j := 0; j < 200; j++ {
					if _, _, err := c.Read(ctx); err != nil {
						return
					}
				}
				mu.Lock()
				turns++
				mu.Unlock()
			}(idle[i])
		}
		wg.Wait()
	}
	pprof.StopCPUProfile()
	f.Close()
	report("after streaming load")
	t.Logf("completed streaming turns: %d", turns)

	for _, c := range idle {
		c.CloseNow()
	}
}
