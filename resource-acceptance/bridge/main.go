package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/coder/websocket"
)

const chunks = 1024
const chunkSize = 64 << 10
const tcpCleanupLimit = 200 * time.Millisecond
const terminal = `{"type":"response.completed","response":{"id":"resp_fixture","output":[]}}`

func payload(i, size int) []byte {
	return []byte(fmt.Sprintf(`{"type":"response.output_text.delta","sequence_number":%d,"delta":"%s"}`, i, strings.Repeat("z", size)))
}
func verify(typ websocket.MessageType, got []byte, seq, size int, last bool) error {
	want := []byte(terminal)
	if !last {
		want = payload(seq, size)
	}
	if typ != websocket.MessageText || !bytes.Equal(got, want) {
		return fmt.Errorf("oracle mismatch sequence=%d terminal=%t bytes=%d", seq, last, len(got))
	}
	return nil
}

// Linux CLOCK_MONOTONIC matches Python time.monotonic_ns in the verified shared time namespace.
func monotonicNS() int64 {
	var ts syscall.Timespec
	_, _, errno := syscall.RawSyscall(syscall.SYS_CLOCK_GETTIME, 1, uintptr(unsafe.Pointer(&ts)), 0)
	if errno != 0 {
		panic(errno)
	}
	return ts.Nano()
}
func event(phase string, values map[string]any) {
	values["phase"] = phase
	values["clock"] = "CLOCK_MONOTONIC"
	values["monotonic_ns"] = monotonicNS()
	if err := json.NewEncoder(os.Stdout).Encode(values); err != nil {
		panic(err)
	}
}
func selftest() error {
	for i := 0; i < 3; i++ {
		if err := verify(websocket.MessageText, payload(i, 17), i, 17, false); err != nil {
			return err
		}
	}
	if err := verify(websocket.MessageText, []byte(terminal), 3, 17, true); err != nil {
		return err
	}
	bad := payload(0, 17)
	bad[len(bad)-3] = 'x'
	for _, tc := range []struct {
		typ  websocket.MessageType
		data []byte
	}{{websocket.MessageText, payload(1, 17)}, {websocket.MessageText, bad}, {websocket.MessageBinary, payload(0, 17)}, {websocket.MessageText, []byte(terminal)}} {
		if verify(tc.typ, tc.data, 0, 17, false) == nil {
			return errors.New("oracle accepted corrupt stream")
		}
	}
	if checkTCPElapsed(tcpCleanupLimit) != nil || checkTCPElapsed(tcpCleanupLimit+time.Nanosecond) == nil {
		return errors.New("TCP elapsed boundary oracle failed")
	}
	joined := make(chan struct{})
	close(joined)
	if _, err := joinTCP(time.Now().Add(-201*time.Millisecond), joined, joined); err == nil {
		return errors.New("TCP oracle accepted joins after common deadline")
	}
	if _, err := joinTCP(time.Now(), joined, joined); err != nil {
		return err
	}
	event("selftest_passed", map[string]any{"synthetic_events": 3, "rejected_wrong_sequence": true, "rejected_wrong_bytes": true, "rejected_wrong_type": true, "rejected_early_terminal": true, "rejected_tcp_total_over_200ms": true, "common_tcp_deadline_tested": true, "actual_image_test": "NOTRUN"})
	return nil
}

type source struct {
	sent    atomic.Int64
	done    chan struct{}
	started atomic.Bool
}
type client struct {
	conn        *websocket.Conn
	tcp         *net.TCPConn
	gate        chan struct{}
	got         chan error
	joined      chan struct{}
	mu          sync.Mutex
	count, size int
}

func dial(ctx context.Context, paused bool) (*client, error) {
	c := &client{gate: make(chan struct{}), got: make(chan error, 1), joined: make(chan struct{})}
	tr := &http.Transport{Proxy: nil, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		n, e := (&net.Dialer{}).DialContext(ctx, network, address)
		if e == nil {
			c.tcp = n.(*net.TCPConn)
			e = c.tcp.SetReadBuffer(16 << 10)
		}
		return n, e
	}}
	conn, _, err := websocket.Dial(ctx, "ws://127.0.0.1:8080/v1/responses", &websocket.DialOptions{HTTPClient: &http.Client{Transport: tr}})
	if err != nil {
		return nil, err
	}
	c.conn = conn
	conn.SetReadLimit(2 << 20)
	if !paused {
		close(c.gate)
	}
	// Exactly one lifetime reader per connection. Idle readers keep processing Ping/Close.
	go func() {
		defer close(c.joined)
		defer tr.CloseIdleConnections()
		select {
		case <-c.gate:
		case <-ctx.Done():
			return
		}
		seq := 0
		for {
			typ, b, e := conn.Read(ctx)
			if e != nil {
				select {
				case c.got <- e:
				default:
				}
				return
			}
			c.mu.Lock()
			n, size := c.count, c.size
			c.mu.Unlock()
			last := seq == n
			e = verify(typ, b, seq, size, last)
			if e != nil {
				select {
				case c.got <- e:
				default:
				}
				return
			}
			if last {
				seq = 0
				c.got <- nil
			} else {
				seq++
			}
		}
	}()
	return c, nil
}
func (c *client) start(ctx context.Context, model string, n, size int) error {
	c.mu.Lock()
	c.count = n
	c.size = size
	c.mu.Unlock()
	b, _ := json.Marshal(map[string]any{"type": "response.create", "model": model, "input": []any{}})
	return c.conn.Write(ctx, websocket.MessageText, b)
}
func (c *client) wait(ctx context.Context) error {
	select {
	case e := <-c.got:
		return e
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (c *client) turn(ctx context.Context, model string, n, size int) error {
	if e := c.start(ctx, model, n, size); e != nil {
		return e
	}
	return c.wait(ctx)
}
func boundedJoin(ch <-chan struct{}, label string) error {
	select {
	case <-ch:
		return nil
	case <-time.After(7 * time.Second):
		return fmt.Errorf("%s did not join in seven seconds", label)
	}
}
func checkTCPElapsed(elapsed time.Duration) error {
	if elapsed < 0 || elapsed > tcpCleanupLimit {
		return fmt.Errorf("TCP CloseNow and both joins exceeded total 200ms: %s", elapsed)
	}
	return nil
}
func joinTCP(started time.Time, backend, reader <-chan struct{}) (time.Duration, error) {
	deadline := started.Add(tcpCleanupLimit)
	for _, ch := range []<-chan struct{}{backend, reader} {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return time.Since(started), errors.New("TCP common 200ms deadline expired")
		}
		timer := time.NewTimer(remaining)
		select {
		case <-ch:
			timer.Stop()
		case <-timer.C:
			return time.Since(started), errors.New("TCP common 200ms deadline expired")
		}
	}
	elapsed := time.Since(started)
	return elapsed, checkTCPElapsed(elapsed)
}
func plateau(ctx context.Context, s *source, total int, hold time.Duration) (int64, int64, int64, error) {
	until := time.Now().Add(15 * time.Second)
	last := int64(-1)
	stable := time.Now()
	stableNS := monotonicNS()
	for {
		select {
		case <-ctx.Done():
			return 0, 0, 0, ctx.Err()
		case <-s.done:
			return 0, 0, 0, errors.New("source ended before blocked proof")
		case <-time.After(250 * time.Millisecond):
		}
		now := s.sent.Load()
		if now != last {
			last = now
			stable = time.Now()
			stableNS = monotonicNS()
		}
		if now >= int64(total) {
			return 0, 0, 0, errors.New("source completed: no blocked proof")
		}
		if now > 0 && time.Since(stable) >= hold {
			return now, stableNS, monotonicNS(), nil
		}
		if time.Now().After(until) && hold < 30*time.Second {
			return 0, 0, 0, errors.New("no progress plateau")
		}
	}
}
func run() error {
	if os.Getenv("BRIDGE_ACCEPTANCE_NETNS") != "1" {
		return errors.New("isolated-netns marker required")
	}
	ifaces, e := net.Interfaces()
	if e != nil {
		return e
	}
	for _, i := range ifaces {
		if i.Flags&net.FlagLoopback == 0 {
			return errors.New("non-loopback interface")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	first := &source{done: make(chan struct{})}
	second := &source{done: make(chan struct{})}
	listener, e := net.Listen("tcp4", "127.0.0.1:9001")
	if e != nil {
		return e
	}
	srv := &http.Server{ReadHeaderTimeout: 5 * time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/v1/responses" {
			http.Error(w, "bad contract", 400)
			return
		}
		var req struct {
			Model string `json:"model"`
		}
		if json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req) != nil {
			http.Error(w, "bad request", 400)
			return
		}
		io.Copy(io.Discard, r.Body)
		r.Body.Close()
		n, size := 1, 32
		var s *source
		switch req.Model {
		case "small":
		case "large":
			size = 1 << 20
		case "flood":
			n = chunks
			size = chunkSize
			s = first
		case "cancel":
			n = 64
			size = 1 << 20
			s = second
		default:
			http.Error(w, "unknown model", 400)
			return
		}
		if s != nil {
			if !s.started.CompareAndSwap(false, true) {
				http.Error(w, "duplicate source", 400)
				return
			}
			defer close(s.done)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		for i := 0; i < n; i++ {
			if _, err := fmt.Fprintf(w, "data: %s\n\n", payload(i, size)); err != nil {
				return
			}
			if err := http.NewResponseController(w).Flush(); err != nil {
				return
			}
			if s != nil {
				s.sent.Add(1)
			}
		}
		fmt.Fprintf(w, "data: %s\n\n", terminal)
		http.NewResponseController(w).Flush()
	})}
	go srv.Serve(listener)
	defer srv.Close()
	var clients []*client
	defer func() {
		for _, c := range clients {
			c.conn.CloseNow()
		}
	}()
	open := func(paused bool) (*client, error) {
		c, e := dial(ctx, paused)
		if e == nil {
			clients = append(clients, c)
		}
		return c, e
	}
	for i := 0; i < 65; i++ {
		c, e := open(false)
		if e != nil {
			return e
		}
		if e = c.turn(ctx, "small", 1, 32); e != nil {
			return fmt.Errorf("small turn %d: %w", i, e)
		}
	}
	// Ping after all 65 completed, while every prior connection remains open.
	for _, c := range clients {
		p, stop := context.WithTimeout(ctx, 5*time.Second)
		e := c.conn.Ping(p)
		stop()
		if e != nil {
			return e
		}
	}
	event("65_completed_open", map[string]any{"completed_turns": 65, "persistent_readers": 65, "ping_all": true})
	b := clients[0]
	if e := b.turn(ctx, "large", 1, 1<<20); e != nil {
		return e
	}
	event("large_normal", map[string]any{"delta_bytes": 1 << 20})
	slow, e := open(true)
	if e != nil {
		return e
	}
	if e = slow.start(ctx, "flood", chunks, chunkSize); e != nil {
		return e
	}
	count, _, _, e := plateau(ctx, first, chunks, 2*time.Second)
	if e != nil {
		return e
	}
	if e = b.turn(ctx, "small", 1, 32); e != nil {
		return e
	}
	event("blocked_hold_start", map[string]any{"source_sent": count, "source_total": chunks, "event_delta_bytes": chunkSize, "source_unfinished": true})
	// Sampling controller independently observes bounded spool throughout this hold.
	count, _, _, e = plateau(ctx, first, chunks, 32*time.Second)
	if e != nil {
		return e
	}
	if e = b.turn(ctx, "small", 1, 32); e != nil {
		return e
	}
	event("blocked_hold_complete", map[string]any{"stable_seconds": 32, "source_sent": count, "source_total": chunks, "unrelated_B_completed": true})
	close(slow.gate)
	if e = slow.wait(ctx); e != nil {
		return e
	}
	if e = boundedJoin(first.done, "drained backend"); e != nil {
		return e
	}
	if e = slow.turn(ctx, "small", 1, 32); e != nil {
		return e
	}
	event("drained", map[string]any{"events_verified": chunks, "delta_bytes_verified": chunks * chunkSize, "exact_text_type_sequence_bytes": true, "terminal_verified": true, "same_connection_next_turn": true})
	blocked, e := open(true)
	if e != nil {
		return e
	}
	if e = blocked.start(ctx, "cancel", 64, 1<<20); e != nil {
		return e
	}
	count, plateauStartNS, plateauEndNS, e := plateau(ctx, second, 64, 2*time.Second)
	if e != nil {
		return e
	}
	if e = b.turn(ctx, "small", 1, 32); e != nil {
		return e
	}
	event("large_blocked_cancel", map[string]any{"source_sent": count, "source_total": 64, "event_delta_bytes": 1 << 20, "plateau_start_ns": plateauStartNS, "plateau_end_ns": plateauEndNS})
	cancelStartNS := monotonicNS()
	started := time.Now()
	if e = blocked.conn.CloseNow(); e != nil {
		return e
	}
	close(blocked.gate)
	elapsed, e := joinTCP(started, second.done, blocked.joined)
	if e != nil {
		return e
	}
	if e = b.turn(ctx, "small", 1, 32); e != nil {
		return e
	}
	event("passed", map[string]any{"tcp_CloseNow_cleanup_ms": float64(elapsed) / float64(time.Millisecond), "tcp_CloseNow_cleanup_ns": elapsed.Nanoseconds(), "tcp_cleanup_limit_ns": tcpCleanupLimit.Nanoseconds(), "tcp_close_start_ns": cancelStartNS, "backend_and_selected_reader_joined": true, "unrelated_B_usable": true, "proper_close_tested": false, "internal_credits_observable": false})
	for _, c := range clients {
		c.conn.CloseNow()
		select {
		case <-c.gate:
		default:
			close(c.gate)
		}
		if e = boundedJoin(c.joined, "client reader"); e != nil {
			return e
		}
	}
	return nil
}
func main() {
	var e error
	if len(os.Args) == 2 && os.Args[1] == "selftest" {
		e = selftest()
	} else if len(os.Args) == 1 {
		e = run()
	} else {
		e = errors.New("usage: probe [selftest]")
	}
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
