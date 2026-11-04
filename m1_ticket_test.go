package main

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"

	"github.com/coder/websocket"
)

const testTicket = "0123456789abcdef0123456789abcdef"

func ledgerWithTicket(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, testTicket), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

// D4: the ticket member is stripped from the body and forwarded to Sub2API
// only as the header; the shared lock is held while the turn is live.
func TestM1TicketStrippedForwardedAndLocked(t *testing.T) {
	ledger := ledgerWithTicket(t)
	locked := make(chan bool, 1)
	up := newSSEUpstream(t)
	up.sse = func(w http.ResponseWriter, r *http.Request) {
		f, err := os.Open(filepath.Join(ledger, testTicket))
		if err == nil {
			// Keeper's release waiter takes LOCK_EX; it must not succeed now.
			locked <- syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil
			f.Close()
		}
		defaultSSE(w, r)
	}
	p := startPair(t, up, func(c *config) { c.ledgerDir = ledger })
	defer p.close()
	conn := dial(t, p, "/v1/responses", http.Header{ticketHeader: {"ffffffffffffffffffffffffffffffff"}})
	defer conn.CloseNow()
	writeFrame(t, conn, `{"type":"response.create","model":"m","`+ticketMember+`":"`+testTicket+`","input":[]}`)
	for _, want := range []string{"response.created", "response.output_text.delta", "response.completed"} {
		if got := readEvent(t, conn)["type"]; got != want {
			t.Fatalf("got %v want %s", got, want)
		}
	}
	if !<-locked {
		t.Fatal("ticket not held under LOCK_SH while the upstream exchange ran")
	}
	up.mu.Lock()
	body, hdr := up.bodies[0], up.headers[0]
	up.mu.Unlock()
	if bytes.Contains(body, []byte(ticketMember)) {
		t.Fatalf("ticket member reached the upstream body: %s", body)
	}
	if hdr.Get(ticketHeader) != testTicket {
		t.Fatalf("ticket header = %q", hdr.Get(ticketHeader))
	}
	// After the turn the lock is dropped, so keeper's blocking LOCK_EX is
	// granted (a leaked lease hangs here until the go test timeout).
	f, _ := os.Open(filepath.Join(ledger, testTicket))
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
}

// flockWaiting reports whether /proc/locks lists a blocked FLOCK waiter on
// the inode of path (a " -> " line), i.e. someone is inside flock(2).
func flockWaiting(t *testing.T, path string) bool {
	t.Helper()
	var st syscall.Stat_t
	if err := syscall.Stat(path, &st); err != nil {
		t.Fatal(err)
	}
	locks, err := os.ReadFile("/proc/locks")
	if err != nil {
		t.Fatal(err)
	}
	suffix := fmt.Sprintf(":%d ", st.Ino)
	for _, l := range strings.Split(string(locks), "\n") {
		if strings.Contains(l, "-> FLOCK") && strings.Contains(l, suffix) {
			return true
		}
	}
	return false
}

func TestM1TicketMissingAndReleased(t *testing.T) {
	ledger := ledgerWithTicket(t)
	b := requestTestBridge(t, 1<<20)
	b.cfg.ledgerDir = ledger
	if l, err := b.admitTicket(1024, ""); l != nil || err != nil {
		t.Fatalf("small request without ticket: %v %v", l, err)
	}
	_, err := b.admitTicket(ticketThreshold, "")
	if e := b.failureAPIError(err); e.Status != 503 || e.Code != "capacity_ticket_missing" {
		t.Fatalf("missing ticket: %+v", e)
	}
	if _, err := b.admitTicket(1, "../etc/passwd"); err == nil {
		t.Fatal("path-like ticket accepted")
	}
	if err := os.Remove(filepath.Join(ledger, testTicket)); err != nil {
		t.Fatal(err)
	}
	_, err = b.admitTicket(ticketThreshold, testTicket)
	if e := b.failureAPIError(err); e.Code != "capacity_ticket_invalid" {
		t.Fatalf("released ticket: %+v", e)
	}
}

// st_nlink: a ticket unlinked while this hop waited for the lock is refused.
func TestM1TicketUnlinkedWhileWaiting(t *testing.T) {
	ledger := ledgerWithTicket(t)
	b := requestTestBridge(t, 1<<20)
	b.cfg.ledgerDir = ledger
	path := filepath.Join(ledger, testTicket)
	keeper, _ := os.Open(path)
	defer keeper.Close()
	if err := syscall.Flock(int(keeper.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := b.admitTicket(ticketThreshold, testTicket); done <- err }()
	for !flockWaiting(t, path) { // admitTicket has opened the file and blocks in flock
		runtime.Gosched()
	}
	_ = os.Remove(path)
	_ = syscall.Flock(int(keeper.Fd()), syscall.LOCK_UN)
	if err := <-done; err == nil || !strings.Contains(err.Error(), "already released") {
		t.Fatalf("want released refusal, got %v", err)
	}
}

func TestM1CgroupHeadroomRefusal(t *testing.T) {
	cg := t.TempDir()
	b := requestTestBridge(t, 1<<20)
	b.cfg.cgroupDir = cg
	write := func(max, cur string) {
		_ = os.WriteFile(filepath.Join(cg, "memory.max"), []byte(max+"\n"), 0o600)
		_ = os.WriteFile(filepath.Join(cg, "memory.current"), []byte(cur+"\n"), 0o600)
	}
	write("max", "1")
	if err := b.memHeadroom(turnMemStep); err != nil {
		t.Fatalf("unbounded cgroup refused: %v", err)
	}
	write("268435456", "200000000")
	if err := b.memHeadroom(turnMemStep); err != nil {
		t.Fatalf("ample headroom refused: %v", err)
	}
	write("268435456", "230000000") // 38 MB left < step + 32 MiB
	err := b.memHeadroom(turnMemStep)
	if e := b.failureAPIError(err); e.Status != 503 || e.Code != "insufficient_memory" || e.Hop != bridgeHop {
		t.Fatalf("tight headroom: %v %+v", err, e)
	}
	_ = os.Remove(filepath.Join(cg, "memory.current"))
	if err := b.memHeadroom(turnMemStep); err == nil {
		t.Fatal("unreadable cgroup passed silently")
	}
}

// The WS turn path reports the headroom refusal as an explicit event.
func TestM1CgroupRefusalOnTurnIsExplicit(t *testing.T) {
	cg := t.TempDir()
	_ = os.WriteFile(filepath.Join(cg, "memory.max"), []byte("100000000\n"), 0o600)
	_ = os.WriteFile(filepath.Join(cg, "memory.current"), []byte("99000000\n"), 0o600)
	p := startPair(t, newSSEUpstream(t), func(c *config) { c.cgroupDir = cg })
	defer p.close()
	conn := dial(t, p, "/v1/responses", nil)
	defer conn.CloseNow()
	writeFrame(t, conn, `{"type":"response.create","model":"m","input":[]}`)
	requireErrorEvent(t, readEvent(t, conn), "insufficient_memory")
	readCloseAfter(t, conn, websocket.StatusTryAgainLater)
}
