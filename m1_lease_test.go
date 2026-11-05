package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"

	"github.com/coder/websocket"
)

// exclusiveLockable reports whether keeper's release waiter could take
// LOCK_EX on the ticket now, i.e. no hop holds LOCK_SH.
func exclusiveLockable(t *testing.T, path string) bool {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) == nil
}

func ticketFrame(id string) string {
	return `{"` + ticketMember + `":"` + id + `","type":"response.create","model":"m","input":[]}`
}

// Review B1 probe as a regression: while turn 1 is in flight, a ticketed
// turn 2 is spooled and queued. Its LOCK_SH must be held while it is queued,
// and when turn 1 fails the queued spool must be closed and the lease
// dropped before the client receives the close frame.
func TestM1QueuedTicketedTurnLeasedUntilFreed(t *testing.T) {
	const tb = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	ledger := t.TempDir()
	pathB := filepath.Join(ledger, tb+ticketSuffix)
	if err := os.WriteFile(pathB, nil, 0o640); err != nil {
		t.Fatal(err)
	}
	spool := t.TempDir()
	proceed := make(chan struct{})
	up := newSSEUpstream(t)
	up.sse = func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_a\"}}\n\n")
		w.(http.Flusher).Flush()
		<-proceed
		fmt.Fprint(w, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"x\"}\n\n")
	}
	p := startPair(t, up, func(c *config) { c.ledgerDir, c.spoolDir = ledger, spool })
	defer p.close()
	// A raw client that never answers the close frame: the bridge's close
	// handshake then stays open while the test inspects spools and locks.
	c, br := rawDial(t, p)
	defer c.Close()
	if err := regressMaskedFrame(c, true, 1, []byte(`{"type":"response.create","model":"m","input":[]}`)); err != nil {
		t.Fatal(err)
	}
	if op, ev, err := regressRawRead(br); err != nil || op != 1 || !strings.Contains(string(ev), "response.created") {
		t.Fatalf("turn 1 first event: %d %s %v", op, ev, err)
	}
	if err := regressMaskedFrame(c, true, 1, []byte(ticketFrame(tb))); err != nil {
		t.Fatal(err)
	}
	for len(openSpools(t, spool)) < 2 { // turn 1's spool and the queued turn 2
		runtime.Gosched()
	}
	if exclusiveLockable(t, pathB) {
		t.Fatal("queued ticketed spool is open without LOCK_SH on its ticket")
	}
	// Turn 1 fails: its next event cannot be charged to the memory budget.
	p.bridge.work.Add(p.bridge.cfg.workBytes)
	defer p.bridge.work.Add(-p.bridge.cfg.workBytes)
	close(proceed)
	if op, ev, err := regressRawRead(br); err != nil || op != 1 || !strings.Contains(string(ev), "response.failed") {
		t.Fatalf("turn 1: want response.failed, got %d %s %v", op, ev, err)
	}
	if op, _, err := regressRawRead(br); err != nil || op != 8 {
		t.Fatalf("want close frame, got %d %v", op, err)
	}
	if n := len(openSpools(t, spool)); n != 0 || !exclusiveLockable(t, pathB) {
		t.Fatalf("at the close frame: %d spools open, ticket B lockable=%v", n, exclusiveLockable(t, pathB))
	}
}

func rawDial(t *testing.T, p *pair) (net.Conn, *bufio.Reader) {
	t.Helper()
	addr := strings.TrimPrefix(p.bSrv.URL, "http://")
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintf(c, "GET /v1/responses HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n", addr)
	br := bufio.NewReader(c)
	if resp, err := http.ReadResponse(br, nil); err != nil || resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("handshake: %v %v", resp, err)
	}
	return c, br
}

// D4: the reader takes LOCK_SH from the frame prefix before it spools any
// frame byte. While keeper holds LOCK_EX the frame is not spooled at all.
func TestM1TicketLeasedBeforeSpooling(t *testing.T) {
	ledger := ledgerWithTicket(t)
	path := filepath.Join(ledger, testTicket+ticketSuffix)
	keeper, _ := os.Open(path)
	defer keeper.Close()
	if err := syscall.Flock(int(keeper.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	spool := t.TempDir()
	p := startPair(t, newSSEUpstream(t), func(c *config) { c.ledgerDir, c.spoolDir = ledger, spool })
	defer p.close()
	conn := dial(t, p, "/v1/responses", nil)
	defer conn.CloseNow()
	writeFrame(t, conn, ticketFrame(testTicket))
	for !flockWaiting(t, path) {
		runtime.Gosched()
	}
	if n := len(openSpools(t, spool)); n != 0 {
		t.Fatalf("%d spools open before the lease was granted", n)
	}
	_ = os.Remove(path) // keeper releases the ticket while the bridge waits
	_ = syscall.Flock(int(keeper.Fd()), syscall.LOCK_UN)
	requireErrorEvent(t, readEvent(t, conn), "capacity_ticket_invalid")
}

// A ticket member that is not the first member is refused explicitly.
func TestM1TicketNotFirstMemberRefused(t *testing.T) {
	ledger := ledgerWithTicket(t)
	p := startPair(t, newSSEUpstream(t), func(c *config) { c.ledgerDir = ledger })
	defer p.close()
	conn := dial(t, p, "/v1/responses", nil)
	defer conn.CloseNow()
	writeFrame(t, conn, `{"type":"response.create","`+ticketMember+`":"`+testTicket+`","input":[]}`)
	requireErrorEvent(t, readEvent(t, conn), "capacity_ticket_invalid")
	readCloseAfter(t, conn, websocket.StatusInternalError)
}

// countingReader counts the bytes the bridge pulled from the client.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// A frame without a usable ticket is refused as it crosses 64 MiB, before
// that much is spooled (review B1), and fails closed with no ledger (M3).
func TestM1UnticketedFrameRefusedAt64MiB(t *testing.T) {
	big := func(prefix string) *countingReader {
		body := io.LimitReader(neverEnding('x'), 80<<20)
		return &countingReader{r: io.MultiReader(strings.NewReader(prefix), body)}
	}
	for _, tc := range []struct{ name, prefix, code string }{
		{"no ticket", `{"type":"response.create","input":"`, "capacity_ticket_missing"},
		{"no ledger", `{"` + ticketMember + `":"` + testTicket + `","input":"`, "capacity_ticket_invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := requestTestBridge(t, 1<<20)
			b.cfg.maxMsgBytes, b.cfg.diskBytes = 256<<20, 1<<30
			r := big(tc.prefix)
			msg := b.spoolFrame(r)
			if e := b.failureAPIError(msg.err); e.Code != tc.code || e.Hop != bridgeHop {
				t.Fatalf("got %v %+v", msg.err, e)
			}
			if r.n >= ticketThreshold+requestReadBytes {
				t.Fatalf("read %d bytes before refusing", r.n)
			}
			msg.release()
			assertRequestResourcesZero(t, b)
		})
	}
}

type neverEnding byte

func (b neverEnding) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = byte(b)
	}
	return len(p), nil
}
