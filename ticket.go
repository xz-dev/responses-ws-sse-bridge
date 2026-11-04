package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"syscall"
)

// Admission ticket lease (large-request-capacity D4). Keeper alone writes the
// ledger; this hop mounts it read-only and holds a blocking shared flock on
// the ticket for the whole handler lifetime (spool and memory included), so
// keeper's release waiter cannot free the reservation while bytes are live.
const (
	ticketHeader    = "X-AI-Gateway-Capacity-Ticket"
	ticketMember    = "ai_gateway_capacity_ticket"
	ticketThreshold = 64 << 20 // bodies at or above this size need a ticket
	ticketIDLen     = 32       // 128-bit id, lowercase hex; the file name in the ledger
)

type ticketError struct {
	code, msg string
}

func (e *ticketError) Error() string { return e.msg }

func (e *ticketError) apiError() *apiError {
	return &apiError{Status: http.StatusServiceUnavailable, Type: "server_error", Code: e.code, Hop: bridgeHop, Message: e.msg}
}

func ticketMissing(size int64) error {
	return &ticketError{"capacity_ticket_missing", fmt.Sprintf("%s received %d bytes (>= %d) without a capacity ticket", bridgeHop, size, int64(ticketThreshold))}
}

func ticketInvalid(why string) error {
	return &ticketError{"capacity_ticket_invalid", fmt.Sprintf("%s capacity ticket invalid: %s", bridgeHop, why)}
}

type ticketLease struct{ f *os.File }

func (l *ticketLease) release() {
	if l != nil && l.f != nil {
		_ = l.f.Close() // closing the descriptor drops the shared flock
		l.f = nil
	}
}

func validTicketID(id string) bool {
	if len(id) != ticketIDLen {
		return false
	}
	for _, c := range id {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// admitTicket returns the lease this hop must hold for a request of size
// bytes carrying id. With no ledger configured the gate is off (nil, nil).
func (b *bridge) admitTicket(size int64, id string) (*ticketLease, error) {
	if b.cfg.ledgerDir == "" {
		return nil, nil
	}
	if id == "" {
		if size >= ticketThreshold {
			return nil, ticketMissing(size)
		}
		return nil, nil
	}
	if !validTicketID(id) {
		return nil, ticketInvalid("malformed id")
	}
	f, err := os.Open(filepath.Join(b.cfg.ledgerDir, id)) // O_RDONLY|O_CLOEXEC
	if err != nil {
		return nil, ticketInvalid("cannot open ticket")
	}
	for {
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_SH) // blocking; no timer
		if !errors.Is(err, syscall.EINTR) {
			break
		}
	}
	if err != nil {
		_ = f.Close()
		return nil, ticketInvalid("cannot lock ticket")
	}
	var st syscall.Stat_t
	if err := syscall.Fstat(int(f.Fd()), &st); err != nil || st.Nlink == 0 {
		_ = f.Close() // released while we waited: no reservation backs us
		return nil, ticketInvalid("ticket already released")
	}
	return &ticketLease{f: f}, nil
}

// frameTicket decodes the ticket member of a turn frame (a short JSON string).
// More than one ticket member, or a non-string value, is invalid.
func frameTicket(src io.ReaderAt, info frameInfo) (string, error) {
	var id string
	seen := false
	for _, m := range info.members {
		if m.key != ticketMember {
			continue
		}
		if seen {
			return "", ticketInvalid("duplicate ticket member")
		}
		seen = true
		n := m.vend - m.vstart
		if n <= 0 || n > 2*ticketIDLen+2 {
			return "", ticketInvalid("ticket member is not a ticket id")
		}
		raw := make([]byte, n)
		if _, err := src.ReadAt(raw, m.vstart); err != nil {
			return "", err
		}
		if err := json.Unmarshal(raw, &id); err != nil {
			return "", ticketInvalid("ticket member is not a string")
		}
	}
	return id, nil
}
