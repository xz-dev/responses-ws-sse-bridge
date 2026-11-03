package main

import (
	"context"
	"errors"
	"sync"
)

var errWorkBudget = errors.New("request spool working-byte budget exhausted")

const (
	requestReadBytes = 32 << 10
	// Charge each file/FIFO node separately from its allocated data blocks.
	// This is conservative bookkeeping, not a filesystem-wide metadata quota.
	requestMetadataBytes = 4096
)

type requestLease struct {
	bridge *bridge
	bytes  int64
	once   sync.Once
}

func (l *requestLease) grow(n int64) bool {
	if !l.bridge.reserve(n) {
		return false
	}
	l.bytes += n
	return true
}

func (l *requestLease) release() {
	l.once.Do(func() { l.bridge.work.Add(-l.bytes) })
}

// requestFIFO holds only closed spools. Its nodes are charged to their spool
// leases; only the active reader and worker keep file descriptors open.
// No network IO, allocation, or file cleanup occurs under this local lock.
type requestFIFO struct {
	mu     sync.Mutex
	head   *wsMsgLite
	tail   *wsMsgLite
	closed bool
	wake   chan struct{}
}

func newRequestFIFO() *requestFIFO { return &requestFIFO{wake: make(chan struct{}, 1)} }

func (q *requestFIFO) push(msg *wsMsgLite) bool {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return false
	}
	if q.tail == nil {
		q.head = msg
	} else {
		q.tail.next = msg
	}
	q.tail = msg
	q.mu.Unlock()
	select {
	case q.wake <- struct{}{}:
	default:
	}
	return true
}

func (q *requestFIFO) pop(ctx context.Context) (*wsMsgLite, bool) {
	for {
		if ctx.Err() != nil {
			return nil, false
		}
		q.mu.Lock()
		if q.head != nil {
			msg := q.head
			q.head = msg.next
			msg.next = nil
			if q.head == nil {
				q.tail = nil
			}
			q.mu.Unlock()
			return msg, true
		}
		closed := q.closed
		q.mu.Unlock()
		if closed {
			return nil, false
		}
		select {
		case <-ctx.Done():
			return nil, false
		case <-q.wake:
		}
	}
}

func (q *requestFIFO) close() {
	q.mu.Lock()
	q.closed = true
	msg := q.head
	q.head, q.tail = nil, nil
	q.mu.Unlock()
	for msg != nil {
		next := msg.next
		msg.next = nil
		msg.release()
		msg = next
	}
	select {
	case q.wake <- struct{}{}:
	default:
	}
}
