package main

import (
	"context"
	"errors"
	"sync"
)

var (
	errWorkBudget = errors.New("memory budget exhausted")
	errDiskBudget = errors.New("spool disk budget exhausted")
)

const (
	requestReadBytes = 32 << 10
	// Charge each file/FIFO node separately from its allocated data blocks.
	// This is conservative bookkeeping, not a filesystem-wide metadata quota.
	requestMetadataBytes = 4096
)

// requestLease holds bytes of the memory budget, or of the disk budget
// when disk is set.
type requestLease struct {
	bridge *bridge
	bytes  int64
	disk   bool
	once   sync.Once
}

func (l *requestLease) grow(n int64) bool {
	ok := l.bridge.reserve
	if l.disk {
		ok = l.bridge.reserveDisk
	}
	if !ok(n) {
		return false
	}
	l.bytes += n
	return true
}

func (l *requestLease) release() {
	l.once.Do(func() {
		if l.disk {
			l.bridge.disk.Add(-l.bytes)
		} else {
			l.bridge.work.Add(-l.bytes)
		}
	})
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
