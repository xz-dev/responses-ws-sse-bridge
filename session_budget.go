package main

import "net/http"

// Per upgraded session: the library's 4-KiB read/write buffers, the active
// upstream HTTP read/write windows, connection/FIFO/control state and the two
// initial goroutine stacks and the reader's drain buffer. Growing request/event storage has separate leases.
// Kernel TCP buffers and pre-upgrade HTTP parsing remain runtime/OS resources,
// not falsely represented as bytes of application heap by this reservation.
const (
	sessionBaseBytes = 36 << 10
	drainBytes       = 4 << 10
)

func sessionWorkingBytes(r *http.Request) int64 {
	n := int64(sessionBaseBytes + 4*(len(r.URL.Path)+len(r.URL.RawQuery)+len(r.Host)))
	for name, values := range r.Header {
		n += int64(4*len(name) + 256)
		for _, value := range values {
			n += int64(4*len(value) + 128)
		}
	}
	return n
}
