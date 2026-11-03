package main

import (
	"io"
	"sync"
)

const httpUploadWindowBytes = 2 * (32 << 10)

// Transport may finish closing a request body after Do returns headers. Keep
// its spool/parse leases alive until that writer has relinquished the body.
type ownedRequestBody struct {
	io.Reader
	done          chan struct{}
	once          sync.Once
	releaseUpload func()
}

func (b *ownedRequestBody) Close() error {
	b.once.Do(func() {
		if b.releaseUpload != nil {
			b.releaseUpload()
		}
		close(b.done)
	})
	return nil
}
