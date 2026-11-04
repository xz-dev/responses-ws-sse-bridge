package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/url"
	"os"
	"sync"
	"syscall"
	"testing"
)

func requestTestBridge(t *testing.T, budget int64) *bridge {
	t.Helper()
	u, _ := url.Parse("http://127.0.0.1:9")
	return newBridge(config{upstream: u, maxMsgBytes: 1 << 20, workBytes: budget, spoolDir: t.TempDir()})
}

func assertRequestResourcesZero(t *testing.T, b *bridge) {
	t.Helper()
	if got := b.work.Load(); got != 0 {
		t.Fatalf("working bytes leaked: %d", got)
	}
	if open := openSpools(t, b.cfg.spoolDir); len(open) != 0 {
		t.Fatalf("spool files leaked: %d open anonymous spools", len(open))
	}
}

func TestRequestSpoolPositiveAndPhysicalCharge(t *testing.T) {
	b := requestTestBridge(t, 64<<10)
	want := bytes.Repeat([]byte("x"), 1024)
	msg := b.spoolFrame(bytes.NewReader(want))
	defer msg.release()
	if msg.err != nil {
		t.Fatal(msg.err)
	}
	if got := readSpool(t, msg.file); !bytes.Equal(got, want) {
		t.Fatal("positive control changed bytes")
	}
	stat, err := msg.file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	physical := stat.Sys().(*syscall.Stat_t).Blocks * 512
	if b.work.Load() < physical+requestMetadataBytes {
		t.Fatalf("physical blocks/metadata not charged: physical=%d quota=%d", physical, b.work.Load())
	}
	msg.release()
	assertRequestResourcesZero(t, b)
}

func TestRequestFIFOLeasesAndClosedDescriptors(t *testing.T) {
	b := requestTestBridge(t, 1<<20)
	q := newRequestFIFO()
	defer q.close()
	before, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		msg := b.spoolFrame(bytes.NewReader([]byte{byte(i)}))
		if msg.err != nil {
			t.Fatal(msg.err)
		}
		if !q.push(&msg) {
			t.Fatal("unexpected closed queue")
		}
	}
	after, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	// D3: anonymous spools have no name to reopen, so each queued create
	// holds exactly its own descriptor until release.
	if got := len(openSpools(t, b.cfg.spoolDir)); got != 20 || len(after) > len(before)+21 {
		t.Fatalf("queued spools: %d open spools, fds %d -> %d", got, len(before), len(after))
	}
	for i := 0; i < 20; i++ {
		msg, ok := q.pop(context.Background())
		if !ok {
			t.Fatal("FIFO dropped accepted create")
		}
		if data := readSpool(t, msg.file); !bytes.Equal(data, []byte{byte(i)}) {
			t.Fatalf("FIFO ordering/content: %d %v", i, data)
		}
		msg.release()
	}
	q.close()
	assertRequestResourcesZero(t, b)
}

type failedRequestReader struct{}

func (failedRequestReader) Read(p []byte) (int, error) {
	copy(p, "broken")
	return 6, io.ErrUnexpectedEOF
}

func TestRequestSpoolErrorsRelease(t *testing.T) {
	b := requestTestBridge(t, 64<<10)
	msg := b.spoolFrame(failedRequestReader{})
	if !errors.Is(msg.err, io.ErrUnexpectedEOF) {
		t.Fatalf("wrong IO failure: %v", msg.err)
	}
	msg.release()
	assertRequestResourcesZero(t, b)
	msg = b.spoolFrame(bytes.NewReader(bytes.Repeat([]byte("q"), 32<<10)))
	if !errors.Is(msg.err, errWorkBudget) {
		t.Fatalf("expected real quota failure, got %v", msg.err)
	}
	msg.release()
	assertRequestResourcesZero(t, b)
}

func TestRequestFIFOPushCloseRaceReleases(t *testing.T) {
	b := requestTestBridge(t, 1<<20)
	q := newRequestFIFO()
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msg := b.spoolFrame(bytes.NewReader([]byte("queued")))
			if msg.err != nil || !q.push(&msg) {
				msg.release()
			}
		}()
	}
	q.close()
	wg.Wait()
	q.close()
	assertRequestResourcesZero(t, b)
}
