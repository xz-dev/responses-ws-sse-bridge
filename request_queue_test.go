package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/url"
	"os"
	"path/filepath"
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
	files, err := filepath.Glob(filepath.Join(b.cfg.spoolDir, "bridge-turn-*"))
	if err != nil || len(files) != 0 {
		t.Fatalf("spool files leaked: %v (%v)", files, err)
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
	got, err := os.ReadFile(msg.name)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("positive control changed bytes: %v", err)
	}
	stat, err := os.Stat(msg.name)
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
	if len(after) > len(before)+1 {
		t.Fatalf("queued spools retained descriptors: %d -> %d", len(before), len(after))
	}
	for i := 0; i < 20; i++ {
		msg, ok := q.pop(context.Background())
		if !ok {
			t.Fatal("FIFO dropped accepted create")
		}
		data, err := os.ReadFile(msg.name)
		if err != nil || !bytes.Equal(data, []byte{byte(i)}) {
			t.Fatalf("FIFO ordering/content: %d %v %v", i, data, err)
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
