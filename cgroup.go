package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Per-hop cgroup headroom refusal (D4 "per-hop memory charge"). Before a
// turn or upstream event starts spooling, this hop reads its own cgroup v2
// memory.max/memory.current and refuses explicitly unless the step plus the
// 32 MiB margin fits. memory.current already includes bytes in use, so the
// check errs toward refusing; nothing queues and no threshold moves.
const (
	cgroupMarginBytes = 32 << 20
	// A turn's in-memory step: read window, JSON scan window and the D3
	// page-cache bound for one written and one read spool.
	turnMemStep = requestReadBytes + eventScanBytes + 2*pageCacheStep
	// An upstream event's step: read/write windows, scan and page cache.
	eventMemStep = 2*eventWindowBytes + eventScanBytes + 2*pageCacheStep
)

// defaultCgroupDir returns the container's own cgroup v2 mount when it
// exposes a memory limit, or "" (check disabled) otherwise.
func defaultCgroupDir() string {
	const dir = "/sys/fs/cgroup"
	if _, err := os.Stat(filepath.Join(dir, "memory.max")); err != nil {
		return ""
	}
	return dir
}

func readCgroupInt(dir, name string) (int64, bool, error) {
	raw, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return 0, false, err
	}
	v := strings.TrimSpace(string(raw))
	if v == "max" {
		return 0, false, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	return n, err == nil, err
}

// memHeadroom returns errInsufficientMemory when the step cannot fit. An
// unreadable cgroup is refused too: the check never silently passes.
func (b *bridge) memHeadroom(step int64) error {
	dir := b.cfg.cgroupDir
	if dir == "" {
		return nil
	}
	limit, bounded, err := readCgroupInt(dir, "memory.max")
	if err != nil {
		return errInsufficientMemory
	}
	if !bounded {
		return nil
	}
	current, ok, err := readCgroupInt(dir, "memory.current")
	if err != nil || !ok {
		return errInsufficientMemory
	}
	if limit-current < step+cgroupMarginBytes {
		return errInsufficientMemory
	}
	return nil
}
