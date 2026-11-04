package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
)

const (
	spoolPhysicalBase = 4096
	// oTmpfile is Linux __O_TMPFILE|O_DIRECTORY (identical on amd64/arm64).
	oTmpfile = 0x410000
	// pageCacheStep bounds dirty and cached spool pages per file (D3).
	pageCacheStep  = 8 << 20
	fadvDontNeed   = 4
	tmpfsMagic     = 0x01021994
	ramfsMagic     = 0x858458f6
	zfsMagic       = 0x2fc12fc1
	defaultReserve = 1 << 30
)

// chargedSpool separates physical data/extent overhead from the independent
// Go/FIFO bookkeeping reservation. Neither charge substitutes for the other.
// Every spool is an anonymous O_TMPFILE: its blocks are freed when the last
// descriptor closes, so a crash leaves nothing behind (D3).
type chargedSpool struct {
	msg                     wsMsgLite
	file                    *os.File
	block, record, reserved int64
	reserve, synced         int64
}

func (b *bridge) newChargedSpool() (*chargedSpool, error) {
	if !b.reserve(requestMetadataBytes + spoolPhysicalBase) {
		return nil, errWorkBudget
	}
	s := &chargedSpool{reserve: b.cfg.diskReserveBytes, msg: wsMsgLite{lease: &requestLease{bridge: b, bytes: requestMetadataBytes + spoolPhysicalBase}}}
	dir := b.cfg.spoolDir
	if dir == "" {
		dir = os.TempDir()
	}
	var fs syscall.Statfs_t
	if err := syscall.Statfs(dir, &fs); err != nil {
		s.release()
		return nil, err
	}
	s.block, s.record = int64(fs.Bsize), int64(fs.Bsize)
	if fs.Type == zfsMagic {
		s.block = 4096
	}
	if s.block <= 0 || s.record <= 0 {
		s.release()
		return nil, errors.New("invalid spool allocation block size")
	}
	file, err := openTmpfile(dir)
	if err != nil {
		s.release()
		return nil, err
	}
	s.file, s.msg.file = file, file
	return s, nil
}

func openTmpfile(dir string) (*os.File, error) {
	// os.OpenFile adds O_CLOEXEC; mode 0600 applies if the file is ever linked.
	return os.OpenFile(dir, os.O_RDWR|oTmpfile, 0o600)
}

func (s *chargedSpool) Write(p []byte) (int, error) {
	end := s.msg.size + int64(len(p))
	data := s.block
	for data < end {
		data *= 2
	}
	// One block per record plus a separately reserved file base covers data
	// extent metadata; the Go/FIFO metadata charge remains wholly independent.
	physical := data + ((data+s.record-1)/s.record)*s.block
	if !s.msg.lease.grow(physical - s.reserved) {
		return 0, errWorkBudget
	}
	s.reserved = physical
	if s.reserve > 0 {
		var fs syscall.Statfs_t
		if err := syscall.Fstatfs(int(s.file.Fd()), &fs); err != nil {
			return 0, err
		}
		if int64(fs.Bavail)*int64(fs.Frsize)-int64(len(p)) < s.reserve {
			return 0, errInsufficientStorage
		}
	}
	n, err := s.file.Write(p)
	s.msg.size += int64(n)
	if n != len(p) && err == nil {
		err = io.ErrShortWrite
	}
	if err == nil && s.msg.size-s.synced >= pageCacheStep {
		err = s.dropWritten()
	}
	return n, err
}

// dropWritten flushes the written range and drops its pages, bounding the
// spool's page cache in this cgroup to about one step (D3).
func (s *chargedSpool) dropWritten() error {
	if err := syscall.Fdatasync(int(s.file.Fd())); err != nil {
		return err
	}
	dropCache(s.file)
	s.synced = s.msg.size
	return nil
}

// dropCache advises the kernel to drop the whole file's clean pages.
func dropCache(f *os.File) {
	_, _, _ = syscall.Syscall6(syscall.SYS_FADVISE64, f.Fd(), 0, 0, fadvDontNeed, 0, 0)
}

// cacheDroppingReader drops a spool's clean pages after each step of reads
// (uploads, event copies), so readers do not refill the cgroup page cache.
type cacheDroppingReader struct {
	r    io.Reader
	f    *os.File
	read int64
}

func (c *cacheDroppingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.read += int64(n)
	if c.read >= pageCacheStep || err != nil {
		dropCache(c.f)
		c.read = 0
	}
	return n, err
}

func (s *chargedSpool) close() error {
	if s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.file, s.msg.file = nil, nil
	return err
}

func (s *chargedSpool) release() {
	_ = s.close()
	s.msg.release()
}

// checkSpoolDir enforces D3 at startup: a private, disk-backed directory that
// supports anonymous files and holds no named file.
func checkSpoolDir(dir string) error {
	st, err := os.Stat(dir)
	if err != nil || !st.IsDir() {
		return fmt.Errorf("SPOOL_DIR %q is not a usable directory", dir)
	}
	if st.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("SPOOL_DIR %q has mode %o; spool directories must be 0700", dir, st.Mode().Perm())
	}
	var fs syscall.Statfs_t
	if err := syscall.Statfs(dir, &fs); err != nil {
		return fmt.Errorf("SPOOL_DIR %q statfs: %w", dir, err)
	}
	if fs.Type == tmpfsMagic || fs.Type == ramfsMagic {
		return fmt.Errorf("SPOOL_DIR %q is memory-backed (tmpfs/ramfs); spools must be on disk", dir)
	}
	f, err := openTmpfile(dir)
	if err != nil {
		return fmt.Errorf("SPOOL_DIR %q does not support O_TMPFILE: %w", dir, err)
	}
	_ = f.Close()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("SPOOL_DIR %q: %w", dir, err)
	}
	for _, e := range entries {
		if e.Type().IsRegular() {
			return fmt.Errorf("SPOOL_DIR %q holds a named file %q; spools are anonymous, remove it after inspection", dir, e.Name())
		}
	}
	return nil
}
