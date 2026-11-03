package main

import (
	"errors"
	"io"
	"os"
	"syscall"
)

const spoolPhysicalBase = 4096

// chargedSpool separates physical data/extent overhead from the independent
// Go/FIFO bookkeeping reservation. Neither charge substitutes for the other.
type chargedSpool struct {
	msg                     wsMsgLite
	file                    *os.File
	block, record, reserved int64
}

func (b *bridge) newChargedSpool(prefix string) (*chargedSpool, error) {
	if !b.reserve(requestMetadataBytes + spoolPhysicalBase) {
		return nil, errWorkBudget
	}
	s := &chargedSpool{msg: wsMsgLite{lease: &requestLease{bridge: b, bytes: requestMetadataBytes + spoolPhysicalBase}}}
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
	if fs.Type == 0x2fc12fc1 {
		s.block = 4096
	}
	if s.block <= 0 || s.record <= 0 {
		s.release()
		return nil, errors.New("invalid spool allocation block size")
	}
	file, err := os.CreateTemp(dir, prefix)
	if err != nil {
		s.release()
		return nil, err
	}
	s.file, s.msg.name = file, file.Name()
	return s, nil
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
	n, err := s.file.Write(p)
	s.msg.size += int64(n)
	if n != len(p) && err == nil {
		err = io.ErrShortWrite
	}
	return n, err
}

func (s *chargedSpool) close() error {
	if s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.file = nil
	return err
}

func (s *chargedSpool) release() {
	_ = s.close()
	s.msg.release()
}
