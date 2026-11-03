package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
)

const eventWindowBytes = 32 << 10

// eventSpoolReader assembles one WHATWG event at a time. Only normalized data
// fields reach disk; comments/metadata never accumulate. No event prefetch.
type eventSpoolReader struct {
	bridge        *bridge
	reader        *bufio.Reader
	max           int64
	first, skipLF bool
	releaseRead   func()
}

func (b *bridge) newEventSpoolReader(body io.Reader, max int64) (*eventSpoolReader, error) {
	release := b.acquire(eventWindowBytes)
	if release == nil {
		return nil, errWorkBudget
	}
	return &eventSpoolReader{bridge: b, reader: bufio.NewReaderSize(body, eventWindowBytes), max: max, first: true, releaseRead: release}, nil
}

func (p *eventSpoolReader) next(ctx context.Context) (result *chargedSpool, err error) {
	var event *chargedSpool
	var output *bufio.Writer
	var releaseWrite func()
	keep := false
	defer func() {
		if releaseWrite != nil {
			releaseWrite()
		}
		if event != nil && !keep {
			event.release()
		}
	}()
	startData := func() error {
		if event == nil {
			releaseWrite = p.bridge.acquire(eventWindowBytes)
			if releaseWrite == nil {
				return errWorkBudget
			}
			var e error
			event, e = p.bridge.newChargedSpool("bridge-event-*.json")
			if e != nil {
				return e
			}
			output = bufio.NewWriterSize(event, eventWindowBytes)
		} else {
			if e := output.WriteByte('\n'); e != nil {
				return e
			}
		}
		return nil
	}
	if p.first {
		p.first = false
		prefix, _ := p.reader.Peek(3)
		if bytes.Equal(prefix, []byte{0xef, 0xbb, 0xbf}) {
			_, _ = p.reader.Discard(3)
		}
	}
	var raw int64
	var name [4]byte
	nameLen := 0
	lineOpen, colon, data, firstValue := false, false, false, false
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		c, e := p.reader.ReadByte()
		if e != nil {
			// Pending data at EOF is discarded, never dispatched without a blank line.
			if errors.Is(e, io.EOF) {
				return nil, io.EOF
			}
			return nil, e
		}
		if p.skipLF {
			p.skipLF = false
			if c == '\n' {
				continue
			}
		}
		raw++
		if raw > p.max {
			return nil, errTooLarge
		}
		if c == '\r' || c == '\n' {
			p.skipLF = c == '\r'
			if !lineOpen {
				raw = 0
				if event != nil {
					if e := output.Flush(); e != nil {
						return nil, e
					}
					keep = true
					return event, nil
				}
			} else if !colon && nameLen == 4 && name == [4]byte{'d', 'a', 't', 'a'} {
				if e := startData(); e != nil {
					return nil, e
				}
			}
			lineOpen, colon, data, firstValue = false, false, false, false
			nameLen = 0
			name = [4]byte{}
			continue
		}
		lineOpen = true
		if !colon {
			if c == ':' {
				colon = true
				data = nameLen == 4 && name == [4]byte{'d', 'a', 't', 'a'}
				firstValue = true
				if data {
					if e := startData(); e != nil {
						return nil, e
					}
				}
			} else {
				if nameLen < len(name) {
					name[nameLen] = c
				}
				if nameLen <= len(name) {
					nameLen++
				}
			}
			continue
		}
		if firstValue {
			firstValue = false
			if c == ' ' {
				continue
			}
		}
		if data {
			if e := output.WriteByte(c); e != nil {
				return nil, e
			}
		}
	}
}
