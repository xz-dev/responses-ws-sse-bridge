package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/coder/websocket"
)

// Covers the scanner's bounded sliding window, temporary fill/copy buffers,
// 10,000-level standard JSON nesting stack, and small routing-key decode.
const eventScanBytes = 384 << 10

func scanWorkingBytes(size int64) int64 {
	if size < 128<<10 {
		return 2*(size+1) + requestMetadataBytes
	}
	return eventScanBytes
}

type eventMember struct {
	keyStart, keyEnd, start, end int64
	key                          string
}

type utf8EventReader struct {
	r   io.Reader
	utf utf8Validator
}

func (r *utf8EventReader) Read(p []byte) (int, error) {
	n, e := r.r.Read(p)
	_, _ = r.utf.Write(p[:n])
	return n, e
}

// walkEventObject validates the complete object, including ignored values,
// without retaining their bytes. Duplicate members retain ordinary JSON
// last-value semantics in the caller; only recognized small keys are decoded.
func walkEventObject(src io.ReaderAt, start, size int64, visit func(eventMember) error) error {
	checked := &utf8EventReader{r: io.NewSectionReader(src, start, size)}
	s := &streamScan{r: checked, buf: make([]byte, 0, 128<<10), readChunk: make([]byte, 16<<10), window: 64 << 10, depthLimit: 9999}
	p, e := s.skipWSPos(0)
	if e != nil {
		return e
	}
	c, ok, e := s.byteAt(p)
	if e != nil || !ok || c != '{' {
		return errMalformed
	}
	p++
	first := true
	for {
		p, e = s.skipWSPos(p)
		if e != nil {
			return e
		}
		c, ok, e = s.byteAt(p)
		if e != nil || !ok {
			return errMalformed
		}
		if c == '}' {
			p++
			break
		}
		if !first {
			if c != ',' {
				return errMalformed
			}
			p++
			p, e = s.skipWSPos(p)
			if e != nil {
				return e
			}
			c, ok, e = s.byteAt(p)
			if e != nil || !ok || c == '}' {
				return errMalformed
			}
		}
		first = false
		if c != '"' {
			return errMalformed
		}
		keyStart := p
		keyEnd, _, e := s.scanStringSpan(p, false)
		if e != nil {
			return e
		}
		var key string
		if keyEnd-keyStart <= 128 {
			var raw [128]byte
			if _, e := src.ReadAt(raw[:keyEnd-keyStart], start+keyStart); e != nil {
				return e
			}
			if e := json.Unmarshal(raw[:keyEnd-keyStart], &key); e != nil {
				return e
			}
		}
		p, e = s.skipWSPos(keyEnd)
		if e != nil {
			return e
		}
		c, ok, e = s.byteAt(p)
		if e != nil || !ok || c != ':' {
			return errMalformed
		}
		valueStart, e := s.skipWSPos(p + 1)
		if e != nil {
			return e
		}
		p, e = s.skipValueSpan(valueStart)
		if e != nil {
			return e
		}
		if e := visit(eventMember{keyStart: start + keyStart, keyEnd: start + keyEnd, start: start + valueStart, end: start + p, key: key}); e != nil {
			return e
		}
		s.compact(p)
	}
	p, e = s.skipWSPos(p)
	if e != nil {
		return e
	}
	if _, ok, e := s.byteAt(p); e != nil {
		return e
	} else if ok {
		return errMalformed
	}
	if !checked.utf.valid() {
		return errMalformed
	}
	return nil
}

func smallEventString(src io.ReaderAt, m eventMember) string {
	if m.end-m.start > 256 || m.end <= m.start {
		return ""
	}
	var raw [256]byte
	if _, e := src.ReadAt(raw[:m.end-m.start], m.start); e != nil {
		return ""
	}
	var value string
	_ = json.Unmarshal(raw[:m.end-m.start], &value)
	return value
}

type eventInfo struct {
	typ, id           string
	seq               int64
	nativeError, done bool
	releaseID         func()
}

func (b *bridge) inspectEvent(event *chargedSpool) (info eventInfo, err error) {
	info.releaseID = func() {}
	if event.msg.size == 6 {
		var raw [6]byte
		if _, e := event.file.ReadAt(raw[:], 0); e == nil && bytes.Equal(raw[:], []byte("[DONE]")) {
			info.done = true
			return info, nil
		}
	}
	var response eventMember
	err = walkEventObject(event.file, 0, event.msg.size, func(m eventMember) error {
		switch m.key {
		case "type":
			info.typ = smallEventString(event.file, m)
		case "sequence_number":
			info.seq = 0
			if m.end-m.start <= 32 {
				var raw [32]byte
				if _, e := event.file.ReadAt(raw[:m.end-m.start], m.start); e != nil {
					return e
				}
				_ = json.Unmarshal(raw[:m.end-m.start], &info.seq)
			}
		case "response":
			response = m
		case "error":
			info.nativeError = true
		}
		return nil
	})
	if err != nil || response.end == 0 {
		return info, err
	}
	var id eventMember
	badID := false
	// A non-object response has no usable response.id, just as before.
	e := walkEventObject(event.file, response.start, response.end-response.start, func(m eventMember) error {
		if !strings.EqualFold(m.key, "id") {
			return nil
		}
		var first [1]byte
		if _, e := event.file.ReadAt(first[:], m.start); e != nil {
			return e
		}
		switch first[0] {
		case '"':
			id = m
		case 'n':
		default:
			badID = true
		}
		return nil
	})
	if e != nil || badID || id.end == 0 {
		return info, nil
	}
	n := id.end - id.start
	release := b.acquire(3*n + requestMetadataBytes)
	if release == nil {
		return info, errWorkBudget
	}
	raw := make([]byte, int(n))
	if _, e := event.file.ReadAt(raw, id.start); e != nil {
		release()
		return info, e
	}
	if e := json.Unmarshal(raw, &info.id); e != nil {
		release()
		info.id = ""
		return info, nil
	}
	// Only the decoded id survives this call; input/decode copies no longer do.
	retained := int64(len(info.id)) + requestMetadataBytes
	b.work.Add(-(3*n + requestMetadataBytes - retained))
	info.releaseID = func() { b.work.Add(-retained) }
	return info, nil
}

func copyEventSpan(dst io.Writer, src io.ReaderAt, start, end int64, buf []byte) error {
	_, e := io.CopyBuffer(dst, io.NewSectionReader(src, start, end-start), buf)
	return e
}

func (b *bridge) writeEvent(ctx context.Context, conn *websocket.Conn, event *chargedSpool, flatError bool) error {
	release := b.acquire(eventWindowBytes)
	if release == nil {
		return errWorkBudget
	}
	defer release()
	buf := make([]byte, eventWindowBytes)
	writer, e := conn.Writer(ctx, websocket.MessageText)
	if e != nil {
		return e
	}
	if !flatError {
		if e := copyEventSpan(writer, event.file, 0, event.msg.size, buf); e != nil {
			return e
		}
	} else {
		fields := make(map[string]*eventMember)
		var retained int64
		defer func() { b.work.Add(-retained) }()
		e = walkEventObject(event.file, 0, event.msg.size, func(m eventMember) error {
			if m.key == "type" || m.key == "sequence_number" {
				return nil
			}
			n := m.keyEnd - m.keyStart
			charge := 3*n + 512
			if !b.reserve(charge) {
				return errWorkBudget
			}
			retained += charge
			raw := make([]byte, int(n))
			if _, e := event.file.ReadAt(raw, m.keyStart); e != nil {
				return e
			}
			var key string
			if e := json.Unmarshal(raw, &key); e != nil {
				return e
			}
			if entry, exists := fields[key]; exists {
				// Updating through the existing key leaves no reference to the
				// new temporary decoded key; duplicate fields remain last-wins.
				*entry = m
				b.work.Add(-charge)
				retained -= charge
			} else {
				fields[key] = &m
				keep := int64(len(key)) + 512
				b.work.Add(-(charge - keep))
				retained -= charge - keep
			}
			return nil
		})
		if e != nil {
			return e
		}
		if _, e := io.WriteString(writer, `{"type":"error","status":400,"error":{"type":"invalid_request_error"`); e != nil {
			return e
		}
		keys := make([]string, 0, len(fields))
		for key := range fields {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			m := fields[key]
			if _, e := io.WriteString(writer, ","); e != nil {
				return e
			}
			if e := copyEventSpan(writer, event.file, m.keyStart, m.keyEnd, buf); e != nil {
				return e
			}
			if _, e := io.WriteString(writer, ":"); e != nil {
				return e
			}
			if e := copyEventSpan(writer, event.file, m.start, m.end, buf); e != nil {
				return e
			}
		}
		if _, e := io.WriteString(writer, "}}"); e != nil {
			return e
		}
	}
	return writer.Close()
}

func (b *bridge) finishEventErr(ctx context.Context, conn *websocket.Conn, started, terminal bool, id string, sequence *int64, cause error, send func(context.Context, *websocket.Conn, *apiError)) error {
	// JSON escaping can expand an arbitrary retained id; reserve encoder and
	// result copies from actual id bytes before constructing the failure frame.
	release := b.acquire(20*int64(len(id)) + 8192)
	if release == nil {
		return errWorkBudget
	}
	defer release()
	return finishErr(ctx, conn, started, terminal, id, sequence, cause, send)
}

func (b *bridge) relaySpooledSSE(ctx context.Context, conn *websocket.Conn, body io.Reader) error {
	reader, e := b.newEventSpoolReader(body, b.cfg.maxBodyBytes)
	if e != nil {
		return e
	}
	defer reader.releaseRead()
	var started bool
	var responseID string
	var releaseID func()
	defer func() {
		if releaseID != nil {
			releaseID()
		}
	}()
	var sequence int64
	for {
		event, e := reader.next(ctx)
		if e != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(e, errWorkBudget) {
				return e
			}
			cause := errUpstreamStream
			if e == io.EOF {
				cause = errTruncated
			}
			return b.finishEventErr(ctx, conn, started, false, responseID, &sequence, cause, b.sendErrorFrame)
		}
		scanRelease := b.acquire(eventScanBytes)
		if scanRelease == nil {
			event.release()
			return errWorkBudget
		}
		info, e := b.inspectEvent(event)
		if e != nil {
			scanRelease()
			event.release()
			if errors.Is(e, errWorkBudget) {
				return e
			}
			return b.finishEventErr(ctx, conn, started, false, responseID, &sequence, errMalformedEvent, b.sendErrorFrame)
		}
		if info.seq > 0 {
			sequence = info.seq
		}
		if info.id != "" {
			if releaseID != nil {
				releaseID()
			}
			responseID = info.id
			releaseID = info.releaseID
		} else {
			info.releaseID()
		}
		e = b.writeEvent(ctx, conn, event, info.typ == "error" && !info.nativeError)
		scanRelease()
		event.release()
		if e != nil {
			return fmt.Errorf("ws event write: %w", e)
		}
		if info.done || info.typ == "error" {
			return nil
		}
		switch info.typ {
		case "response.created", "response.in_progress":
			started = true
		case "response.completed", "response.incomplete", "response.failed":
			return nil
		}
	}
}
