package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
)

// Streaming top-level JSON scanner. A response.create frame is inspected
// only as deep as needed: top-level member keys are decoded (they are
// small), and each member's value is validated byte-by-byte as it streams
// through a fixed window — never materialised. Only the value's byte
// range in the source is recorded, so the rewrite reads it back from the
// disk spool rather than from memory.
//
// object layout: '{' ws member ws (',' ws member ws)* '}'
// member:        string ws ':' ws value

var errMalformed = errors.New("malformed JSON frame")

// frameInfo holds only what routing decisions need from a client frame.
type frameInfo struct {
	typeVal    string // decoded "type"
	prevID     string // decoded "previous_response_id"
	genFalse   bool   // generate === false
	model      string // decoded "model" (only parsed for prewarm echo)
	isCreate   bool
	havePrevID bool
	haveModel  bool

	// Top-level member spans for the streaming rewrite. Members are
	// copied verbatim; we never decode values, so only the decoded key
	// and the value's byte range in the source are recorded.
	members []memberSpan
}

// memberSpan is one top-level member: the decoded key plus the value's
// byte range in the source frame (which may be a disk spool).
type memberSpan struct {
	key          string
	vstart, vend int64 // value byte offsets in the source
}

// streamScan walks a JSON document from an io.Reader in fixed-size chunks,
// tracking absolute byte offsets. It implements a minimal, strictly-valid
// JSON recogniser: enough to (a) reject malformed documents and (b) report
// each top-level member's value byte range and the decoded values of the
// small routing fields (type/model/previous_response_id/generate).
//
// Window discipline: a member VALUE may be arbitrarily large and is scanned
// token-by-token with no retention; a member KEY and the routing-field
// scalar values are the only pieces buffered, capped at maxToken (4 MiB).
// Strings inside composite values are scanned without decoding and are not
// subject to this cap — see scanStringSpan's `decode` argument.
const maxToken = 4 << 20

type streamScan struct {
	r   io.Reader
	buf []byte // sliding window of unprocessed input
	off int64  // absolute offset of buf[0]
	eof bool
	err error
}

func newStreamScan(r io.Reader) *streamScan {
	return &streamScan{r: r, buf: make([]byte, 0, 64<<10)}
}

// abs returns the absolute offset of window index i.
func (s *streamScan) abs(i int) int64 { return s.off + int64(i) }

// fill reads more input until buf reaches `need` bytes total or eof.
// Compaction of the consumed prefix is done separately via compact().
func (s *streamScan) fill(need int) error {
	for len(s.buf) < need && !s.eof {
		chunk := make([]byte, 64<<10)
		n, err := s.r.Read(chunk)
		s.buf = append(s.buf, chunk[:n]...)
		if err == io.EOF {
			s.eof = true
		} else if err != nil {
			return err
		}
	}
	return nil
}

// compact drops buffer bytes before absolute position keep, which is the
// oldest position still referenced by an open member span.
func (s *streamScan) compact(keep int64) {
	n := int(keep - s.off)
	if n <= 0 || n > len(s.buf) {
		return
	}
	s.buf = append(s.buf[:0], s.buf[n:]...)
	s.off = keep
}

// byteAt returns the byte at absolute position p, reading as needed.
// Positions already passed stay reachable (spans reference absolute
// offsets in the spool file, not this buffer), so the window may compact
// to p once we have advanced — keeping resident memory O(window), not
// O(value).
func (s *streamScan) byteAt(p int64) (byte, bool, error) {
	i := int(p - s.off)
	if i < 0 {
		return 0, false, errors.New("streamScan: offset rewound")
	}
	for i >= len(s.buf) {
		if s.eof {
			return 0, false, nil
		}
		if err := s.fill(i + 1); err != nil {
			return 0, false, err
		}
	}
	// Bound the window: once it exceeds ~4 chunk sizes past the read point,
	// drop consumed bytes. A rewind to < s.off is only legal for callers
	// that never passed the point — none do.
	if i >= 256<<10 {
		s.compact(p - (64 << 10))
		i = int(p - s.off)
	}
	return s.buf[i], true, nil
}

// skipWSPos returns the absolute position of the first non-ws byte at/after p.
func (s *streamScan) skipWSPos(p int64) (int64, error) {
	for {
		b, ok, err := s.byteAt(p)
		if err != nil {
			return p, err
		}
		if !ok {
			return p, nil
		}
		if b != ' ' && b != '\t' && b != '\n' && b != '\r' {
			return p, nil
		}
		p++
	}
}

// scanStringSpan returns the absolute end offset (past closing quote) of
// the string starting at p (which must be '"'). When decode is true the
// decoded string is also returned and its length is capped at maxToken
// (only member keys and routing scalars pass decode=true). When decode is
// false the string is validated byte-by-byte with no allocation and no
// length cap — used for strings nested inside large member values.
func (s *streamScan) scanStringSpan(p int64, decode bool) (end int64, decoded string, err error) {
	b, ok, err := s.byteAt(p)
	if err != nil || !ok || b != '"' {
		return p, "", errMalformed
	}
	var sb strings.Builder
	i := p + 1
	for {
		c, ok, err := s.byteAt(i)
		if err != nil {
			return p, "", err
		}
		if !ok {
			return p, "", errMalformed
		}
		if decode && i-p > maxToken {
			return p, "", errMalformed
		}
		switch c {
		case '"':
			return i + 1, sb.String(), nil
		case '\\':
			c2, ok, err := s.byteAt(i + 1)
			if err != nil || !ok {
				return p, "", errMalformed
			}
			switch c2 {
			case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
				if decode {
					sb.WriteByte(unescapeByte(c2))
				}
				i += 2
			case 'u':
				hex := make([]byte, 4)
				for k := 0; k < 4; k++ {
					h, ok, err := s.byteAt(i + 2 + int64(k))
					if err != nil || !ok || !isHex(h) {
						return p, "", errMalformed
					}
					hex[k] = h
				}
				if decode {
					sb.WriteRune(rune(hexVal(hex)))
				}
				i += 6
			default:
				return p, "", errMalformed
			}
		default:
			if c < 0x20 {
				return p, "", errMalformed // raw control char in string
			}
			if decode {
				sb.WriteByte(c)
			}
			i++
		}
	}
}

func unescapeByte(c byte) byte {
	switch c {
	case 'b':
		return '\b'
	case 'f':
		return '\f'
	case 'n':
		return '\n'
	case 'r':
		return '\r'
	case 't':
		return '\t'
	default:
		return c
	}
}

func hexVal(hex []byte) int64 {
	var v int64
	for _, c := range hex {
		v *= 16
		switch {
		case c >= '0' && c <= '9':
			v += int64(c - '0')
		case c >= 'a' && c <= 'f':
			v += int64(c-'a') + 10
		case c >= 'A' && c <= 'F':
			v += int64(c-'A') + 10
		}
	}
	return v
}

func isHex(b byte) bool {
	return (b >= '0' && b <= '9') || (b >= 'a' && b <= 'f') || (b >= 'A' && b <= 'F')
}

// skipValueSpan returns the absolute end offset of the JSON value starting
// at p (after ws), validating it as it goes without retaining bytes. It
// recognises strings, numbers, true/false/null, objects and arrays. Nested
// composites are validated iteratively on a fixed-size explicit stack, so
// nesting depth never maps to goroutine stack growth and is hard-capped by
// maxJSONDepth.
func (s *streamScan) skipValueSpan(p int64) (int64, error) {
	p, err := s.skipWSPos(p)
	if err != nil {
		return p, err
	}
	b, ok, err := s.byteAt(p)
	if err != nil {
		return p, err
	}
	if !ok {
		return p, errMalformed
	}
	switch b {
	case '"':
		end, _, err := s.scanStringSpan(p, false)
		return end, err
	case '{', '[':
		return s.skipComposite(p)
	case 't':
		return s.lit(p, "true")
	case 'f':
		return s.lit(p, "false")
	case 'n':
		return s.lit(p, "null")
	default:
		return s.number(p)
	}
}

// maxJSONDepth bounds composite nesting accepted by the streaming
// validator. Real Responses requests nest well under ~16 levels; 128 is a
// generous bound that keeps an adversarial frame's parser state O(limit)
// instead of O(message bytes). Over the limit the frame is rejected before
// any upstream side effect.
const maxJSONDepth = 128

var errTooDeep = errors.New("JSON nesting exceeds the parser depth limit")

// frameState is one open composite on the iterative parse stack.
//
//	objPhase: 0 = expect key-or-close, 1 = expect colon, 2 = expect value,
//	          3 = expect ','-or-'}'
//	arrPhase: 0 = expect value-or-']', 2 = expect value (after a comma),
//	          3 = expect ','-or-']'
type frameState struct {
	isObj bool
	phase uint8
	first bool // no element consumed yet
}

// skipComposite validates and skips a nested { or [ from p (the open byte)
// using an explicit depth-bounded stack instead of recursion: stack use is
// O(maxJSONDepth) entries, never O(nesting). Each element is validated,
// separators must be well-formed (no trailing/leading/double commas, no
// missing colon in an object), and strings inside are checked.
func (s *streamScan) skipComposite(p int64) (int64, error) {
	open, ok, err := s.byteAt(p)
	if err != nil || !ok {
		return p, errMalformed
	}
	if open != '{' && open != '[' {
		return p, errMalformed
	}

	stack := make([]frameState, 1, 16)
	stack[0] = frameState{isObj: open == '{', first: true}
	i := p + 1

	for len(stack) > 0 {
		top := &stack[len(stack)-1]
		i, err = s.skipWSPos(i)
		if err != nil {
			return p, err
		}
		b, ok, err := s.byteAt(i)
		if err != nil {
			return p, err
		}
		if !ok {
			return p, errMalformed
		}

		switch top.phase {
		case 0: // expect first element or close
			if top.isObj {
				if b == '}' {
					if !top.first {
						return p, errMalformed // trailing comma
					}
					stack = stack[:len(stack)-1]
					i++
					markConsumed(stack)
					continue
				}
				if b != '"' {
					return p, errMalformed
				}
				end, _, err := s.scanStringSpan(i, false)
				if err != nil {
					return p, err
				}
				i = end
				top.phase = 1
				continue
			}
			if b == ']' {
				if !top.first {
					return p, errMalformed
				}
				stack = stack[:len(stack)-1]
				i++
				markConsumed(stack)
				continue
			}
			// fall through to value
		case 1: // object: expect colon
			if b != ':' {
				return p, errMalformed
			}
			top.phase = 2
			i++
			continue
		case 2: // expect value (object after colon, or array after comma)
			// fall through to value
		case 3: // expect ',' or close
			if top.isObj {
				if b == '}' {
					stack = stack[:len(stack)-1]
					i++
					markConsumed(stack)
					continue
				}
				if b != ',' {
					return p, errMalformed
				}
				top.phase = 0
				i++
				continue
			}
			if b == ']' {
				stack = stack[:len(stack)-1]
				i++
				markConsumed(stack)
				continue
			}
			if b != ',' {
				return p, errMalformed
			}
			top.phase = 2
			i++
			continue
		}

		// Value token begins at i (phase 0 for arrays, phase 2).
		switch b {
		case '{', '[':
			if len(stack) >= maxJSONDepth {
				return p, errTooDeep
			}
			stack = append(stack, frameState{isObj: b == '{', first: true})
			i++
		case '"':
			end, _, err := s.scanStringSpan(i, false)
			if err != nil {
				return p, err
			}
			i = end
			top.phase = 3
			top.first = false
		case 't':
			end, err := s.lit(i, "true")
			if err != nil {
				return p, err
			}
			i = end
			top.phase = 3
			top.first = false
		case 'f':
			end, err := s.lit(i, "false")
			if err != nil {
				return p, err
			}
			i = end
			top.phase = 3
			top.first = false
		case 'n':
			end, err := s.lit(i, "null")
			if err != nil {
				return p, err
			}
			i = end
			top.phase = 3
			top.first = false
		default:
			end, err := s.number(i)
			if err != nil {
				return p, err
			}
			i = end
			top.phase = 3
			top.first = false
		}
	}
	return i, nil
}

// markConsumed records a just-closed composite as a completed element of
// its parent frame (which then expects ',' or its own close byte).
func markConsumed(stack []frameState) {
	if len(stack) == 0 {
		return
	}
	stack[len(stack)-1].phase = 3
	stack[len(stack)-1].first = false
}


func (s *streamScan) lit(p int64, word string) (int64, error) {
	for i, c := range []byte(word) {
		b, ok, err := s.byteAt(p + int64(i))
		if err != nil {
			return p, err
		}
		if !ok || b != c {
			return p, errMalformed
		}
	}
	end := p + int64(len(word))
	// next byte must be a delimiter or EOF/ws
	nb, ok, err := s.byteAt(end)
	if err != nil {
		return p, err
	}
	if ok && nb != ',' && nb != '}' && nb != ']' && nb != ' ' && nb != '\t' && nb != '\n' && nb != '\r' {
		return p, errMalformed
	}
	return end, nil
}

// number validates a JSON number per RFC 8259 §6: -?(0|[1-9]\d*)(\.\d+)?([eE][+-]?\d+)?
func (s *streamScan) number(p int64) (int64, error) {
	at := func(i int64) (byte, bool) {
		b, ok, _ := s.byteAt(i)
		return b, ok
	}
	i := p
	if b, _ := at(i); b == '-' {
		i++
	}
	// int part
	b, ok := at(i)
	if !ok || b < '0' || b > '9' {
		return p, errMalformed
	}
	if b == '0' {
		i++
	} else {
		for b, _ := at(i); b >= '0' && b <= '9'; b, _ = at(i) {
			i++
		}
	}
	// frac
	if b, _ := at(i); b == '.' {
		i++
		b, ok = at(i)
		if !ok || b < '0' || b > '9' {
			return p, errMalformed
		}
		for b, _ := at(i); b >= '0' && b <= '9'; b, _ = at(i) {
			i++
		}
	}
	// exp
	if b, _ := at(i); b == 'e' || b == 'E' {
		i++
		if b, _ := at(i); b == '+' || b == '-' {
			i++
		}
		b, ok = at(i)
		if !ok || b < '0' || b > '9' {
			return p, errMalformed
		}
		for b, _ := at(i); b >= '0' && b <= '9'; b, _ = at(i) {
			i++
		}
	}
	// must end at a delimiter
	b, ok = at(i)
	if ok && b != ',' && b != '}' && b != ']' && b != ' ' && b != '\t' && b != '\n' && b != '\r' {
		return p, errMalformed
	}
	return i, nil
}

// inspectFrameStream validates the frame and records each top-level
// member's value byte span in one streaming pass over the spool. Only
// routing-field scalars are decoded; large member values are validated and
// left on disk — nothing complete is materialised. Validation completes
// before any upstream side effect, which is what the disk spool exists for.
func inspectFrameStream(r io.Reader) (frameInfo, error) {
	var info frameInfo
	s := newStreamScan(r)

	p, err := s.skipWSPos(0)
	if err != nil {
		return info, err
	}
	b, ok, err := s.byteAt(p)
	if err != nil {
		return info, err
	}
	if !ok || b != '{' {
		return info, errMalformed
	}
	p++

	seen := map[string]bool{}
	expectKey := true
	for {
		p, err = s.skipWSPos(p)
		if err != nil {
			return info, err
		}
		b, ok, err = s.byteAt(p)
		if err != nil {
			return info, err
		}
		if !ok {
			return info, errMalformed
		}
		if b == '}' {
			if expectKey && len(info.members) > 0 {
				return info, errMalformed // trailing comma
			}
			p++
			break
		}
		if !expectKey {
			if b != ',' {
				return info, errMalformed
			}
			p++
			expectKey = true
			continue
		}
		if b != '"' {
			return info, errMalformed
		}
		keyEnd, key, err := s.scanStringSpan(p, true)
		if err != nil {
			return info, err
		}
		p, err = s.skipWSPos(keyEnd)
		if err != nil {
			return info, err
		}
		b, ok, err = s.byteAt(p)
		if err != nil {
			return info, err
		}
		if !ok || b != ':' {
			return info, errMalformed
		}
		vstart, err := s.skipWSPos(p + 1)
		if err != nil {
			return info, err
		}

		seen[key] = seen[key] // touch to quiet vet on unused map in some builds
		switch key {
		case "type":
			if seen[key] {
				return info, errMalformed
			}
			seen[key] = true
			end, str, serr := s.scanStringSpan(vstart, true)
			if serr != nil {
				return info, serr
			}
			info.typeVal = str
			p = end
		case "model":
			if seen[key] {
				return info, errMalformed
			}
			seen[key] = true
			end, str, serr := s.scanStringSpan(vstart, true)
			if serr != nil {
				return info, serr
			}
			info.model = str
			info.haveModel = true
			p = end
		case "previous_response_id":
			if seen[key] {
				info.havePrevID = true // ambiguous
			}
			seen[key] = true
			end, err := s.decodePrevID(vstart, &info)
			if err != nil {
				return info, err
			}
			p = end
		case "generate":
			if seen[key] {
				return info, errMalformed
			}
			seen[key] = true
			b, _, err := s.byteAt(vstart)
			if err != nil {
				return info, err
			}
			end, err := s.lit(vstart, "false")
			if err == nil {
				info.genFalse = true
				p = end
			} else {
				end, err = s.skipValueSpan(vstart)
				if err != nil {
					return info, err
				}
				p = end
			}
			_ = b
		default:
			end, err := s.skipValueSpan(vstart)
			if err != nil {
				return info, err
			}
			p = end
		}
		info.members = append(info.members, memberSpan{key: key, vstart: vstart, vend: p})
		expectKey = false
		// The member's bytes are consumed; drop them from the window so a
		// large value does not grow the resident buffer. Spans refer to
		// absolute offsets in the spool, not to this buffer.
		s.compact(p)
	}

	// nothing but ws may follow the top-level object
	p, err = s.skipWSPos(p)
	if err != nil {
		return info, err
	}
	if _, ok, err := s.byteAt(p); err != nil {
		return info, err
	} else if ok {
		return info, errMalformed
	}
	info.isCreate = info.typeVal == "response.create"
	return info, nil
}

// decodePrevID decodes a previous_response_id value (string|null) starting
// at p and returns its end offset. Non-string values set havePrevID to
// force rejection; an empty string is treated as absent.
func (s *streamScan) decodePrevID(p int64, info *frameInfo) (int64, error) {
	b, ok, err := s.byteAt(p)
	if err != nil {
		return p, err
	}
	if !ok {
		return p, errMalformed
	}
	if b == '"' {
		end, str, err := s.scanStringSpan(p, true)
		if err != nil {
			return p, err
		}
		info.prevID = str
		if str != "" {
			info.havePrevID = true
		}
		return end, nil
	}
	if b == 'n' {
		return s.lit(p, "null") // null = absent
	}
	// non-string/non-null: still validate the value, but reject the parent
	end, err := s.skipValueSpan(p)
	info.havePrevID = true
	return end, err
}

// rewrittenBody computes the upstream POST body from the member spans of
// an already-validated frame. It returns the body as an io.Reader backed
// by the source ReaderAt (the request spool) plus small literal segments
// — a known Content-Length with no second copy and no materialisation.
//
// WS-mode-only members ("type", "generate", "previous_response_id") are
// dropped, "stream" is forced to exactly one true member, and every other
// member is relayed verbatim as "key":<verbatim value bytes>.
//
// The source must outlive the returned reader's use.
func rewrittenBody(src io.ReaderAt, info frameInfo) (io.Reader, int64) {
	var segs []io.Reader
	var total int64
	push := func(r io.Reader, n int64) {
		segs = append(segs, r)
		total += n
	}
	push(strings.NewReader("{"), 1)
	first := true
	sawStream := false
	for _, m := range info.members {
		switch m.key {
		case "type", "generate", "previous_response_id":
			continue // WS-mode-only; never reach HTTP
		}
		if m.key == "stream" && sawStream {
			continue // dedupe: exactly one stream member
		}
		if !first {
			push(strings.NewReader(","), 1)
		}
		first = false
		if m.key == "stream" {
			sawStream = true
			lit := `"stream":true`
			push(strings.NewReader(lit), int64(len(lit)))
			continue
		}
		key, _ := json.Marshal(m.key)
		push(bytes.NewReader(key), int64(len(key)))
		// vstart is the value's first byte; vend is its end (the span is
		// copied verbatim, so original value formatting is preserved).
		// Emit "key": then the value span.
		colon := []byte{':'}
		push(bytes.NewReader(colon), 1)
		push(io.NewSectionReader(src, m.vstart, m.vend-m.vstart), m.vend-m.vstart)
	}
	if !sawStream {
		if !first {
			push(strings.NewReader(","), 1)
		}
		lit := `"stream":true`
		push(strings.NewReader(lit), int64(len(lit)))
	}
	push(strings.NewReader("}"), 1)
	return io.MultiReader(segs...), total
}

// inspectFrame is the byte-slice convenience form used by tests/benches.
func inspectFrame(frame []byte) (frameInfo, error) {
	return inspectFrameStream(bytes.NewReader(frame))
}

// rewriteBody writes the rewrite to dst; used by tests.
func rewriteBody(dst *bytes.Buffer, frame []byte) error {
	info, err := inspectFrameStream(bytes.NewReader(frame))
	if err != nil {
		return err
	}
	r, _ := rewrittenBody(bytes.NewReader(frame), info)
	_, err = io.Copy(dst, r)
	return err
}
