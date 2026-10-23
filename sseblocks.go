package main

import (
	"errors"
	"io"
)

// blockPeek wraps an SSE byte stream and reports, for each dispatched
// event block, whether the block contained at least one `data` field.
// go-sse (correctly) yields an Event for both cases — `data:\n\n`
// dispatches an event with an empty data string per WHATWG §9.2.6, and a
// metadata-only block (id:/event:/retry: only) does not dispatch a
// message at all — but the yielded Event looks identical from the
// outside. The relay contract needs the distinction: an empty-data
// event is malformed Responses JSON, while a metadata-only block is
// ignored.
//
// The reader splits the stream into blocks at blank lines (\n, \r, and
// \r\n all end a line per WHATWG §9.2.8) and pushes one flag onto a
// fixed ring each time a block closes. The relay pops one flag per
// yielded event; the two stay in lockstep because go-sse yields an event
// exactly when a non-empty block closes — and at EOF for the pending
// block, which we mark final=true.
//
// A line is a `data` field when the bytes before the first ':' (or the
// whole line, if it has no colon) equal "data". Detection is O(1) per
// byte, retains only flags, and never touches block content.
type blockPeek struct {
	r io.Reader

	// ring of parsed block flags; the parser cannot complete more
	// blocks than its own buffer allows before the consumer drains.
	flags [64]blockFlag
	head  int
	tail  int
	count int

	// per-line state
	lineLen  int     // bytes consumed on the current line
	isData   bool    // current line's field name is exactly "data"
	sawColon bool    // a ':' appeared on the current line

	// per-block state
	hasData   bool // current block contains ≥1 data field
	blockOpen bool // a content line opened the current block

	// line-terminator state
	atLineStart bool // next byte begins a fresh line
	prevCR      bool // previous byte was a lone '\r' (may pair with '\n')
}

// newBlockPeek starts at the beginning of a line (and a block).
func newBlockPeek(r io.Reader) *blockPeek {
	return &blockPeek{r: r, atLineStart: true}
}

type blockFlag struct {
	hasData bool
	final   bool // block closed by EOF, not a blank line
}

// Read implements io.Reader; every byte feeds the O(1) state machine.
func (b *blockPeek) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	for i := 0; i < n; i++ {
		b.scan(p[i])
	}
	if (err == io.EOF || errors.Is(err, errStreamEOF)) && b.blockOpen {
		// Unterminated trailing block: go-sse yields it at EOF, so its
		// flag must be consumable too. errStreamEOF is the
		// eofMarkingReader sentinel for a mid-block end.
		b.push(blockFlag{hasData: b.hasData, final: true})
		b.blockOpen = false
	}
	return n, err
}

// scan advances the block machine by one byte.
func (b *blockPeek) scan(c byte) {
	if c == '\n' || c == '\r' {
		if c == '\n' && b.prevCR {
			// '\n' completing a "\r\n" pair — part of the previous
			// terminator, not a new one.
			b.prevCR = false
			return
		}
		b.prevCR = c == '\r'
		if b.atLineStart {
			// Blank line: closes the current block if one is open.
			if b.blockOpen {
				b.push(blockFlag{hasData: b.hasData})
				b.blockOpen = false
				b.hasData = false
			}
			return
		}
		// Terminated a content line.
		if b.isData {
			b.hasData = true
		}
		b.lineLen = 0
		b.isData = false
		b.sawColon = false
		b.atLineStart = true
		return
	}

	b.prevCR = false
	b.blockOpen = true
	if b.atLineStart {
		b.atLineStart = false
		b.lineLen = 0
		b.isData = true // tentative: line is `data` until disproven
		b.sawColon = false
	}
	if b.isData && !b.sawColon {
		if c == ':' {
			// A ':' ends the field name. It is `data` only if exactly
			// the 4 bytes "data" preceded it; a ':' as the first byte
			// is a comment line (empty field name).
			b.sawColon = true
			if b.lineLen != 4 {
				b.isData = false
			}
		} else if b.lineLen < 4 {
			if c != "data"[b.lineLen] {
				b.isData = false // some other field name
			}
		} else {
			// 5th+ byte of the field name: name is longer than "data".
			b.isData = false
		}
	}
	b.lineLen++
}

func (b *blockPeek) push(f blockFlag) {
	if b.count == len(b.flags) {
		// The parser drains one flag per yielded event, so the ring can
		// only overflow if event dispatch is broken entirely; dropping
		// is the safe fallback (the event is then treated per the
		// existing malformed-event rule).
		return
	}
	b.flags[b.head] = f
	b.head = (b.head + 1) % len(b.flags)
	b.count++
}

// next returns the flag recorded for the next dispatched block.
func (b *blockPeek) next() (blockFlag, bool) {
	if b.count == 0 {
		return blockFlag{}, false
	}
	f := b.flags[b.tail]
	b.tail = (b.tail + 1) % len(b.flags)
	b.count--
	return f, true
}
