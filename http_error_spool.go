package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/coder/websocket"
)

func (b *bridge) relayHTTPError(ctx context.Context, conn *websocket.Conn, response *http.Response) error {
	release := b.acquire(eventWindowBytes + eventScanBytes)
	if release == nil {
		return errWorkBudget
	}
	defer release()
	spool, e := b.newChargedSpool()
	if e != nil {
		return e
	}
	defer spool.release()
	buf := make([]byte, eventWindowBytes)
	for {
		n, e := response.Body.Read(buf)
		if n > 0 {
			if spool.msg.size+int64(n) > b.cfg.maxBodyBytes {
				b.sendErrorFrame(ctx, conn, errTooLarge)
				return nil
			}
			if _, e := spool.Write(buf[:n]); e != nil {
				return e
			}
		}
		if e != nil {
			if e != io.EOF {
				b.sendErrorFrame(ctx, conn, errTooLarge)
				return nil
			}
			break
		}
	}
	var value eventMember
	valid := walkEventObject(spool.file, 0, spool.msg.size, func(m eventMember) error {
		if m.key == "error" {
			value = m
		}
		return nil
	}) == nil
	if valid && value.end > value.start {
		var first [1]byte
		_, _ = spool.file.ReadAt(first[:], value.start)
		if first[0] != 'n' {
			writer, e := conn.Writer(ctx, websocket.MessageText)
			if e != nil {
				return e
			}
			if _, e = fmt.Fprintf(writer, `{"type":"error","status":%d,"error":`, response.StatusCode); e != nil {
				return e
			}
			if e := copyEventSpan(writer, spool.file, value.start, value.end, buf); e != nil {
				return e
			}
			if _, e := io.WriteString(writer, "}"); e != nil {
				return e
			}
			return writer.Close()
		}
	}
	// Only a bounded text prefix is retained for non-JSON upstream errors.
	prefix := make([]byte, 0, 404)
	reader := bufio.NewReaderSize(io.NewSectionReader(spool.file, 0, spool.msg.size), 4096)
	leading, beyondNonSpace := true, false
	for {
		r, _, e := reader.ReadRune()
		if e != nil {
			break
		}
		if leading && unicode.IsSpace(r) {
			continue
		}
		leading = false
		if len(prefix) < 400 {
			prefix = utf8.AppendRune(prefix, r)
			if len(prefix) > 400 {
				prefix = prefix[:400]
			}
		} else if !unicode.IsSpace(r) {
			beyondNonSpace = true
		}
	}
	message := string(prefix)
	if !beyondNonSpace {
		message = strings.TrimRightFunc(message, unicode.IsSpace)
	}
	frame, _ := json.Marshal(map[string]any{"type": "error", "status": response.StatusCode, "error": map[string]any{"type": "upstream_error", "message": message}})
	return sendText(ctx, conn, frame)
}
