package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/coder/websocket"
)

// openSpools lists this process's open anonymous spool files in dir. An
// O_TMPFILE spool has no name; /proc shows it as "<dir>/#<ino> (deleted)".
func openSpools(t *testing.T, dir string) []os.FileInfo {
	t.Helper()
	fds, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	var out []os.FileInfo
	for _, fd := range fds {
		p := filepath.Join("/proc/self/fd", fd.Name())
		target, err := os.Readlink(p)
		if err != nil || !strings.HasPrefix(target, dir+"/") {
			continue
		}
		if st, err := os.Stat(p); err == nil && st.Mode().IsRegular() {
			out = append(out, st)
		}
	}
	return out
}

func readSpool(t *testing.T, f *os.File) []byte {
	t.Helper()
	st, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(io.NewSectionReader(f, 0, st.Size()))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// D3: spools are anonymous (no directory entry), mode 0600, and closing the
// last descriptor frees them.
func TestM1SpoolIsAnonymousAndFreedOnClose(t *testing.T) {
	b := requestTestBridge(t, 1<<20)
	msg := b.spoolFrame(bytes.NewReader([]byte(`{"a":1}`)))
	if msg.err != nil {
		t.Fatal(msg.err)
	}
	if entries, _ := os.ReadDir(b.cfg.spoolDir); len(entries) != 0 {
		t.Fatalf("spool created a named file: %v", entries)
	}
	if got := readSpool(t, msg.file); string(got) != `{"a":1}` {
		t.Fatalf("spool content %q", got)
	}
	if n := len(openSpools(t, b.cfg.spoolDir)); n != 1 {
		t.Fatalf("open spools = %d, want 1", n)
	}
	msg.release()
	assertRequestResourcesZero(t, b)
}

// D3: a write that would leave less than the reserve free is refused with
// the explicit storage error, never written and never moved to memory.
func TestM1SpoolDiskReserveRefuses(t *testing.T) {
	b := requestTestBridge(t, 1<<20)
	var fs syscall.Statfs_t
	if err := syscall.Statfs(b.cfg.spoolDir, &fs); err != nil {
		t.Fatal(err)
	}
	b.cfg.diskReserveBytes = int64(fs.Bavail)*int64(fs.Frsize) + 1
	msg := b.spoolFrame(bytes.NewReader([]byte(`{"a":1}`)))
	if !errors.Is(msg.err, errInsufficientStorage) {
		t.Fatalf("want insufficient storage, got %v", msg.err)
	}
	msg.release()
	assertRequestResourcesZero(t, b)
	if e := b.failureAPIError(msg.err); e.Status != 507 || e.Code != "insufficient_storage" || e.Hop != bridgeHop {
		t.Fatalf("api error %+v", e)
	}
}

func TestM1CheckSpoolDir(t *testing.T) {
	good := t.TempDir()
	if err := os.Chmod(good, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := checkSpoolDir(good); err != nil {
		t.Fatalf("private disk dir refused: %v", err)
	}
	loose := t.TempDir()
	_ = os.Chmod(loose, 0o755)
	if err := checkSpoolDir(loose); err == nil || !strings.Contains(err.Error(), "0700") {
		t.Fatalf("mode 0755 accepted: %v", err)
	}
	named := t.TempDir()
	_ = os.Chmod(named, 0o700)
	if err := os.WriteFile(filepath.Join(named, "bridge-turn-1.json"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := checkSpoolDir(named); err == nil || !strings.Contains(err.Error(), "named file") {
		t.Fatalf("named file accepted: %v", err)
	}
	var fs syscall.Statfs_t
	if syscall.Statfs("/dev/shm", &fs) == nil && fs.Type == tmpfsMagic {
		shm, err := os.MkdirTemp("/dev/shm", "bridge-spool-")
		if err == nil {
			defer os.RemoveAll(shm)
			_ = os.Chmod(shm, 0o700)
			if err := checkSpoolDir(shm); err == nil || !strings.Contains(err.Error(), "memory-backed") {
				t.Fatalf("tmpfs accepted: %v", err)
			}
		}
	}
}

// Deviation from tasks 2.1 (spool closed at turn end, ticket accounts the
// whole turn): an upstream that answers with an error before reading the
// body must not stall the turn. The turn ends with the explicit error event,
// the spool is closed and the socket still carries the next turn.
func TestM1UpstreamEarlyErrorDoesNotStallTurn(t *testing.T) {
	var calls atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 { // answer without reading the 8 MiB body
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			io.WriteString(w, `{"error":{"message":"too big","type":"invalid_request_error","code":"request_too_large"}}`)
			return
		}
		regressSSE(w, r)
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	bs := httptest.NewServer(newBridge(config{upstream: u, maxMsgBytes: 16 << 20, maxBodyBytes: 16 << 20, spoolDir: dir}))
	defer bs.Close()
	conn, _, err := websocket.Dial(context.Background(), wsURL(bs.URL)+"/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	conn.SetReadLimit(-1)
	big := `{"type":"response.create","model":"m","input":"` + strings.Repeat("x", 8<<20) + `"}`
	if err := conn.Write(context.Background(), websocket.MessageText, []byte(big)); err != nil {
		t.Fatal(err)
	}
	_, ev, err := conn.Read(context.Background())
	if err != nil || !bytes.Contains(ev, []byte(`"type":"error"`)) || !bytes.Contains(ev, []byte("request_too_large")) {
		t.Fatalf("want explicit error event, got %s %v", ev, err)
	}
	for len(openSpools(t, dir)) != 0 { // a spool kept past the turn hangs here
		runtime.Gosched()
	}
	if err := conn.Write(context.Background(), websocket.MessageText, []byte(`{"type":"response.create","model":"m","input":[]}`)); err != nil {
		t.Fatal(err)
	}
	for {
		_, ev, err := conn.Read(context.Background())
		if err != nil {
			t.Fatalf("socket did not survive the error turn: %v", err)
		}
		if bytes.Contains(ev, []byte(`"response.completed"`)) {
			return
		}
	}
}
