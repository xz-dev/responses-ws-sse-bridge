// responses-ws-sse-bridge is a stateless WebSocket → HTTP/SSE converter for
// the OpenAI Responses WebSocket mode.
//
// Position in the chain:
//
//	client → APISIX → Sub2API (passthrough, auth only) → openai-api-cache-keeper
//	  → THIS SERVICE → inner APISIX model selector → AISIX | CPA
//
// The inner APISIX selector classifies a POST /v1/responses JSON body by model
// id ("/" → CPA, otherwise → AISIX) and cannot route WebSocket. This service
// terminates the Responses WebSocket mode and converts every turn into a
// streaming HTTP POST so turns classify exactly like HTTP requests.
//
// It retains nothing between turns. The keeper in front expands every
// previous_response_id it recognizes, so a non-empty previous_response_id that
// reaches this bridge is unresolvable by construction and is answered with
// previous_response_not_found — the same contract the keeper uses.
package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

type config struct {
	listen       string
	upstream     *url.URL
	maxSockets   int
	maxMsgBytes  int64
	maxBodyBytes int64
	workBytes    int64
	turnIdle     time.Duration
	spoolDir     string // dir for request spools; "" = os.TempDir()
}

func loadConfig() (config, error) {
	listen := os.Getenv("LISTEN_ADDR")
	if listen == "" {
		listen = ":8080"
	}
	raw := os.Getenv("UPSTREAM_URL")
	if raw == "" {
		return config{}, errors.New("UPSTREAM_URL is required")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return config{}, fmt.Errorf("UPSTREAM_URL: %w", err)
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return config{}, errors.New("UPSTREAM_URL must not contain userinfo, query, or fragment")
	}
	switch u.Scheme {
	case "http", "https":
	default:
		return config{}, errors.New("UPSTREAM_URL scheme must be http or https")
	}
	sockets, err := envInt("MAX_SOCKETS", 64)
	if err != nil {
		return config{}, err
	}
	msg, err := envInt64("MAX_MESSAGE_BYTES", 16<<20)
	if err != nil {
		return config{}, err
	}
	body, err := envInt64("MAX_BODY_BYTES", 16<<20)
	if err != nil {
		return config{}, err
	}
	idle := 0
	if v := os.Getenv("TURN_IDLE_TIMEOUT_SECONDS"); v != "" {
		idle, err = envInt("TURN_IDLE_TIMEOUT_SECONDS", 0)
		if err != nil {
			return config{}, err
		}
	}
	work, err := envInt64("WORK_MAX_BYTES", 256<<20)
	if err != nil {
		return config{}, err
	}
	if sockets <= 0 || msg <= 0 || body <= 0 || work <= 0 || idle < 0 {
		return config{}, errors.New("limits must be positive; TURN_IDLE_TIMEOUT_SECONDS may be 0 (disabled)")
	}
	spoolDir := os.Getenv("SPOOL_DIR")
	if spoolDir == "" {
		spoolDir = os.TempDir()
	}
	if st, err := os.Stat(spoolDir); err != nil || !st.IsDir() {
		return config{}, fmt.Errorf("SPOOL_DIR %q is not a usable directory (set it or TMPDIR to a writable dir; the image ships /spool)", spoolDir)
	}
	return config{
		listen: listen, upstream: u,
		maxSockets: sockets, maxMsgBytes: msg, maxBodyBytes: body,
		workBytes: work,
		turnIdle:  time.Duration(idle) * time.Second,
		spoolDir:  spoolDir,
	}, nil
}

func envInt(name string, def int) (int, error) {
	v := os.Getenv(name)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}
	return n, nil
}

func envInt64(name string, def int64) (int64, error) {
	v := os.Getenv(name)
	if v == "" {
		return def, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}
	return n, nil
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		port := os.Getenv("PORT")
		if port == "" {
			port = "8080"
		}
		conn, err := net.DialTimeout("tcp", "127.0.0.1:"+port, 2*time.Second)
		if err != nil {
			os.Exit(1)
		}
		conn.Close()
		os.Exit(0)
	}

	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	br := newBridge(cfg)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.Handle("/", br)

	srv := &http.Server{
		Addr:              cfg.listen,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		srv.Close()
	}()
	log.Printf("responses-ws-sse-bridge listening on %s upstream=%s", cfg.listen, cfg.upstream)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("serve: %v", err)
	}
}
