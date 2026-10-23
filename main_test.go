package main

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHealthcheckAddress(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/healthz" {
			t.Errorf("unexpected probe: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	addr := strings.TrimPrefix(server.URL, "http://")
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name, listen, port string
	}{
		{"listen address", addr, ""},
		{"empty host", ":" + port, ""},
		{"wildcard IPv4", "0.0.0.0:" + port, ""},
		{"explicit PORT override", "127.0.0.1:1", port},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("LISTEN_ADDR", tt.listen)
			t.Setenv("PORT", tt.port)
			t.Setenv("UPSTREAM_URL", "")
			if err := healthcheck(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestHealthcheckRequiresHTTP200(t *testing.T) {
	for _, status := range []int{http.StatusNoContent, http.StatusServiceUnavailable, http.StatusFound} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/healthz" {
					w.Header().Set("Location", "/other")
					w.WriteHeader(status)
					return
				}
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()
			t.Setenv("LISTEN_ADDR", strings.TrimPrefix(server.URL, "http://"))
			_, port, _ := net.SplitHostPort(strings.TrimPrefix(server.URL, "http://"))
			t.Setenv("PORT", port)
			if err := healthcheck(); err == nil {
				t.Fatalf("accepted /healthz status %d", status)
			}
		})
	}
}

func TestHealthcheckIgnoresOldDefaultListener(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:8080")
	if err != nil {
		t.Skipf("cannot bind old default port: %v", err)
	}
	other := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	other.Listener.Close()
	other.Listener = listener
	other.Start()
	defer other.Close()
	t.Setenv("LISTEN_ADDR", "")
	t.Setenv("PORT", "")
	if err := healthcheck(); err != nil {
		t.Fatalf("default :8080 probe: %v", err)
	}

	bridge, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := bridge.Addr().String()
	bridge.Close()
	t.Setenv("LISTEN_ADDR", addr)
	t.Setenv("PORT", "")
	if err := healthcheck(); err == nil {
		t.Fatalf("accepted unrelated listener on 8080 while bridge %s is closed", addr)
	}
}

func TestHealthcheckTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()
	t.Setenv("LISTEN_ADDR", strings.TrimPrefix(server.URL, "http://"))
	t.Setenv("PORT", "")
	start := time.Now()
	if err := healthcheck(); err == nil {
		t.Fatal("accepted a health endpoint that never responds")
	}
	if elapsed := time.Since(start); elapsed < time.Second || elapsed > 3*time.Second {
		t.Fatalf("expected 2s timeout, got %s", elapsed)
	}
}
