// Command mock 是可控的 webhook 下游：可脚本化状态码/Retry-After/挂起，并记录收到的请求。
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"
)

type config struct {
	StatusCode int
	RetryAfter time.Duration
	Hang       time.Duration
}

type record struct {
	IdempotencyKey string
	Body           string
	ArrivedAt      time.Time
	StatusCode     int
}

type server struct {
	mu       sync.Mutex
	cfg      config
	requests []record
}

func (s *server) handleConfig(w http.ResponseWriter, r *http.Request) {
	var c struct {
		StatusCode        int `json:"status_code"`
		RetryAfterSeconds int `json:"retry_after_seconds"`
		HangMs            int `json:"hang_ms"`
	}
	_ = json.NewDecoder(r.Body).Decode(&c)
	if c.StatusCode == 0 {
		c.StatusCode = 200
	}
	s.mu.Lock()
	s.cfg = config{
		StatusCode: c.StatusCode,
		RetryAfter: time.Duration(c.RetryAfterSeconds) * time.Second,
		Hang:       time.Duration(c.HangMs) * time.Millisecond,
	}
	s.mu.Unlock()
	fmt.Fprintf(w, "configured: %+v\n", s.cfg)
}

func (s *server) handleRequests(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, rec := range s.requests {
		fmt.Fprintf(w, "key=%s status=%d arrived=%s body=%s\n",
			rec.IdempotencyKey, rec.StatusCode, rec.ArrivedAt.Format(time.RFC3339Nano), rec.Body)
	}
}

func (s *server) handleDownstream(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	key := r.Header.Get("Idempotency-Key")

	s.mu.Lock()
	cfg := s.cfg
	s.requests = append(s.requests, record{
		IdempotencyKey: key,
		Body:           string(b),
		ArrivedAt:      time.Now(),
		StatusCode:     cfg.StatusCode,
	})
	s.mu.Unlock()

	if cfg.Hang > 0 {
		time.Sleep(cfg.Hang)
	}
	if cfg.RetryAfter > 0 {
		w.Header().Set("Retry-After", fmt.Sprintf("%d", int(cfg.RetryAfter/time.Second)))
	}
	w.WriteHeader(cfg.StatusCode)
}

func main() {
	s := &server{cfg: config{StatusCode: 200}}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /__config", s.handleConfig)
	mux.HandleFunc("GET /__requests", s.handleRequests)
	mux.HandleFunc("POST /", s.handleDownstream)

	addr := getenv("LISTEN", ":9000")
	fmt.Printf("mock receiver on %s\n", addr)
	_ = http.ListenAndServe(addr, mux)
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
