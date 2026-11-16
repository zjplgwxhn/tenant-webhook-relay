package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"webhook-relay/internal/backoff"
	"webhook-relay/internal/delayed"
	"webhook-relay/internal/dlq"
	"webhook-relay/internal/drainrate"
	"webhook-relay/internal/idempotency"
	"webhook-relay/internal/ingest"
	"webhook-relay/internal/limiter"
	"webhook-relay/internal/metrics"
	"webhook-relay/internal/queue"
	"webhook-relay/internal/reaper"
	"webhook-relay/internal/state"
	"webhook-relay/internal/tenant"
	"webhook-relay/internal/worker"
)

func main() {
	redisAddr := getenv("REDIS_ADDR", "localhost:6379")
	httpAddr := getenv("HTTP_ADDR", ":8080")
	queueCap := getenvInt("QUEUE_CAPACITY", 10000)
	globalCap := int(getenvInt("GLOBAL_MAX_CONCURRENCY", 2048))
	tenants := parseTenants(getenv("TENANTS", ""))

	client := redis.NewClient(&redis.Options{Addr: redisAddr})

	reg := prometheus.NewRegistry()
	m := metrics.NewMetrics(reg)
	for _, t := range tenants {
		m.QueueCapacity.WithLabelValues(t.ID).Set(float64(queueCap))
	}

	store := state.NewRedisTaskStore(client)
	q := queue.NewRedisQueue(client, queueCap)
	keys := idempotency.NewKeyGenerator()
	registry := tenant.NewStatic(tenants...)
	dr := drainrate.New(0.2)
	handler := ingest.NewHandler(keys, q, store, registry, m, dr)

	lim := limiter.NewLimiter(globalCap, 100*time.Millisecond, m)
	b := backoff.Backoff{Base: time.Second, Factor: 2.0, Cap: 60 * time.Second}
	d := delayed.NewRedisDelayed(client)
	dl := dlq.NewRedisDLQ(client)
	leaseDur := time.Duration(getenvInt("LEASE_DURATION", 30)) * time.Second
	reaperInterval := time.Duration(getenvInt("REAPER_INTERVAL", 15)) * time.Second
	sup := worker.NewSupervisor(tenants, q, store, store, lim, &http.Client{Timeout: 20 * time.Second}, b, d, dl, leaseDur, m, dr)
	sched := worker.NewScheduler(tenants, d, store, 500*time.Millisecond)
	rpr := reaper.New(tenants, client, reaperInterval, m)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	go sup.Start(ctx)
	go sched.Run(ctx)
	go rpr.Run(ctx)
	go probeRedis(ctx, client, m)

	mux := http.NewServeMux()
	mux.Handle("POST /v1/events", handler)
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	srv := &http.Server{Addr: httpAddr, Handler: mux}

	log.Printf("relay listening on %s (redis=%s tenants=%d)", httpAddr, redisAddr, len(tenants))
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()
	<-ctx.Done()
	_ = srv.Shutdown(context.Background())
	log.Printf("relay stopped")
}

// probeRedis 周期探活，更新 redis_up 指标。
func probeRedis(ctx context.Context, client *redis.Client, m *metrics.Metrics) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := client.Ping(ctx).Err(); err != nil {
				m.RedisUp.Set(0)
			} else {
				m.RedisUp.Set(1)
			}
		}
	}
}

// parseTenants 解析 "t1=http://host:9000,t2=..." 格式（每租户并发默认 8）。
func parseTenants(s string) []tenant.Tenant {
	var out []tenant.Tenant
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, dest, ok := strings.Cut(part, "=")
		if !ok || id == "" || dest == "" {
			continue
		}
		out = append(out, tenant.Tenant{ID: id, Destination: dest, MaxConcurrency: 8, RetryWindow: 24 * time.Hour})
	}
	return out
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}
