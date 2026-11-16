package worker

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"webhook-relay/internal/backoff"
	"webhook-relay/internal/delayed"
	"webhook-relay/internal/dlq"
	"webhook-relay/internal/idempotency"
	"webhook-relay/internal/limiter"
	"webhook-relay/internal/queue"
	"webhook-relay/internal/state"
	"webhook-relay/internal/tenant"
)

type env struct {
	w       *Worker
	srv     *httptest.Server
	client  *redis.Client
	store   *state.RedisTaskStore
	queue   *queue.RedisQueue
	gotKey  *string
	gotBody *string
}

func newEnv(t *testing.T, statusCode int, retryAfter time.Duration, retryWindow time.Duration, hang ...time.Duration) *env {
	var h time.Duration
	if len(hang) > 0 {
		h = hang[0]
	}
	var gotKey, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("Idempotency-Key")
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		if h > 0 {
			time.Sleep(h)
		}
		if retryAfter > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(int(retryAfter/time.Second)))
		}
		w.WriteHeader(statusCode)
	}))
	t.Cleanup(srv.Close)

	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { c.Close() })

	st := state.NewRedisTaskStore(c)
	q := queue.NewRedisQueue(c, 10)
	d := delayed.NewRedisDelayed(c)
	dl := dlq.NewRedisDLQ(c)

	tn := tenant.Tenant{ID: "t1", Destination: srv.URL, MaxConcurrency: 1, RetryWindow: retryWindow}
	lim := limiter.NewLimiter(10, 50*time.Millisecond, nil)
	lim.Register("t1", 1)

	b := backoff.Backoff{Base: time.Second, Factor: 2.0, Cap: 60 * time.Second}
	w := New(tn, q, st, st, lim, &http.Client{Timeout: 20 * time.Second}, b, d, dl, nil, nil)
	return &env{w: w, srv: srv, client: c, store: st, queue: q, gotKey: &gotKey, gotBody: &gotBody}
}

func (e *env) enqueue(id, key, payload string, ingestedAt time.Time) {
	_ = e.store.Put(context.Background(), state.Task{
		ID:             state.TaskID(id),
		TenantID:       "t1",
		IdempotencyKey: idempotency.Key(key),
		Payload:        []byte(payload),
		ContentType:    "application/json",
		Status:         state.StatusEnqueued,
		IngestedAt:     ingestedAt,
	}, 25*time.Hour)
	_ = e.queue.Enqueue(context.Background(), "t1", state.TaskID(id))
}

func TestWorkerDeliversSuccess(t *testing.T) {
	e := newEnv(t, http.StatusOK, 0, 24*time.Hour)
	e.enqueue("t-1", "t1:e1", `{"a":1}`, time.Now())

	e.w.deliverOnce(context.Background())

	if *e.gotKey != "t1:e1" {
		t.Fatalf("Idempotency-Key = %q, want t1:e1", *e.gotKey)
	}
	if *e.gotBody != `{"a":1}` {
		t.Fatalf("body = %q, want %q", *e.gotBody, `{"a":1}`)
	}
	got, _ := e.store.Get(context.Background(), "t-1")
	if got.Status != state.StatusDelivered {
		t.Fatalf("status = %q, want delivered", got.Status)
	}
	if n := e.client.LLen(context.Background(), "inflight:t1").Val(); n != 0 {
		t.Fatalf("inflight = %d, want 0", n)
	}
}

func TestWorkerFailureSchedulesRetry(t *testing.T) {
	e := newEnv(t, http.StatusInternalServerError, 0, 24*time.Hour)
	e.enqueue("t-1", "t1:e1", `{"a":1}`, time.Now())

	e.w.deliverOnce(context.Background())

	got, _ := e.store.Get(context.Background(), "t-1")
	if got.Status != state.StatusRetryScheduled {
		t.Fatalf("status = %q, want retry_scheduled", got.Status)
	}
	if got.Attempt != 1 {
		t.Fatalf("attempt = %d, want 1", got.Attempt)
	}
	if n := e.client.ZCard(context.Background(), "delay:t1").Val(); n != 1 {
		t.Fatalf("delay size = %d, want 1", n)
	}
	if n := e.client.LLen(context.Background(), "inflight:t1").Val(); n != 0 {
		t.Fatalf("inflight = %d, want 0", n)
	}
}

func TestWorkerWindowExhaustedDead(t *testing.T) {
	e := newEnv(t, http.StatusInternalServerError, 0, time.Hour)
	e.enqueue("t-1", "t1:e1", `{"a":1}`, time.Now().Add(-2*time.Hour))

	e.w.deliverOnce(context.Background())

	got, _ := e.store.Get(context.Background(), "t-1")
	if got.Status != state.StatusDead {
		t.Fatalf("status = %q, want dead", got.Status)
	}
	if n := e.client.LLen(context.Background(), "dlq:t1").Val(); n != 1 {
		t.Fatalf("dlq depth = %d, want 1", n)
	}
}

func TestWorkerHonorsRetryAfter(t *testing.T) {
	e := newEnv(t, http.StatusTooManyRequests, 30*time.Second, 24*time.Hour)
	e.enqueue("t-1", "t1:e1", `{"a":1}`, time.Now())

	e.w.deliverOnce(context.Background())

	got, _ := e.store.Get(context.Background(), "t-1")
	if got.Status != state.StatusRetryScheduled {
		t.Fatalf("status = %q, want retry_scheduled", got.Status)
	}
	// next_retry_at 应约为 now + 30s（±5s 容差）
	lo, hi := time.Now().Add(25*time.Second), time.Now().Add(35*time.Second)
	if got.NextRetryAt.Before(lo) || got.NextRetryAt.After(hi) {
		t.Fatalf("next_retry_at = %v, want ~now+30s", got.NextRetryAt)
	}
}

func TestWorkerPoisonMessageSkips(t *testing.T) {
	e := newEnv(t, http.StatusOK, 0, 24*time.Hour)
	_ = e.queue.Enqueue(context.Background(), "t1", "ghost")

	e.w.deliverOnce(context.Background())

	if got := e.w.PoisonTotal(); got != 1 {
		t.Fatalf("poison total = %d, want 1", got)
	}
	if n := e.client.LLen(context.Background(), "inflight:t1").Val(); n != 0 {
		t.Fatalf("inflight = %d, want 0", n)
	}
}

func TestWorkerHealsRetryScheduled(t *testing.T) {
	e := newEnv(t, http.StatusOK, 0, 24*time.Hour)
	// 直接放一个 retry_scheduled 状态的任务（模拟 scheduler 崩溃残留）。
	_ = e.store.Put(context.Background(), state.Task{
		ID:             "t-1",
		TenantID:       "t1",
		IdempotencyKey: idempotency.Key("t1:e1"),
		Payload:        []byte(`{"a":1}`),
		ContentType:    "application/json",
		Status:         state.StatusRetryScheduled,
		IngestedAt:     time.Now(),
	}, 25*time.Hour)
	_ = e.queue.Enqueue(context.Background(), "t1", "t-1")

	e.w.deliverOnce(context.Background())

	got, _ := e.store.Get(context.Background(), "t-1")
	if got.Status != state.StatusDelivered {
		t.Fatalf("status = %q, want delivered", got.Status)
	}
}

func TestWorkerHeartbeatRenews(t *testing.T) {
	// 下游挂起 250ms；租约 100ms、续租 30ms。心跳应让租约在投递期间保持续。
	e := newEnv(t, http.StatusOK, 0, 24*time.Hour, 250*time.Millisecond)
	e.w.leaseDur = 100 * time.Millisecond
	e.w.renewEvery = 30 * time.Millisecond
	e.enqueue("t-1", "t1:e1", `{"a":1}`, time.Now())

	e.w.deliverOnce(context.Background())

	got, _ := e.store.Get(context.Background(), "t-1")
	if got.Status != state.StatusDelivered {
		t.Fatalf("status = %q, want delivered", got.Status)
	}
	if !got.LeaseExpiresAt.After(time.Now()) {
		t.Fatalf("lease not renewed: %v", got.LeaseExpiresAt)
	}
}
