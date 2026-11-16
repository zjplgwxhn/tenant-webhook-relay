package worker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"webhook-relay/internal/backoff"
	"webhook-relay/internal/delayed"
	"webhook-relay/internal/dlq"
	"webhook-relay/internal/limiter"
	"webhook-relay/internal/queue"
	"webhook-relay/internal/state"
	"webhook-relay/internal/tenant"
)

func TestWorkerStaleApplyDoesNotAck(t *testing.T) {
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { c.Close() })

	st := state.NewRedisTaskStore(c)
	q := queue.NewRedisQueue(c, 10)
	d := delayed.NewRedisDelayed(c)
	dl := dlq.NewRedisDLQ(c)

	var bumpOnce sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 模拟 reaper 在投递期间接管：bump fencing token。
		bumpOnce.Do(func() {
			_ = c.HIncrBy(context.Background(), "task:t-1", "fencing_token", 1).Err()
		})
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	tn := tenant.Tenant{ID: "t1", Destination: srv.URL, MaxConcurrency: 1, RetryWindow: 24 * time.Hour}
	lim := limiter.NewLimiter(10, 50*time.Millisecond, nil)
	lim.Register("t1", 1)

	b := backoff.Backoff{Base: time.Second, Factor: 2.0, Cap: 60 * time.Second}
	w := New(tn, q, st, st, lim, &http.Client{Timeout: 20 * time.Second}, b, d, dl, nil, nil)

	_ = st.Put(context.Background(), state.Task{
		ID: "t-1", TenantID: "t1", IdempotencyKey: "t1:e1",
		Payload: []byte(`{"a":1}`), ContentType: "application/json",
		Status: state.StatusEnqueued, IngestedAt: time.Now(),
	}, 25*time.Hour)
	_ = q.Enqueue(context.Background(), "t1", "t-1")

	w.deliverOnce(context.Background())

	// 旧 token 的 Apply 被拒，worker 不得再 Ack（否则会删除新 worker 的 inflight 项）。
	if n := c.LLen(context.Background(), "inflight:t1").Val(); n != 1 {
		t.Fatalf("inflight = %d, want 1 (stale worker must not Ack)", n)
	}
	got, _ := st.Get(context.Background(), "t-1")
	if got.Status != state.StatusInFlight {
		t.Fatalf("status = %q, want in_flight (stale write rejected)", got.Status)
	}
}
