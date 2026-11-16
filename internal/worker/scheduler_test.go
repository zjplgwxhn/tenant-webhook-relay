package worker

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"webhook-relay/internal/delayed"
	"webhook-relay/internal/state"
	"webhook-relay/internal/tenant"
)

func TestSchedulerPollOnce(t *testing.T) {
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { c.Close() })

	st := state.NewRedisTaskStore(c)
	d := delayed.NewRedisDelayed(c)

	if err := st.Put(context.Background(), state.Task{ID: "t-1", Status: state.StatusRetryScheduled, IngestedAt: time.Now()}, 25*time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := d.Schedule(context.Background(), "t1", "t-1", time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}

	s := NewScheduler([]tenant.Tenant{{ID: "t1"}}, d, st, time.Second)
	s.pollOnce(context.Background())

	got, _ := st.Get(context.Background(), "t-1")
	if got.Status != state.StatusEnqueued {
		t.Fatalf("status = %q, want enqueued", got.Status)
	}
	if n := c.LLen(context.Background(), "queue:t1").Val(); n != 1 {
		t.Fatalf("queue depth = %d, want 1", n)
	}
}
