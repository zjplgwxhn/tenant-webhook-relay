package reaper

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"webhook-relay/internal/state"
	"webhook-relay/internal/tenant"
)

func newTestReaper(t *testing.T) (*Reaper, *redis.Client, *state.RedisTaskStore) {
	s := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { c.Close() })
	st := state.NewRedisTaskStore(c)
	r := New([]tenant.Tenant{{ID: "t1"}}, c, time.Second, nil)
	return r, c, st
}

func TestReclaimExpired(t *testing.T) {
	r, c, st := newTestReaper(t)
	if err := st.Put(context.Background(), state.Task{ID: "t-1", Status: state.StatusInFlight, FencingToken: 1, LeaseExpiresAt: time.Now().Add(-time.Second)}, 25*time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := c.LPush(context.Background(), "inflight:t1", "t-1").Err(); err != nil {
		t.Fatal(err)
	}

	r.sweepOnce(context.Background())

	got, _ := st.Get(context.Background(), "t-1")
	if got.Status != state.StatusEnqueued {
		t.Fatalf("status = %q, want enqueued", got.Status)
	}
	if got.FencingToken != 2 {
		t.Fatalf("token = %d, want 2", got.FencingToken)
	}
	if n := c.LLen(context.Background(), "inflight:t1").Val(); n != 0 {
		t.Fatalf("inflight = %d, want 0", n)
	}
	if n := c.LLen(context.Background(), "queue:t1").Val(); n != 1 {
		t.Fatalf("queue = %d, want 1", n)
	}
}

func TestReclaimLiveLeaseNotTouched(t *testing.T) {
	r, c, st := newTestReaper(t)
	if err := st.Put(context.Background(), state.Task{ID: "t-1", Status: state.StatusInFlight, FencingToken: 1, LeaseExpiresAt: time.Now().Add(time.Hour)}, 25*time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := c.LPush(context.Background(), "inflight:t1", "t-1").Err(); err != nil {
		t.Fatal(err)
	}

	r.sweepOnce(context.Background())

	got, _ := st.Get(context.Background(), "t-1")
	if got.Status != state.StatusInFlight {
		t.Fatalf("status = %q, want in_flight", got.Status)
	}
	if n := c.LLen(context.Background(), "inflight:t1").Val(); n != 1 {
		t.Fatalf("inflight = %d, want 1", n)
	}
}

func TestReclaimEnqueuedStranded(t *testing.T) {
	r, c, st := newTestReaper(t)
	if err := st.Put(context.Background(), state.Task{ID: "t-1", Status: state.StatusEnqueued}, 25*time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := c.LPush(context.Background(), "inflight:t1", "t-1").Err(); err != nil {
		t.Fatal(err)
	}

	r.sweepOnce(context.Background())

	got, _ := st.Get(context.Background(), "t-1")
	if got.Status != state.StatusEnqueued {
		t.Fatalf("status = %q, want enqueued", got.Status)
	}
	if got.FencingToken != 1 {
		t.Fatalf("token = %d, want 1", got.FencingToken)
	}
	if n := c.LLen(context.Background(), "queue:t1").Val(); n != 1 {
		t.Fatalf("queue = %d, want 1", n)
	}
}

func TestReclaimRetryScheduledStranded(t *testing.T) {
	r, c, st := newTestReaper(t)
	if err := st.Put(context.Background(), state.Task{ID: "t-1", Status: state.StatusRetryScheduled}, 25*time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := c.LPush(context.Background(), "inflight:t1", "t-1").Err(); err != nil {
		t.Fatal(err)
	}

	r.sweepOnce(context.Background())

	got, _ := st.Get(context.Background(), "t-1")
	if got.Status != state.StatusEnqueued {
		t.Fatalf("status = %q, want enqueued", got.Status)
	}
	if n := c.LLen(context.Background(), "queue:t1").Val(); n != 1 {
		t.Fatalf("queue = %d, want 1", n)
	}
}

func TestReclaimTerminalCleansUp(t *testing.T) {
	r, c, st := newTestReaper(t)
	if err := st.Put(context.Background(), state.Task{ID: "t-1", Status: state.StatusDelivered}, 25*time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := c.LPush(context.Background(), "inflight:t1", "t-1").Err(); err != nil {
		t.Fatal(err)
	}

	r.sweepOnce(context.Background())

	if n := c.LLen(context.Background(), "inflight:t1").Val(); n != 0 {
		t.Fatalf("inflight = %d, want 0", n)
	}
	if n := c.LLen(context.Background(), "queue:t1").Val(); n != 0 {
		t.Fatalf("queue = %d, want 0 (terminal must not be re-delivered)", n)
	}
}
