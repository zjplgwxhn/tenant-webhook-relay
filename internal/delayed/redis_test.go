package delayed

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"webhook-relay/internal/state"
)

func newTestDelayed(t *testing.T) (*RedisDelayed, *redis.Client) {
	s := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { c.Close() })
	return NewRedisDelayed(c), c
}

func TestScheduleThenPollDue(t *testing.T) {
	d, c := newTestDelayed(t)
	if err := d.Schedule(context.Background(), "t1", "a", time.Unix(100, 0)); err != nil {
		t.Fatal(err)
	}
	ids, err := d.Poll(context.Background(), "t1", time.Unix(200, 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != state.TaskID("a") {
		t.Fatalf("ids = %v, want [a]", ids)
	}
	if n := c.LLen(context.Background(), "queue:t1").Val(); n != 1 {
		t.Fatalf("queue depth = %d, want 1", n)
	}
}

func TestPollFutureNotReturned(t *testing.T) {
	d, c := newTestDelayed(t)
	if err := d.Schedule(context.Background(), "t1", "a", time.Unix(300, 0)); err != nil {
		t.Fatal(err)
	}
	ids, err := d.Poll(context.Background(), "t1", time.Unix(200, 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 0 {
		t.Fatalf("ids = %v, want empty", ids)
	}
	if n := c.LLen(context.Background(), "queue:t1").Val(); n != 0 {
		t.Fatalf("queue depth = %d, want 0", n)
	}
}

func TestPollAtomic(t *testing.T) {
	d, c := newTestDelayed(t)
	d.Schedule(context.Background(), "t1", "a", time.Unix(100, 0))
	d.Schedule(context.Background(), "t1", "b", time.Unix(100, 0))
	ids, err := d.Poll(context.Background(), "t1", time.Unix(200, 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 {
		t.Fatalf("ids = %v, want 2 items", ids)
	}
	if n := c.ZCard(context.Background(), "delay:t1").Val(); n != 0 {
		t.Fatalf("delay size = %d, want 0", n)
	}
	if n := c.LLen(context.Background(), "queue:t1").Val(); n != 2 {
		t.Fatalf("queue depth = %d, want 2", n)
	}
}
