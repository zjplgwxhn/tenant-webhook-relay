package queue

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"webhook-relay/internal/state"
)

func newTestQueue(t *testing.T, cap int64) *RedisQueue {
	s := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { c.Close() })
	return NewRedisQueue(c, cap)
}

func TestEnqueueUnderCapacity(t *testing.T) {
	q := newTestQueue(t, 2)
	if err := q.Enqueue(context.Background(), "t1", "a"); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(context.Background(), "t1", "b"); err != nil {
		t.Fatal(err)
	}
	d, err := q.Depth(context.Background(), "t1")
	if err != nil {
		t.Fatal(err)
	}
	if d != 2 {
		t.Fatalf("depth = %d, want 2", d)
	}
}

func TestEnqueueAtCapacityReturnsFull(t *testing.T) {
	q := newTestQueue(t, 2)
	if err := q.Enqueue(context.Background(), "t1", "a"); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(context.Background(), "t1", "b"); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(context.Background(), "t1", "c"); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("got %v, want ErrQueueFull", err)
	}
	d, _ := q.Depth(context.Background(), "t1")
	if d != 2 {
		t.Fatalf("depth = %d, want 2", d)
	}
}

func TestDepthEmpty(t *testing.T) {
	q := newTestQueue(t, 2)
	d, err := q.Depth(context.Background(), "t1")
	if err != nil {
		t.Fatal(err)
	}
	if d != 0 {
		t.Fatalf("depth = %d, want 0", d)
	}
}

func TestEnqueueNeverExceedsCapacity(t *testing.T) {
	q := newTestQueue(t, 3)
	var ok, full int
	for i := 0; i < 10; i++ {
		id := state.TaskID(fmt.Sprintf("id-%d", i))
		switch err := q.Enqueue(context.Background(), "t1", id); {
		case err == nil:
			ok++
		case errors.Is(err, ErrQueueFull):
			full++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if ok != 3 || full != 7 {
		t.Fatalf("ok=%d full=%d, want 3/7", ok, full)
	}
	d, _ := q.Depth(context.Background(), "t1")
	if d != 3 {
		t.Fatalf("depth = %d, want 3", d)
	}
}
