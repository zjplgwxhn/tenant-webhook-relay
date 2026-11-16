package dlq

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"webhook-relay/internal/state"
)

func newTestDLQ(t *testing.T) (*RedisDLQ, *redis.Client) {
	s := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { c.Close() })
	return NewRedisDLQ(c), c
}

func TestDeadMovesToDLQ(t *testing.T) {
	d, c := newTestDLQ(t)
	if err := c.LPush(context.Background(), "inflight:t1", "a").Err(); err != nil {
		t.Fatal(err)
	}
	if err := d.Dead(context.Background(), "t1", state.TaskID("a")); err != nil {
		t.Fatal(err)
	}
	if n := c.LLen(context.Background(), "inflight:t1").Val(); n != 0 {
		t.Fatalf("inflight = %d, want 0", n)
	}
	if n := c.LLen(context.Background(), "dlq:t1").Val(); n != 1 {
		t.Fatalf("dlq depth = %d, want 1", n)
	}
}
