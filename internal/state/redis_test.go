package state

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"webhook-relay/internal/idempotency"
)

func newTestStore(t *testing.T) *RedisTaskStore {
	s := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { c.Close() })
	return NewRedisTaskStore(c)
}

func TestPutGetRoundTrip(t *testing.T) {
	st := newTestStore(t)
	want := Task{
		ID:             "t-1",
		TenantID:       "t1",
		IdempotencyKey: idempotency.Key("t1:e1"),
		Payload:        []byte(`{"a":1}`),
		ContentType:    "application/json",
		Status:         StatusEnqueued,
		Attempt:        0,
	}
	if err := st.Put(context.Background(), want, 25*time.Hour); err != nil {
		t.Fatal(err)
	}
	got, err := st.Get(context.Background(), "t-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != want.ID || got.IdempotencyKey != want.IdempotencyKey ||
		got.Status != want.Status || got.Attempt != want.Attempt ||
		string(got.Payload) != string(want.Payload) || got.ContentType != want.ContentType {
		t.Fatalf("round trip mismatch: %+v", got)
	}
}

func TestGetMissingReturnsErrNotFound(t *testing.T) {
	st := newTestStore(t)
	if _, err := st.Get(context.Background(), "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}

func TestPutSetsTTL(t *testing.T) {
	st := newTestStore(t)
	if err := st.Put(context.Background(), Task{ID: "t-1", Status: StatusEnqueued}, 25*time.Hour); err != nil {
		t.Fatal(err)
	}
	ttl := st.client.TTL(context.Background(), "task:t-1").Val()
	if ttl <= 0 || ttl > 25*time.Hour {
		t.Fatalf("TTL out of range: %v", ttl)
	}
}
