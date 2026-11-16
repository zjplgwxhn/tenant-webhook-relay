package queue

import (
	"context"
	"errors"
	"testing"

	"webhook-relay/internal/state"
)

func TestDequeueMovesToInflight(t *testing.T) {
	q := newTestQueue(t, 10)
	if err := q.Enqueue(context.Background(), "t1", "a"); err != nil {
		t.Fatal(err)
	}
	id, err := q.Dequeue(context.Background(), "t1")
	if err != nil {
		t.Fatal(err)
	}
	if id != state.TaskID("a") {
		t.Fatalf("dequeued %q, want a", id)
	}
	if d, _ := q.Depth(context.Background(), "t1"); d != 0 {
		t.Fatalf("queue depth = %d, want 0", d)
	}
	if n := q.client.LLen(context.Background(), "inflight:t1").Val(); n != 1 {
		t.Fatalf("inflight depth = %d, want 1", n)
	}
}

func TestDequeueEmptyReturnsErrEmpty(t *testing.T) {
	q := newTestQueue(t, 10)
	if _, err := q.Dequeue(context.Background(), "t1"); !errors.Is(err, ErrEmpty) {
		t.Fatalf("got %v, want ErrEmpty", err)
	}
}

func TestAckRemovesFromInflight(t *testing.T) {
	q := newTestQueue(t, 10)
	if err := q.Enqueue(context.Background(), "t1", "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Dequeue(context.Background(), "t1"); err != nil {
		t.Fatal(err)
	}
	if err := q.Ack(context.Background(), "t1", "a"); err != nil {
		t.Fatal(err)
	}
	if n := q.client.LLen(context.Background(), "inflight:t1").Val(); n != 0 {
		t.Fatalf("inflight depth = %d, want 0", n)
	}
}
