package queue

import (
	"context"
	"testing"

	"webhook-relay/internal/state"
)

func TestRequeueMovesBackToQueue(t *testing.T) {
	q := newTestQueue(t, 10)
	if err := q.Enqueue(context.Background(), "t1", "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Dequeue(context.Background(), "t1"); err != nil {
		t.Fatal(err)
	}
	if n := q.client.LLen(context.Background(), "inflight:t1").Val(); n != 1 {
		t.Fatalf("inflight = %d, want 1", n)
	}
	if err := q.Requeue(context.Background(), "t1", "a"); err != nil {
		t.Fatal(err)
	}
	if d, _ := q.Depth(context.Background(), "t1"); d != 1 {
		t.Fatalf("queue depth = %d, want 1", d)
	}
	if n := q.client.LLen(context.Background(), "inflight:t1").Val(); n != 0 {
		t.Fatalf("inflight = %d, want 0", n)
	}
	if id, err := q.Dequeue(context.Background(), "t1"); err != nil || id != state.TaskID("a") {
		t.Fatalf("dequeued %q err=%v, want a", id, err)
	}
}
