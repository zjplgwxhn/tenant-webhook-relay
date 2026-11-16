package state

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestScheduleRetrySetsFields(t *testing.T) {
	st := newTestStore(t)
	if err := st.Put(context.Background(), Task{ID: "t-1", Status: StatusInFlight, Attempt: 1, FencingToken: 2}, 25*time.Hour); err != nil {
		t.Fatal(err)
	}
	at := time.Unix(1700000000, 0)
	if err := st.ScheduleRetry(context.Background(), "t-1", 2, at, 2); err != nil {
		t.Fatal(err)
	}
	got, err := st.Get(context.Background(), "t-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != StatusRetryScheduled {
		t.Fatalf("status = %q, want retry_scheduled", got.Status)
	}
	if got.Attempt != 2 {
		t.Fatalf("attempt = %d, want 2", got.Attempt)
	}
	if got.NextRetryAt.UnixNano() != at.UnixNano() {
		t.Fatalf("next_retry_at = %v, want %v", got.NextRetryAt, at)
	}
}

func TestScheduleRetryConflict(t *testing.T) {
	st := newTestStore(t)
	if err := st.Put(context.Background(), Task{ID: "t-1", Status: StatusEnqueued}, 25*time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := st.ScheduleRetry(context.Background(), "t-1", 1, time.Now(), 0); !errors.Is(err, ErrConflict) {
		t.Fatalf("got %v, want ErrConflict", err)
	}
}

func TestScheduleRetryStaleToken(t *testing.T) {
	st := newTestStore(t)
	if err := st.Put(context.Background(), Task{ID: "t-1", Status: StatusInFlight, FencingToken: 2}, 25*time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := st.ScheduleRetry(context.Background(), "t-1", 2, time.Now(), 5); !errors.Is(err, ErrConflict) {
		t.Fatalf("got %v, want ErrConflict", err)
	}
}

func TestIngestedAtRoundTrip(t *testing.T) {
	st := newTestStore(t)
	ing := time.Unix(1700000001, 123)
	if err := st.Put(context.Background(), Task{ID: "t-1", Status: StatusEnqueued, IngestedAt: ing}, 25*time.Hour); err != nil {
		t.Fatal(err)
	}
	got, err := st.Get(context.Background(), "t-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.IngestedAt.UnixNano() != ing.UnixNano() {
		t.Fatalf("ingested_at = %v, want %v", got.IngestedAt, ing)
	}
}
