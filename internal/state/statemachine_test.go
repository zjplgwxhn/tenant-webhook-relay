package state

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestApplyValidTransition(t *testing.T) {
	st := newTestStore(t)
	if err := st.Put(context.Background(), Task{ID: "t-1", Status: StatusEnqueued}, 25*time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := st.Apply(context.Background(), "t-1", Transition{From: StatusEnqueued, To: StatusInFlight}, 0); err != nil {
		t.Fatal(err)
	}
	got, _ := st.Get(context.Background(), "t-1")
	if got.Status != StatusInFlight {
		t.Fatalf("status = %q, want in_flight", got.Status)
	}
}

func TestApplyWrongFromConflict(t *testing.T) {
	st := newTestStore(t)
	if err := st.Put(context.Background(), Task{ID: "t-1", Status: StatusEnqueued}, 25*time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := st.Apply(context.Background(), "t-1", Transition{From: StatusInFlight, To: StatusDelivered}, 0); !errors.Is(err, ErrConflict) {
		t.Fatalf("got %v, want ErrConflict", err)
	}
}

func TestApplyTerminalRejected(t *testing.T) {
	st := newTestStore(t)
	if err := st.Put(context.Background(), Task{ID: "t-1", Status: StatusDelivered}, 25*time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := st.Apply(context.Background(), "t-1", Transition{From: StatusEnqueued, To: StatusInFlight}, 0); !errors.Is(err, ErrConflict) {
		t.Fatalf("got %v, want ErrConflict", err)
	}
}

func TestApplyStaleTokenRejected(t *testing.T) {
	st := newTestStore(t)
	if err := st.Put(context.Background(), Task{ID: "t-1", Status: StatusInFlight, FencingToken: 5}, 25*time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := st.Apply(context.Background(), "t-1", Transition{From: StatusInFlight, To: StatusDelivered}, 3); !errors.Is(err, ErrConflict) {
		t.Fatalf("got %v, want ErrConflict", err)
	}
	got, _ := st.Get(context.Background(), "t-1")
	if got.Status != StatusInFlight {
		t.Fatalf("status = %q, want unchanged in_flight", got.Status)
	}
}

func TestApplyTokenMatch(t *testing.T) {
	st := newTestStore(t)
	if err := st.Put(context.Background(), Task{ID: "t-1", Status: StatusInFlight, FencingToken: 5}, 25*time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := st.Apply(context.Background(), "t-1", Transition{From: StatusInFlight, To: StatusDelivered}, 5); err != nil {
		t.Fatalf("token match should succeed: %v", err)
	}
}
