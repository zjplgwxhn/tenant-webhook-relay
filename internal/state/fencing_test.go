package state

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestAcquireInFlight(t *testing.T) {
	st := newTestStore(t)
	if err := st.Put(context.Background(), Task{ID: "t-1", Status: StatusEnqueued}, 25*time.Hour); err != nil {
		t.Fatal(err)
	}
	token, err := st.AcquireInFlight(context.Background(), "t-1", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if token != 1 {
		t.Fatalf("token = %d, want 1", token)
	}
	got, _ := st.Get(context.Background(), "t-1")
	if got.Status != StatusInFlight {
		t.Fatalf("status = %q, want in_flight", got.Status)
	}
	if !got.LeaseExpiresAt.After(time.Now()) {
		t.Fatalf("lease not set: %v", got.LeaseExpiresAt)
	}
}

func TestAcquireInFlightFromRetryScheduled(t *testing.T) {
	st := newTestStore(t)
	if err := st.Put(context.Background(), Task{ID: "t-1", Status: StatusRetryScheduled}, 25*time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AcquireInFlight(context.Background(), "t-1", 30*time.Second); err != nil {
		t.Fatalf("retry_scheduled should be acquirable: %v", err)
	}
}

func TestAcquireInFlightConflict(t *testing.T) {
	st := newTestStore(t)
	if err := st.Put(context.Background(), Task{ID: "t-1", Status: StatusDelivered}, 25*time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AcquireInFlight(context.Background(), "t-1", 30*time.Second); !errors.Is(err, ErrConflict) {
		t.Fatalf("got %v, want ErrConflict", err)
	}
}

func TestRenewMatch(t *testing.T) {
	st := newTestStore(t)
	exp := time.Now().Add(5 * time.Second)
	if err := st.Put(context.Background(), Task{ID: "t-1", Status: StatusInFlight, FencingToken: 7, LeaseExpiresAt: exp}, 25*time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := st.Renew(context.Background(), "t-1", 7, 30*time.Second); err != nil {
		t.Fatal(err)
	}
	got, _ := st.Get(context.Background(), "t-1")
	if !got.LeaseExpiresAt.After(exp) {
		t.Fatalf("lease not extended: %v <= %v", got.LeaseExpiresAt, exp)
	}
}

func TestRenewStale(t *testing.T) {
	st := newTestStore(t)
	if err := st.Put(context.Background(), Task{ID: "t-1", Status: StatusInFlight, FencingToken: 7}, 25*time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := st.Renew(context.Background(), "t-1", 3, 30*time.Second); !errors.Is(err, ErrConflict) {
		t.Fatalf("got %v, want ErrConflict", err)
	}
}
