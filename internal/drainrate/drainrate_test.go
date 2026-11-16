package drainrate

import (
	"testing"
	"time"
)

func TestRateColdStart(t *testing.T) {
	e := New(0.2)
	if got := e.Rate("t1"); got != 1.0 {
		t.Fatalf("cold start rate = %v, want 1.0", got)
	}
}

func TestObserveRaisesRate(t *testing.T) {
	e := New(0.2)
	for i := 0; i < 5; i++ {
		e.Observe("t1")
		time.Sleep(5 * time.Millisecond)
	}
	if got := e.Rate("t1"); got <= 1.0 {
		t.Fatalf("rate = %v, want > 1.0 after observes", got)
	}
}

func TestRatePerTenant(t *testing.T) {
	e := New(0.2)
	e.Observe("t1")
	if got := e.Rate("t2"); got != 1.0 {
		t.Fatalf("t2 rate = %v, want 1.0 (isolated)", got)
	}
}
