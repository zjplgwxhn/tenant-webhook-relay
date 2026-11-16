package ingest

import (
	"testing"
	"time"
)

func TestComputeNormal(t *testing.T) {
	c := ClampingRetryAfter{Min: time.Second, Max: time.Minute}
	if got := c.Compute(10, 1.0); got != 10*time.Second {
		t.Fatalf("got %v", got)
	}
}

func TestComputeFractional(t *testing.T) {
	c := ClampingRetryAfter{Min: time.Second, Max: time.Minute}
	if got := c.Compute(10, 2.0); got != 5*time.Second {
		t.Fatalf("got %v", got)
	}
}

func TestComputeFloorToOne(t *testing.T) {
	c := ClampingRetryAfter{Min: time.Second, Max: time.Minute}
	if got := c.Compute(0, 1.0); got != time.Second {
		t.Fatalf("got %v", got)
	}
}

func TestComputeCapsAtMax(t *testing.T) {
	c := ClampingRetryAfter{Min: time.Second, Max: time.Minute}
	if got := c.Compute(1000, 1.0); got != time.Minute {
		t.Fatalf("got %v", got)
	}
}

func TestComputeZeroRateNoPanic(t *testing.T) {
	c := ClampingRetryAfter{Min: time.Second, Max: time.Minute}
	if got := c.Compute(10, 0.0); got != 10*time.Second {
		t.Fatalf("got %v", got)
	}
}
