package limiter

import (
	"context"
	"testing"
	"time"
)

func TestAcquireReleaseReuse(t *testing.T) {
	l := NewLimiter(10, 50*time.Millisecond, nil)
	l.Register("t1", 2)
	rel, err := l.Acquire(context.Background(), "t1")
	if err != nil {
		t.Fatal(err)
	}
	rel()
	rel2, err := l.Acquire(context.Background(), "t1")
	if err != nil {
		t.Fatalf("reacquire failed: %v", err)
	}
	rel2()
}

func TestGlobalTimeout(t *testing.T) {
	l := NewLimiter(1, 20*time.Millisecond, nil)
	l.Register("t1", 5)
	rel, err := l.Acquire(context.Background(), "t1")
	if err != nil {
		t.Fatal(err)
	}
	defer rel()
	if _, err := l.Acquire(context.Background(), "t1"); err == nil {
		t.Fatal("expected global timeout, got nil")
	}
}

func TestTenantIsolation(t *testing.T) {
	l := NewLimiter(10, 20*time.Millisecond, nil)
	l.Register("t1", 1)
	l.Register("t2", 1)
	rel1, err := l.Acquire(context.Background(), "t1")
	if err != nil {
		t.Fatal(err)
	}
	rel2, err := l.Acquire(context.Background(), "t2")
	if err != nil {
		t.Fatalf("t2 blocked by t1: %v", err)
	}
	rel1()
	rel2()
}

func TestRegisterZeroCapDefaultsToOne(t *testing.T) {
	l := NewLimiter(10, 50*time.Millisecond, nil)
	l.Register("t1", 0)
	rel, err := l.Acquire(context.Background(), "t1")
	if err != nil {
		t.Fatalf("acquire with zero cap: %v", err)
	}
	rel()
}
