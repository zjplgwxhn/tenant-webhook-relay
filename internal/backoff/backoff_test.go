package backoff

import (
	"math/rand"
	"testing"
	"time"
)

func TestNextBoundedByCap(t *testing.T) {
	b := Backoff{Base: time.Second, Factor: 2.0, Cap: 60 * time.Second}
	rng := rand.New(rand.NewSource(1))
	for attempt := 0; attempt < 20; attempt++ {
		if d := b.Next(attempt, rng); d > b.Cap {
			t.Fatalf("attempt %d: %v > cap %v", attempt, d, b.Cap)
		}
	}
}

func TestNextZeroAttempt(t *testing.T) {
	b := Backoff{Base: time.Second, Factor: 2.0, Cap: 60 * time.Second}
	rng := rand.New(rand.NewSource(1))
	if d := b.Next(0, rng); d > b.Base {
		t.Fatalf("attempt 0: %v > base %v", d, b.Base)
	}
}

func TestNextRange(t *testing.T) {
	b := Backoff{Base: time.Second, Factor: 2.0, Cap: 60 * time.Second}
	rng := rand.New(rand.NewSource(42))
	max := 8 * time.Second // attempt=3: min(60s, 1s*2^3)
	for i := 0; i < 100; i++ {
		d := b.Next(3, rng)
		if d < 0 || d > max {
			t.Fatalf("got %v, want [0, %v]", d, max)
		}
	}
}

func TestNextDeterministic(t *testing.T) {
	b := Backoff{Base: time.Second, Factor: 2.0, Cap: 60 * time.Second}
	rng1 := rand.New(rand.NewSource(7))
	rng2 := rand.New(rand.NewSource(7))
	if b.Next(2, rng1) != b.Next(2, rng2) {
		t.Fatal("same seed should produce same delay")
	}
}
