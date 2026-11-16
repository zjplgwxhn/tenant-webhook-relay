package idempotency

import "testing"

func TestGenerateUsesProducerKey(t *testing.T) {
	g := &KeyGenerator{NewID: func() string { return "uuid-1" }}
	if got := g.Generate("t1", "e1"); got != Key("t1:e1") {
		t.Fatalf("got %q, want %q", got, "t1:e1")
	}
}

func TestGenerateFallsBackToNewID(t *testing.T) {
	g := &KeyGenerator{NewID: func() string { return "uuid-1" }}
	if got := g.Generate("t1", ""); got != Key("uuid-1") {
		t.Fatalf("got %q, want %q", got, "uuid-1")
	}
}

func TestGenerateDeterministicAcrossCalls(t *testing.T) {
	g := NewKeyGenerator()
	if a, b := g.Generate("t1", "e1"), g.Generate("t1", "e1"); a != b {
		t.Fatalf("same input produced %q and %q", a, b)
	}
}
