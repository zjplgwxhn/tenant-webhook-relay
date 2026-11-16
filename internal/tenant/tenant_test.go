package tenant

import (
	"context"
	"errors"
	"testing"
)

func TestStaticGetFound(t *testing.T) {
	s := NewStatic(Tenant{ID: "t1", Destination: "http://example.com/hook", MaxConcurrency: 8})
	got, err := s.Get(context.Background(), "t1")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "t1" || got.Destination != "http://example.com/hook" || got.MaxConcurrency != 8 {
		t.Fatalf("got %+v", got)
	}
}

func TestStaticGetNotFound(t *testing.T) {
	s := NewStatic(Tenant{ID: "t1", Destination: "http://example.com/hook", MaxConcurrency: 8})
	if _, err := s.Get(context.Background(), "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}
