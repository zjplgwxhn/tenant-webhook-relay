package ingest

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"webhook-relay/internal/idempotency"
	"webhook-relay/internal/queue"
	"webhook-relay/internal/state"
)

type fakeQueue struct {
	enqueueErr error
	depth      int64
	enqueued   []state.TaskID
}

func (f *fakeQueue) Enqueue(ctx context.Context, tenantID string, id state.TaskID) error {
	if f.enqueueErr != nil {
		return f.enqueueErr
	}
	f.enqueued = append(f.enqueued, id)
	return nil
}

func (f *fakeQueue) Depth(ctx context.Context, tenantID string) (int64, error) {
	return f.depth, nil
}

func (f *fakeQueue) Dequeue(ctx context.Context, tenantID string) (state.TaskID, error) {
	return "", queue.ErrEmpty
}

func (f *fakeQueue) Ack(ctx context.Context, tenantID string, id state.TaskID) error {
	return nil
}

func (f *fakeQueue) Requeue(ctx context.Context, tenantID string, id state.TaskID) error {
	return nil
}

type fakeTaskStore struct {
	puts []state.Task
}

func (f *fakeTaskStore) Put(ctx context.Context, t state.Task, ttl time.Duration) error {
	f.puts = append(f.puts, t)
	return nil
}

func (f *fakeTaskStore) Get(ctx context.Context, id state.TaskID) (state.Task, error) {
	return state.Task{}, state.ErrNotFound
}

func postEvent(h *Handler, tenantID, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(body))
	if tenantID != "" {
		req.Header.Set("X-Tenant-ID", tenantID)
	}
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

func TestServeHTTPValidRequest202(t *testing.T) {
	q := &fakeQueue{}
	s := &fakeTaskStore{}
	h := NewHandler(idempotency.NewKeyGenerator(), q, s, nil, nil, nil)

	rr := postEvent(h, "t1", `{"producer_key":"e1","payload":{"a":1}}`)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("code = %d, want 202", rr.Code)
	}
	if len(s.puts) != 1 {
		t.Fatalf("stored %d tasks, want 1", len(s.puts))
	}
	got := s.puts[0]
	if got.IdempotencyKey != idempotency.Key("t1:e1") {
		t.Fatalf("idempotency key = %q, want t1:e1", got.IdempotencyKey)
	}
	if got.Status != state.StatusEnqueued {
		t.Fatalf("status = %q, want enqueued", got.Status)
	}
	if got.IngestedAt.IsZero() {
		t.Fatal("IngestedAt not set")
	}
	if len(q.enqueued) != 1 || q.enqueued[0] != got.ID {
		t.Fatalf("enqueued = %v, want [%s]", q.enqueued, got.ID)
	}
}

func TestServeHTTPQueueFull429RetryAfter(t *testing.T) {
	q := &fakeQueue{enqueueErr: queue.ErrQueueFull, depth: 10}
	s := &fakeTaskStore{}
	h := NewHandler(idempotency.NewKeyGenerator(), q, s, nil, nil, nil)

	rr := postEvent(h, "t1", `{"producer_key":"e1","payload":{}}`)

	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("code = %d, want 429", rr.Code)
	}
	if got := rr.Header().Get("Retry-After"); got != "10" {
		t.Fatalf("Retry-After = %q, want 10", got)
	}
}

func TestServeHTTPSameProducerKeySameIdempotencyKey(t *testing.T) {
	q := &fakeQueue{}
	s := &fakeTaskStore{}
	h := NewHandler(idempotency.NewKeyGenerator(), q, s, nil, nil, nil)

	postEvent(h, "t1", `{"producer_key":"e1","payload":{}}`)
	postEvent(h, "t1", `{"producer_key":"e1","payload":{"x":2}}`)

	if len(s.puts) != 2 {
		t.Fatalf("stored %d tasks, want 2", len(s.puts))
	}
	if s.puts[0].IdempotencyKey != s.puts[1].IdempotencyKey {
		t.Fatalf("keys differ: %q vs %q", s.puts[0].IdempotencyKey, s.puts[1].IdempotencyKey)
	}
}

func TestServeHTTPWithoutProducerKeyUsesUUID(t *testing.T) {
	keys := &idempotency.KeyGenerator{NewID: func() string { return "uuid-1" }}
	q := &fakeQueue{}
	s := &fakeTaskStore{}
	h := NewHandler(keys, q, s, nil, nil, nil)

	postEvent(h, "t1", `{"payload":{}}`)

	if len(s.puts) != 1 {
		t.Fatalf("stored %d tasks, want 1", len(s.puts))
	}
	if got := s.puts[0].IdempotencyKey; got != idempotency.Key("uuid-1") {
		t.Fatalf("idempotency key = %q, want uuid-1", got)
	}
}

func TestServeHTTPMissingTenant400(t *testing.T) {
	q := &fakeQueue{}
	s := &fakeTaskStore{}
	h := NewHandler(idempotency.NewKeyGenerator(), q, s, nil, nil, nil)

	rr := postEvent(h, "", `{"payload":{}}`)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("code = %d, want 400", rr.Code)
	}
}

func TestServeHTTPInvalidJSON400(t *testing.T) {
	q := &fakeQueue{}
	s := &fakeTaskStore{}
	h := NewHandler(idempotency.NewKeyGenerator(), q, s, nil, nil, nil)

	rr := postEvent(h, "t1", `{not json`)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("code = %d, want 400", rr.Code)
	}
}

func TestServeHTTPRedisError503(t *testing.T) {
	q := &fakeQueue{enqueueErr: errors.New("redis down")}
	s := &fakeTaskStore{}
	h := NewHandler(idempotency.NewKeyGenerator(), q, s, nil, nil, nil)

	rr := postEvent(h, "t1", `{"payload":{}}`)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("code = %d, want 503", rr.Code)
	}
}
