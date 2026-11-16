package ingest

import (
	"net/http"
	"testing"

	"webhook-relay/internal/idempotency"
	"webhook-relay/internal/tenant"
)

func TestServeHTTPUnknownTenant404(t *testing.T) {
	q := &fakeQueue{}
	s := &fakeTaskStore{}
	reg := tenant.NewStatic(tenant.Tenant{ID: "t1", Destination: "http://example.com", MaxConcurrency: 1})
	h := NewHandler(idempotency.NewKeyGenerator(), q, s, reg, nil, nil)

	rr := postEvent(h, "unknown", `{"payload":{}}`)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("code = %d, want 404", rr.Code)
	}
}
