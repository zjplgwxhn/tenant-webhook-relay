package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMetricsIncrement(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	m.IngestTotal.WithLabelValues("t1", "202").Inc()
	m.Ingest429.WithLabelValues("t1").Inc()
	m.DeliverySuccess.WithLabelValues("t1").Inc()
	m.LeaseExpired.Inc()
	m.StaleWriteRejected.Inc()
	m.GlobalAcquireTimeout.Inc()
	m.RedisUp.Set(1)

	if got := testutil.ToFloat64(m.IngestTotal.WithLabelValues("t1", "202")); got != 1 {
		t.Fatalf("ingest_total = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.Ingest429.WithLabelValues("t1")); got != 1 {
		t.Fatalf("ingest_429 = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.DeliverySuccess.WithLabelValues("t1")); got != 1 {
		t.Fatalf("delivery_success = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.LeaseExpired); got != 1 {
		t.Fatalf("lease_expired = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.StaleWriteRejected); got != 1 {
		t.Fatalf("stale_write = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.GlobalAcquireTimeout); got != 1 {
		t.Fatalf("global_timeout = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.RedisUp); got != 1 {
		t.Fatalf("redis_up = %v, want 1", got)
	}
}
