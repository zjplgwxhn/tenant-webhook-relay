package metrics

import "github.com/prometheus/client_golang/prometheus"

// Metrics 是设计文档 §5 的全部 Prometheus collector。
type Metrics struct {
	IngestTotal              *prometheus.CounterVec
	Ingest429                *prometheus.CounterVec
	QueueCapacity            *prometheus.GaugeVec
	InFlight                 *prometheus.GaugeVec
	InFlightGlobal           prometheus.Gauge
	GlobalAcquireTimeout     prometheus.Counter
	DeliveryAttempts         *prometheus.CounterVec
	DeliverySuccess          *prometheus.CounterVec
	DeliveryPermanentFailure *prometheus.CounterVec
	RetryDelay               *prometheus.HistogramVec
	LeaseExpired             prometheus.Counter
	StaleWriteRejected       prometheus.Counter
	RedisUp                  prometheus.Gauge
}

func NewMetrics(reg *prometheus.Registry) *Metrics {
	m := &Metrics{
		IngestTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "webhook_ingest_total", Help: "Ingest requests by status.",
		}, []string{"tenant", "status"}),
		Ingest429: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "webhook_ingest_429_total", Help: "Ingest requests rejected with 429.",
		}, []string{"tenant"}),
		QueueCapacity: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "webhook_queue_capacity", Help: "Tenant queue capacity.",
		}, []string{"tenant"}),
		InFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "webhook_in_flight", Help: "In-flight deliveries per tenant.",
		}, []string{"tenant"}),
		InFlightGlobal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "webhook_in_flight_global", Help: "Global in-flight deliveries.",
		}),
		GlobalAcquireTimeout: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "webhook_global_acquire_timeout_total", Help: "Global slot acquisition timeouts.",
		}),
		DeliveryAttempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "webhook_delivery_attempts_total", Help: "Delivery attempts by downstream status.",
		}, []string{"tenant", "status_code"}),
		DeliverySuccess: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "webhook_delivery_success_total", Help: "Successful deliveries.",
		}, []string{"tenant"}),
		DeliveryPermanentFailure: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "webhook_delivery_permanent_failure_total", Help: "Permanent delivery failures (DLQ).",
		}, []string{"tenant"}),
		RetryDelay: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "webhook_retry_delay_seconds",
			Help:    "Retry delay in seconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{"tenant"}),
		LeaseExpired: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "webhook_lease_expired_total", Help: "Leases reclaimed by the reaper.",
		}),
		StaleWriteRejected: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "webhook_stale_write_rejected_total", Help: "Stale fencing-token writes rejected.",
		}),
		RedisUp: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "redis_up", Help: "Redis health (1=up, 0=down).",
		}),
	}
	reg.MustRegister(
		m.IngestTotal, m.Ingest429, m.QueueCapacity,
		m.InFlight, m.InFlightGlobal, m.GlobalAcquireTimeout,
		m.DeliveryAttempts, m.DeliverySuccess, m.DeliveryPermanentFailure,
		m.RetryDelay, m.LeaseExpired, m.StaleWriteRejected, m.RedisUp,
	)
	return m
}
