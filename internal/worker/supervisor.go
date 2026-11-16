package worker

import (
	"context"
	"net/http"
	"time"

	"webhook-relay/internal/backoff"
	"webhook-relay/internal/delayed"
	"webhook-relay/internal/dlq"
	"webhook-relay/internal/drainrate"
	"webhook-relay/internal/limiter"
	"webhook-relay/internal/metrics"
	"webhook-relay/internal/queue"
	"webhook-relay/internal/state"
	"webhook-relay/internal/tenant"
)

// Supervisor 为每个租户注册并发上限并拉起 MaxConcurrency 个 worker goroutine。
type Supervisor struct {
	tenants  []tenant.Tenant
	queue    queue.Queue
	tasks    state.TaskStore
	sm       state.StateMachine
	limiter  *limiter.Limiter
	client   *http.Client
	backoff  backoff.Backoff
	delayed  delayed.DelayedQueue
	dlq      dlq.DeadLetterQueue
	leaseDur time.Duration
	m        *metrics.Metrics
	dr       *drainrate.Estimator
}

func NewSupervisor(tenants []tenant.Tenant, q queue.Queue, tasks state.TaskStore, sm state.StateMachine, lim *limiter.Limiter, client *http.Client, b backoff.Backoff, d delayed.DelayedQueue, dlq dlq.DeadLetterQueue, leaseDur time.Duration, m *metrics.Metrics, dr *drainrate.Estimator) *Supervisor {
	return &Supervisor{
		tenants:  tenants,
		queue:    q,
		tasks:    tasks,
		sm:       sm,
		limiter:  lim,
		client:   client,
		backoff:  b,
		delayed:  d,
		dlq:      dlq,
		leaseDur: leaseDur,
		m:        m,
		dr:       dr,
	}
}

// Start 注册所有租户的并发槽位，再为每个租户拉起 worker 池。
func (s *Supervisor) Start(ctx context.Context) {
	for _, t := range s.tenants {
		s.limiter.Register(t.ID, t.MaxConcurrency)
	}
	for _, t := range s.tenants {
		for i := 0; i < t.MaxConcurrency; i++ {
			w := New(t, s.queue, s.tasks, s.sm, s.limiter, s.client, s.backoff, s.delayed, s.dlq, s.m, s.dr)
			w.leaseDur = s.leaseDur
			w.renewEvery = s.leaseDur / 3
			go w.Run(ctx)
		}
	}
}
