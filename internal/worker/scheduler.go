package worker

import (
	"context"
	"log"
	"time"

	"webhook-relay/internal/delayed"
	"webhook-relay/internal/state"
	"webhook-relay/internal/tenant"
)

// Scheduler 周期轮询各租户的 delay 集合，把到期任务搬回 queue 并转 enqueued。
type Scheduler struct {
	tenants  []tenant.Tenant
	delayed  delayed.DelayedQueue
	sm       state.StateMachine
	interval time.Duration
}

func NewScheduler(tenants []tenant.Tenant, d delayed.DelayedQueue, sm state.StateMachine, interval time.Duration) *Scheduler {
	return &Scheduler{tenants: tenants, delayed: d, sm: sm, interval: interval}
}

func (s *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.pollOnce(ctx)
		}
	}
}

// pollOnce 轮询所有租户的到期任务：搬回 queue 并转 enqueued（忽略冲突，幂等）。
func (s *Scheduler) pollOnce(ctx context.Context) {
	for _, t := range s.tenants {
		ids, err := s.delayed.Poll(ctx, t.ID, time.Now())
		if err != nil {
			log.Printf("poll %s error: %v", t.ID, err)
			continue
		}
		for _, id := range ids {
			if err := s.sm.Apply(ctx, id, state.Transition{From: state.StatusRetryScheduled, To: state.StatusEnqueued}, 0); err != nil {
				log.Printf("apply retry_scheduled->enqueued %s error: %v", id, err)
			}
		}
	}
}
