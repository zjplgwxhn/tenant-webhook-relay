package drainrate

import (
	"sync"
	"time"
)

// Estimator 是每租户成功投递速率的 EWMA（α 平滑，设计 §2.2）。
type Estimator struct {
	mu    sync.Mutex
	alpha float64
	m     map[string]*ewma
}

type ewma struct {
	rate float64
	last time.Time
}

func New(alpha float64) *Estimator {
	return &Estimator{alpha: alpha, m: make(map[string]*ewma)}
}

// Observe 记录一次成功投递，更新该租户的 EWMA 速率。
func (e *Estimator) Observe(tenantID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	t := e.m[tenantID]
	if t == nil {
		t = &ewma{}
		e.m[tenantID] = t
	}
	now := time.Now()
	if !t.last.IsZero() {
		if interval := now.Sub(t.last).Seconds(); interval > 0 {
			instant := 1.0 / interval
			t.rate = e.alpha*instant + (1-e.alpha)*t.rate
		}
	}
	t.last = now
}

// Rate 返回租户的 EWMA 速率（events/s）；冷启动/下限回退 1.0。
func (e *Estimator) Rate(tenantID string) float64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	if t := e.m[tenantID]; t != nil && t.rate >= 1.0 {
		return t.rate
	}
	return 1.0
}
