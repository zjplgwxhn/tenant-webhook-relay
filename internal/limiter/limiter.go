package limiter

import (
	"context"
	"errors"
	"time"

	"webhook-relay/internal/metrics"
)

var (
	ErrGlobalTimeout       = errors.New("global concurrency limit timeout")
	ErrTenantNotRegistered = errors.New("tenant not registered")
)

// Limiter 是两级并发控制：每租户信号量 + 全局信号量。
// 一次投递需同时持有租户槽位与全局槽位（设计 §2.1）。
// 信号量约定：向 chan 发送 = 获取槽位（满则阻塞），从 chan 接收 = 释放槽位。
type Limiter struct {
	tenants map[string]chan struct{}
	global  chan struct{}
	timeout time.Duration // 全局槽位获取超时
	m       *metrics.Metrics
}

// NewLimiter 创建两级限流器。Register 必须在 Acquire 之前完成（启动期一次性注册）。
func NewLimiter(globalCap int, globalTimeout time.Duration, m *metrics.Metrics) *Limiter {
	return &Limiter{
		tenants: make(map[string]chan struct{}),
		global:  make(chan struct{}, globalCap),
		timeout: globalTimeout,
		m:       m,
	}
}

// Register 注册一个租户的槽位（大小为该租户最大并发）。
func (l *Limiter) Register(tenantID string, cap int) {
	if cap <= 0 {
		cap = 1
	}
	l.tenants[tenantID] = make(chan struct{}, cap)
}

// Acquire 先取租户槽位（本地、快速），再取全局槽位（带超时）。
// 返回 release 释放两个槽位；全局超时返回 ErrGlobalTimeout（租户槽位已释放）。
func (l *Limiter) Acquire(ctx context.Context, tenantID string) (func(), error) {
	ch := l.tenants[tenantID]
	if ch == nil {
		return nil, ErrTenantNotRegistered
	}
	select {
	case ch <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	timer := time.NewTimer(l.timeout)
	defer timer.Stop()
	select {
	case l.global <- struct{}{}:
		if l.m != nil {
			l.m.InFlight.WithLabelValues(tenantID).Inc()
			l.m.InFlightGlobal.Inc()
		}
		return func() {
			if l.m != nil {
				l.m.InFlight.WithLabelValues(tenantID).Dec()
				l.m.InFlightGlobal.Dec()
			}
			<-l.global
			<-ch
		}, nil
	case <-timer.C:
		<-ch // 超时释放租户槽位，避免泄漏
		if l.m != nil {
			l.m.GlobalAcquireTimeout.Inc()
		}
		return nil, ErrGlobalTimeout
	case <-ctx.Done():
		<-ch
		return nil, ctx.Err()
	}
}
