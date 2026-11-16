package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"webhook-relay/internal/state"
	"webhook-relay/internal/tenant"
)

type fakeSM struct {
	mu        sync.Mutex
	renewErr  error // 固定返回（failFirst 耗尽后）
	failFirst int   // 前 N 次返回 transient 错误
	calls     int
}

func (f *fakeSM) AcquireInFlight(ctx context.Context, id state.TaskID, d time.Duration) (uint64, error) {
	return 1, nil
}
func (f *fakeSM) Apply(ctx context.Context, id state.TaskID, tr state.Transition, tok uint64) error {
	return nil
}
func (f *fakeSM) ScheduleRetry(ctx context.Context, id state.TaskID, a int, t time.Time, tok uint64) error {
	return nil
}
func (f *fakeSM) Renew(ctx context.Context, id state.TaskID, tok uint64, d time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.failFirst > 0 {
		f.failFirst--
		return errors.New("transient redis error")
	}
	return f.renewErr
}
func (f *fakeSM) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func TestHeartbeatStopsOnConflict(t *testing.T) {
	sm := &fakeSM{renewErr: state.ErrConflict}
	w := &Worker{tenant: tenant.Tenant{ID: "t1"}, sm: sm, renewEvery: 5 * time.Millisecond, leaseDur: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	go w.heartbeat(ctx, "t-1", 1)
	time.Sleep(30 * time.Millisecond)
	cancel()
	time.Sleep(5 * time.Millisecond)
	if got := sm.callCount(); got != 1 {
		t.Fatalf("renew calls = %d, want 1 (stop on conflict)", got)
	}
}

func TestHeartbeatContinuesOnTransient(t *testing.T) {
	sm := &fakeSM{failFirst: 2}
	w := &Worker{tenant: tenant.Tenant{ID: "t1"}, sm: sm, renewEvery: 5 * time.Millisecond, leaseDur: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	go w.heartbeat(ctx, "t-1", 1)
	time.Sleep(30 * time.Millisecond)
	cancel()
	time.Sleep(5 * time.Millisecond)
	if got := sm.callCount(); got < 3 {
		t.Fatalf("renew calls = %d, want >= 3 (continue on transient)", got)
	}
}
