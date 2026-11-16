package delayed

import (
	"context"
	"time"

	"webhook-relay/internal/state"
)

// DelayedQueue 是重试延迟调度：delay:{tenant} 有序集合，score = next_retry_at（毫秒）。
type DelayedQueue interface {
	Schedule(ctx context.Context, tenantID string, id state.TaskID, at time.Time) error
	// Poll 取出所有到期（score <= now）的 task，原子搬回 queue:{tenant}。
	Poll(ctx context.Context, tenantID string, now time.Time) ([]state.TaskID, error)
}
