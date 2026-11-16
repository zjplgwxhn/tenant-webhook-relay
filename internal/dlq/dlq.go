package dlq

import (
	"context"

	"webhook-relay/internal/state"
)

// DeadLetterQueue 记录永久失败的任务（移出 inflight，进入 dlq:{tenant} 供排查）。
type DeadLetterQueue interface {
	Dead(ctx context.Context, tenantID string, id state.TaskID) error
}
