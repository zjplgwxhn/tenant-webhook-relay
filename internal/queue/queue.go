package queue

import (
	"context"
	"errors"

	"webhook-relay/internal/state"
)

// ErrQueueFull 表示租户队列已满，调用方应返回 429 + Retry-After。
var ErrQueueFull = errors.New("tenant queue full")

// ErrEmpty 表示队列为空。
var ErrEmpty = errors.New("queue empty")

// Queue 是每租户的有界 Redis List（queue:{tenant}）。
type Queue interface {
	Enqueue(ctx context.Context, tenantID string, id state.TaskID) error
	Depth(ctx context.Context, tenantID string) (int64, error)
	// Dequeue 原子搬移 queue:{t} → inflight:{t}；空返回 ErrEmpty。
	Dequeue(ctx context.Context, tenantID string) (state.TaskID, error)
	// Ack 从 inflight:{t} 移除已投递成功的 task id。
	Ack(ctx context.Context, tenantID string, id state.TaskID) error
	// Requeue 把投递失败的 task id 从 inflight:{t} 原子搬回 queue:{t}。
	Requeue(ctx context.Context, tenantID string, id state.TaskID) error
}
