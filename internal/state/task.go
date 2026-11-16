package state

import (
	"context"
	"errors"
	"time"

	"webhook-relay/internal/idempotency"
)

// TaskID 是投递任务的内部唯一标识，与幂等键（idempotency.Key）区分：
// 同一事件重复入站会产生不同 TaskID，但（生产者带 key 时）相同幂等键。
type TaskID string

// Status 是投递任务状态机的取值。
type Status string

const (
	StatusEnqueued       Status = "enqueued"
	StatusInFlight       Status = "in_flight"
	StatusRetryScheduled Status = "retry_scheduled"
	StatusDelivered      Status = "delivered"
	StatusDead           Status = "dead"
)

// Task 是投递任务的领域模型，持久化到 Redis hash task:{id}。
// NextRetryAt/LeaseExpiresAt/FencingToken/LastError 由 M3/M4 使用，M1 不写。
type Task struct {
	ID             TaskID
	TenantID       string
	IdempotencyKey idempotency.Key
	Destination    string
	Payload        []byte
	ContentType    string
	Status         Status
	Attempt        int
	IngestedAt     time.Time
	NextRetryAt    time.Time
	LeaseExpiresAt time.Time
	FencingToken   uint64
	LastError      string
}

// ErrNotFound 表示任务记录不存在。
var ErrNotFound = errors.New("task not found")

// TaskStore 持久化投递任务记录。
type TaskStore interface {
	Put(ctx context.Context, t Task, ttl time.Duration) error
	Get(ctx context.Context, id TaskID) (Task, error)
}

// Transition 是一次状态机转移。
type Transition struct {
	From, To Status
}

// ErrConflict 表示状态转移被拒（当前状态与 From 不符，或键不存在）。
var ErrConflict = errors.New("state transition conflict")

// StateMachine 执行受控的状态转移（Redis Lua CAS + fencing token）。
type StateMachine interface {
	// AcquireInFlight 原子地：status ∈ {enqueued, retry_scheduled} → token++ + 设 lease + status=in_flight，返回 token。
	AcquireInFlight(ctx context.Context, id TaskID, leaseDur time.Duration) (uint64, error)
	// Apply 带 fencing token 的 CAS：fencing_token==token（token==0 不校验）且 status==From → To。
	Apply(ctx context.Context, id TaskID, tr Transition, token uint64) error
	// ScheduleRetry 带 token 的 CAS：in_flight → retry_scheduled + attempt + next_retry_at。
	ScheduleRetry(ctx context.Context, id TaskID, attempt int, nextRetryAt time.Time, token uint64) error
	// Renew 续租：fencing_token==token 才延长 lease。
	Renew(ctx context.Context, id TaskID, token uint64, leaseDur time.Duration) error
}
