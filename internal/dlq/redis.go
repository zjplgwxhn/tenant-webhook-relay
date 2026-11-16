package dlq

import (
	"context"

	"github.com/redis/go-redis/v9"

	"webhook-relay/internal/state"
)

const (
	inflightKeyPrefix = "inflight:"
	dlqKeyPrefix      = "dlq:"
)

// deadScript 原子地把 task id 从 inflight 移入 dlq:{tenant}。
const deadScript = `
redis.call('LREM', KEYS[1], 1, ARGV[1])
redis.call('LPUSH', KEYS[2], ARGV[1])
return 1
`

// RedisDLQ 是 DeadLetterQueue 的 Redis 实现。
type RedisDLQ struct {
	client *redis.Client
}

func NewRedisDLQ(client *redis.Client) *RedisDLQ {
	return &RedisDLQ{client: client}
}

func (d *RedisDLQ) Dead(ctx context.Context, tenantID string, id state.TaskID) error {
	return d.client.Eval(ctx, deadScript, []string{inflightKeyPrefix + tenantID, dlqKeyPrefix + tenantID}, string(id)).Err()
}
