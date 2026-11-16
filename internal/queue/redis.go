package queue

import (
	"context"
	"errors"

	"github.com/redis/go-redis/v9"

	"webhook-relay/internal/state"
)

const queueKeyPrefix = "queue:"
const inflightKeyPrefix = "inflight:"

// enqueueScript 原子地做有界入队：LLEN < cap 才 LPUSH，避免并发下超容量。
const enqueueScript = `
if redis.call('LLEN', KEYS[1]) < tonumber(ARGV[1]) then
  redis.call('LPUSH', KEYS[1], ARGV[2])
  return 1
end
return 0
`

// RedisQueue 是 Queue 的 Redis 实现。
type RedisQueue struct {
	client *redis.Client
	cap    int64
}

func NewRedisQueue(client *redis.Client, cap int64) *RedisQueue {
	return &RedisQueue{client: client, cap: cap}
}

func (q *RedisQueue) Enqueue(ctx context.Context, tenantID string, id state.TaskID) error {
	key := queueKeyPrefix + tenantID
	n, err := q.client.Eval(ctx, enqueueScript, []string{key}, q.cap, string(id)).Int()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrQueueFull
	}
	return nil
}

func (q *RedisQueue) Depth(ctx context.Context, tenantID string) (int64, error) {
	return q.client.LLen(ctx, queueKeyPrefix+tenantID).Result()
}

// Dequeue 原子地把最老（队尾）的 task id 从 queue 搬到 inflight。
func (q *RedisQueue) Dequeue(ctx context.Context, tenantID string) (state.TaskID, error) {
	id, err := q.client.LMove(ctx, queueKeyPrefix+tenantID, inflightKeyPrefix+tenantID, "RIGHT", "LEFT").Result()
	if errors.Is(err, redis.Nil) {
		return "", ErrEmpty
	}
	if err != nil {
		return "", err
	}
	return state.TaskID(id), nil
}

// Ack 从 inflight 移除已成功投递的 task id。
func (q *RedisQueue) Ack(ctx context.Context, tenantID string, id state.TaskID) error {
	return q.client.LRem(ctx, inflightKeyPrefix+tenantID, 0, string(id)).Err()
}

// requeueScript 原子地把 task id 从 inflight 搬回 queue。
const requeueScript = `
redis.call('LREM', KEYS[1], 1, ARGV[1])
redis.call('LPUSH', KEYS[2], ARGV[1])
return 1
`

// Requeue 把投递失败的 task id 从 inflight 原子搬回 queue（无容量限制）。
func (q *RedisQueue) Requeue(ctx context.Context, tenantID string, id state.TaskID) error {
	return q.client.Eval(ctx, requeueScript, []string{inflightKeyPrefix + tenantID, queueKeyPrefix + tenantID}, string(id)).Err()
}
