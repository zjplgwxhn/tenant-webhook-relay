package delayed

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"webhook-relay/internal/state"
)

const (
	delayKeyPrefix = "delay:"
	queueKeyPrefix = "queue:" // 与 queue 包一致
)

// pollScript 原子地把到期 task id 从 delay 搬到 queue，返回搬移的 id 列表。
const pollScript = `
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
for _, id in ipairs(ids) do
  redis.call('LPUSH', KEYS[2], id)
  redis.call('ZREM', KEYS[1], id)
end
return ids
`

// RedisDelayed 是 DelayedQueue 的 Redis 实现。
type RedisDelayed struct {
	client *redis.Client
}

func NewRedisDelayed(client *redis.Client) *RedisDelayed {
	return &RedisDelayed{client: client}
}

func (d *RedisDelayed) Schedule(ctx context.Context, tenantID string, id state.TaskID, at time.Time) error {
	return d.client.ZAdd(ctx, delayKeyPrefix+tenantID, redis.Z{Score: float64(at.UnixMilli()), Member: string(id)}).Err()
}

func (d *RedisDelayed) Poll(ctx context.Context, tenantID string, now time.Time) ([]state.TaskID, error) {
	res, err := d.client.Eval(ctx, pollScript, []string{delayKeyPrefix + tenantID, queueKeyPrefix + tenantID}, strconv.FormatInt(now.UnixMilli(), 10)).Result()
	if err != nil {
		return nil, err
	}
	arr, ok := res.([]interface{})
	if !ok {
		return nil, nil
	}
	ids := make([]state.TaskID, 0, len(arr))
	for _, v := range arr {
		switch s := v.(type) {
		case string:
			ids = append(ids, state.TaskID(s))
		case []byte:
			ids = append(ids, state.TaskID(string(s)))
		}
	}
	return ids, nil
}
