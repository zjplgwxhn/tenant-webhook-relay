package state

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"webhook-relay/internal/idempotency"
)

const taskKeyPrefix = "task:"

func timeToNano(t time.Time) string {
	return strconv.FormatInt(t.UnixNano(), 10)
}

func nanoToTime(s string) time.Time {
	n, _ := strconv.ParseInt(s, 10, 64)
	return time.Unix(0, n)
}

// RedisTaskStore 把 Task 存为 Redis hash task:{id}。
type RedisTaskStore struct {
	client *redis.Client
}

func NewRedisTaskStore(client *redis.Client) *RedisTaskStore {
	return &RedisTaskStore{client: client}
}

// Put 写任务字段并设置 TTL（幂等键/记录存活窗口，设计 §3.2）。
func (s *RedisTaskStore) Put(ctx context.Context, t Task, ttl time.Duration) error {
	key := taskKeyPrefix + string(t.ID)
	fields := map[string]interface{}{
		"idempotency_key":  string(t.IdempotencyKey),
		"tenant_id":        t.TenantID,
		"destination":      t.Destination,
		"payload":          string(t.Payload),
		"content_type":     t.ContentType,
		"status":           string(t.Status),
		"attempt_count":    strconv.Itoa(t.Attempt),
		"ingested_at":      timeToNano(t.IngestedAt),
		"next_retry_at":    timeToNano(t.NextRetryAt),
		"fencing_token":    strconv.FormatUint(t.FencingToken, 10),
		"lease_expires_at": timeToNano(t.LeaseExpiresAt),
	}
	if err := s.client.HSet(ctx, key, fields).Err(); err != nil {
		return err
	}
	return s.client.Expire(ctx, key, ttl).Err()
}

// Get 读取任务记录；键缺失映射 ErrNotFound。
func (s *RedisTaskStore) Get(ctx context.Context, id TaskID) (Task, error) {
	key := taskKeyPrefix + string(id)
	m, err := s.client.HGetAll(ctx, key).Result()
	if err != nil {
		return Task{}, err
	}
	if len(m) == 0 {
		return Task{}, ErrNotFound
	}
	attempt, _ := strconv.Atoi(m["attempt_count"])
	fencing, _ := strconv.ParseUint(m["fencing_token"], 10, 64)
	return Task{
		ID:             id,
		TenantID:       m["tenant_id"],
		IdempotencyKey: idempotency.Key(m["idempotency_key"]),
		Destination:    m["destination"],
		Payload:        []byte(m["payload"]),
		ContentType:    m["content_type"],
		Status:         Status(m["status"]),
		Attempt:        attempt,
		IngestedAt:     nanoToTime(m["ingested_at"]),
		NextRetryAt:    nanoToTime(m["next_retry_at"]),
		FencingToken:   fencing,
		LeaseExpiresAt: nanoToTime(m["lease_expires_at"]),
	}, nil
}

// acquireInFlightScript: status ∈ {enqueued, retry_scheduled} → token++ + lease + in_flight，返回新 token。
const acquireInFlightScript = `
local status = redis.call('HGET', KEYS[1], 'status')
if status == 'enqueued' or status == 'retry_scheduled' then
  local token = redis.call('HINCRBY', KEYS[1], 'fencing_token', 1)
  redis.call('HSET', KEYS[1], 'status', 'in_flight', 'lease_expires_at', tostring(tonumber(ARGV[1]) + tonumber(ARGV[2])))
  return token
end
return 0
`

func (s *RedisTaskStore) AcquireInFlight(ctx context.Context, id TaskID, leaseDur time.Duration) (uint64, error) {
	n, err := s.client.Eval(ctx, acquireInFlightScript, []string{taskKeyPrefix + string(id)},
		strconv.FormatInt(time.Now().UnixNano(), 10), strconv.FormatInt(int64(leaseDur), 10)).Int64()
	if err != nil {
		return 0, err
	}
	if n == 0 {
		return 0, ErrConflict
	}
	return uint64(n), nil
}

// applyScript: token-CAS（token==0 不校验 token）。status==From → To。
const applyScript = `
local status = redis.call('HGET', KEYS[1], 'status')
if status == ARGV[1] then
  local tok = tonumber(redis.call('HGET', KEYS[1], 'fencing_token'))
  local want = tonumber(ARGV[3])
  if want == 0 or (tok and tok == want) then
    redis.call('HSET', KEYS[1], 'status', ARGV[2])
    return 1
  end
end
return 0
`

func (s *RedisTaskStore) Apply(ctx context.Context, id TaskID, tr Transition, token uint64) error {
	n, err := s.client.Eval(ctx, applyScript, []string{taskKeyPrefix + string(id)},
		string(tr.From), string(tr.To), strconv.FormatUint(token, 10)).Int()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrConflict
	}
	return nil
}

// scheduleRetryScript: token-CAS（token==0 不校验）。in_flight → retry_scheduled + attempt + next。
const scheduleRetryScript = `
local status = redis.call('HGET', KEYS[1], 'status')
if status == 'in_flight' then
  local tok = tonumber(redis.call('HGET', KEYS[1], 'fencing_token'))
  local want = tonumber(ARGV[3])
  if want == 0 or (tok and tok == want) then
    redis.call('HSET', KEYS[1], 'status', 'retry_scheduled', 'attempt_count', ARGV[1], 'next_retry_at', ARGV[2])
    return 1
  end
end
return 0
`

func (s *RedisTaskStore) ScheduleRetry(ctx context.Context, id TaskID, attempt int, nextRetryAt time.Time, token uint64) error {
	n, err := s.client.Eval(ctx, scheduleRetryScript, []string{taskKeyPrefix + string(id)},
		strconv.Itoa(attempt), timeToNano(nextRetryAt), strconv.FormatUint(token, 10)).Int()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrConflict
	}
	return nil
}

// renewScript: token 匹配才续 lease。
const renewScript = `
local tok = tonumber(redis.call('HGET', KEYS[1], 'fencing_token'))
local want = tonumber(ARGV[1])
if tok and tok == want then
  redis.call('HSET', KEYS[1], 'lease_expires_at', tostring(tonumber(ARGV[2]) + tonumber(ARGV[3])))
  return 1
end
return 0
`

func (s *RedisTaskStore) Renew(ctx context.Context, id TaskID, token uint64, leaseDur time.Duration) error {
	n, err := s.client.Eval(ctx, renewScript, []string{taskKeyPrefix + string(id)},
		strconv.FormatUint(token, 10), strconv.FormatInt(time.Now().UnixNano(), 10), strconv.FormatInt(int64(leaseDur), 10)).Int()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrConflict
	}
	return nil
}
