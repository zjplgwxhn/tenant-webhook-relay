package reaper

import (
	"context"
	"log"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"webhook-relay/internal/metrics"
	"webhook-relay/internal/tenant"
)

const (
	taskKeyPrefix     = "task:"
	inflightKeyPrefix = "inflight:"
	queueKeyPrefix    = "queue:"
)

// reclaimScript 原子回收所有"未活跃持有"的 inflight 任务：
//   - 终态/无记录 → 仅清理 inflight 泄漏；
//   - 其它（enqueued/retry_scheduled/in_flight 租约过期）→ token++ + status=enqueued + inflight→queue。
const reclaimScript = `
local status = redis.call('HGET', KEYS[1], 'status')
local expires = redis.call('HGET', KEYS[1], 'lease_expires_at')
if status == false or status == 'delivered' or status == 'dead' then
  redis.call('LREM', KEYS[2], 1, ARGV[1])
  return 1
end
local leased = (status == 'in_flight' and expires and tonumber(expires) >= tonumber(ARGV[2]))
if not leased then
  redis.call('HINCRBY', KEYS[1], 'fencing_token', 1)
  redis.call('HSET', KEYS[1], 'status', 'enqueued')
  redis.call('LREM', KEYS[2], 1, ARGV[1])
  redis.call('LPUSH', KEYS[3], ARGV[1])
  return 1
end
return 0
`

// Reaper 周期扫描各租户 inflight，回收租约过期的孤儿任务。
type Reaper struct {
	tenants  []tenant.Tenant
	client   *redis.Client
	interval time.Duration
	m        *metrics.Metrics
}

func New(tenants []tenant.Tenant, client *redis.Client, interval time.Duration, m *metrics.Metrics) *Reaper {
	return &Reaper{tenants: tenants, client: client, interval: interval, m: m}
}

func (r *Reaper) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.sweepOnce(ctx)
		}
	}
}

// sweepOnce 扫描所有租户的 inflight，逐个尝试回收。
func (r *Reaper) sweepOnce(ctx context.Context) {
	for _, t := range r.tenants {
		ids, err := r.client.LRange(ctx, inflightKeyPrefix+t.ID, 0, -1).Result()
		if err != nil {
			log.Printf("reaper list inflight %s error: %v", t.ID, err)
			continue
		}
		for _, id := range ids {
			r.reclaim(ctx, t.ID, id)
		}
	}
}

func (r *Reaper) reclaim(ctx context.Context, tenantID, id string) {
	n, err := r.client.Eval(ctx, reclaimScript,
		[]string{taskKeyPrefix + id, inflightKeyPrefix + tenantID, queueKeyPrefix + tenantID},
		id, strconv.FormatInt(time.Now().UnixNano(), 10)).Int()
	if err != nil {
		log.Printf("reaper reclaim %s error: %v", id, err)
		return
	}
	if n == 1 && r.m != nil {
		r.m.LeaseExpired.Inc()
	}
}
