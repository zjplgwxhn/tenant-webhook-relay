# Webhook 中继 · 实现计划

> 对应设计文档：`docs/design-webhook-relay.md`。每个里程碑标注对应设计小节，便于追溯。

## 0. 仓库结构与里程碑依赖

```
webhook-relay/
├── cmd/
│   ├── relay/main.go          # 服务入口（M1 起逐步挂载）
│   └── mock/main.go           # mock receiver（M5 完善）
├── internal/
│   ├── ingest/                # M1
│   ├── idempotency/           # M1
│   ├── queue/                 # M1..M4（Redis List）
│   ├── state/                 # M1..M4（Task / TaskStore / StateMachine）
│   ├── limiter/               # M2
│   ├── worker/                # M2..M4
│   ├── backoff/               # M3
│   ├── delayed/               # M3（delay 有序集合）
│   ├── dlq/                   # M3
│   ├── lease/                 # M4
│   ├── reaper/                # M4
│   └── metrics/               # M5
├── scripts/                   # M5：chaos-a..d.sh、bench.sh
├── deploy/prometheus.yml      # M5
├── docs/
├── docker-compose.yml
└── Makefile
```

依赖：M1 → M2 → M3 → M4；M5 与 M2..M4 平行推进（指标埋点贯穿，M5 集中补全 + 混沌验证）。

---

## M1 — ingest + 幂等键 + Redis List 入队 + 429/Retry-After

对应设计：§1.2、§2.1、§2.2。

### 核心接口与结构体（不实现）

```go
// internal/idempotency
type IdempotencyKey string

type KeyGenerator struct {
    newID func() string // 注入 UUID 生成，便于测试
}
// Generate 有 producerKey 时返回 "{tenantID}:{producerKey}"，否则 newID()。
func (g *KeyGenerator) Generate(tenantID, producerKey string) IdempotencyKey
```

```go
// internal/queue
type TaskID string

var ErrQueueFull = errors.New("tenant queue full")

type Queue interface {
    // Enqueue 有界入队（Lua：LLEN < cap 才 LPUSH queue:{tenant}），满返回 ErrQueueFull。
    Enqueue(ctx context.Context, tenantID string, id TaskID) error
    Depth(ctx context.Context, tenantID string) (int64, error)
}
```

```go
// internal/state
type Status string

const (
    StatusEnqueued       Status = "enqueued"
    StatusInFlight       Status = "in_flight"
    StatusRetryScheduled Status = "retry_scheduled"
    StatusDelivered      Status = "delivered"
    StatusDead           Status = "dead"
)

type Task struct {
    ID             TaskID
    TenantID       string
    IdempotencyKey idempotency.IdempotencyKey
    Destination    string        // 下游 URL（来自租户配置）
    Payload        []byte
    ContentType    string
    Status         Status
    Attempt        int           // Redis hash 字段 attempt_count
    NextRetryAt    time.Time     // M3 起使用
    LeaseExpiresAt time.Time     // M4 起使用
    FencingToken   uint64        // M4 起使用
    LastError      string
}

type TaskStore interface {
    Put(ctx context.Context, t Task, ttl time.Duration) error // 写 task:{id} + TTL
    Get(ctx context.Context, id TaskID) (Task, error)
}
```

```go
// internal/ingest
type RetryAfterCalc interface {
    // Compute clamp(ceil(depth/drainRate), 1s, 60s)。
    Compute(queueDepth int64, drainRate float64) time.Duration
}

type IngestRequest struct {
    ProducerKey string          `json:"producer_key,omitempty"`
    Payload     json.RawMessage `json:"payload"`
}

type IngestHandler struct {
    keys       *idempotency.KeyGenerator
    queue      queue.Queue
    tasks      state.TaskStore
    retryAfter RetryAfterCalc
    cap        int64 // 租户队列容量
}
func (h *IngestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request)
```

### 单元测试列表（测行为）

1. `idempotency`：有 producer_key → 恒等 `tenant:key`；无 key → 两次不同；同 key 重复调用（生产者重发）→ 相同。
2. `queue.Enqueue`（miniredis）：未满成功且 Depth+1；满返回 `ErrQueueFull` 且长度不变。
3. `ingest`：合法请求 → 202 + task 已 Put + 已入队 + 幂等键落入记录；队列满 → 429 + `Retry-After`∈[1,60]；缺 `X-Tenant-ID` → 400；非法 JSON → 400。
4. `RetryAfterCalc`：正常值正确；<1s 钳到 1s；>60s 钳到 60s；drainRate=0 用下限 1 event/s 不除零。

### 验收命令

```bash
make redis && make run
curl -si -X POST localhost:8080/v1/events \
  -H 'Content-Type: application/json' -H 'X-Tenant-ID: t1' \
  -d '{"producer_key":"e1","payload":{"a":1}}'        # 期望 202
# 队列满 → 429 + Retry-After
for i in $(seq 1 200); do
  curl -s -o /dev/null -w '%{http_code}\n' -X POST localhost:8080/v1/events \
    -H 'X-Tenant-ID: t1' -d '{}'
done | sort | uniq -c        # 期望出现 429
```

### 明确不要做

- 不做 worker/投递（M2+）；不做退避/DLQ（M3）；不做租约/fencing/reaper（M4）。
- 不做入站去重、payload 签名/鉴权（设计非目标）。
- 不做 drain_rate 的真实 EWMA（M1 用全局 fallback 常量；EWMA 在 M3 投递成功后接上）。
- 不引入 Redis Streams/consumer group（设计选定 List）。

---

## M2 — 租户 worker pool + 全局信号量 + LMOVE + 状态机基础写回

对应设计：§2.1（两级并发）、§3.3（状态机基础，无 fencing）。

### 核心接口与结构体（不实现）

```go
// internal/queue（M2 新增方法）
type Queue interface {
    Enqueue(ctx context.Context, tenantID string, id TaskID) error
    Depth(ctx context.Context, tenantID string) (int64, error)
    // Dequeue 原子搬移：LMOVE queue:{tenant} inflight:{tenant}；空返回 ErrEmpty。
    Dequeue(ctx context.Context, tenantID string) (TaskID, error)
    // Ack 投递成功：LREM inflight:{tenant} id。
    Ack(ctx context.Context, tenantID string, id TaskID) error
}

var ErrEmpty = errors.New("queue empty")
```

```go
// internal/limiter
type Limiter interface {
    // Acquire 先取租户槽位，再取全局槽位（超时 50-100ms）。
    // 成功返回 release；全局饱和返回 error（稍后重试）。
    Acquire(ctx context.Context, tenantID string) (release func(), err error)
}
```

```go
// internal/state（新增）
type Transition struct {
    From, To Status
}

type StateMachine interface {
    // Apply 原子转移（Lua）：仅当当前 status==From 才写 To。
    Apply(ctx context.Context, id TaskID, tr Transition) error
}
```

```go
// internal/worker
type Worker struct {
    tenantID string
    queue    queue.Queue
    tasks    state.TaskStore
    sm       state.StateMachine
    limiter  limiter.Limiter
    client   *http.Client // Timeout: 20s
}
func (w *Worker) Run(ctx context.Context) error
// 循环：Acquire → Dequeue → Apply(enqueued→in_flight) → POST
//   → 2xx: Apply(in_flight→delivered)+Ack；否则: Apply(in_flight→enqueued)（M2 立即重试）
```

### 单元测试列表

1. `limiter`：获取+释放可复用；全局饱和 → 超时 error；租户隔离（t1 占满不影响 t2）。
2. `queue`：Dequeue 原子搬移（queue−1 / inflight+1）；空返回 ErrEmpty；Ack 移除 inflight。
3. `StateMachine`：`enqueued→in_flight` 成功；From 不符 → 拒绝；`in_flight→delivered` 成功；终态上再 Apply → 拒绝。
4. `worker`（fake 依赖 + httptest 下游）：2xx → delivered+Ack；5xx → 回到 enqueued（M2 无退避）。

### 验收命令

```bash
make accept-m2   # go test ./internal/worker/ -tags integration
# 断言：真实 Redis + httptest 下游，投递后 task:<id> status == delivered，inflight 已清空
```

### 明确不要做

- 不做退避/delay 集合/DLQ（M3）；不做租约/fencing/reaper（M4）。
- 状态写回不带 fencing token（M4 才加）。
- 不做全局信号量的 WFQ 公平队列（设计已拒绝）；只做超时非阻塞获取。
- 不做完整 Prometheus 指标（留最小日志，M5 补全）。

---

## M3 — 指数退避 + full jitter + delay 有序集合 + 窗口耗尽进 DLQ

对应设计：§3.1、§3.2（窗口）、§3.3（窗口给终止）。

### 核心接口与结构体（不实现）

```go
// internal/backoff
type Backoff struct {
    Base   time.Duration // 1s
    Factor float64       // 2.0
    Cap    time.Duration // 60s
}
// Next = random(0, min(Cap, Base * Factor^attempt))，full jitter。
func (b Backoff) Next(attempt int, rng *rand.Rand) time.Duration
```

```go
// internal/delayed
type DelayedQueue interface {
    // Schedule 放 delay:{tenant}，score = at。
    Schedule(ctx context.Context, tenantID string, id state.TaskID, at time.Time) error
    // Poll 取出 score <= now 的 task，原子搬回 queue:{tenant}（Lua）。
    Poll(ctx context.Context, tenantID string, now time.Time) ([]state.TaskID, error)
}
```

```go
// internal/dlq
type DeadLetterQueue interface {
    Dead(ctx context.Context, id state.TaskID, reason string) error // 状态 dead，移出 inflight/queue
}
```

worker 失败路径（M3）：`next_retry_at = now + backoff.Next(attempt)`；若 `next_retry_at` 超出投递窗口 → `dlq.Dead`；否则 `Schedule` + 状态 `retry_scheduled`。下游 429 带 `Retry-After` 时以该头为准（封顶）。

### 单元测试列表

1. `Backoff.Next`：随 attempt 增长；≤ Cap（注入确定性 rng）；0 ≤ 值 ≤ min(Cap, Base·Factor^attempt)；attempt=0 ≤ Base。
2. `DelayedQueue`：未来时间不返回；到期返回并移入 ready queue；Poll 原子（ZREM+LPUSH）。
3. 窗口判断：未超窗口 → retry_scheduled；超窗口 → DLQ（dead、不再重试）。
4. worker：5xx → retry_scheduled + delay 有 score；429+Retry-After → 按头调度。

### 验收命令

```bash
make accept-m3   # go test ./internal/backoff/... ./internal/delayed/... ./internal/dlq/... -tags integration
# 手动：下游持续 500，投递后
redis-cli ZSCORE delay:t1 <id>   # 有 score（退避中）
redis-cli HGET task:<id> status  # retry_scheduled →（窗口耗尽）dead
```

### 明确不要做

- 不做租约/fencing/reaper（M4）。
- 不做超出"尊重 Retry-After 头 + 指数退避"的自适应退避策略。
- 不做入站背压与全局饱和的联动（那是 M5 验证的指标联动，不是新逻辑）。

---

## M4 — 租约心跳独立 goroutine + fencing token CAS + reaper

对应设计：§3.3。

### 核心接口与结构体（不实现）

```go
// internal/lease
type Lease struct {
    Duration   time.Duration // 30s
    RenewEvery time.Duration // 10s
}
// Acquire：SET task:{id}:lease token NX PX Duration，成功返回 token 与 stop()。
// renew(token) 由独立 goroutine 每 RenewEvery 调用，延长过期时间。
func (l *Lease) Acquire(ctx context.Context, id state.TaskID) (token uint64, stop func(), err error)
```

```go
// internal/fencing
type Fencing interface {
    // NextToken = INCR fencing:{id}，单调递增。
    NextToken(ctx context.Context, id state.TaskID) (uint64, error)
    // Apply 带 token CAS：record.fencing_token==token 且 status==From 才写。
    Apply(ctx context.Context, id state.TaskID, token uint64, tr state.Transition) error
}
```

```go
// internal/reaper
type Reaper struct {
    queue    queue.Queue
    tasks    state.TaskStore
    lease    lease.Lease
    fencing  fencing.Fencing
    interval time.Duration // L/2 = 15s
}
// Run 周期扫描各租户 inflight：租约过期 → Lua（token++ → LMOVE inflight→queue）。
// 靠 Lua 原子性避免多 reaper 竞争，无需选主。
func (r *Reaper) Run(ctx context.Context) error
```

worker 流程（M4）：`Acquire → token`；起心跳 goroutine；POST；`Fencing.Apply(token, delivered|retry_scheduled)`；`stop()`。

### 单元测试列表

1. `Lease`：首次 Acquire 成功返回 token；未过期重复 Acquire 失败（NX）；过期后可重获。
2. **心跳与投递解耦**：下游挂起 15s（>RenewEvery）期间心跳仍续租（lease 过期时间被推进）；POST 结束 stop() 停止续租。
3. `Fencing`：旧 token 写 → 拒绝且状态不变；当前 token → 应用；token 单调递增。
4. `Reaper`：inflight 中租约过期 → 搬回 queue 且 token 递增；存活 → 不动；多 reaper 并发不重复搬移。
5. **接管后旧写被拒**：A token=1 租约过期 → reaper token=2 重投 → B 成功 → A 迟到 CAS(token=1) 被拒，状态保持 delivered。

### 验收命令

```bash
make accept-m4   # go test ./internal/lease/... ./internal/reaper/... -tags integration
# 手动演示：投递中 kill -9 worker 进程，观察
redis-cli HGET task:<id> fencing_token   # token 递增
redis-cli HGET task:<id> status          # 最终 delivered（B 接管成功）
```

### 明确不要做

- 不做分布式锁/Redlock（设计已拒绝）。
- 不做 Streams 迁移（坚持 List + inflight + reaper）。
- 不做 reaper 的 leader election（Lua 原子性已避免竞争，无需选主）。

---

## M5 — Prometheus 指标 + mock receiver + 四个故障注入脚本 + 压测

对应设计：§4、§5。

### 核心接口与结构体（不实现）

```go
// internal/metrics
type Metrics struct {
    IngestTotal          *prometheus.CounterVec   // tenant, status
    Ingest429            *prometheus.CounterVec   // tenant
    QueueDepth           *prometheus.GaugeVec     // tenant
    QueueCapacity        *prometheus.GaugeVec     // tenant
    InFlight             *prometheus.GaugeVec     // tenant
    InFlightGlobal       prometheus.Gauge
    GlobalAcquireTimeout prometheus.Counter
    DeliveryAttempts     *prometheus.CounterVec   // tenant, status_code
    DeliverySuccess      *prometheus.CounterVec
    DeliveryPermanentFailure *prometheus.CounterVec
    RetryDelay           *prometheus.HistogramVec // tenant
    LeaseExpired         prometheus.Counter
    StaleWriteRejected   prometheus.Counter
}
func NewMetrics(reg *prometheus.Registry) *Metrics
```

```go
// cmd/mock
type MockReceiver struct {
    StatusCode int           // 可脚本化
    RetryAfter time.Duration
    Hang       time.Duration // >0 则挂起
    requests   []Record      // Idempotency-Key / 到达时间 / 状态码
}
// GET /__requests  返回请求日志（验证同 key 重投）
// POST /__config   设置行为（状态码 / Retry-After / 挂起）
```

脚本：`scripts/chaos-a.sh`（kill worker）、`chaos-b.sh`（Redis down）、`chaos-c.sh`（下游 429/5xx）、`chaos-d.sh`（5 慢 + 5 健康租户）、`bench.sh`（hey/vegeta/k6）。

### 单元测试列表

1. `Metrics`（prometheus/testutil）：触发各行为后断言 /metrics 计数与标签（429 计数、delivery success、lease_expired、stale_write_rejected）。
2. `MockReceiver`：按配置返回状态码/Retry-After；记录 Idempotency-Key 与到达时间；挂起模式保持连接。
3. chaos 脚本（integration）：
   - A：同 key 两次投递 + 终态 delivered。
   - B：Redis down 期间零重复 + 恢复续投。
   - C：退避/Retry-After 被遵守 + 最终 DLQ。
   - D：健康租户 P99 退化 < X ms。

### 验收命令

```bash
make chaos-a   # kill worker → 同 key 重投 → 终态 delivered
make chaos-b   # Redis 宕机 → 零重复 + 恢复续投
make chaos-c   # 下游 429/5xx → 退避 + DLQ
make chaos-d   # 多租户过载 → 健康租户 P99 不退化
make bench     # 压测：报告 P99 / 吞吐 / 429 率
curl -s localhost:9090/metrics | grep webhook_delivery_success_total
```

### 明确不要做

- 不做生产级告警规则（Alertmanager/Prometheus rule）——超范围。
- 不做 Grafana dashboard（可选，不在本计划）。
- 不做混沌自动化编排框架（chaos monkey）；只做确定性脚本。
- 不自行实现网络故障注入（iptables/toxiproxy 由脚本调用现成工具）。

---

## 组件清单（docker-compose.yml）

| 服务 | 镜像/构建 | 角色 | 关键配置 | 端口 |
|---|---|---|---|---|
| redis | `redis:7-alpine` | 唯一事实源（队列 + 状态 + fencing） | `--appendonly yes --appendfsync everysec`，卷 `redis-data` | 6379 |
| relay | `build: .` | 服务本体 | `REDIS_ADDR=redis:6379`，`HTTP_ADDR=:8080`，`METRICS_ADDR=:9090` | 8080（ingest）/ 9090（metrics） |
| mock | `build: ./cmd/mock` | 可控下游（fault injection 目标） | `LISTEN=:9000`，默认 `DEFAULT_STATUS=200` | 9000 |
| prometheus | `prom/prometheus` | 抓取 relay `/metrics` | 挂载 `deploy/prometheus.yml`（scrape relay:9090） | 9091→9090 |

依赖：`relay` 依赖 `redis`；`mock`/`prometheus` 独立。

## Makefile 目标列表

| 目标 | 命令 | 说明 |
|---|---|---|
| `make test` | `go test ./...` | 全部单元测试（miniredis，无需外部依赖） |
| `make build` | `go build ./cmd/relay` | 构建 |
| `make run` | `docker compose up -d redis && go run ./cmd/relay` | 启动 Redis + 服务 |
| `make accept-m1` … `accept-m5` | `go test ./internal/... -tags integration` | 各里程碑集成验收 |
| `make chaos-a` | `bash scripts/chaos-a.sh` | 场景 A：kill worker |
| `make chaos-b` | `bash scripts/chaos-b.sh` | 场景 B：Redis 宕机 |
| `make chaos-c` | `bash scripts/chaos-c.sh` | 场景 C：下游 429/5xx |
| `make chaos-d` | `bash scripts/chaos-d.sh` | 场景 D：多租户过载 |
| `make bench` | `bash scripts/bench.sh` | 压测（P99 / 吞吐 / 429 率） |
| `make lint` | `golangci-lint run` | 静态检查 |
