# 租户感知 Webhook 中继服务 · 设计文档

## 0. 范围与规模锚点

**定位**：多租户事件投递中继——接收生产者事件，按租户隔离地、可靠地投递到各租户配置的下游 HTTP 端点。

**规模锚点（默认值，可配，改数字不改设计）**：
- 1,000 租户；峰值 10,000 events/s 入站；单租户峰值 100 events/s。
- 投递目标：24h 内至少一次成功投递（At-Least-Once），99.9% 事件在 10 分钟内完成首次投递。
- 单租户下游默认承受 ~8 并发 HTTP。

**非目标**：payload 签名/校验（HMAC per-tenant secret）、**入站侧幂等去重**（与出站幂等键是两件事）、下游鉴权、事件内容重写。

**持久化 SLA（显式声明）**：Redis 开启 AOF `appendfsync everysec`，接受极端宕机下最多丢失最近 1s 内已入队的事件。该窗口是**持久化** SLA，与投递语义是两件事：At-Least-Once 投递是"**以事件已持久化入队为前提**"的承诺。

---

## 1. 核心投递语义

### 1.1 语义选择：**At-Least-Once**

投递是"向不受信任、有状态的第三方 HTTP 端点发 POST"。中继无法原子协调"已投递"与"接收方已处理"——不存在跨任意 HTTP 端点的两阶段提交。不可能来自一个**歧义窗口**：

> 中继发出 POST 后若连接超时/断开，无法区分"接收方处理了但 ACK 丢失"与"接收方根本没收到"。

歧义窗口内：重试 → 可能重复；不重试 → 可能丢失。真实可选项只有两个：

| 语义 | 歧义窗口内行为 | 代价 |
|---|---|---|
| At-Most-Once | 不重试 | 丢事件 |
| **At-Least-Once** | 重试 | 可能重复 |

webhook 场景选 At-Least-Once：接收方天然要容忍重复，而"静默丢业务事件"不可接受。Exactly-Once 不是"更难实现"，而是**在此边界条件下语义上不可表达**——除非接收方参与分布式事务，任意客户端点不会配合。

- **替代方案及拒绝理由**：
  - **Exactly-Once（outbox + 接收方 ACK 两阶段握手）**：强制接收方持久化后回 ACK，把 webhook 接收方当分布式事务参与者；fire-and-forget 的 HTTP handler 不会配合。拒绝。
  - **At-Most-Once**：网络瞬断、下游 5xx 会静默丢事件。拒绝。
  - **中继自己记"发过哪些"来凑 Exactly-Once**：中继只能记"自己发了几次"，无法知道"接收方处理了几次"；去重只能发生在接收方。拒绝。

### 1.2 幂等键：**中继生成（ingest 时生成一次，跨重试复用），透传给接收方**

**为什么是中继生成而非接收方生成**：接收方在首次收到前不知道事件存在，无法为"首次投递就丢失、随后重试"预生成稳定键。键必须产生于第一个 hop 之前，否则重试会带不同键、去重失效。

**生成规则**：
- **生产者提供稳定 `producer_key` / `event_id` 时**：以 `{tenant_id}:{producer_key}` 作为幂等键。此时键是确定性的——**生产者侧重发会得到同一键**，接收方据此去重。
- **生产者未提供时**：中继在 ingest 铸 UUID。此时**不承诺入站重发的去重**——同一事件重复入站会产生不同键、成为两个独立投递任务。

关键区分：即便用 `producer_key` 得到确定性键，中继也**不做入站去重**（非目标），重复入站仍会被投递两次；确定性键的作用只是让接收方**有机会**把两次投递折叠成一次。一句话（简历可直接用）：**"At-Least-Once 投递 + 一等公民的幂等键透传，把 Exactly-Once 交给唯一可能实现它的地方——接收方。"**

**中继承担 / 不承担**：

| 中继承担 | 中继不承担 |
|---|---|
| 为每个事件生成并持久化一个稳定幂等键（确定性程度取决于生产者是否给 key） | 保证接收方真的按键去重（那是接收方契约） |
| 所有投递尝试携带同一键 | 保证接收方"恰好应用一次" |
| 在键的 Redis 记录上维护投递状态，直至重试窗口结束 | 入站侧的去重（重复入站会各自投递） |
| 让"接收方去重"成为**可能** | 对"已处理两次"的接收方做补偿/回滚 |

- **替代方案及拒绝理由**：
  - **接收方生成幂等键**：接收方无法在首次收到前预生成稳定键，重试会带不同键。拒绝。
  - **中继完全不谈幂等键**：技术上正确但把重复问题甩给接收方且不给抓手。拒绝。
  - **中继内存去重来假装 Exactly-Once**：内存去重重启即失，反而漏投，是 at-most-once 的伪装。拒绝。

---

## 2. 租户隔离的并发模型

### 2.1 队列持久化 + 两级并发

**队列是 Redis List，不是内存 channel。** 每租户一个 List，task id 在三个位置之间迁移，且任意时刻**恰在其中一个**：

```
 delay:{tenant}  (sorted set, score = next_retry_at)  ──到期──►  queue:{tenant} (ready)
                                                                      │  LMOVE（原子）
                                                                      ▼
                                                          inflight:{tenant}  ──成功──►  移除（终态）
                                                                      │
                                                        lease 过期 → reaper 搬回 queue
```

- **入队**：`LPUSH queue:{tenant} task_id`（持久化）。
- **出队（崩溃安全）**：`LMOVE queue:{tenant} inflight:{tenant}` 原子搬移，task id 永远落在某个 List 里，不存在"pop 后、写状态前崩溃导致 task id 蒸发"的窗口。
- **续投/故障转移**：task 记录（Redis hash）持有 `status / fencing_token / lease_expires_at / next_retry_at`。**reaper** 每 L/2 扫描各 `inflight` List，对 `lease_expires_at < now` 的 task id 执行 Lua（原子地校验租约过期 → fencing_token++ → `LMOVE` 回 `queue`）。这就是"崩溃后接管"的落地。
- **持久化策略**：AOF `appendfsync everysec`（主）+ 周期性 RDB 快照（冷启动快速加载/备份）。Redis 重启后 AOF 回放恢复 `queue`/`inflight`/`delay` 与全部 task 记录，worker 与 reaper 从中断处续投。

**两级并发（资源安全 vs 公平，分层明确）**：

- **租户级**：`tenant_max_concurrency`（默认 8）有界信号量，限制单租户在途投递 → 限制单个慢/挂下游的爆炸半径，**这是提供公平的一层**。
- **全局级**：`global_max_concurrency`（默认 2048）共享信号量，限制全进程在途 HTTP → 防 FD/端口/连接池耗尽，**这是资源安全兜底，不是公平机制**。
- 一次投递需同时获得租户槽位 + 全局槽位。

**全局信号量的公平性边界**：当足够多慢租户同时占满全局 2048 槽位时，健康租户也会在全局层竞争——这是**资源耗尽，不是 bug**，无法在不牺牲吞吐的前提下"凭空"让健康租户优先。缓解：

1. **全局槽位获取带超时、非阻塞**（50–100ms 拿不到则稍后重试），避免无限阻塞的 goroutine 堆积，并产出可观测的过载信号（`webhook_global_acquire_timeout_total`）。
2. **全局饱和的系统级响应是入站背压**：在门口以 429 卸流，而不是在已饱和的池子内部做公平排队。
3. **选型按"稳态不绑定"原则**：`global_max ≥ 稳态活跃租户的 Σ tenant_max_concurrency × 冗余系数`，使每租户池成为稳态下的绑定约束，全局层只在病态聚合过载时兜底。

- **替代方案及拒绝理由**：
  - **内存 channel 队列**：进程崩溃丢未投递事件，与"Redis 为唯一事实源""崩溃不丢已入队事件"矛盾。拒绝。
  - **Redis List 用裸 `BRPOP`（无 inflight 中转）**：pop 后、写状态前崩溃，task id 从队列蒸发，续投无从谈起。拒绝。
  - **全局信号量加 per-tenant 加权公平队列（WFQ）**：复杂度高，且解决的是"资源已耗尽时谁来排队"——而正确做法是此时背压而非排队。拒绝。
  - **全局上限 = 各租户上限之和（预留式）**：要么预留浪费，要么不预留形同虚设。拒绝。

### 2.2 队列满策略：**立即拒绝，返回 429 + `Retry-After`，不阻塞**

**为什么拒绝而非阻塞**：① 阻塞把生产者延迟耦合到最慢下游，一个租户故障变成其生产者的尾延迟飙升；② 阻塞等于造了一个看不见的队列，长度/年龄不可观测；③ 拒绝是 O(1) 非阻塞，ingest 路径延迟与投递健康完全解耦。

**`Retry-After` 计算**：

```
Retry-After = clamp(ceil(queue_depth / drain_rate), 1s, 60s)

drain_rate = 租户最近 60s 成功投递速率，用指数滑动平均 EWMA(α=0.2) 平滑
冷启动 / 样本不足（<10 个观测）时回退到全局平均 drain_rate
分母下限 1 event/s（避免除零 + 过度乐观的 Retry-After）
```

- **替代方案及拒绝理由**：
  - **有界队列 + 阻塞等待**：尾延迟耦合 + 队头阻塞 + 积压不可观测。拒绝。
  - **无界队列**：快产慢耗租户 OOM / 饿死其他租户。拒绝。
  - **拒绝但返回 503**：503 让生产者误以为整个中继挂了；429 精确表达"该租户过载、稍后重试"且能带 `Retry-After`。拒绝。

---

## 3. 重试与故障转移

### 3.1 指数退避参数

| 参数 | 取值 | 说明 |
|---|---|---|
| 基础延迟 | 1s | 首次重试前等待 |
| 倍数 | 2.0 | 指数增长 |
| 单次延迟上限 | 60s | 延迟不无限膨胀 |
| 抖动 | **full jitter**：`delay = random(0, min(cap, base·2^attempt))` | 打散多租户同步重试的惊群 |
| 重试窗口（投递截止） | 24h（每租户可配） | 真正的 knob，退避只是窗口内的间隔 |

实际调度：`1s → 2s → 4s → 8s → 16s → 32s → ~60s(cap) → 60s…`，直到窗口耗尽。**投递窗口是主参数，退避只是把尝试铺在窗口里**。

- **替代方案及拒绝理由**：
  - **固定重试次数 + 固定间隔**：无法覆盖"下游维护 30 分钟"，5 次后必然进死信；间隔固定与恢复节奏错位。拒绝。
  - **无抖动指数退避**：同批失败事件同步重试，制造人工尖峰，可能再打垮刚恢复的下游。拒绝。
  - **退避参数作为主配置、窗口由退避推出**：主次颠倒——承诺的是"24h 内送达"，不是"重试 N 次"。拒绝。

### 3.2 重试窗口与幂等键 Redis TTL 对齐

**不变量**：

```
TTL(idempotency_key) ≥ max_retry_window + max_attempt_duration + clock_skew
```

具体取值：`TTL = 24h + 20s(HTTP 超时) + 30s(租约) + 余量 ≈ 25h`。

**为什么必须对齐**：键必须活满整个重试窗口，否则窗口内的一次重试因键过期而铸新键 → 接收方无法去重 → 确定性重复。这是正确性耦合，不是容量优化。

- 窗口每租户可配时，TTL 在 enqueue 时按 `该租户窗口 + 余量` 一次性设定。
- 终态：投递成功 → 键可立即删，或降 TTL 到短 `receiver_dedup_grace`（10 分钟）；终态失败（DLQ）→ 保留键至窗口结束，保证人工重放仍用同一键。

- **替代方案及拒绝理由**：
  - **键永久保留**：省 TTL 心智但无限增长、内存泄漏，且掩盖"投递早已终结"。拒绝。
  - **TTL = 窗口，无余量**：边界上一场慢 POST 撞上键过期，产生新键。拒绝。
  - **键只存内存不落 Redis**：重启丢键，重试铸新键，违背"稳定键"承诺。拒绝。

### 3.3 崩溃恢复：**租约（lease）+ fencing token**

两个必须分别解决的失效：

- **风险 A（崩溃后无限重试 / 任务卡死）**：worker dequeue 后、标记结果前崩溃，任务永远停在"在途"。
- **风险 B（状态被旧 worker 覆盖）**：worker A POST 超时 20s；其间租约（10s）过期，worker B 接管、成功投递、写 `delivered`（token=2）；A 的调用此时才返回，拿旧 token=1 写"失败"，把 B 的 `delivered` 覆盖回 `retrying` → 不该有的再投递。

**解法（各管一件事）**：

- **租约**：dequeue 即原子获取（`SET task_id owner token NX PX L`，L=30s），续租每 L/3=10s。worker 死 → 租约到期 → reaper 搬回队列，其他 worker 接管。**租约限定僵尸窗口为 L**，解决风险 A 的"卡死"。
- **续租与投递解耦**：**租约心跳由独立 goroutine 驱动，与投递 goroutine 分离**。投递 goroutine 只做 `http.Client.Do`（自身 20s 超时）+ 带 fencing token 的 CAS 写回；心跳 goroutine 每 10s 续租。这样一次长 POST（甚至 GC 停顿）不会阻塞续租。即便极端情况下租约仍过期、B 接管，fencing token 保证 A 的迟到写被拒——最坏结果只是一次重复投递，At-Least-Once 下可接受，不会状态损坏。
- **fencing token**：每次租约获取/接管，token 单调递增；所有状态写用 Redis Lua CAS：`if record.fencing_token != my_token then return STALE`。旧 token 写被拒，解决风险 B。
- **投递截止（窗口）**：窗口耗尽仍未成功 → 终态 DLQ，不再重试。解决风险 A 的"无限重试"。

三者分工：**租约给活性、fencing 给安全性、窗口给终止。**

**为什么不用分布式锁（Redlock / SET NX PX）**：① 锁的前提是持有者主动释放，崩溃的 worker 永不释放——最终仍靠过期，那它已经是租约了；② 锁只给互斥不给版本号，锁过期、B 接管后还活着的慢 worker A 依然能写，锁无法拒绝旧写——这正是 fencing token 要补的；③ Redlock 依赖时钟假设与多数派，这里用不上也脆弱。我们要的不是互斥，而是"死 worker 的任务可回收 + 旧 worker 的写被拒"，一个 Redis Lua 脚本同时给出租约 + fencing，比锁管理器简单得多。

**关键设计点（简历可直接用）**：**"worker 无状态 + Redis 为唯一事实源 → 故障转移免费：任何 worker 都能接管孤儿任务；正确性来自租约 + fencing token，而非粘性分配或分布式锁。"**

投递任务状态机：

```
 enqueued ──► in_flight(leased, token=k) ──2xx──► delivered (终态)
                 │  │
                 │  └─超时/5xx──► retry_scheduled (退避, 下次 lease 时 token++)
                 │
                 └─窗口耗尽──► dead → DLQ (终态)
```

- **替代方案及拒绝理由**：
  - **分布式锁（Redlock）**：互斥 + 主动释放，扛不住崩溃，无版本号拒旧写，依赖时钟/多数派。拒绝。
  - **固定重试次数、无租约无 fencing**：无法界定僵尸窗口、无法拒旧写，崩溃后状态可能被覆盖成错误终态。拒绝。
  - **只做队列、无状态机（失败就重新入队）**：丢失"从未尝试/已尝试 N 次将在 T 重试/终态成功/终态失败"的区分，无法做退避调度、DLQ、观测。拒绝。

---

## 4. 可验证的失效场景（故障注入）

> 注入工具：可控 mock 接收方（Go `httptest` 或独立 mock 服务，可脚本化返回任意状态码/挂起/统计请求）；网络故障用 toxiproxy/iptables；进程崩溃用 `kill -9`。固化为自动化 chaos 测试。

### 场景 A：投递中途 kill worker（`SIGKILL`）

- **注入**：下游 mock 挂起；worker POST 中 `kill -9`。
- **预期**：租约 ≤L 内过期；reaper 将 task id 从 `inflight` 搬回 `queue`；另一 worker 以**相同 `Idempotency-Key`、更高 fencing token** 重投；2×L 秒后无任务停在 `inflight`。
- **验证**：① mock 记录到两条 `Idempotency-Key` 完全相同的 POST；② Redis 中 task 终态 `delivered` 且 `attempt_count` 正确；③ `webhook_in_flight` 无泄漏。

### 场景 B：Redis 不可用（分区/宕机）

- **注入**：kill Redis 或代理返回超时。
- **预期**：worker **停止推进但不崩溃、不重复投递**——它们无法 lease/CAS，必须停下来而非猜；ingest 返回 503；`redis_up=0` 告警；恢复后 AOF 回放，worker/reaper **从中断处续投**，已持久化状态的事件不重投。
- **验证**：① Redis down 期间零重复投递；② ingest 返回 503；③ 恢复后 outage 前已入队事件（在 AOF 1s 持久化窗口内）最终送达、终态一致、无丢失。

### 场景 C：下游持续返回 429 / 5xx

- **注入**：mock 返回 429 带 `Retry-After: 30`（另测一版不带该头），持续到窗口耗尽。
- **预期**：带 `Retry-After` 时**尊重该头**（下次尝试延迟 30s，封顶）；不带时走指数退避 + 抖动；全程受每租户并发上限约束；窗口耗尽 → DLQ 并告警。
- **验证**：① mock 记录到达间隔，断言符合 Retry-After/退避；② 窗口后 `webhook_delivery_permanent_failure` 与 DLQ 深度 +1；③ 下游看到的并发 ≤ 租户上限。

### 场景 D：多租户过载下的隔离（慢租户 + 健康租户混合）

- **注入**：**5 个慢租户**（下游挂起/极慢）+ **5 个健康租户**（下游正常），同时灌入。
- **预期**：慢租户各自受 `tenant_max_concurrency` 限制、队列满后 429 + `Retry-After`；**健康租户 P99 在全局资源耗尽前保持平稳**（全局 2048 未被打满时，公平由每租户池保证）；一旦慢租户聚合占满全局槽位，系统以**入站背压**而非静默丢健康租户的工作来响应。
- **验证**：① 慢租户溢出返回 429 + Retry-After；② 健康租户 P99 与无慢租户时相比退化 < X ms（X 由压测基线确定，写进简历："N 个慢租户下健康租户 P99 仍 ≤ X ms"）；③ `webhook_global_acquire_timeout_total` 上升时，健康租户未被静默丢弃——被拒的请求带显式 429/背压信号。

---

## 5. 观测指标（支撑第 4 节验证，Prometheus）

每个 metric 带 `tenant` 标签（全局指标不带）。

| 指标 | 类型 | 用途 |
|---|---|---|
| `webhook_ingest_total{tenant,status}` | Counter | 入站量 / 503 来源 |
| `webhook_ingest_429_total{tenant}` | Counter | 队列溢出（场景 D 验证点） |
| `webhook_queue_depth{tenant}` / `webhook_queue_capacity{tenant}` | Gauge | Retry-After 计算 + 溢出预警 |
| `webhook_in_flight{tenant}` / `webhook_in_flight_global` | Gauge | 两级并发是否生效 |
| `webhook_global_acquire_timeout_total` | Counter | 全局槽位超时（全局饱和信号） |
| `webhook_delivery_attempts_total{tenant,status_code}` | Counter | 投递尝试 + 下游状态分布 |
| `webhook_delivery_success_total` / `webhook_delivery_permanent_failure_total` | Counter | 终态（场景 C 验证点） |
| `webhook_retry_delay_seconds{tenant}` | Histogram | 退避/Retry-After 是否被遵守 |
| `webhook_lease_expired_total` / `webhook_stale_write_rejected_total` | Counter | 租约接管 / fencing 拒旧写（场景 A/B） |
| `redis_up` | Gauge | Redis 健康（场景 B 告警源） |

这些指标是第 4 节每个场景的**验证断言来源**——"零重复投递""并发不超上限""健康租户 P99 不退化"都落在具体 metric 上。

---

## 6. 待确认 / 可调默认值

1. **规模锚点**（1k 租户 / 10k eps）——改数字会改第 2、3 节的并发与队列默认值，结构不变。
2. **payload 签名**是否纳入范围——纳入则加一节（HMAC per-tenant secret + `X-Signature`），不影响本文决策。
3. **投递窗口默认 24h** 是否符合 SLA 叙事（部分简历项目写"8h 内送达"更激进）。
4. **AOF `everysec` vs `always`**：本文选 everysec（1s 丢失窗口换吞吐）；若你的简历要"零丢失"叙事，改 `always`，但需补 fsync 吞吐的代价说明。
