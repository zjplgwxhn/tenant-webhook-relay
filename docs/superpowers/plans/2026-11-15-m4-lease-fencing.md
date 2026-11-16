# M4: 租约心跳 + fencing token CAS + reaper 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** 用租约（独立心跳 goroutine）+ fencing token CAS + reaper 接管，收口 M2/M3 的崩溃残留问题。

**Architecture:** 状态机所有写操作改为 fencing token CAS；worker 出队后 `AcquireInFlight`（token++ + 设 lease + status=in_flight），起独立心跳续租，投递后用 token 写回；reaper 周期扫描 inflight，原子回收租约过期的孤儿任务。

**Tech Stack:** Go 1.22+，go-redis/v9，miniredis/v2。

**Spec:** `docs/design-webhook-relay.md`（§3.3）。

## Global Constraints

- 租约 `L=30s`，续租 `L/3=10s`；HTTP 超时 20s < L。
- fencing token 单调递增（`HINCRBY task:{id} fencing_token 1`），存于任务 hash。
- 所有状态写 Lua CAS：`fencing_token == token` 才写；`token==0` 表示不校验（scheduler/heal 用）。
- reaper 周期 `L/2=15s`，原子回收（Lua：检查 in_flight + 租约过期 → token++ + status=enqueued + inflight→queue）。
- `AcquireInFlight` 接受 `enqueued|retry_scheduled`（把 M3 的 heal 折进来）。

## Review Focus

1. 旧 token 写被拒：worker A token=1 租约过期后，A 的迟到写（token=1）不覆盖 B（token=2）的状态。
2. 心跳独立：POST 挂起 > RenewEvery 期间续租仍进行（不因同步 Do 阻塞）。
3. reaper 原子性：回收 = token++ + status=enqueued + 搬 queue，一步完成不丢不重。
4. AcquireInFlight 幂等：enqueued/retry_scheduled 都能进 in_flight，其它状态拒。
5. 租约存续的任务不被 reaper 回收。

---

## Task 1: state — fencing token + 租约

**Files:** Modify `internal/state/task.go`、`internal/state/redis.go`、`internal/state/schedule_retry_test.go`（改为带 token）

**Interfaces (Produces):**
- `StateMachine` 改为：
  - `AcquireInFlight(ctx, id TaskID, leaseDur time.Duration) (uint64, error)`
  - `Apply(ctx, id TaskID, tr Transition, token uint64) error`（token==0 不校验）
  - `ScheduleRetry(ctx, id TaskID, attempt int, nextRetryAt time.Time, token uint64) error`
  - `Renew(ctx, id TaskID, token uint64, leaseDur time.Duration) error`

- [ ] Step 1: 写失败测试 — `TestAcquireInFlight`（返回 token、status=in_flight、lease 已设）、`TestAcquireInFlightConflict`（非 enqueued/retry_scheduled）、`TestApplyTokenMatch`、`TestApplyStaleTokenRejected`、`TestRenewMatch`、`TestRenewStaleRejected`
- [ ] Step 2: 跑 → FAIL
- [ ] Step 3: 实现（4 个 Lua）
- [ ] Step 4: 跑 → PASS
- [ ] Step 5: commit

## Task 2: reaper 包

**Files:** Create `internal/reaper/reaper.go`、`internal/reaper/redis_test.go`；Modify `internal/queue`（加 `ListInflight`）

**Interfaces:**
- Consumes: `state.TaskID`
- Produces: `queue.ListInflight(ctx, tenantID) ([]state.TaskID, error)`；`reaper.New(tenants, client, interval)`；`(*Reaper).Run(ctx)` + `(*Reaper).sweepOnce(ctx)`

- [ ] Step 1: 写失败测试 — `TestReclaimExpired`（in_flight + 过期 → status=enqueued + token++ + inflight→queue）、`TestReclaimLiveLeaseNotTouched`
- [ ] Step 2: 跑 → FAIL
- [ ] Step 3: 实现（reclaim Lua + sweepOnce + Run）
- [ ] Step 4: 跑 → PASS
- [ ] Step 5: commit

## Task 3: worker 改造 — lease + heartbeat + token CAS

**Files:** Modify `internal/worker/worker.go`、`internal/worker/worker_test.go`

**Interfaces:**
- worker：`AcquireInFlight` 拿 token → 起独立心跳 goroutine（`Renew` 每 10s）→ 投递 → 成功 `Apply(in_flight→delivered, token)` / 失败 `ScheduleRetry(..., token)` → 停心跳。
- scheduler：`Apply(retry_scheduled→enqueued, 0)`。

- [ ] Step 1: 写失败测试 — `TestWorkerHeartbeatRenews`（下游挂起 >10s，断言 lease 被续）、`TestWorkerStaleWriteRejected`（token=1 过期后 reaper 接管 token=2，A 迟到写被拒）
- [ ] Step 2: 跑 → FAIL
- [ ] Step 3: 实现
- [ ] Step 4: 跑 → PASS
- [ ] Step 5: commit

## Task 4: main.go 接线 + smoke

**Files:** Modify `cmd/relay/main.go`

- [ ] Step 1: 接线 reaper（`go reaper.Run(ctx)`）
- [ ] Step 2: `go build` + `go vet`
- [ ] Step 3: smoke — 投递中 kill worker → reaper 15s 内接管 → 同 key 重投 → delivered
- [ ] Step 4: commit
