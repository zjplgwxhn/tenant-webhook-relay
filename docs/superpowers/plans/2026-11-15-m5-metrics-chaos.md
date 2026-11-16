# M5: Prometheus 指标 + mock receiver + 故障注入脚本 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** 暴露 Prometheus 指标（设计 §5），提供可控 mock 下游，固化为四个故障注入脚本 + 压测脚本。

**Architecture:** 新增 `internal/metrics`（collectors）；把 `*Metrics` 注入 ingest/worker/limiter/reaper；`cmd/mock` 是脚本化下游；`scripts/chaos-{a,b,c,d}.sh` 与 `bench.sh` 编排验证。

**Tech Stack:** Go 1.22+，prometheus/client_golang，net/http。

**Spec:** `docs/design-webhook-relay.md`（§4、§5）。

## Global Constraints

- 指标名/标签严格按设计 §5（`webhook_*`，带 `tenant` 标签的用 Vec）。
- 指标用 `prometheus.NewRegistry()`（非默认全局），便于测试与隔离。
- `redis_up` 由 main.go 的探活 goroutine 周期 `Ping` 更新。
- 故障脚本断言走 `/metrics` 输出（`curl .../metrics | grep`）。

## Review Focus

1. 指标重复注册 panic：每个 collector 只注册一次（NewMetrics 只调用一次）。
2. 429 指标：队列满时 `webhook_ingest_429_total` 与 `webhook_ingest_total{status="429"}` 都 +1。
3. 死信指标：窗口耗尽进 DLQ → `webhook_delivery_permanent_failure_total` +1。
4. 租约接管指标：reaper 回收 → `webhook_lease_expired_total` +1。
5. mock receiver 的 `/__requests` 能记录 Idempotency-Key 与到达时间。

---

## Task 1: metrics 包

**Files:** Create `internal/metrics/metrics.go`、`internal/metrics/metrics_test.go`

**Interfaces (Produces):** `metrics.Metrics`（设计 §5 全部 collector）、`metrics.NewMetrics(reg *prometheus.Registry) *Metrics`

- [ ] Step 1: 写失败测试 — `TestMetricsRegistered`（testutil 采集断言每个 collector 存在且初始为 0）、`TestMetricsIncrement`（Inc 后值正确）
- [ ] Step 2: 跑 → FAIL
- [ ] Step 3: 实现
- [ ] Step 4: 跑 → PASS
- [ ] Step 5: commit

## Task 2: 指标接线

**Files:** Modify `internal/ingest/handler.go`、`internal/worker/worker.go`、`internal/worker/supervisor.go`、`internal/limiter/limiter.go`、`internal/reaper/reaper.go`、`cmd/relay/main.go`

**Interfaces:** 各构造器加 `m *metrics.Metrics`（或窄接口）；main.go 挂 `/metrics` + `redis_up` 探活。

- [ ] Step 1: 接线（ingest 计数、worker 投递/死信/退避/旧写、limiter 超时、reaper 回收）
- [ ] Step 2: `go build` + `go vet`（确保无遗漏编译错）
- [ ] Step 3: `go test` 全绿（既有测试不破坏）
- [ ] Step 4: commit

## Task 3: mock receiver

**Files:** Create `cmd/mock/main.go`

**Interfaces:** `GET/POST /__config`（设 status/retry_after/hang）、`GET /__requests`（请求日志）、`POST /`（下游）

- [ ] Step 1: 实现（可脚本化状态码/Retry-After/挂起，记录 Idempotency-Key + 到达时间）
- [ ] Step 2: `go build ./cmd/mock`
- [ ] Step 3: commit

## Task 4: 故障注入 + 压测脚本

**Files:** Create `scripts/chaos-a.sh`、`chaos-b.sh`、`chaos-c.sh`、`chaos-d.sh`、`bench.sh`

- [ ] Step 1: 四个脚本（A kill worker / B Redis 宕机 / C 下游 429/5xx / D 多租户过载），断言走 `/metrics`
- [ ] Step 2: `bench.sh`（hey，报告 P99/吞吐/429 率）
- [ ] Step 3: 手动跑 `chaos-c`（最轻量）验证脚本可用
- [ ] Step 4: commit
