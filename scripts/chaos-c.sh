#!/usr/bin/env bash
# 场景 C：下游持续返回 429/5xx → 退避调度 → 窗口耗尽进 DLQ。
set -euo pipefail
cd "$(dirname "$0")/.."

mkdir -p bin
go build -o bin/devredis ./cmd/devredis
go build -o bin/mock ./cmd/mock
go build -o bin/relay ./cmd/relay

bin/devredis >/dev/null 2>&1 & REDIS_PID=$!
bin/mock >/dev/null 2>&1 & MOCK_PID=$!
REDIS_ADDR=127.0.0.1:6379 TENANTS='t1=http://127.0.0.1:9000' HTTP_ADDR=:8080 bin/relay >/dev/null 2>&1 & RELAY_PID=$!
trap 'kill -9 $REDIS_PID $MOCK_PID $RELAY_PID 2>/dev/null || true' EXIT

sleep 2

# 下游持续 500
curl -s -X POST localhost:9000/__config -d '{"status_code":500}' >/dev/null

echo "== 投递一个事件（下游 500） =="
curl -s -o /dev/null -w 'ingest status=%{http_code}\n' \
  -X POST localhost:8080/v1/events -H 'Content-Type: application/json' -H 'X-Tenant-ID: t1' \
  -d '{"producer_key":"e1","payload":{"a":1}}'

sleep 3  # 等至少一次投递尝试

echo "== 断言：投递尝试发生、进入退避 =="
curl -s localhost:8080/metrics | grep -E 'webhook_delivery_attempts_total|webhook_retry_delay_seconds' | grep -v '^#'
echo "chaos-c 脚本执行完成（完整 DLQ 断言需把 RetryWindow 调小，见设计 §4 场景 C）"
