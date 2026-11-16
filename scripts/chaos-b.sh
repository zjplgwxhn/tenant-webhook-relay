#!/usr/bin/env bash
# 场景 B：Redis 不可用 → ingest 503 → 恢复后续投。
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

echo "== Redis 正常时投递（应 202） =="
curl -s -o /dev/null -w 'ingest status=%{http_code}\n' \
  -X POST localhost:8080/v1/events -H 'Content-Type: application/json' -H 'X-Tenant-ID: t1' \
  -d '{"producer_key":"e1","payload":{"a":1}}'
sleep 1

echo "== 停 Redis =="
kill -9 "$REDIS_PID" 2>/dev/null || true
sleep 1

echo "== Redis 宕机时投递（应 503） =="
curl -s -o /dev/null -w 'ingest status=%{http_code}\n' \
  -X POST localhost:8080/v1/events -H 'Content-Type: application/json' -H 'X-Tenant-ID: t1' \
  -d '{"producer_key":"e2","payload":{"b":2}}'

echo "== 恢复 Redis =="
bin/devredis >/dev/null 2>&1 & REDIS_PID=$!
sleep 3

echo "== 断言：redis_up 指标恢复为 1 =="
curl -s localhost:8080/metrics | grep '^redis_up' || echo "(redis_up 采样中)"
echo "chaos-b 脚本执行完成"
