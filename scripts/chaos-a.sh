#!/usr/bin/env bash
# 场景 A：投递中 kill worker → 租约过期 → reaper 接管 → 同 key 重投 → delivered。
set -euo pipefail
cd "$(dirname "$0")/.."

mkdir -p bin
go build -o bin/devredis ./cmd/devredis
go build -o bin/mock ./cmd/mock
go build -o bin/relay ./cmd/relay

bin/devredis >/dev/null 2>&1 & REDIS_PID=$!
bin/mock >/dev/null 2>&1 & MOCK_PID=$!
trap 'kill -9 $REDIS_PID $MOCK_PID ${RELAY_PID:-} 2>/dev/null || true' EXIT

sleep 1

start_relay() {
  REDIS_ADDR=127.0.0.1:6379 TENANTS='t1=http://127.0.0.1:9000' HTTP_ADDR=:8080 \
    LEASE_DURATION=1 REAPER_INTERVAL=1 bin/relay >/dev/null 2>&1 & RELAY_PID=$!
}

# 下游挂起 10s（长于租约 1s），让首个 worker 卡在投递中
curl -s -X POST localhost:9000/__config -d '{"hang_ms":10000,"status_code":200}' >/dev/null

start_relay
sleep 2

echo "== 投递事件（worker 卡在挂起中） =="
curl -s -o /dev/null -w 'ingest status=%{http_code}\n' \
  -X POST localhost:8080/v1/events -H 'Content-Type: application/json' -H 'X-Tenant-ID: t1' \
  -d '{"producer_key":"e1","payload":{"a":1}}'
sleep 1

echo "== kill worker（模拟崩溃） =="
kill -9 "$RELAY_PID" 2>/dev/null || true
sleep 1

echo "== 重启 relay，reaper 接管租约过期的任务 =="
start_relay
sleep 4

echo "== 断言：同 key 至少两次投递 =="
curl -s localhost:9000/__requests | grep -c 'key=t1:e1' | awk '{ if ($1 >= 2) print "chaos-a PASS (deliveries=" $1 ")"; else print "chaos-a FAIL" }'
