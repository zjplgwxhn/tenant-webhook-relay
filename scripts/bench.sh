#!/usr/bin/env bash
# 压测：hey 报告 P99 / 吞吐 / 429 率。
set -euo pipefail
cd "$(dirname "$0")/.."

mkdir -p bin
go build -o bin/devredis ./cmd/devredis
go build -o bin/mock ./cmd/mock
go build -o bin/relay ./cmd/relay

bin/devredis >/dev/null 2>&1 & REDIS_PID=$!
bin/mock >/dev/null 2>&1 & MOCK_PID=$!
REDIS_ADDR=127.0.0.1:6379 TENANTS='t1=http://127.0.0.1:9000' HTTP_ADDR=:8080 \
  QUEUE_CAPACITY=1000 bin/relay >/dev/null 2>&1 & RELAY_PID=$!
trap 'kill -9 $REDIS_PID $MOCK_PID $RELAY_PID 2>/dev/null || true' EXIT

sleep 2

command -v hey >/dev/null 2>&1 || { echo "需要安装 hey: go install github.com/rakyll/hey@latest"; exit 1; }

N=${BENCH_N:-1000}
C=${BENCH_C:-50}

echo "== hey 压测（$N 请求 / $C 并发） =="
hey -n "$N" -c "$C" -m POST -H 'Content-Type: application/json' -H 'X-Tenant-ID: t1' \
  -d '{"producer_key":"bench","payload":{"a":1}}' http://localhost:8080/v1/events

echo "== 429 率 =="
curl -s localhost:8080/metrics | grep -E 'webhook_ingest_429_total|webhook_ingest_total' | grep -v '^#'
