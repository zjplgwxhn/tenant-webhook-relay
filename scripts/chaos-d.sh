#!/usr/bin/env bash
# 场景 D：多租户过载（5 慢 + 5 健康），验证健康租户隔离。
set -euo pipefail
cd "$(dirname "$0")/.."

mkdir -p bin
go build -o bin/devredis ./cmd/devredis
go build -o bin/mock ./cmd/mock
go build -o bin/relay ./cmd/relay

bin/devredis >/dev/null 2>&1 & REDIS_PID=$!
bin/mock >/dev/null 2>&1 & MOCK_PID=$!
TENANTS="slow1=http://127.0.0.1:9000,slow2=http://127.0.0.1:9000,slow3=http://127.0.0.1:9000,slow4=http://127.0.0.1:9000,slow5=http://127.0.0.1:9000,fast1=http://127.0.0.1:9000,fast2=http://127.0.0.1:9000,fast3=http://127.0.0.1:9000,fast4=http://127.0.0.1:9000,fast5=http://127.0.0.1:9000"
REDIS_ADDR=127.0.0.1:6379 TENANTS="$TENANTS" HTTP_ADDR=:8080 bin/relay >/dev/null 2>&1 & RELAY_PID=$!
trap 'kill -9 $REDIS_PID $MOCK_PID $RELAY_PID 2>/dev/null || true' EXIT

sleep 2

echo "== 慢租户灌入（下游挂起） =="
curl -s -X POST localhost:9000/__config -d '{"hang_ms":5000,"status_code":200}' >/dev/null
for i in $(seq 1 50); do
  curl -s -o /dev/null -X POST localhost:8080/v1/events -H 'X-Tenant-ID: slow1' -d '{"payload":{}}' &
done

echo "== 健康租户投递（应快速成功） =="
start=$(date +%s%N)
curl -s -o /dev/null -w 'fast1 ingest status=%{http_code}\n' \
  -X POST localhost:8080/v1/events -H 'Content-Type: application/json' -H 'X-Tenant-ID: fast1' \
  -d '{"producer_key":"e1","payload":{"a":1}}'
end=$(date +%s%N)
echo "fast1 ingest latency: $(( (end - start) / 1000000 )) ms"

echo "chaos-d 脚本执行完成（健康租户 P99 需压测基线，见设计 §4 场景 D）"
