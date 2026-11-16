# Webhook Relay — 常用目标
# 用法：make <target>

GO           ?= go
TAGS         ?= integration
REDIS_ADDR   ?= localhost:6379
HTTP_ADDR    ?= :8080
METRICS_ADDR ?= :9090

.PHONY: test build run accept-m1 accept-m2 accept-m3 accept-m4 accept-m5 \
        chaos-a chaos-b chaos-c chaos-d bench lint up down redis-up redis-down

## 单元测试（miniredis，无需外部依赖）
test:
	$(GO) test ./...

## 构建 relay 二进制到 bin/relay
build:
	$(GO) build -o bin/relay ./cmd/relay

## 启动 Redis（docker）+ 本地跑 relay
run: redis-up
	REDIS_ADDR=$(REDIS_ADDR) HTTP_ADDR=$(HTTP_ADDR) METRICS_ADDR=$(METRICS_ADDR) $(GO) run ./cmd/relay

## 各里程碑集成验收（-tags integration）
accept-m1:
	$(GO) test ./internal/ingest/... ./internal/idempotency/... ./internal/queue/... ./internal/state/... -tags $(TAGS)

accept-m2:
	$(GO) test ./internal/worker/... ./internal/limiter/... -tags $(TAGS)

accept-m3:
	$(GO) test ./internal/backoff/... ./internal/delayed/... ./internal/dlq/... -tags $(TAGS)

accept-m4:
	$(GO) test ./internal/lease/... ./internal/reaper/... -tags $(TAGS)

accept-m5:
	$(GO) test ./internal/metrics/... ./cmd/mock/... -tags $(TAGS)

## 故障注入（依赖 M5 脚本 + 全量 compose）
chaos-a:
	bash scripts/chaos-a.sh

chaos-b:
	bash scripts/chaos-b.sh

chaos-c:
	bash scripts/chaos-c.sh

chaos-d:
	bash scripts/chaos-d.sh

## 压测
bench:
	bash scripts/bench.sh

## 静态检查
lint:
	golangci-lint run

## 基础设施
up:
	docker compose up -d

down:
	docker compose down

redis-up:
	docker compose up -d redis

redis-down:
	docker compose stop redis
