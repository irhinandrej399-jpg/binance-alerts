-include .env
DATABASE_URL ?= postgres://market:local-development-only@localhost:54329/market?sslmode=disable
SYMBOLS ?= BTCUSDT,ETHUSDT
TEST_DATABASE_URL ?= postgres://market:local-development-only@localhost:54329/market?sslmode=disable
TEST_KAFKA_BROKERS ?= localhost:19092
export TRACKING_MODE TRACKING_TOPIC TRACKING_GROUP TRACKING_REFRESH_INTERVAL DATABASE_URL SYMBOLS BINANCE_WS_URL KAFKA_BROKERS KAFKA_TOPIC HTTP_ADDR BATCH_SIZE BATCH_TIMEOUT POLL_INTERVAL RETRY_DELAY OUTBOX_RETENTION

.PHONY: dev-up dev-down migrate run check integration build

dev-up:
	docker compose -f deploy/compose/compose.yaml up -d --wait postgres kafka
	docker compose -f deploy/compose/compose.yaml run --rm kafka-init
	docker compose -f deploy/compose/compose.yaml run --rm tracking-init

dev-down:
	docker compose -f deploy/compose/compose.yaml down

migrate:
	go run ./services/market-data/cmd/market-data migrate

run:
	go run ./services/market-data/cmd/market-data

check:
	go vet ./...
	go test -race ./...

integration:
	TEST_DATABASE_URL='$(TEST_DATABASE_URL)' TEST_KAFKA_BROKERS='$(TEST_KAFKA_BROKERS)' go test -race -tags=integration -count=1 ./services/market-data/...

build:
	go build -o bin/market-data ./services/market-data/cmd/market-data
