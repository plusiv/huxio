.DEFAULT_GOAL := help
SHELL := /bin/bash

BINARY  := huxio
VERSION ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo dev)
LDFLAGS := -X main.version=$(VERSION)

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Build the binary into bin/huxio
	go build -ldflags "$(LDFLAGS)" -o bin/$(BINARY) ./cmd/huxio

.PHONY: run
run: ## Run all roles in one process
	go run ./cmd/huxio serve --role=all

.PHONY: fmt
fmt: ## Format all Go code
	gofmt -w .

.PHONY: vet
vet: ## Run go vet
	go vet ./...

.PHONY: lint
lint: ## Run golangci-lint
	golangci-lint run

.PHONY: test
test: ## Run unit tests with the race detector
	go test -race $$(go list ./... | grep -v '/tests/integrations')

.PHONY: test-integration
test-integration: ## Run integration tests (requires Docker)
	go test -race -count=1 ./tests/integrations/...

.PHONY: test-all
test-all: test test-integration ## Run every test

.PHONY: cover
cover: ## Coverage across every test, including the integration suite
	go test -count=1 -coverpkg=./internal/...,./configs,./migrations \
		-coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1

.PHONY: migrate-up
migrate-up: ## Apply schema migrations
	go run ./cmd/huxio migrate up

.PHONY: migrate-down
migrate-down: ## Roll back the last migration
	go run ./cmd/huxio migrate down

.PHONY: migrate-status
migrate-status: ## Show migration status
	go run ./cmd/huxio migrate status

.PHONY: migrate-create
migrate-create: ## Create a migration: make migrate-create NAME=add_something
	@test -n "$(NAME)" || (echo "NAME is required" && exit 1)
	@next=$$(printf "%05d" $$(( $$(ls migrations/schema/*.sql 2>/dev/null | wc -l | tr -d ' ') + 1 ))); \
	file="migrations/schema/$${next}_$(NAME).sql"; \
	printf -- "-- +goose Up\n\n-- +goose Down\n" > $$file; \
	echo "created $$file"

.PHONY: keygen-env
keygen-env: ## Write the two required secrets into .env (does not overwrite)
	@test ! -f .env || (echo ".env already exists; edit it by hand" && exit 1)
	@cp .env.example .env
	@key=$$(go run ./cmd/huxio keygen); \
	  jwt=$$(head -c 32 /dev/urandom | base64); \
	  sed -i.bak "s|^HUXIO_ENCRYPTION_KEY=.*|HUXIO_ENCRYPTION_KEY=$$key|" .env; \
	  sed -i.bak "s|^HUXIO_JWT_SECRET=.*|HUXIO_JWT_SECRET=$$jwt|" .env; \
	  rm -f .env.bak
	@echo "wrote .env with a fresh encryption key and JWT secret"

.PHONY: up
up: ## Bring up the whole stack in Docker: Postgres, API, 2 workers, metrics
	docker compose up -d --build

.PHONY: down
down: ## Tear down the Docker stack
	docker compose down

.PHONY: logs
logs: ## Follow the stack's logs
	docker compose logs -f api worker

.PHONY: dev-up
dev-up: ## Start only the dependencies (Postgres, Prometheus, Grafana)
	docker compose -f deploy/docker-compose.yml up -d

.PHONY: dev-down
dev-down: ## Stop the local stack
	docker compose -f deploy/docker-compose.yml down

.PHONY: docker-build
docker-build: ## Build the container image
	docker build -t huxio:$(VERSION) .

.PHONY: openapi
openapi: ## Write the generated OpenAPI document to openapi.json
	go run ./cmd/huxio openapi -o openapi.json

# The harness truncates every table it runs against, so the bench targets use
# their own throwaway database on port 5433 rather than whatever the dev stack
# or HUXIO_DATABASE_URL points at. Override BENCH_DSN to run somewhere else.
BENCH_CONTAINER := huxio-bench-db
BENCH_PORT      := 5433
BENCH_DSN       ?= postgres://postgres:postgres@localhost:$(BENCH_PORT)/bench?sslmode=disable
BENCH           := go run ./cmd/huxio bench --dsn='$(BENCH_DSN)'

.PHONY: bench-db
bench-db: ## Start the throwaway Postgres the benchmarks run against
	@docker inspect $(BENCH_CONTAINER) >/dev/null 2>&1 || \
		docker run -d --name $(BENCH_CONTAINER) \
			-e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=bench \
			-p $(BENCH_PORT):5432 postgres:16-alpine >/dev/null
	@docker start $(BENCH_CONTAINER) >/dev/null 2>&1 || true
	@until docker exec $(BENCH_CONTAINER) pg_isready -U postgres >/dev/null 2>&1; do sleep 1; done
	@echo "benchmark database ready on port $(BENCH_PORT)"

.PHONY: bench-db-down
bench-db-down: ## Remove the throwaway benchmark database
	@docker rm -f $(BENCH_CONTAINER) >/dev/null 2>&1 || true
	@echo "benchmark database removed"

.PHONY: bench
bench: bench-db ## Run the throughput benchmark scenario
	$(BENCH) --scenario=throughput

.PHONY: bench-isolation
bench-isolation: bench-db ## Run the isolation scenario (the product claim)
	$(BENCH) --scenario=isolation

.PHONY: bench-all
bench-all: bench-db ## Run every short benchmark scenario
	$(BENCH) --scenario=throughput
	$(BENCH) --scenario=isolation
	$(BENCH) --scenario=retry-storm
	$(BENCH) --scenario=cold-start

.PHONY: bench-baseline
bench-baseline: bench-db ## Record the current throughput run as the CI baseline
	@mkdir -p bench/baselines
	$(BENCH) --scenario=throughput --duration=30s
	cp bench/results/throughput-*.json bench/baselines/throughput.json
	@echo "baseline recorded"
