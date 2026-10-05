SHELL := /bin/bash
GOBIN := $(shell go env GOPATH)/bin
export PATH := $(GOBIN):$(PATH)

COMPOSE := docker compose
LINES ?= 100000
RATE ?= 0
DATASET ?= hdfs

.DEFAULT_GOAL := help

.PHONY: help
help: ## List targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-18s %s\n", $$1, $$2}'

.PHONY: up
up: ## Build images and start the full stack, waiting for health checks
	@mkdir -p data/run
	$(COMPOSE) up -d --build --wait

.PHONY: down
down: ## Stop the stack and delete its volumes
	$(COMPOSE) down -v --remove-orphans

.PHONY: logs
logs: ## Follow logs from the Go services
	$(COMPOSE) logs -f agent collector indexer query-api

.PHONY: build
build: ## Build all binaries into bin/
	go build -o bin/ ./cmd/...

.PHONY: proto
proto: ## Regenerate Go code from proto/
	buf lint
	buf generate

.PHONY: test
test: test-unit test-integration ## Run unit and integration tests

.PHONY: test-unit
test-unit: ## Unit tests with the race detector
	go test -race -count=1 ./...

.PHONY: test-integration
test-integration: ## Integration tests against Kafka and OpenSearch containers (needs Docker)
	go test -race -count=1 -tags=integration -timeout=15m ./integration/...

.PHONY: lint
lint: ## golangci-lint and buf lint
	golangci-lint run ./...
	buf lint

.PHONY: bench
bench: ## Go microbenchmarks; raw output saved to results/
	@mkdir -p results
	@f=results/bench-$$(date -u +%Y%m%dT%H%M%SZ).txt; \
	{ echo "# command: make bench"; echo "# commit: $$(git rev-parse --short HEAD 2>/dev/null || echo none)"; \
	  go test -run='^$$' -bench=. -benchmem -count=5 ./...; } 2>&1 | tee $$f; echo "saved $$f"

.PHONY: data
data: ## Download a Loghub dataset into data/loghub (DATASET=hdfs|apache)
	scripts/download_loghub.sh $(DATASET)

.PHONY: e2e
e2e: ## Replay LINES Loghub lines at RATE/s through the running stack and check for loss
	scripts/e2e.sh $(LINES) $(RATE)

.PHONY: ci
ci: ## Run the GitHub Actions workflow locally with act (one job at a time: parallel jobs race on the shared toolcache)
	act push --concurrent-jobs 1 -P ubuntu-latest=catthehacker/ubuntu:act-latest --container-architecture linux/$(shell uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')
