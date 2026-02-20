.PHONY: build run test lint clean conformance conformance-all conformance-grpc docker-up docker-down dev docker-dev

CONFORMANCE_RUNNER = ../ojs-conformance/runner/http
CONFORMANCE_GRPC_RUNNER = ../ojs-conformance/runner/grpc
CONFORMANCE_SUITES = ../../suites
OJS_URL ?= http://localhost:8080
GRPC_URL ?= localhost:9090
REDIS_URL ?= redis://localhost:6379

build:
	go build -o bin/ojs-server ./cmd/ojs-server

run: build
	OJS_ALLOW_INSECURE_NO_AUTH=true REDIS_URL=$(REDIS_URL) ./bin/ojs-server

test:
	go test ./... -race -cover

lint:
	golangci-lint run ./...

lint-vet:
	go vet ./...

clean:
	rm -rf bin/

# Docker
docker-up:
	docker compose -f docker/docker-compose.yml up --build -d

docker-down:
	docker compose -f docker/docker-compose.yml down

# Conformance tests (require running server + Redis)
conformance: conformance-all

conformance-all:
	cd $(CONFORMANCE_RUNNER) && go run . -url $(OJS_URL) -suites $(CONFORMANCE_SUITES) -redis $(REDIS_URL)

conformance-level-0:
	cd $(CONFORMANCE_RUNNER) && go run . -url $(OJS_URL) -suites $(CONFORMANCE_SUITES) -level 0 -redis $(REDIS_URL)

conformance-level-1:
	cd $(CONFORMANCE_RUNNER) && go run . -url $(OJS_URL) -suites $(CONFORMANCE_SUITES) -level 1 -redis $(REDIS_URL)

conformance-level-2:
	cd $(CONFORMANCE_RUNNER) && go run . -url $(OJS_URL) -suites $(CONFORMANCE_SUITES) -level 2 -redis $(REDIS_URL)

conformance-level-3:
	cd $(CONFORMANCE_RUNNER) && go run . -url $(OJS_URL) -suites $(CONFORMANCE_SUITES) -level 3 -redis $(REDIS_URL)

conformance-level-4:
	cd $(CONFORMANCE_RUNNER) && go run . -url $(OJS_URL) -suites $(CONFORMANCE_SUITES) -level 4 -redis $(REDIS_URL)

# Development with hot reload
dev:
	air -c .air.toml

docker-dev:
	docker compose -f docker/docker-compose.yml --profile dev up --build

# gRPC Conformance tests (require running server with gRPC enabled + Redis)
conformance-grpc:
	cd $(CONFORMANCE_GRPC_RUNNER) && go run . -url $(GRPC_URL) -suites $(CONFORMANCE_SUITES) -redis $(REDIS_URL)

conformance-grpc-level-0:
	cd $(CONFORMANCE_GRPC_RUNNER) && go run . -url $(GRPC_URL) -suites $(CONFORMANCE_SUITES) -level 0 -redis $(REDIS_URL)

conformance-grpc-level-1:
	cd $(CONFORMANCE_GRPC_RUNNER) && go run . -url $(GRPC_URL) -suites $(CONFORMANCE_SUITES) -level 1 -redis $(REDIS_URL)

conformance-grpc-level-2:
	cd $(CONFORMANCE_GRPC_RUNNER) && go run . -url $(GRPC_URL) -suites $(CONFORMANCE_SUITES) -level 2 -redis $(REDIS_URL)

conformance-grpc-level-3:
	cd $(CONFORMANCE_GRPC_RUNNER) && go run . -url $(GRPC_URL) -suites $(CONFORMANCE_SUITES) -level 3 -redis $(REDIS_URL)

conformance-grpc-level-4:
	cd $(CONFORMANCE_GRPC_RUNNER) && go run . -url $(GRPC_URL) -suites $(CONFORMANCE_SUITES) -level 4 -redis $(REDIS_URL)
