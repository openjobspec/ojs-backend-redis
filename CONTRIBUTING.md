# Contributing

Thanks for contributing to `ojs-backend-redis`.

## Prerequisites

- Go 1.23+
- Redis 7.x
- Docker (optional)

## Local setup

```bash
git clone https://github.com/openjobspec/ojs-backend-redis.git
cd ojs-backend-redis
make build
```

Run with Redis:

```bash
REDIS_URL=redis://localhost:6379 make run
```

## Quality checks

Run tests:

```bash
REDIS_URL=redis://localhost:6379 make test
```

Run lints:

```bash
golangci-lint run ./...
```

If `golangci-lint` is not installed:

```bash
go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
```

## Pull request guidelines

1. Create a topic branch from `main`.
2. Keep changes scoped and include tests for behavior changes.
3. Ensure build and tests pass locally.
4. Open a PR with a clear description of the change and motivation.

By participating, you agree to follow the [Code of Conduct](CODE_OF_CONDUCT.md).
