# ojs-backend-redis

![CI](https://github.com/openjobspec/ojs-backend-redis/actions/workflows/ci.yml/badge.svg)
![Conformance](https://github.com/openjobspec/ojs-backend-redis/raw/main/.github/badges/conformance.svg)
![Security](https://github.com/openjobspec/ojs-backend-redis/actions/workflows/security.yml/badge.svg)

A Redis-backed implementation of the [OpenJobSpec (OJS)](https://github.com/openjobspec) specification — a standard interface for distributed job queues and workflow orchestration. This backend gives you a production-ready job server with priority queuing, retries, scheduling, cron jobs, workflows, and dead letter handling, all powered by Redis.

## Key Features

- **Full OJS compliance** — Passes conformance levels 0–4 with 17 supported capabilities
- **Priority queuing** — Jobs are ordered by priority (-100 to 100) within each queue
- **Retry policies** — Exponential, linear, and constant backoff with jitter and non-retryable error patterns
- **Scheduled jobs** — Delay execution until a specific time
- **Cron scheduling** — Register recurring jobs with standard cron expressions and timezone support
- **Workflows** — Chain (sequential), group (parallel), and batch execution with callbacks
- **Dead letter queue** — Inspect, retry, or delete failed jobs that have exhausted retries
- **Job deduplication** — Unique policies with configurable conflict resolution
- **Queue management** — Pause/resume queues, view per-queue statistics
- **Batch enqueue** — Atomically submit multiple jobs in a single request
- **Graceful shutdown** — Clean signal handling with in-flight request draining

## Prerequisites

- **Go** 1.23 or later
- **Redis** 7.x
- **Docker** and **Docker Compose** (optional, for containerized setup)

## Installation

Clone the repository and build:

```bash
git clone https://github.com/openjobspec/ojs-backend-redis.git
cd ojs-backend-redis
make build
```

This compiles the server binary to `bin/ojs-server`.

## Quick Start

### Option 1: Docker Compose (recommended)

Start both Redis and the OJS server with a single command:

```bash
make docker-up
```

The server will be available at `http://localhost:8080`. To stop:

```bash
make docker-down
```

### Option 2: Run locally

Start a Redis instance, then run the server:

```bash
# Start Redis (if not already running)
redis-server &

# Run the OJS server
make run
```

### Verify it's working

```bash
curl http://localhost:8080/ojs/v1/health
```

## Usage Examples

### Enqueue a job

```bash
curl -X POST http://localhost:8080/ojs/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "type": "email.send",
    "args": [{"to": "user@example.com", "subject": "Hello"}],
    "options": {
      "queue": "default",
      "priority": 10
    }
  }'
```

### Fetch a job (as a worker)

```bash
curl -X POST http://localhost:8080/ojs/v1/workers/fetch \
  -H "Content-Type: application/json" \
  -d '{
    "queues": ["default"],
    "worker_id": "worker-1"
  }'
```

### Acknowledge job completion

```bash
curl -X POST http://localhost:8080/ojs/v1/workers/ack \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": "<job-id>",
    "worker_id": "worker-1",
    "result": {"status": "delivered"}
  }'
```

### Report job failure

```bash
curl -X POST http://localhost:8080/ojs/v1/workers/nack \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": "<job-id>",
    "worker_id": "worker-1",
    "error": "connection timeout"
  }'
```

### Register a cron job

```bash
curl -X POST http://localhost:8080/ojs/v1/cron \
  -H "Content-Type: application/json" \
  -d '{
    "name": "daily-report",
    "schedule": "0 9 * * *",
    "timezone": "America/New_York",
    "type": "reports.generate",
    "queue": "reports"
  }'
```

### Create a workflow (chain)

```bash
curl -X POST http://localhost:8080/ojs/v1/workflows \
  -H "Content-Type: application/json" \
  -d '{
    "type": "chain",
    "steps": [
      {"type": "extract.data", "queue": "etl"},
      {"type": "transform.data", "queue": "etl"},
      {"type": "load.data", "queue": "etl"}
    ]
  }'
```

### Batch enqueue

```bash
curl -X POST http://localhost:8080/ojs/v1/jobs/batch \
  -H "Content-Type: application/json" \
  -d '{
    "jobs": [
      {"type": "email.send", "args": [{"to": "a@example.com"}], "options": {"queue": "default"}},
      {"type": "email.send", "args": [{"to": "b@example.com"}], "options": {"queue": "default"}}
    ]
  }'
```

## API Reference

### System

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/ojs/manifest` | Server manifest (version, capabilities) |
| `GET` | `/ojs/v1/health` | Health check with Redis latency |

### Jobs

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/ojs/v1/jobs` | Enqueue a job |
| `GET` | `/ojs/v1/jobs/{id}` | Get job details |
| `DELETE` | `/ojs/v1/jobs/{id}` | Cancel a job |
| `POST` | `/ojs/v1/jobs/batch` | Batch enqueue multiple jobs |

### Workers

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/ojs/v1/workers/fetch` | Fetch available jobs from queues |
| `POST` | `/ojs/v1/workers/ack` | Acknowledge job completion |
| `POST` | `/ojs/v1/workers/nack` | Report job failure |
| `POST` | `/ojs/v1/workers/heartbeat` | Extend visibility timeout |

### Queues

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/ojs/v1/queues` | List all queues |
| `GET` | `/ojs/v1/queues/{name}/stats` | Get queue statistics |
| `POST` | `/ojs/v1/queues/{name}/pause` | Pause a queue |
| `POST` | `/ojs/v1/queues/{name}/resume` | Resume a paused queue |

### Dead Letter

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/ojs/v1/dead-letter` | List dead letter jobs |
| `POST` | `/ojs/v1/dead-letter/{id}/retry` | Retry a dead letter job |
| `DELETE` | `/ojs/v1/dead-letter/{id}` | Delete a dead letter job |

### Cron

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/ojs/v1/cron` | List registered cron jobs |
| `POST` | `/ojs/v1/cron` | Register a cron job |
| `DELETE` | `/ojs/v1/cron/{name}` | Delete a cron job |

### Workflows

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/ojs/v1/workflows` | Create a workflow |
| `GET` | `/ojs/v1/workflows/{id}` | Get workflow status |
| `DELETE` | `/ojs/v1/workflows/{id}` | Cancel a workflow |

## Configuration

The server is configured through environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `OJS_PORT` | `8080` | HTTP server listen port |
| `REDIS_URL` | `redis://localhost:6379` | Redis connection URL |

The `REDIS_URL` follows the standard Redis URI format: `redis://[user[:password]@]host[:port][/db]`

## Project Structure

```
ojs-backend-redis/
├── cmd/
│   └── ojs-server/
│       └── main.go              # Application entry point
├── internal/
│   ├── api/                     # HTTP handlers and middleware
│   │   ├── handler_jobs.go      # Job CRUD endpoints
│   │   ├── handler_workers.go   # Worker fetch/ack/nack/heartbeat
│   │   ├── handler_workflows.go # Workflow orchestration
│   │   ├── handler_dead_letter.go
│   │   ├── handler_batch.go
│   │   ├── handler_queues.go
│   │   ├── handler_cron.go
│   │   ├── handler_system.go    # Health and manifest
│   │   ├── middleware.go
│   │   └── errors.go
│   ├── core/                    # Domain models and interfaces
│   │   ├── backend.go           # Backend interface definition
│   │   ├── job.go               # Job and request structs
│   │   ├── state.go             # Job state machine
│   │   ├── validate.go          # Request validation
│   │   ├── retry.go             # Retry policy configuration
│   │   ├── unique.go            # Deduplication policy
│   │   └── ...
│   ├── redis/                   # Redis backend implementation
│   │   ├── backend.go           # Core Redis operations
│   │   ├── codec.go             # Job serialization
│   │   └── keys.go              # Redis key schema
│   ├── scheduler/               # Background processing loops
│   │   └── scheduler.go
│   └── server/                  # HTTP server setup
│       ├── config.go            # Environment-based configuration
│       └── server.go            # Router and route registration
├── docker/
│   ├── Dockerfile               # Multi-stage production build
│   └── docker-compose.yml       # Redis + OJS server stack
├── .github/workflows/
│   ├── ci.yml                   # Build, lint, and test pipeline
│   └── conformance.yml          # OJS conformance test suite
├── Makefile
├── go.mod
└── go.sum
```

## Development

### Makefile Targets

| Target | Description |
|--------|-------------|
| `make build` | Compile the server binary |
| `make run` | Build and run the server |
| `make test` | Run tests with race detection and coverage |
| `make lint` | Run static analysis (`go vet`) |
| `make clean` | Remove build artifacts |
| `make docker-up` | Start Redis + server via Docker Compose |
| `make docker-down` | Stop Docker Compose services |
| `make conformance` | Run all OJS conformance test levels |
| `make conformance-level-N` | Run a specific conformance level (0–4) |

### Running Tests

```bash
make test
```

### Conformance Tests

The conformance test suite validates this implementation against the OJS specification. It requires the [ojs-conformance](https://github.com/openjobspec/ojs-conformance) repository checked out as a sibling directory:

```bash
# With the server running:
make conformance           # All levels
make conformance-level-0   # Individual level
```

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for setup,
testing, and pull request expectations.

Please also review:

- [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)
- [SECURITY.md](SECURITY.md)
- [CHANGELOG.md](CHANGELOG.md)

## Observability

### OpenTelemetry

The server supports distributed tracing via OpenTelemetry. Set the following environment variable to enable:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
```

Traces are exported in OTLP format over gRPC. Compatible with Jaeger, Zipkin, Grafana Tempo, and any OTLP-compatible collector.

You can also use the legacy env vars `OJS_OTEL_ENABLED=true` and `OJS_OTEL_ENDPOINT` for explicit control.

## Production Deployment Notes

- **Rate limiting**: This server does not enforce request rate limits. Place a reverse proxy (e.g., Nginx, Envoy, or a cloud load balancer) in front of the server to add rate limiting in production.
- **Authentication**: Set `OJS_API_KEY` to require Bearer token auth on all endpoints. For local-only testing, set `OJS_ALLOW_INSECURE_NO_AUTH=true`.
- **TLS**: Terminate TLS at a reverse proxy or load balancer rather than at the application level.

## License

See the [OpenJobSpec](https://github.com/openjobspec) organization for licensing details.



