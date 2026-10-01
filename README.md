# equalix-go

> Eventually-fair weighted-fair scheduler for high-throughput multi-tenant systems — written in Go.

[![Go Version](https://img.shields.io/badge/go-1.22+-00ADD8?logo=go)](https://go.dev)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Status](https://img.shields.io/badge/status-pre--alpha-orange.svg)](#status)

`equalix-go` is a Go implementation of [Equalix](https://github.com/synanton/equalix), a scheduler that sits between a task queue and a rate-limited executor, guaranteeing proportional fairness across tenants while maximizing throughput.

It preserves the fairness guarantees of the Java reference implementation while leveraging Go's concurrency model, low memory footprint, and single-binary deployment.

---

## Status

**Pre-alpha.** Under active development. Not yet production-ready. See the [epic ticket](docs/epic.md) for the phased roadmap.

---

## Features

- **Weighted-fair scheduling** via persistent virtual-time finish tags
- **Approximate in-flight counting** using Count-Min Sketch (fixed ~128 KB for 10k keys)
- **Adaptive RPS control** with latency EMA, error-rate brake, and dead-band dampener
- **Watchdog reconciliation** to repair CMS drift against the authoritative task table
- **Hierarchical fairness** with per-tenant quotas and anti-starvation aging
- **Sequential execution mode** with sequence-based boosts
- **Horizontal scalability** via stateless compute + PostgreSQL + Redis
- **Prometheus metrics** and structured logging
- **Single static binary** with YAML/env configuration

---

## Architecture

`Equalix-Go` follows a hexagonal (ports-and-adapters) architecture:

```text
┌─────────────────────────────────────────────────┐
│                  cmd/equalix-go                 │
│                     (main)                      │
└───────────────────────┬─────────────────────────┘
                        │
        ┌───────────────┼───────────────┐
        │               │               │
┌───────▼──────┐ ┌──────▼──────┐ ┌──────▼──────┐
│  REST API    │ │  Jobs       │ │  Metrics    │
│  (chi)       │ │  (tickers)  │ │ (prometheus)│
└───────┬──────┘ └──────┬──────┘ └──────┬──────┘
        │               │               │
        └───────────────┼───────────────┘
                        │
                ┌───────▼────────┐
                │  Domain Core   │
                │  (pure Go)     │
                └───────┬────────┘
                        │
        ┌───────────────┼───────────────┐
        │               │               │
┌───────▼──────┐ ┌──────▼──────┐ ┌──────▼──────┐
│  PostgreSQL  │ │   Redis     │ │  Executor   │
│  (pgx)       │ │ (go-redis)  │ │  (HTTP)     │
└──────────────┘ └─────────────┘ └─────────────┘
```

### Core Algorithm

A task's dispatch priority is:

```text
priority = finishTag + (inFlightCount × penaltyFactor / weight)

```
where `finishTag` is derived from persistent virtual time:

```text
finishTag = max(client_virtual_time.virtual_finish, V) + quantum / weight
```

`inFlightCount` is estimated by a Count-Min Sketch, avoiding expensive `COUNT(*)` queries on the hot path.

See [`docs/architecture.md`](docs/architecture.md) for details.

---

## Quick Start

### Prerequisites

- Go 1.22+
- PostgreSQL 14+
- Redis 6+ (optional; required for cross-instance CMS)
- Docker (for local development)

### Build

```bash
git clone https://github.com/synanton/equalix-go.git
cd equalix-go
go build -o bin/equalix-go ./cmd/equalix-go
```

### Run with Docker Compose

bash

```bash
docker compose up -d
./bin/equalix-go --config configs/local.yaml
```

### Minimal Configuration

```yaml
server:
  port: 8080

database:
  url: postgres://equalix:equalix@localhost:5432/equalix?sslmode=disable

redis:
  url: redis://localhost:6379/0
  enabled: true

scheduler:
  dispatch_interval_ms: 100
  priority_calculator_interval_ms: 200
  watchdog_interval_ms: 300000

cms:
  width: 20000
  depth: 5

adaptive_rps:
  enabled: true
  target_latency_ms: 500
  error_rate_threshold: 0.05
```

See [`docs/configuration.md`](https://docs/configuration.md) for the full reference.

------

## API

### Complete a Task (webhook)



```http
POST /api/v1/tasks/{id}/complete
Content-Type: application/json

{
  "result": "success",
  "duration_ms": 142,
  "error": null
}
```

See [`docs/api.md`](https://docs/api.md) for all endpoints.

------

## Configuration Reference

| Key                                         | Default  | Description                      |
| ------------------------------------------- | -------- | -------------------------------- |
| `server.port`                               | `8080`   | HTTP listen port                 |
| `database.url`                              | —        | PostgreSQL connection string     |
| `redis.url`                                 | —        | Redis connection string          |
| `redis.enabled`                             | `false`  | Enable Redis-backed CMS          |
| `scheduler.dispatch_interval_ms`            | `100`    | Dispatcher tick interval         |
| `scheduler.priority_calculator_interval_ms` | `200`    | Priority calculator tick         |
| `scheduler.watchdog_interval_ms`            | `300000` | Watchdog reconciliation interval |
| `cms.width`                                 | `20000`  | Count-Min Sketch width           |
| `cms.depth`                                 | `5`      | Count-Min Sketch depth           |
| `adaptive_rps.enabled`                      | `true`   | Enable adaptive RPS controller   |
| `adaptive_rps.target_latency_ms`            | `500`    | Target latency for scaling       |
| `adaptive_rps.error_rate_threshold`         | `0.05`   | Error rate triggering brake      |

------

## Observability

### Metrics (Prometheus)

- `equalix_tasks_dispatched_total{tenant}`
- `equalix_tasks_completed_total{tenant,result}`
- `equalix_dispatch_latency_seconds`
- `equalix_cms_drift_estimate`
- `equalix_rps_current`
- `equalix_rps_target`

### Endpoints

- `GET /healthz` — liveness
- `GET /readyz` — readiness
- `GET /metrics` — Prometheus metrics

### Logging

Structured JSON logs via `log/slog`. Set `LOG_LEVEL=debug` for verbose output.

------

## Development

### Run tests

```bash
go test ./...
```

### Run integration tests (requires Docker)

```bash
go test -tags=integration ./test/integration/...
```



### Run conformance suite

```bash
go test -tags=conformance ./test/conformance/...
```

### Run differential tests against Java Equalix

```bash
make differential-test JAVA_EQUALIX_PATH=/path/to/equalix
```

------

## Roadmap

See the [epic ticket](https://docs/epic.md) for the full phased plan:

- **Phase 0** — Specification extraction
- **Phase 1** — Domain core (pure Go)
- **Phase 2** — Ports and adapters
- **Phase 3** — Scheduled jobs and recovery
- **Phase 4** — Adaptive RPS controller
- **Phase 5** — Conformance and differential testing
- **Phase 6** — Configuration, observability, deployment
- **Phase 7** — Documentation and release

------

## Relation to Java Equalix

`equalix-go` is a **spec-first reimplementation**, not a direct port. It aims for behavioral parity with the [Java reference implementation](https://github.com/synanton/equalix) while using idiomatic Go. The Java version serves as an executable oracle for differential testing.

Fairness guarantees, virtual-time semantics, CMS behavior, and adaptive RPS logic are preserved. Framework-specific details (Spring DI, JPA, ShedLock) are replaced with Go-native equivalents (`pgx`, advisory locks, goroutines).

------

## Contributing

Contributions welcome. Please read [`CONTRIBUTING.md`](https://contributing.md/) and open an issue before submitting large changes.

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-change`)
3. Add tests for new behavior
4. Ensure `go test ./...` and `golangci-lint run` pass
5. Submit a pull request

------

## License

Apache License 2.0. See [`LICENSE`](https://license/).

------

## Acknowledgments

Inspired by and derived from the [Equalix](https://github.com/synanton/equalix) Java project.



```
**Note:** The ticket and README above assume the epic lives in `docs/epic.md` and the design doc in `docs/design.md`. Adjust paths and links to match your repository layout. The README intentionally marks the project as pre-alpha and points to the epic for the roadmap, which sets accurate expectations until Phase 5 conformance testing is complete.
```
