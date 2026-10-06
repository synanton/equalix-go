# equalix-go

> Spec-first Go implementation of Equalix — an eventually-fair, weighted-fair scheduler for high-throughput multi-tenant systems.

[![Go Version](https://img.shields.io/badge/go-1.22+-00ADD8?logo=go)](https://go.dev)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Status](https://img.shields.io/badge/status-pre--alpha-orange.svg)](#status)

`equalix-go` is an **independent reimplementation** of [Equalix](https://github.com/synanton/equalix), reconstructed from the design specification and validated against the Java implementation used as an **executable oracle**.

This is **not a language port**. The Java code is the behavioral reference; the Go code is a separate, idiomatic implementation that must independently reproduce the scheduling semantics and pass differential, conformance, and performance testing.

---

## Status

**Pre-alpha.** Under active development. Not production-ready. See [`docs/IMPLEMENTATION.md`](docs/IMPLEMENTATION.md) for the current phase and [`CONTRIBUTING.md`](CONTRIBUTING.md) for the acceptance gates.

*State as of: `298bef2` (2026-10-06) — EQLX-7 complete, v0.1.0 released.*
*Java oracle at `11ef025` (unchanged). Bumped on phase transitions per CONTRIBUTING, not every merge.*

### Maturity

| Area | Implemented | Unit/integration tested | Conformance-validated | Benchmark-validated | Differentially validated vs Java |
|---|---|---|---|---|---|
| Domain core (virtual time, priority, CMS, selection) | ✅ | ✅ | ✅ (1:2:7 + seeded) | ✅ (hot-path benches) | ✅ warm-class w2000 (JvG 5/5, GvG 5/5 shares; cold 3/5 characterization) |
| Postgres adapter | ✅ | ✅ (testcontainers) | ⬜ | ⬜ | ✅ warm-class (same runs) |
| HTTP surface | ✅ | ✅ (contract tests) | ⬜ | ⬜ | ✅ warm-class (same runs) |
| Jobs (dispatcher, calculator, watchdog, timeout) | ✅ | ✅ | ⬜ | ⬜ | ✅ warm-class (same runs) |
| Jobs (recovery) | — (no such service; CORRECTION-2) | — | — | — | — |

`⬜` = not yet; nothing in this table is claimed before its evidence exists.
`🔶` = ran and produced a comparison (smoke-scale); ✅ requires meaningful
workloads with real fairness measurement (EQLX-5 gate: warm-class w2000,
JvG 0/5 fails vs GvG 0/5 fails, within one-run slack).

#### Differential dimensions (gate-level)

Run-level evidence: [`docs/evidence/eqlx-5-warm-class.md`](docs/evidence/eqlx-5-warm-class.md)
(20-run matrix; raw `results.json` retained 90 days per CI run).

The differential column above is per-area; gates differ in what "validated"
can mean. Fairness, order, and quota compare Go-to-Java on mechanisms Java
has. Starvation splits in two:

| Gate | Oracle counterpart | Differential status |
|---|---|---|
| Fairness shares (§4 bound) | Java weighted shares | ✅ warm-class (JvG 5/5, GvG 5/5; cold 3/5 JvG characterization) |
| Dispatch order (diagnostic) | Java dispatch sequence | 🔶 evidence only, never gates (diverges every run on both pairs, including GvG) |
| Quota bound | Java `maxPerClient` enforcement | ⬜ (fixtures green, no live run) |
| Starvation — promotion deadline | Java `max-queued-time-ms` (60s) | 🔶 characterized, not gated (cold asymmetry Go ~780 / Java ~1000; warm 0/0 both sides) |
| Starvation — K=3 window gate | **none** (harness-original, CORRECTION-3) | — by construction; validated by fixture + spec, never by differential |

---

## What It Is

`equalix-go` sits between a task queue and a rate-limited executor. It solves a problem ordinary queues handle poorly: **weighted fairness across tenants** combined with **adaptive backpressure**, without paying `COUNT(*)` on the hot path.

It targets asynchronous inference workloads where many tenants share LLM/GPU capacity.

### Core Mechanisms

- **Virtual-time scheduling** with persistent per-key finish tags (`client_virtual_time`, `scheduler_virtual_clock`)
- **Count-Min Sketch** for O(1) approximate in-flight counting (~2.6 MB at `width=65536`, `depth=5`)
- **Adaptive RPS controller** with latency EMA, error-rate brake, and dead-band dampener
- **Watchdog reconciliation** (every 5 min) to repair `client_counts` and CMS drift against the authoritative task table
- **Hierarchical fairness** — `flat` mode validated (EQLX-5 warm-class); `hierarchical` path lands in EQLX-9 behind the existing `fairness_mode` flag (default `flat`, frozen behavior)
- Per-key hard quotas, anti-starvation aging plus `max-queued-time-ms` promotion
- **Sequential execution mode** with per-key ordering, block recovery, and result passthrough

### Planned (EQLX-8 next, then EQLX-9)

- **Redis-backed CMS** — shared cross-instance sketch (EQLX-8); local sketch until then (per-instance in-flight views — no shared-fairness claim yet)
- **Hierarchical fairness path** — validated independently in EQLX-9; flat path unaffected

### Task Lifecycle

```text
RECEIVED → QUEUED → DISPATCHED → COMMITTED → SUCCEEDED
                                             → FAILED
                                             → TIMEOUT
```

Ingestion persists tasks as `RECEIVED` (no priority yet, keeps ingestion fast). The priority calculator tags them `QUEUED`; the dispatcher moves them to `DISPATCHED`; the executor adapter marks `COMMITTED` on HTTP 2xx; the completion webhook sets the terminal state.

### Priority Formula

```text
priority = round(finishTag) + (inFlightCount × penaltyFactor / weight)
```

where the finish tag is derived from persistent virtual time:

```text
finishTag = max(client_virtual_time.virtual_finish, V) + quantum / weight
penaltyFactor = 1000 / currentRps
```

`inFlightCount` is estimated by the CMS, avoiding expensive `COUNT(*)` queries on the hot path. `V` is the system virtual time (`scheduler_virtual_clock`), `quantum` defaults to `1000`. An idle key restarts at `V`, so it cannot bank credit and burst later. See the Java reference [`docs/design.md`](https://github.com/synanton/equalix/blob/main/docs/design.md) (§5.5, §9) for the full model.

Measured fairness in Java, with tenants at weights 1 : 2 : 7 continuously backlogged over 10,000 dispatches against PostgreSQL: exact shares of 10% / 20% / 70%, worst case 2 tasks off the weighted share in any window.

---

## Why Go

- **Single static binary** — millisecond startup, tiny container images, no JVM
- **Memory efficiency** — typically 4–6× lower RSS than equivalent JVM services in containers
- **Concurrency model** — goroutines and channels map naturally to the dispatcher, watchdog, RPS controller, and priority calculator jobs
- **Idiomatic dependency stack** — `pgx`, `chi`, `prometheus/client_golang` (`go-redis` declared; Redis-backed CMS lands in EQLX-8)

---

## Two Tracks, One Repository

`equalix-go` is deliberately both a production project and a training project. The two tracks share evidence but have different acceptance gates.

```text
              equalix-go
                   │
    ┌──────────────┴──────────────┐
    │                             │
Production                    Training
    │                             │
reliability               idiomatic Go
performance               concurrency
deployment                distributed systems
operations                algorithms
    │                             │
    └──────────────┬──────────────┘
                   │
            shared evidence
          tests / benchmarks /
          differential oracle
```

- **Production track** — a real service that can be deployed and operated. Acceptance requires load, failure, and recovery evidence (see [`CONTRIBUTING.md`](CONTRIBUTING.md#production-gate)).
- **Training track** — a rigorous vehicle for learning Go, concurrency, distributed systems, and algorithm implementation through a non-trivial problem. Acceptance requires idiomatic code, tests, benchmarks, and differential comparison.
- **Shared evidence** — the conformance suite, differential tests, and benchmarks serve both tracks.

The Java implementation is not a template. It is the **oracle** against which the Go implementation is validated.

---

## Architecture

Hexagonal (ports and adapters):

```text
┌─────────────────────────────────────────────────┐
│                  cmd/equalix-go                 │
│                     (main)                      │
└───────────────────────┬─────────────────────────┘
                        │
        ┌───────────────┼───────────────┐
        │               │               │
┌───────▼──────┐ ┌──────▼──────┐ ┌──────▼──────┐
│   REST API   │ │    Jobs     │ │  Metrics    │
│    (chi)     │ │  (tickers)  │ │(prometheus) │
└───────┬──────┘ └──────┬──────┘ └──────┬──────┘
        │               │               │
        └───────────────┼───────────────┘
                        │
                ┌───────▼────────┐
                │  Domain Core   │
                │   (pure Go)    │
                └───────┬────────┘
                        │
        ┌───────────────┼───────────────┐
        │               │               │
┌───────▼──────┐ ┌──────▼──────┐ ┌──────▼──────┐
│  PostgreSQL  │ │    Redis    │ │  Executor   │
│    (pgx)     │ │ (go-redis)  │ │   (HTTP)    │
└──────────────┘ └─────────────┘ └─────────────┘
```

The domain core is pure Go: no `database/sql`, no HTTP, no Redis. Adapters depend on ports (interfaces defined in the domain or a `port` package). This is what makes the algorithm testable without infrastructure.

See [`docs/architecture.md`](docs/architecture.md) for details (planned; Java reference is [`docs/design.md`](https://github.com/synanton/equalix/blob/main/docs/design.md)).

---

## Quick Start

### Prerequisites

- Go 1.22+
- PostgreSQL 14+
- Redis 6+ (optional; required for cross-instance CMS)
- Docker + Docker Compose (for local development)

### Build

```bash
git clone https://github.com/synanton/equalix-go.git
cd equalix-go
go build -o bin/equalix-go ./cmd/equalix-go
```

### Run with Docker Compose

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
  enabled: false # set true for shared cross-instance CMS

scheduler:
  priority_calculator_interval_ms: 100 # RECEIVED → QUEUED (Java default)
  dispatch_interval_ms: 50             # QUEUED → DISPATCHED (Java default)
  watchdog_interval_ms: 300000        # 5 min reconciliation (Java default)
  max_tasks_in_process: 5000
  max_per_client_quota: 500            # 0 = disabled
  fairness_mode: flat                 # flat | hierarchical

cms:
  width: 65536 # ε = 2/width (Java default)
  depth: 5     # δ = (1/2)^depth (Java default)

adaptive_rps:
  enabled: true
  target_latency_ms: 200
  error_rate_threshold: 0.05
```

Config keys are Go-side naming; defaults mirror the Java `application.yml` (`docs/configuration.md` in the Java repo). See `docs/configuration.md` for the full reference (planned).

---

## API

All `/api/v1/**` routes require header `X-API-Key` (mirrors Java).

| Method | Endpoint                      | Description                                              |
| ------ | ----------------------------- | -------------------------------------------------------- |
| `POST` | `/api/v1/tasks`               | Ingest a task (`fairnessKey`, `weight`, `payload`, ...)  |
| `GET`  | `/api/v1/tasks/{id}`          | Status & progress                                        |
| `GET`  | `/api/v1/tasks?fairnessKey=`  | List tasks for a fairness key (optional `status`)        |
| `POST` | `/api/v1/tasks/{id}/complete` | Mark task complete (success/fail)                        |
| `GET`  | `/api/v1/status`              | In-flight estimate (`inFlight`) and current RPS          |

### Ingest a Task

```bash
curl -X POST http://localhost:8080/api/v1/tasks \
  -H "Content-Type: application/json" \
  -H "X-API-Key: changeme" \
  -d '{
    "fairnessKey": "tenant-123",
    "weight": 1.0,
    "payload": "aGVsbG8="
  }'
```

Response: `201 Created` with the task UUID.

### Complete a Task (webhook)

```http
POST /api/v1/tasks/{id}/complete
Content-Type: application/json
X-API-Key: changeme

{"success": true, "result": "b3V0cHV0"}
```

On `success=false`, `error` is required. Duplicate completions of terminal tasks are ignored. See `docs/api.md` for all endpoints (planned; Java reference is `docs/api-reference.md`).

---

## Configuration Reference

| Key                                         | Default  | Description                                |
| ------------------------------------------- | -------- | ------------------------------------------ |
| `server.port`                               | `8080`   | HTTP listen port                           |
| `database.url`                              | —        | PostgreSQL connection string               |
| `redis.url`                                 | —        | Redis connection string                    |
| `redis.enabled`                             | `false`  | Enable Redis-backed (shared) CMS           |
| `scheduler.dispatch_interval_ms`            | `50`     | Dispatcher tick interval (Java default)    |
| `scheduler.priority_calculator_interval_ms` | `100`    | Priority calculator tick (Java default)    |
| `scheduler.watchdog_interval_ms`            | `300000` | Watchdog reconciliation interval (5 min)   |
| `scheduler.max_tasks_in_process`            | `5000`   | Global concurrency cap                     |
| `scheduler.max_per_client_quota`            | `500`    | Hard per-key ceiling (`0` = disabled)      |
| `scheduler.fairness_mode`                   | `flat`   | `flat` \| `hierarchical`                   |
| `cms.width`                                 | `65536`  | Count-Min Sketch width                     |
| `cms.depth`                                 | `5`      | Count-Min Sketch depth                     |
| `adaptive_rps.enabled`                      | `true`   | Enable adaptive RPS controller             |
| `adaptive_rps.target_latency_ms`            | `200`    | Target latency for scaling (Java default)  |
| `adaptive_rps.error_rate_threshold`         | `0.05`   | Error rate triggering emergency brake      |

---

## Observability

### Metrics (Prometheus)

Served at `GET /metrics` (unauthenticated — do not expose publicly).
Wire names are canonical per spec §13 (Prometheus metric naming); the
sole source is `internal/adapter/metrics` — this table mirrors it:

- `equalix_tasks_dispatched_total{tenant}` (tenant-capped)
- `equalix_tasks_completed_total{tenant,result}` (tenant-capped)
- `equalix_dispatch_decision_latency_seconds` (histogram)
- `equalix_timeout_detection_latency_seconds` (histogram)
- `equalix_watchdog_reconciliation_duration_seconds` (histogram)
- `equalix_cms_warmup_duration_seconds` (histogram)
- `equalix_rps_current`, `equalix_received_queue_depth` (gauges)
- `equalix_cms_drift_estimate{tenant}` (gauge, capped + truncated)
- `equalix_metrics_cardinality_exceeded_total{metric}` (over-cap drops)

Deferred (no port source yet): `rps_target`, brake/deadband state
gauges. Java reference metrics (`/actuator/prometheus`):
`equalix_task_duration_seconds`, `equalix_task_errors_total`,
`equalix_adaptive_rps`, `equalix_cms_estimation_drift{...}`,
`equalix_hierarchy_dispatches_total{layer,node}`.

### Endpoints

- `GET /healthz` — liveness (200 while serving, including drain)
- `GET /readyz` — readiness (DB ping, lock probe, migrations applied; 503 on failure or drain)
- `GET /metrics` — Prometheus metrics

### Logging

Structured JSON logs via `log/slog`. Set `LOG_LEVEL=debug` for verbose output.

---

## Development

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for setup, workflow, gates, and review checklists.

Common commands:

```bash
make test              # unit tests
make test-integration  # requires Docker
make test-conformance  # fairness invariants
make test-differential # against Java Equalix (differential tag; live runs need oracle + DBs, see test/differential/)
make lint              # golangci-lint
make bench             # benchmarks
```

---

## Roadmap

See [`docs/IMPLEMENTATION.md`](docs/IMPLEMENTATION.md) for the phased plan and the current milestone.

- Phase 0 — Specification extraction
- Phase 1 — Domain core (pure Go)
- Phase 2 — Ports and adapters
- Phase 3 — Scheduled jobs and recovery
- Phase 4 — Adaptive RPS controller
- Phase 5 — Conformance and differential testing
- Phase 6 — Configuration, observability, deployment
- Phase 7 — Documentation and release

---

## Relation to Java Equalix

The Java implementation is the behavioral reference and executable oracle. `equalix-go`:

- Preserves fairness semantics, virtual-time behavior, CMS behavior, and adaptive RPS logic
- Replaces framework-specific mechanisms (Spring DI, JPA, ShedLock) with Go-native equivalents (`pgx`, advisory locks, goroutines)
- Is validated by differential testing: same workload, same configuration, compare dispatch decisions and fairness metrics (harness in `test/differential/`; warm-class w2000 gate green — see maturity table)
- Is not a class-by-class translation and does not aim to be

Key Java references:

- Design: `docs/design.md` (priority pipeline, CMS, adaptive RPS, watchdog, sequential, hierarchical)
- Configuration: `docs/configuration.md` (all defaults)
- REST: `docs/api-reference.md` (ingestion, completion webhook, management)

---

## Contributing

Contributions welcome. Please read [`CONTRIBUTING.md`](CONTRIBUTING.md) before opening a PR.

1. Fork the repository
2. Create a feature branch (`git checkout -b EQLX-my-change`)
3. Add tests for new behavior
4. Ensure `make test` and `make lint` pass
5. Submit a pull request (merging is manual — a maintainer merges after review)

---

## License

Apache License 2.0. See [LICENSE](LICENSE).

---

## Acknowledgments

Inspired by and derived from the [Equalix](https://github.com/synanton/equalix) Java project.
