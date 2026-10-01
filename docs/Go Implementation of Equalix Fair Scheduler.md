# EPIC: equalix-go — Go Implementation of Equalix Fair Scheduler

------

## Summary

Create `equalix-go`, a spec-first Go implementation of the Equalix eventually-fair weighted-fair scheduler currently implemented in Java 21 + Spring Boot. The Go version must preserve the fairness guarantees, virtual-time scheduling algorithm, Count-Min Sketch in-flight estimation, adaptive RPS control, watchdog reconciliation, and sequential execution semantics of the Java reference implementation, while using idiomatic Go concurrency, minimal dependencies, and a single static binary.

**Type:** Epic
**Priority:** High
**Estimated Effort:** 3–5 engineer-months
**Labels:** `go`, `scheduler`, `fairness`, `rewrite`, `spec-first`

------

## Background

Equalix is a high-throughput multi-tenant weighted-fair scheduler. It sits between a task queue and a rate-limited executor and ensures proportional fairness across tenants using:

- Persistent virtual-time scheduling with per-client finish tags
- Count-Min Sketch (CMS) for O(1) approximate in-flight counting
- Adaptive RPS controller with latency EMA and error-rate dead-band
- Watchdog reconciliation every 5 minutes to repair CMS drift
- Hierarchical fairness, hard per-tenant quotas, anti-starvation aging
- Sequential execution mode with sequence-based boosts

The Java version is mature, tested, and has measured fairness results (no tenant more than 2 tasks off its weighted share in a 1:2:7 workload). This epic covers building a Go implementation that matches those guarantees.

### Why Go

- **Memory efficiency:** 4–6× lower RSS than equivalent JVM services in containers
- **Cold start:** Single static binary with millisecond startup
- **Concurrency:** Goroutines and channels map naturally to the dispatcher, watchdog, RPS controller, and priority calculator jobs
- **Deployment simplicity:** No JVM, no Spring Boot, small container images

------

## Goals

1. Implement all core Equalix algorithms in pure, testable Go packages.
2. Preserve fairness semantics measured in the Java reference implementation.
3. Provide idiomatic Go adapters: `pgx` (PostgreSQL), `go-redis` (Redis), `chi` (HTTP), `prometheus/client_golang` (metrics).
4. Ship as a single static binary with YAML/env configuration.
5. Achieve behavioral parity validated by a conformance test suite and differential testing against the Java version.
6. Document the architecture, configuration, and operational runbooks.

## Non-Goals

- Feature additions beyond Java Equalix parity
- Breaking changes to REST API contracts (except where Java exposes none)
- Multi-language SDKs
- Kubernetes operators or Helm charts (separate epic)

------

## Approach: Spec-First, Not Direct Port

Do **not** translate Java classes one-to-one. Instead:

1. Extract a formal behavioral spec from `docs/design.md` and the Java source (used as an executable oracle).
2. Implement the domain core in pure Go with table-driven tests.
3. Define ports as Go interfaces; build adapters separately.
4. Validate with a conformance suite and differential tests.

This avoids inheriting Spring/JPA idioms and produces maintainable, idiomatic Go.

------

## Phases and Deliverables

### Phase 0 — Specification Extraction

**Deliverable:** `docs/spec.md` + `docs/api.md` + `docs/schema.sql`

- Extract full PostgreSQL DDL from Java Flyway migrations (`db/migration/V*.sql`)
- Document REST API contracts from Spring controllers (ingestion, completion webhook, management)
- Formalize domain invariants: virtual finish tags, weight normalization, penalty factor, aging, quotas, hierarchical fairness
- Document CMS semantics: update timing, decay, drift correction, Redis-backed cross-instance behavior
- Document sequential execution mode state machine and `client_sequence_state` semantics
- Document watchdog reconciliation two-phase rebuild algorithm and drift thresholds
- Document adaptive RPS controller dead-band dampener, latency EMA, error-rate logic
- Document failure modes: executor failures, DB outages, Redis loss, partial writes, retries
- Document CMS warm-up procedure and Redis Lua script behavior

**Acceptance:** Spec reviewed and signed off; every algorithm in Java has a corresponding spec section.

------

### Phase 1 — Domain Core (Pure Go)

**Deliverable:** `internal/domain/` package with 100% unit test coverage of core algorithms

- `Task`, `ClientCounts`, `ClientVirtualTime`, `ClientSequenceState` types
- Priority calculator: `priority = finishTag + (inFlightCount × penaltyFactor / weight)`
- Virtual-time calculation: `finishTag = max(virtual_finish, V) + quantum / weight`
- Count-Min Sketch implementation (or wrap `shenwei356/countminsketch` after audit)
- Hierarchical fairness resolution
- Anti-starvation aging
- Hard per-tenant quota enforcement
- Sequential execution state machine
- Table-driven tests for all invariants

**Acceptance:** Unit tests pass; fairness properties verified in isolation.

------

### Phase 2 — Ports and Adapters

**Deliverable:** `internal/port/`, `internal/adapter/` packages

- Define Go interfaces: `TaskRepository`, `CMS`, `RedisStore`, `Executor`, `Clock`, `Metrics`
- PostgreSQL adapter using `pgx` (including `SELECT … FOR UPDATE SKIP LOCKED`)
- Redis adapter using `go-redis`, including Lua `EVAL` scripts for CMS
- HTTP executor client with timeouts and retries
- REST adapter using `chi` for ingestion and completion webhook
- Kafka ingestion adapter using `segmentio/kafka-go` (optional, feature-flagged)
- Prometheus metrics adapter
- Structured logging with `slog`

**Acceptance:** Integration tests pass against containerized PostgreSQL and Redis.

------

### Phase 3 — Scheduled Jobs and Recovery

**Deliverable:** `internal/jobs/` package

- Priority calculator job (configurable interval)
- Dispatcher job with transaction boundaries and duplicate-completion handling
- Watchdog reconciliation with two-phase rebuild
- Task timeout service
- CMS warm-up listener
- Recovery service for stuck tasks
- Cross-instance locking (ShedLock equivalent via PostgreSQL advisory locks or `pg_advisory_lock`)

**Acceptance:** Crash-recovery tests pass; watchdog repairs CMS drift within tolerance.

------

### Phase 4 — Adaptive RPS Controller

**Deliverable:** `internal/adaptive/` package

- Latency EMA calculation
- Error-rate emergency brake
- Dead-band dampener
- Throttle state machine
- Integration with dispatcher

**Acceptance:** Controller adapts correctly under simulated latency and error injection.

------

### Phase 5 — Conformance and Differential Testing

**Deliverable:** `test/conformance/` and `test/differential/`

- Conformance suite: fairness ratios, anti-starvation, crash recovery, duplicate completions, watchdog drift, RPS adaptation
- Differential tests: same workload against Java and Go, compare dispatch decisions and fairness metrics
- Reproduce Java's 1:2:7 fairness measurement with ≤2-task deviation
- Load tests at target throughput

**Acceptance:** Go implementation matches Java fairness within measurement tolerance.

------

### Phase 6 — Configuration, Observability, Deployment

**Deliverable:** `cmd/equalix-go/`, `configs/`, `deploy/`

- YAML + env configuration binding (defaults matching Java)
- Graceful shutdown with `context` cancellation
- Prometheus metrics endpoint
- Health and readiness endpoints
- Multi-stage Dockerfile producing a minimal image
- Operational runbook in `docs/runbook.md`

**Acceptance:** Binary runs, passes smoke tests, and exposes metrics/health.

------

### Phase 7 — Documentation and Release

- `README.md` (see separate deliverable)
- `docs/architecture.md`
- `docs/configuration.md`
- `docs/api.md`
- `docs/runbook.md`
- `CHANGELOG.md`
- `v0.1.0` tagged release

------

## Testing Strategy

| Layer        | Tooling                       | Coverage Target          |
| ------------ | ----------------------------- | ------------------------ |
| Domain core  | `go test`, table-driven       | 90%+                     |
| Adapters     | `testcontainers-go`           | 80%+                     |
| Jobs         | Integration + fault injection | Critical paths           |
| Conformance  | Custom harness                | All fairness invariants  |
| Differential | Java + Go side-by-side        | Dispatch decision parity |
| Load         | `k6` or `vegeta`              | Target throughput        |

------

## Acceptance Criteria

1. All phases complete and merged to `main`.
2. Conformance suite passes with fairness deviation ≤2 tasks per window.
3. Differential tests show ≥99% dispatch decision parity with Java on identical workloads.
4. Binary RSS ≤25% of equivalent Java service under identical load.
5. Startup time ≤100ms.
6. Documentation complete and reviewed.
7. `v0.1.0` released.

------

## Risks and Mitigations

| Risk                                       | Mitigation                                              |
| ------------------------------------------ | ------------------------------------------------------- |
| Spec gaps cause behavioral divergence      | Use Java as executable oracle; differential testing     |
| Redis CMS Lua semantics differ             | Port Lua scripts verbatim; test cross-instance behavior |
| ShedLock equivalent missing in Go          | Use PostgreSQL advisory locks; document trade-offs      |
| Sequential mode underspecified             | Reverse-engineer from Java; add explicit spec section   |
| Team unfamiliar with Go concurrency idioms | Pair programming; code review checklist                 |

------

## Dependencies

- PostgreSQL 14+ (for `SKIP LOCKED` and advisory locks)
- Redis 6+ (for Lua `EVAL`)
- Go 1.22+
- Access to Java Equalix repository for reference and differential testing

------

## References

- Java Equalix: https://github.com/synanton/equalix
- Design document: `docs/design.md`
- Go libraries: `pgx`, `go-redis`, `chi`, `segmentio/kafka-go`, `prometheus/client_golang`, `slog`