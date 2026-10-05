# Implementation Guide

This document tells you **what to build next** and **in what order**. It is the operational companion to the epic ticket and the design specification.

The goal is to reach a working, testable slice as fast as possible, then grow outward from the domain core.

Java oracle references used throughout:

- Algorithm: `docs/design.md` (§5 pipeline, §7 CMS, §8 adaptive RPS, §9 fairness, §14 sequential)
- Defaults: `docs/configuration.md` (Java `application.yml` values)
- REST: `docs/api-reference.md`
- Migrations: `src/main/resources/db/migration/V1..V5`

---

## Guiding Principles

1. **Domain first.** The scheduling algorithm must be correct and fully tested before any adapter exists.
2. **Spec before code.** If a behavior is not written down, write it down first.
3. **Java is the oracle, not the template.** Use it to resolve ambiguity, not to structure Go code.
4. **Prove it in a test.** Every algorithm must have a test that would fail if the algorithm were wrong.
5. **No infrastructure in the domain.** `internal/domain` imports only the standard library and pure helper packages.

---

## Phase 0 — Specification Extraction

**Goal:** Produce `docs/spec.md`, `docs/api.md`, `docs/schema.sql` from the Java reference.

### Tasks

- [ ] **Extract DDL.** Copy every Flyway migration from the Java repo (`src/main/resources/db/migration/V*.sql`) into `migrations/`:
  - `V1__create_tasks_and_client_counts.sql` — `tasks` + `client_counts` + indexes
  - `V2__create_shedlock.sql` — `shedlock` table (replaced in Go by advisory locks; keep for reference)
  - `V3__add_sequential_execution.sql` — sequential columns + `client_sequence_state`
  - `V4__add_persistent_virtual_time.sql` — `client_virtual_time` + `scheduler_virtual_clock`
  - `V5__add_hierarchical_scheduling.sql` — `hierarchy_node` + `idx_tasks_queued_by_key`
  Consolidate into a single `docs/schema.sql` for reference.
- [ ] **Document REST API.** Read the Spring controllers and write `docs/api.md` (OpenAPI-style or hand-written). Include ingestion (`POST /api/v1/tasks`), status (`GET /api/v1/tasks/{id}`, list by `fairnessKey`), completion webhook (`POST /api/v1/tasks/{id}/complete` with `success`/`result`/`error`), system snapshot (`GET /api/v1/status` → `inFlight`, `currentRps`), `X-API-Key` auth, and error envelope (`code`, `message`, `timestamp`, `fieldErrors`).
- [ ] **Formalize domain invariants.** Write `docs/spec.md` sections for:
  - Virtual time: `V`, `T_k`, `finishTag = max(virtual_finish, V) + quantum / weight` (`quantum` default 1000)
  - Weight normalization and hierarchical fairness (`flat` | `hierarchical`, path keys like `acme/sales`)
  - Penalty factor (`1000 / currentRps`) and in-flight pressure
  - Anti-starvation aging (`none` | `linear` | `log` | `power`, `lambda`, `gamma`, `candidate-pool-size`) and `max-queued-time-ms` promotion (priority → 0)
  - Per-tenant quotas (`max-per-client-quota`, `0` = disabled) and global cap (`max-tasks-in-process`)
  - Sequential execution state machine (`client_sequence_state`, block recovery, result passthrough)
- [ ] **Document CMS semantics.** Width/depth defaults (`65536`/`5`, `ε = 2/w`, `δ = (1/2)^d`), `add`/`estimateCount` (with `max(0, …)` guard), transaction-aware buffering (apply after commit, discard on rollback), Redis-backed cross-instance behavior (hash `{namespace}:v2` + `:total`, Lua `EVAL`, pipelined batch reads, fallback-to-local), warm-up procedure.
- [ ] **Document watchdog reconciliation.** Two-phase rebuild (1. repair `client_counts` from `SELECT fairness_key, COUNT(*) ... WHERE status IN ('DISPATCHED','COMMITTED')`, 2. rebuild CMS from that snapshot), interval (5 min), drift metrics per key/layer.
- [ ] **Document adaptive RPS controller.** Window (100 completions), `min-samples` (10), latency EMA (`α` default 0.7), error-rate brake (`×0.5` when `error_rate > 0.05`), increase/decrease factors (`×1.05` / `×0.9` around `target-latency-ms` ±20%), dead-band dampener (`direction-change-confirmations` default 3, emergency brake bypasses it), adjustment interval (2000 ms), `initial/min/max-rps` (1/1/100).
- [ ] **Document failure modes.** Executor failure (stays `DISPATCHED`, timeout service → `TIMEOUT`), DB outage, Redis loss (fallback-to-local), partial writes, retries, duplicate completions (terminal duplicates ignored).
- [ ] **Document the Redis Lua scripts.** Reproduce them verbatim in `docs/spec.md` with explanations.

**Definition of done:** Every algorithm in the Java code has a corresponding, reviewable section in `docs/spec.md`. A new engineer could implement from the spec without reading Java.

---

## Phase 1 — Domain Core (Pure Go)

**Goal:** A fully tested, I/O-free scheduling core that can be demonstrated in a unit test.

### First Milestone: "Fairness in a Test"

This is the single most important early deliverable. It proves the core works before any infrastructure exists.

**Target test:**

```go
// test/conformance/fairness_test.go
func TestWeightedFairness_1_2_7(t *testing.T) {
    // Tenants with weights 1, 2, 7
    // Continuous backlog (each tenant always has a pending task)
    // Run the in-memory dispatcher for 1000 dispatch decisions
    // Assert: each tenant's actual share is within 2 tasks of its expected share
}
```

This test exercises: virtual time, priority calculation, CMS in-flight estimation, dispatcher selection loop — all in memory, no PostgreSQL, no Redis, no HTTP. It mirrors the Java measured result (1 : 2 : 7 → 10% / 20% / 70%, ≤2-task deviation).

### Tasks

- [ ] `internal/domain/task.go` — Task struct with ID, FairnessKey, Weight, Sequence fields, EnqueuedAt
- [ ] `internal/domain/client.go` — ClientVirtualTime, ClientCounts, ClientSequenceState
- [ ] `internal/domain/virtualtime.go` — `V`, `T_k`, `finishTag = max(virtual_finish, V) + quantum / weight`
- [ ] `internal/domain/priority.go` — `priority = round(finishTag) + (inFlight × penaltyFactor / weight)`
- [ ] `pkg/cms/cms.go` — Count-Min Sketch (or wrap and audit an existing library; must support 64-bit key hash + per-row mixing, `max(0, …)` guard, any depth)
- [ ] `internal/domain/dispatcher.go` — pure selection logic (given candidate tasks + state, return next task; flat ordering by `(priority, created_at, id)`)
- [ ] `internal/domain/fairness.go` — anti-starvation aging (`linear`/`log`/`power`), quota enforcement, hierarchical resolution
- [ ] `internal/domain/sequence.go` — sequential execution state machine
- [ ] `internal/domain/clock.go` — Clock interface; FakeClock for tests
- [ ] Table-driven tests for every file above
- [ ] The 1:2:7 fairness conformance test

**Definition of done:** `go test ./internal/domain/... ./pkg/cms/... ./test/conformance/...` passes, race detector clean, and the fairness test shows ≤2-task deviation on 1:2:7.

---

## Phase 2 — Ports and Adapters

**Goal:** Wire the domain core to real infrastructure.

### Tasks

- [ ] `internal/port/repository.go` — TaskRepository interface
- [ ] `internal/port/cms.go` — CMSStore interface (local + Redis)
- [ ] `internal/port/executor.go` — Executor interface
- [ ] `internal/port/metrics.go` — Metrics interface
- [ ] `internal/adapter/postgres/` — pgx implementation with `SELECT … FOR UPDATE SKIP LOCKED`, quota-aware query, `(status, priority)` ordering
- [ ] `internal/adapter/redis/` — go-redis implementation with Lua `EVAL`
- [ ] `internal/adapter/executor/` — HTTP executor client with timeouts and retries (`{base-url}/tasks/{id}/execute`, 2xx → `COMMITTED`)
- [ ] `internal/adapter/http/` — chi REST adapter (ingestion + completion webhook + `X-API-Key`)
- [ ] `internal/adapter/metrics/` — Prometheus adapter
- [ ] Integration tests with testcontainers-go

**Definition of done:** Integration tests pass against real PostgreSQL and Redis. The 1:2:7 conformance test passes against the real adapters.

---

## Phase 3 — Scheduled Jobs and Recovery

**Goal:** The dispatcher loop, watchdog, and recovery services run as background jobs.

### Tasks

- [ ] `internal/jobs/dispatcher.go` — ticker-driven dispatch loop (default 50 ms) with context cancellation; starvation promotion; `freeSlots = maxTasksInProcess − globalInFlight` capped by `ceil(currentRps × intervalSeconds)` when adaptive RPS is on
- [ ] `internal/jobs/priority.go` — priority calculator job (default 100 ms)
- [ ] `internal/jobs/watchdog.go` — two-phase reconciliation (default 5 min)
- [ ] `internal/jobs/timeout.go` — task timeout service (`task-timeout-ms`, `0` = off)
- [ ] `internal/jobs/warmup.go` — CMS warm-up listener (rebuild from in-flight tasks on startup)
- [ ] `internal/jobs/recovery.go` — stuck-task / block / passthrough recovery
- [ ] Cross-instance locking via PostgreSQL advisory locks (`pg_advisory_lock`) — Go replacement for ShedLock
- [ ] Fault-injection tests: kill dispatcher mid-transaction, verify recovery

**Definition of done:** Crash-recovery tests pass. Watchdog repairs CMS drift within tolerance. Two instances coordinate correctly.

---

## Phase 4 — Adaptive RPS Controller

**Goal:** Dispatch rate adapts to downstream latency and errors.

### Tasks

- [ ] `internal/adaptive/ema.go` — latency EMA (`smoothed = α·latency + (1−α)·smoothed`)
- [ ] `internal/adaptive/brake.go` — error-rate emergency brake (`×0.5`, never dampened)
- [ ] `internal/adaptive/dampener.go` — dead-band dampener (reversal needs N agreeing evaluations)
- [ ] `internal/adaptive/controller.go` — throttle state machine (window 100, `min-samples` 10, interval-gated evaluation)
- [ ] Wire into dispatcher (budget cap) and priority calculator (`penaltyFactor`)
- [ ] Simulation tests: inject latency and errors, verify adaptation (no collapse to `min-rps` on a single spike)

**Definition of done:** Controller adapts to injected latency and errors; dead-band prevents oscillation.

---

## Phase 5 — Conformance and Differential Testing

**Goal:** Prove behavioral equivalence with Java Equalix.

### Tasks

- [ ] `test/conformance/` — full fairness, aging, quota, sequential, recovery invariant suite
- [ ] `test/differential/` — harness that runs identical workloads against Java and Go, compares dispatch decisions
- [ ] Reproduce Java's 1:2:7 measurement with ≤2-task deviation
- [ ] Load tests at target throughput (`k6` or `vegeta`)
- [ ] Benchmark suite: dispatch latency, throughput, memory

**Definition of done:** Differential tests show ≥99% dispatch decision parity. Fairness deviation ≤2 tasks per window. Benchmarks published in `docs/benchmarks.md`.

---

## Phase 6 — Configuration, Observability, Deployment

**Goal:** Operable service.

### Tasks

- [x] YAML + env config binding with defaults matching Java (`--config` + env + flags, pinned precedence; `docs/runbook.md` §4)
- [x] Graceful shutdown via context cancellation (SIGTERM → /readyz 503 → HTTP drain → runner drain → exit 0)
- [x] Prometheus metrics endpoint (`/metrics`, capped tenants, per-instance registry)
- [x] `/healthz` and `/readyz` (liveness vs readiness, GAP-5 closed)
- [x] Multi-stage Dockerfile (distroless/static:nonroot, 14.7MB)
- [x] `docker-compose.yml` for local development (root compose: PG + service with migrate-on-startup)
- [x] `docs/runbook.md` (startup/shutdown, NTP, config, failure modes, observability, incidents, migrate)

**Definition of done:** Binary runs, smoke tests pass, metrics and health endpoints respond.

---

## Phase 7 — Documentation and Release

- [ ] `docs/architecture.md`
- [ ] `docs/configuration.md`
- [ ] `docs/api.md`
- [ ] `docs/runbook.md`
- [ ] `docs/benchmarks.md`
- [ ] `CHANGELOG.md`
- [ ] `v0.1.0` tag

---

## How to Use Java as the Oracle

When the spec is ambiguous:

1. Write a small Java test that exercises the behavior.
2. Observe the output.
3. Encode the observed behavior as a Go test.
4. Implement to satisfy the Go test.
5. Add the behavior to `docs/spec.md`.

Never copy Java structure. Copy behavior, then redesign for Go.

---

## What to Do Today

If you are starting from an empty repository, do this:

1. Create the directory skeleton from `CONTRIBUTING.md`.
2. Initialize `go mod init github.com/synanton/equalix-go`.
3. Add `Makefile` with `test`, `lint`, `build` targets.
4. Write `internal/domain/virtualtime.go` and its test.
5. Write `internal/domain/priority.go` and its test.
6. Write `pkg/cms/cms.go` and its test.
7. Write `internal/domain/dispatcher.go` and the 1:2:7 fairness test.
8. Commit: `feat(domain): first fairness slice with 1:2:7 conformance test`.

That is your first PR. It is self-contained, testable, and demonstrates the core idea without any infrastructure.

---

## Definition of Done (per phase)

A phase is complete when:

- All tasks are checked off.
- All tests pass with `-race`.
- `golangci-lint run` is clean.
- The phase's acceptance criteria (above) are met.
- `docs/spec.md` reflects any behavior discovered during implementation.
- `CHANGELOG.md` is updated.

---

## Suggested First Commit Sequence

If you want the shortest path from "empty repo" to "demonstrable scheduler," commit in this order:

1. `chore: initialize module and repo skeleton`
2. `feat(domain): add virtual-time finish tag calculation`
3. `feat(domain): add priority calculation`
4. `feat(cms): add Count-Min Sketch`
5. `feat(domain): add in-memory dispatcher selection`
6. `test(conformance): 1:2:7 fairness invariant`
7. `docs(spec): document virtual time and priority`
8. `docs(spec): document CMS semantics`

After step 6 you have a working, tested fairness core. Everything after that is wiring it to real infrastructure and proving it holds under load, failure, and differential comparison.

The two documents above (`CONTRIBUTING.md` and `docs/IMPLEMENTATION.md`) plus the updated `README.md` give the project a clear identity, a clear contribution bar, and a concrete next action. That is enough to start.
