# Architecture

Equalix-go is a Go reimplementation of the Equalix fair scheduler
(Java oracle: `synanton/equalix`, `main`). Hexagonal layout: a pure
domain core, port interfaces, and adapters. Data flows ingest →
tag → dispatch → execute → complete; background jobs own each stage.

## Map

| Layer | Path | Role |
|---|---|---|
| Domain core | `internal/domain/` | Virtual time (`virtualtime.go`), priority formula (`priority.go`), dispatch selection + aging (`dispatcher.go`), task model (`task.go`). Pure functions + in-memory store; no I/O, no clocks except injected. |
| Ports | `internal/port/` | `TaskRepository`, `CountsRepository`, `SequenceStateRepository`, `VirtualTimeRepository`, `CMSStore`, `Metrics` (+ EQLX-6 timing observations), `Locker`, `Transactor`. Contracts only. |
| Postgres adapter | `internal/adapter/postgres/` | pgx stores, advisory-lock locker, goose migrate-on-startup (`migrate.go`, embedded `migrations/`). DB clock owns `updated_at` (trigger). |
| HTTP adapter | `internal/adapter/http/` | chi router: `/api/v1/*` (auth), `/metrics` (Prometheus, unauthenticated), `/healthz` (liveness), `/readyz` (readiness). |
| Executor adapter | `internal/adapter/executor/` | HTTP executor client (POST task, webhook completion). |
| Metrics adapter | `internal/adapter/metrics/` | Prometheus registry-per-instance; sole source of wire names (spec §13). |
| CMS | `pkg/cms/` | Count-Min Sketch (65536×5, FNV-1a + SplitMix64, no decay — spec §4). |
| Adaptive | `internal/adaptive/` | RPS controller: latency EMA, error brake, dead-band dampener, FailedSends input. |
| Jobs | `internal/jobs/` | dispatcher, calculator, watchdog, timeout + runner (graceful drain, error streaks). |
| Service | `cmd/equalix-go/` | Wiring: flags/env/file config, migrate, probes, listener, jobs, shutdown ordering. |

## Request lifecycle

1. `POST /api/v1/tasks` → persisted RECEIVED → 201 with server ID.
2. Calculator tick (100ms): batch RECEIVED → reserve finish tags → priority → QUEUED; starved tasks promoted to 0.
3. Dispatcher tick (50ms): budget = min(maxInProcess − inFlight, RPS × interval); select by (priority, created, id) under per-key quota; send pool executes.
4. Executor runs the task, POSTs the completion webhook → SUCCEEDED/FAILED/TIMEOUT; CMS + counts released post-commit.
5. Timeout sweep (dispatcher interval): over-age in-flight → TIMEOUT, skip-and-continue on races. Watchdog (5m): rebuilds CMS from DB, publishes drift.

## Clocks

Single DB clock for `updated_at` (trigger on all tables); app-side
`now − updatedAt` for completion latency (deliberate parity choice);
DB-time SQL for thresholds; virtual time monotonic via upserts. Full
treatment: spec §13 clock NOTEs.

## Startup / shutdown ordering

Config → optional migrate (advisory-locked, fail-fast) → readiness
probes → CMS warm-up → bind → jobs → serving. SIGTERM → `/readyz`
503 → HTTP drain → runner drain → exit 0; `/healthz` 200 throughout.
Details: `docs/runbook.md`.
