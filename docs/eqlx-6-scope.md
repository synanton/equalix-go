# EQLX-6 scope — deployability layer

Phase goal: the service becomes something another operator can run
without the author. Prometheus exporter, Dockerfile, migrate-on-startup,
runbook, health/ready semantics.

## Blocking choices (decided before implementation)

### Prometheus adapter

1. **Cardinality policy for tenant labels.** `tasks_dispatched_total{tenant}`,
   `tasks_completed_total{tenant,result}` — at N tenants, N or 2N series;
   at 10k tenants a cardinality bomb. Options: (a) full labels capped at
   K tenants (`metrics.tenant_cardinality_cap`, default 1000); (b) top-N +
   `"other"` bucket; (c) aggregate only + per-tenant admin endpoint.
   **Decision: (a) cap-at-K.** Pin the over-cap behavior in code:
   silently drop, log once, and expose
   `equalix_metrics_cardinality_exceeded_total`.
2. **Registry scope.** `prometheus.NewRegistry()` per adapter instance,
   handed to the HTTP handler — never the process-global default (two
   test instances in one process would collide). No production cost.
3. **Histograms, not summaries** (aggregate across instances; summaries
   don't). Wire names below; bucket boundaries per metric:
   - `dispatch_decision_latency_seconds` — µs scale:
     `[1e-6, 5e-6, 1e-5, 5e-5, 1e-4, 5e-4, 1e-3]`
   - `timeout_detection_latency_seconds` — sweep scale:
     `[0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]`
   - `watchdog_reconciliation_duration_seconds` — second scale:
     `[0.1, 0.5, 1, 5, 10, 30, 60, 300]`
   - `cms_warmup_duration_seconds` — startup scale:
     `[0.01, 0.05, 0.1, 0.5, 1, 5, 30]`
   - Plus gauges/counters: `rps_target`, `rps_current`, `brake_active`,
     `deadband_active`, tenant-capped dispatch/completion counters.
   **Name reconciliation (verified 2026-10-05):** EQLX-4 scope uses bare
   logical names (`dispatch_decision_latency`, …); one `port` comment
   cites `equalix_dispatch_latency_seconds` (different stem — dispatch
   vs dispatch_decision). Canonical wire names are the `_seconds` forms
   above (Prometheus convention); the adapter maps EQLX-4 logical names
   to them 1:1. Any future rename goes through this table, not comments.
4. **Exporter location.** Same mux (`server.metrics_path`, default
   `/metrics`), documented as not-for-public-exposure. A separate port
   is ops config later, not code now.

### Dockerfile

5. **Base image: `distroless/static:nonroot`.** Small, CA certs
   included, no shell to exploit, standard non-root user. (scratch lacks
   CA certs; alpine adds musl + shell for no benefit here.)
6. **Healthcheck without a shell: `equalix-go healthcheck` subcommand**
   probing `http://localhost:$PORT/readyz`, exit 0/1. No extra artifact,
   works with `docker run --health-cmd` and k8s probes.
7. **Config: keep YAML + env overrides** (EQLX-3/4 pattern); document a
   minimal env-only deployment for ConfigMap-free k8s. Go 1.22 pin,
   `CGO_ENABLED=0`, `-trimpath -ldflags="-s -w"`, multi-stage build.

### migrate-on-startup

8. **Embedded (goose `embed.FS`) by default, `--migrations-dir`
   override opt-in** — single artifact normally, file-based control
   when the operator wants to inspect SQL first. Closes deferred GAP-6.
9. **Opt-in, default false** (`EQUALIX_MIGRATE_ON_STARTUP=true`).
   Always-on serializes multi-instance deploys on the advisory lock;
   single-instance operators can enable freely, multi-instance runs
   migrations as a separate step.
10. **Fail-fast on migration error** (exit 1, no partial startup).
    Long migrations (table rewrites) that outlive the readiness timeout
    invite orchestrator restarts mid-migration: document out-of-band
    execution for those; no in-code duration estimation.

### health/ready (GAP-5 closure)

11. **Semantics:**
    - `/healthz` (liveness) — process alive, no dependencies. 200 in
      normal operation; 503 the instant graceful shutdown begins (pod
      leaves service before teardown).
    - `/readyz` (readiness) — DB ping, lock acquire/release probe,
      migrations applied. 200 only when all pass; 503 during startup,
      DB outage, and shutdown drain.
12. **Shutdown ordering (pinned):** SIGTERM → `/healthz` 503
    immediately → HTTP drain → `/readyz` 503 after drain → runner
    cancel → exit. This ordering is the rolling-update contract.

## Non-blocking deferrals

- Helm chart / K8s manifests — operator artifact, out of scope.
- Multi-arch image (amd64 + arm64) — add if needed.
- Config hot-reload — restart-to-apply.
- Alert thresholds — runbook guidance only, no shipped Prometheus rules.
- OpenTelemetry tracing — separate phase if ever; metrics ≠ tracing.
- `/metrics` authentication — network policy, not application.

## Acceptance criteria

- `docker run` with a mounted config: migrations applied, `/readyz`
  200 within 30s.
- `/metrics` returns the four timing histograms, RPS gauges, capped
  tenant-labeled counters.
- `equalix-go healthcheck` exits 0 healthy, non-zero otherwise.
- SIGTERM → clean exit 0, never signal-terminated.
- A reader who has never seen the project follows the runbook to a
  running instance.

## Test strategy

- Prometheus: adapter unit tests (registration, cap enforcement,
  histogram observe of a known value); integration test asserting
  expected series on `/metrics`.
- Dockerfile: CI job builds the image, runs it against compose PG,
  curls `/readyz` and `/metrics`, asserts responses.
- migrate-on-startup: fresh DB, disabled → `/readyz` 503; enabled →
  200 with schema present.
- health/ready: contract tests per state (starting, ready, draining,
  shutdown) asserting codes through the state machine.
- Runbook: "fresh operator" checklist in the PR, not automated.

## Run evidence (EQLX-6 completion)

- One image from main against the compose stack: `/readyz` green,
  `/metrics` served, SIGTERM → exit 0.
- Migration-on-startup against a fresh DB with the advisory lock
  visible in `pg_locks` during the run.
- `/healthz` vs `/readyz` transitions captured across a
  startup + shutdown cycle.

## Sequence

1. Prometheus adapter (only piece with a code surface — first).
2. Dockerfile + healthcheck subcommand.
3. migrate-on-startup.
4. health/ready semantics.
5. Runbook.
6. EQLX-6 evidence PR with the run evidence above.

Prometheus first; Dockerfile, health/ready, and runbook proceed in
parallel once the exporter shape is set.
