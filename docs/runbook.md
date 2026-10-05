# Runbook — operating equalix-go

For the operator who has never seen the project. Every claim here is
either code-pinned (file cited) or observed-and-dated; guidance without
a provenance label is a guess and should be treated as one.

## 1. Startup

```sh
# Env-only (no config file needed):
EQUALIX_DSN='postgres://USER:PASS@HOST:5432/equalix_go?sslmode=disable' \
EQUALIX_API_KEY=secret \
EQUALIX_EXECUTOR_BASE_URL=http://executor:9000 \
./equalix-go
```

Startup order (fail-fast — any step failing exits 1 before listeners bind):

1. Config resolve (flags > env > `/etc/equalix/config.yaml` > built-ins).
2. Optional migrate (`EQUALIX_MIGRATE_ON_STARTUP=true`): advisory-locked
   goose up, then unlock. See §7.
3. Readiness probes (DB ping, lock acquire/release) — must pass before bind.
4. CMS warm-up from in-flight rows (read-only rebuild, no drift publish).
5. Listener bind → jobs start → `serving` milestone.

Watch the milestones on stdout (`startup milestone phase=…`):
`main-entry → config-loaded → [migrating → migrated] → readiness-ok → serving`.

## 2. Shutdown

SIGTERM (or SIGINT) → `/readyz` flips 503 immediately → in-flight HTTP
drains within grace → runner drains → exit 0. `/healthz` stays 200
throughout (liveness, not readiness — a draining pod must not be
restarted). Startup failure (bad config, failed probes, failed
migration) exits 1 with the cause on stderr; a clean drain always
exits 0, never signal-terminated.

## 3. NTP requirement (narrowed)

- **DB host needs NTP, absolutely.** Scheduling thresholds
  (`updated_at` trigger stamps, timeout scans) run on the DB clock
  (spec §13 single-clock NOTE); a skewed DB shifts them.
- **App hosts need NTP only for the mixed-clock latency path**
  (`now − updatedAt` at completion, the deliberate parity choice):
  skew δ biases observed latency by δ (5% at 200ms target per 10ms
  skew). Fairness order is skew-proof (virtual-time upserts, DB-time
  SQL); promotion/timeout *timing* shifts with skew (spec §11).

## 4. Config reference

Service flags (env form in parentheses; file keys match flag names):

| Flag | Env | Default | Notes |
|---|---|---|---|
| `--dsn` | `EQUALIX_DSN` | — (required) | Postgres DSN; **must bypass PgBouncer** (session advisory locks, §7) |
| `--addr` | `EQUALIX_ADDR` | `:8080` | HTTP listen address |
| `--api-key` | `EQUALIX_API_KEY` | — (required) | Prefer env (flag visible in `ps`) |
| `--max-payload-bytes` | `EQUALIX_MAX_PAYLOAD_BYTES` | `1048576` | Java `app.queue.max-payload-bytes` parity |
| `--executor-base-url` | `EQUALIX_EXECUTOR_BASE_URL` | — (optional) | Unset → dispatcher unwired (ingest + tagging only) |
| `--metrics-path` | `EQUALIX_METRICS_PATH` | `/metrics` | Unauthenticated — do not expose publicly |
| `--metrics-tenant-cap` | `EQUALIX_METRICS_TENANT_CAP` | `1000` | Over-cap samples drop + count as cardinality-exceeded |
| `--metrics-drift-max-keys` | `EQUALIX_METRICS_DRIFT_MAX_KEYS` | `1000` | Sorted truncation per drift report |
| `--migrate-on-startup` | `EQUALIX_MIGRATE_ON_STARTUP` | `false` | Opt-in; explicit false vetoes env/file |
| `--migrations-dir` | `EQUALIX_MIGRATIONS_DIR` | embedded FS | External goose files for inspect-first operators |
| `--config` | — | `/etc/equalix/config.yaml` | Missing default ignored; missing explicit path fatal |

Scheduler (code defaults, `jobs.DefaultConfig` — restart-to-apply, no hot-reload):

| Key | Default | Notes |
|---|---|---|
| dispatcher interval | `50ms` | Tick cadence; timeout sweep rides it (no separate knob, Java parity) |
| calculator interval | `100ms` | Priority tagging cadence |
| watchdog interval | `5m` | Reconcile cadence |
| max tasks in process | `5000` | Dispatch budget ceiling |
| max per-client quota | `500` | Fairness ceiling per key |
| worker poll size | `100` | Batch bound (validated 1–10000) |
| max queued time | `60s` | Starvation-promotion deadline |
| task timeout | `300s` | Stuck-task sweep threshold |
| dispatch workers | `32` | Send pool size |
| shutdown grace | `30s` | Must cover a multiple of every job interval |
| error streak threshold | `5` | Tick-failure tolerance before Loop aborts |
| penalty factor | `1000.0` | Priority pressure input |

Adaptive RPS: target latency `200ms`, error threshold `0.05`, initial
RPS `1`, 5%/2s ramp (Java `application.yml` parity).

## 5. Failure modes (spec §11, with harness pointers)

- Executor 5xx / `success=false` → task stays `DISPATCHED` (send path)
  or `FAILED`; slots release only on terminal transition.
- Executor timeout/unreachable → timeout sweep marks `TIMEOUT` after
  `task-timeout`; RPS brakes on error rate meanwhile.
- PG down during dispatch → tick fails, next tick retries (single-tx,
  no partial dispatch); during completion → webhook 5xx, executor
  redelivers, terminal-duplicate rule keeps it safe.
- Crash mid-dispatch → committed rows stay `DISPATCHED` (sweep picks
  up), uncommitted roll back to `QUEUED`. Crash between DB commit and
  CMS flush → undercount until watchdog rebuild (§8 window).
- Duplicate dispatch across instances → prevented by
  `FOR UPDATE SKIP LOCKED` + `@Version`.
- Double webhook delivery → ignored (terminal-state check).
- Clock skew → shifts promotion/timeout timing, never fairness order.
- Concurrent watchdog → advisory-locked; repair idempotent regardless.

## 6. Observability (with observed typicals, dated 2026-10-05)

Wire names canonical per spec §13 (sole source:
`internal/adapter/metrics`). Typical values below are **idle-observed**
(45s, empty DB, no executor) unless noted — order-of-magnitude
guidance, not SLOs. Populate loaded typicals from warm-CI snapshots
as they accumulate; do not invent them.

| Series | Type | Idle observed | Loaded guidance |
|---|---|---|---|
| `equalix_dispatch_decision_latency_seconds` | histogram | no samples idle (no dispatches) | µs–low-ms; buckets to 1ms. In-memory rank bench ~67µs/400-candidate pool (different path — same order) |
| `equalix_timeout_detection_latency_seconds` | histogram | no samples idle | ~sweep interval (50ms) under light race load; diverges Java-vs-Go per-tick under completion-heavy load (spec NOTE, expected) |
| `equalix_watchdog_reconciliation_duration_seconds` | histogram | ~13ms/tick empty DB | Scales with row counts (EQLX-3b: 20k rows → ~190ms). Alarm above minutes |
| `equalix_cms_warmup_duration_seconds` | histogram | ~6ms empty DB | Startup-only; alarm above seconds |
| `equalix_rps_current` | gauge | — | Cold ramp 1→20s; operating 20–65 (Java ~20/63, Go ~25/52 on w2000). 0 with unwired dispatcher (never set — not a stall) |
| `equalix_received_queue_depth` | gauge | 0 idle | Backlog size; sustained growth = dispatch slower than ingest |
| `equalix_cms_drift_estimate{tenant}` | gauge | 0 idle | Nonzero = repair ran (bug by definition — drift is always a bug); alarm on any sustained nonzero |
| `equalix_metrics_cardinality_exceeded_total{metric}` | counter | 0 | Any increment = tenant cap hit; raise cap or investigate tenant explosion |

Alarm conditions: sustained drift nonzero; cardinality-exceeded
increasing; `rps_current` pinned at floor under load with empty error
budget (vs RPS frozen post-drain, which is normal — completion-driven
evaluation, spec NOTE); `/readyz` non-200.

## 7. migrate-on-startup

Opt-in (`EQUALIX_MIGRATE_ON_STARTUP=true`, default false).
Single-instance: enable freely. Multi-instance: migrate as a separate
step (concurrent startups serialize on the advisory lock — safe but
they queue behind it).

- Lock: session-scoped `pg_advisory_lock` on a fixed key, dedicated
  non-pooled direct connection (**must bypass PgBouncer** —
  transaction pooling silently breaks session locks). Explicit
  unlock-then-close (LIFO); close releases on any failure path.
- Bounds: `statement_timeout` 300s (execution), `lock_timeout` 30s
  (table-lock waits). The advisory-lock wait itself is bounded by
  statement_timeout (300s), not lock_timeout — accepted and
  documented, not re-engineered. Rewrites expected past 300s run
  out-of-band.
- Fail-fast: any migration error exits 1 before pools/probes/listeners.
- Collision semantics: another migrator on the same key blocks
  (mutual serialization), never double-applies.

## 8. Common incidents

- **Stuck DISPATCHED**: timeout sweep owns them (`task-timeout`
  default 300s); check sweep ticks in logs, then executor health.
- **CMS drift nonzero**: watchdog repairs + rebuilds; investigate the
  writer that diverged (crash between commit and flush is the known
  window).
- **RPS floored**: if backlog present + errors low → check latency
  signal; if backlog drained → normal freeze (completion-driven
  evaluation), not a stall.
- **DB unreachable**: ticks fail with streaks; service stays up,
  `/readyz` 503s, RPS brakes on error rate. Recover DB, no restart needed.
- **`/readyz` not-ready**: body names the check — `db`, `lock`,
  `migrations` (fresh DB / partial apply / unreadable source), or
  `draining` (SIGTERM received — normal).
- **Swarm healthcheck crash-loop**: Docker HEALTHCHECK targets
  `/readyz` (readiness proxy). On Swarm/compose without an orchestrator
  removing unhealthy containers from rotation, a DB blip restarts the
  container instead of waiting it out. Swarm users: `HEALTHCHECK NONE`
  or point the check at `/healthz`. k8s users: ignore HEALTHCHECK,
  use livenessProbe `/healthz` + readinessProbe `/readyz`.
- **Cold-start promotion asymmetry**: Java promotes ~25% more than Go
  on burst workloads (slower ramp × 60s deadline) — characterized,
  not a bug. Do not investigate; compare warm-class runs.
- **Probe cost at scale**: `/readyz` runs DB ping + lock probe +
  version count per scrape. At high pod counts with 5s probes,
  consider a 2–3s TTL cache on the checks. Informational — not worth
  building until scrape load shows up in profiles.
