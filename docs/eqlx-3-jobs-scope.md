# EQLX-3 Jobs — scope

Scheduled jobs wiring the domain + adapters into running goroutines:
dispatcher, priority calculator, watchdog, timeout sweep, locker-backed
coordination, startup recovery. Scope doc first; skeleton + jobs after.

Conventions: blocking items need a recorded choice before code; non-blocking
items ride along. DECISION/ceiling references point at `docs/spec.md`.

---

## Blocking

### 1. Job skeleton: errgroup + recoverable ticks

- `errgroup.WithContext` over all jobs. Tick errors (query failure,
  lock contention) are **recoverable**: log + continue, never return —
  a transient DB blip must not kill the process.
- Only **startup errors** fail the group: no DB pool, lock infra broken,
  CMS warm-up snapshot unreadable.
- Shutdown: cancel + `context.WithTimeout` grace (configurable, default
  30s). Jobs select `ctx.Done()` **between iterations, never mid-tick**;
  an in-flight `Transact` aborts via ctx cancellation (pgx honors it).
- Tick loops use `time.Ticker` (fixed interval, no drift accumulation);
  slow ticks skip beats rather than pile up (drain-on-wakeup).

### 2. Dispatcher: Transact + async bounded send

- One tick = one `Transact`: `FindAndLockDispatchable` → `Save(DISPATCHED)`
  per task → `Counts.Increment` → `VirtualTime.RecordDispatch` → commit.
  Matches DECISION-3 (atomic dispatch) and the B1 port contract.
- **Executor send is post-commit and async**, via a bounded worker pool
  (semaphore channel, cap = `dispatch_workers`, default 32 — separate from
  `maxTasksInProcess`, which bounds *slots*, not senders). Rationale: a slow
  executor must not stall the tick, and unbounded goroutines are not an
  option. Backpressure: when the pool is full the tick skips sending and
  the next tick retries (task is already DISPATCHED; timeout bounds the wait).
- **Executor error → leave DISPATCHED for timeout.** Java parity: send
  errors are logged, never thrown; only 2xx marks COMMITTED (port doc on
  `Executor.Send`). No immediate FAILED — the timeout sweep owns it.
- Starvation promotion (`PromoteStarved`) runs at tick top, before select.

### 3. Priority calculator: per-tick, quota-aware

- Each tick: `FindReceived(worker_poll_size)` → per task `Reserve` tag →
  `CalculatePriority` (CMS estimate + penalty factor read from the RPS
  controller handle; fixed 1000/initial-rps until EQLX-4 wires the live value)
  → `Save(QUEUED)`. Sequential boost/penalty via `SequentialAdjust`.
- Promotion is **per-tick** (`FindStarved` + priority 0), not accumulated.
- `RankByAging` enters only at *selection* when `policy != none`, always
  with `maxPerClient` (CORRECTION-1). Default `none` = flat path, zero
  aging cost (§6.3 invocation note).

### 4. Watchdog: thresholdless repair, two phases, one boundary

- Every `watchdog.interval` (default 5m, Java parity): **phase 1** repairs
  every mismatched `client_counts` row (no threshold — any drift is a bug,
  repair is idempotent); **phase 2** measures drift, publishes
  (`PublishDrift`), then rebuilds CMS from the repaired snapshot.
- Phase boundary: phase 2 starts after phase 1's last repair commits —
  single run, sequential, never interleaved. No "stabilization" wait: the
  snapshot is authoritative the moment counts match the task table.
- Drift *threshold* is deliberately absent (parity: Java has none, only the
  metric cap). If operators want alerting thresholds, that's EQLX-6
  runbook material, not scheduler behavior.

### 5. Timeout sweep: config interval, Java-unknown cadence

- `FindTimedOut(task_timeout)` → `TIMEOUT` + `LastError` + release
  (counts/CMS) + sequential block flag, per tick of its own ticker.
- Cadence default 30s is a **chosen value, parity unknown** (Java's
  `TaskTimeoutScheduler` fixed delay was not extracted). Recorded here so
  EQLX-5 can flag it if differential timing diverges.

### 6. Locker: session-scope advisory locks, crash-safe by close

- `pg_try_advisory_lock(bigint)` on a **held dedicated connection**
  (`pool.Acquire` at Lock, held until release): session scope means a
  crashed process releases on connection close — no TTL machinery needed.
  Lock key = stable 64-bit hash of the job name (document the function;
  distinct from the CMS hasher).
- Non-blocking acquire: held-by-peer → tick skipped, no queueing.
- Answers the deferred lease question: no lease, no TTL; liveness comes
  from TCP close, matching Java's ShedLock-over-JDBC closely enough that
  no spec change is needed.

### 7. Recovery: old-only, lock-guarded

- At startup, under `Locker("recovery")`: mark `TIMEOUT` every
  DISPATCHED/COMMITTED task with `updated_at` older than
  `startup − task_timeout`, releasing slots. **Old-only**: a fresh
  in-flight row may belong to a live peer — touching it would steal work.
- With `task-timeout-ms = 0` (disabled), recovery still releases nothing
  and logs the skip (explicit, not silent).
- Steady-state stuck tasks belong to the timeout sweep, not recovery.

---

## Non-blocking

- **Metrics**: `port.Metrics` fakes (in-memory recorder) until EQLX-6
  Prometheus; call sites (`RecordDispatch`, `RecordCompletion`,
  `PublishDrift`, `SetRPS`) are wired now so the adapter swap is mechanical.
- **CMS**: local `pkg/cms` wrapped as `port.CMSStore` + transaction-aware
  buffering decorator (spec §4.4) in the jobs layer; Redis adapter stays
  EQLX-3-out (cross-instance fairness is Phase-5 differential scope).
- **Config**: plain Go struct with Java defaults (spec §10); YAML/env
  binding is EQLX-6. No new keys except `dispatch_workers` (default 32),
  `shutdown_grace` (default 30s), `timeout_sweep_interval` (default 30s).
- **Completion ownership**: the chi handler keeps serving the webhook;
  jobs do not take it over in this phase — but the DB half
  (`Save` + `Decrement`) moves into `Transact`, closing window (1) from
  the §13 NOTE. CMS + metrics stay post-commit (window (2), structural).

---

## Out of scope (EQLX-4+)

- Adaptive RPS controller (fixed penalty factor until then).
- Hierarchical dispatch planner (flat mode only; H-mode is Phase-5 scope).
- Kafka ingestion, gRPC, dead-letter UI, OpenAPI generation.
- Migrate-on-startup wiring (goose files exist; service phase wires them).
