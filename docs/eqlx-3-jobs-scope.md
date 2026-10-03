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
- **Startup-error rule (no exceptions):** startup = pre-launch readiness
  only — DB pool ping, Redis ping (if CMS mode is redis), lock-infra
  acquire/release probe. A failed probe returns from `Run` for main to
  **exit non-zero** (orchestrator retries with its own backoff; the process
  never invents retry loops). Once goroutines launch, *every* tick error is
  recoverable, including first-tick connection errors. N consecutive
  failures (`error_streak_threshold`, default 5) log at error level with
  the streak count but never kill the process: a crash-loop is worse than
  a stalled scheduler, and EQLX-6 alerting catches the stall.
- Shutdown: cancel + `context.WithTimeout` grace (`shutdown_grace`,
  default 30s). The loop never interrupts a running tick; shutdown works
  because tick operations are ctx-aware (pgx aborts transactions on
  cancellation) — grace covers cancel → operations returning, not tick
  boundaries. A tick whose operations ignore ctx can overrun grace; every
  job's tick takes ctx and passes it to every blocking call.
- **Grace vs worst-case tick (pinned relationship):** grace is a *drain
  budget*, not an interval multiple — it must cover one in-flight tick
  plus send-pool drain on the hot loop. Batch caps (`worker_poll_size`,
  `freeSlots ≤ max_tasks_in_process`) bound work per tick; `Config.Validate`
  enforces `shutdown_grace ≥ max(10s, 100×dispatcher_interval)` so the two
  numbers cannot drift apart silently. A pathologically slow DB can still
  overrun any grace — the rule keeps honest configs honest, not physics.
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
  option.
- **Pool behavior (pinned): claim-limited, no queue.** Each tick claims
  `min(freeSlots, pool_free_capacity)` and submits exactly that many sends;
  the remaining slots wait for the next tick. No bounded-blocking queue:
  a queue would park DISPATCHED tasks in memory while holding fairness
  slots, turning executor slowness into a memory problem. Under sustained
  executor latency the slot cap is simply never fully used — honest
  backpressure, visible in metrics.
- **Send-failure signal for EQLX-4 (pinned):** the pool exposes an atomic
  `FailedSends()` counter (incremented on every non-2xx/transport failure),
  read but not acted on in this phase. Rationale: with async send, executor
  failures otherwise surface only via missing webhooks, leaving the EQLX-4
  error brake blind to the failure mode it exists to catch. The counter is
  the brake's future input; EQLX-3 wires nothing to it.
- **Executor error → leave DISPATCHED for timeout.** Java parity: send
  errors are logged, never thrown; only 2xx marks COMMITTED (port doc on
  `Executor.Send`). No immediate FAILED — the timeout sweep owns it.
- Starvation promotion (`PromoteStarved`) runs at tick top, before select.

### 3. Priority calculator: per-tick, quota-aware

- Each tick: `FindReceived(worker_poll_size)` → per task `Reserve` tag →
  `CalculatePriority` (CMS estimate + penalty factor from config key
  `penalty_factor`, default 1000.0 = 1000/initial-rps Java parity; EQLX-4
  swaps in the live value without touching code) → `Save(QUEUED)`.
  Sequential boost/penalty via `SequentialAdjust`.
- Promotion is **per-tick** (`FindStarved` + priority 0), not accumulated.
- **Ownership (pinned): the calculator writes, the dispatcher reads.**
  `PromoteStarved` (priority-0 writes) belongs to the calculator tick;
  `RankByAging` belongs to the *dispatcher selection path*, always with
  `maxPerClient` (CORRECTION-1). Default `none` = flat path, zero
  aging cost (§6.3 invocation note).

### 4. Watchdog: thresholdless repair, two phases, one boundary

- Every `watchdog.interval` (default 5m, Java parity): **phase 1** repairs
  every mismatched `client_counts` row (no threshold — any drift is a bug,
  repair is idempotent); **phase 2** measures drift, publishes
  (`PublishDrift`), then rebuilds CMS from the repaired snapshot.
- Phase boundary: phase 2 starts after phase 1's last repair commits —
  single run, sequential, never interleaved. No "stabilization" wait: the
  snapshot is authoritative the moment counts match the task table.
  **Transition criterion (pinned): phase 1 completes when the counts load
  returns the full key→count map; phase 2 iterates that map and publishes
  the drift report.** There is no other gate — no key count, no time bound.
  **Phase-2 failure aborts the run and retries from phase 1 next tick** —
  never resumes mid-map, so a half-published report cannot pair with a
  half-rebuilt CMS.
- Drift *threshold* is deliberately absent (parity: Java has none, only the
  metric cap). If operators want alerting thresholds, that's EQLX-6
  runbook material, not scheduler behavior.

### 5. Timeout sweep: dispatcher cadence (CORRECTION-2)

- `FindTimedOut(task_timeout)` → `TIMEOUT` + `LastError` + release
  (counts/CMS) + sequential block flag, per tick of its own ticker.
- Cadence corrected to Java parity: `TaskTimeoutScheduler` runs at
  `dispatcher-interval` (50ms), batch `worker-poll-size` — the earlier
  "30s, parity unknown" was a guess made before the scheduler annotation
  was read. No separate interval knob (see CORRECTION-2 commit).

### 6. Locker: session-scope advisory locks, crash-safe by close

- `pg_try_advisory_lock(bigint)` on a **held dedicated connection**
  (`pool.Acquire` at Lock, held until release): session scope means a
  crashed process releases on connection close — no TTL machinery needed.
  Lock key = stable 64-bit hash of the job name (document the function;
  distinct from the CMS hasher).
- **Conn reservation (pinned): the locker owns a separate single-connection
  pool** (`pgxpool`, `MaxConns: 1`), not a checked-out conn from the job
  pool. Rationale: a lifetime-held checkout silently shrinks the job pool
  by one and surprises anyone tuning `pool_max` in production; a dedicated
  1-conn pool makes the reservation explicit and self-sizing.
- **Hash function (pinned): FNV-1a 64** (`hash/fnv`) over
  `"equalix:lock:" + jobName`. Deterministic across instances, restarts,
  and language implementations — a future non-Go coordinator computes the
  same key. (Same family as the CMS hasher by coincidence, not by
  contract; the prefix keeps the namespaces disjoint.)
- Non-blocking acquire: held-by-peer → tick skipped, no queueing.
- Answers the deferred lease question: no lease, no TTL; liveness comes
  from TCP close, matching Java's ShedLock-over-JDBC closely enough that
  no spec change is needed.

---

## Non-blocking

- **Metrics**: `port.Metrics` fakes (in-memory recorder) until EQLX-6
  Prometheus; call sites (`RecordDispatch`, `RecordCompletion`,
  `PublishDrift`, `SetRPS`) are wired now so the adapter swap is mechanical.
- **CMS**: local `pkg/cms` wrapped as `port.CMSStore` + transaction-aware
  buffering decorator (spec §4.4) in the jobs layer; Redis adapter stays
  EQLX-3-out (cross-instance fairness is Phase-5 differential scope).
- **Config**: plain Go struct with Java defaults (spec §10); YAML/env
  binding is EQLX-6. New keys: `dispatch_workers` (32), `shutdown_grace`
  (30s), `error_streak_threshold` (5),
  `penalty_factor` (1000.0). (`timeout_sweep_interval` deleted: the sweep
  ticks on `dispatcher_interval`, Java parity. `recovery_enabled` deleted
  with §7.)
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

---

## 3a run evidence (pinned definition)

"3a runs" means this scripted scenario, pass or fail — not "the service
starts":

1. Start the service (test binary or local main) against a fresh database.
2. Submit N=100 tasks through `POST /api/v1/tasks` across 3 fairness keys
   (weights 1:2:7, continuous backlog per key).
3. Observe all 100 reach terminal states via completion webhooks (stub
   executor auto-completes) within a bounded time.
4. Show per-key dispatch shares ≈ 10/20/70 from metrics/logs counters.
5. Watchdog + timeout are registered as **no-op stubs** for 3a (their
   bodies land in 3b). "Ticks clean" means the loop registered, ticked at
   its interval, and returned no error — it validates the runner with
   multiple concurrent jobs, not the job bodies. Full watchdog/timeout
   behavior gets its own evidence scenario in 3b.

Until this scenario exists and passes, "3a complete" is unclaimable.
