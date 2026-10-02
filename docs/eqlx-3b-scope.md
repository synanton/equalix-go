# EQLX-3b Scope — watchdog, timeout sweep, warm-up, overflow mitigation

Closes out EQLX-3: the two stub jobs get bodies, startup gets its warm-up,
and the risk-table overflow item gets its mechanism. Scope doc first;
implementation after green-light. Conventions as in `eqlx-3-jobs-scope.md`:
blocking choices up front, Java parity cited per pin.

Headline finding from Java (read, not inferred): **there is no recovery
service and no startup stuck-task handling.** `CmsWarmUpListener` on
`ApplicationReadyEvent` calls `watchdogService.warmUpCms()` — a read-only
`GROUP BY` + `cms.rebuild`, explicitly without drift publish ("empty sketch
would otherwise read as underestimate"). Stuck `DISPATCHED` tasks are owned
entirely by `TaskTimeoutService.expireTimedOutTasks()` → `TIMEOUT` + slot
release + sequential block. Everything below follows from that.

---

## Blocking

### 1. Watchdog: full-scan parity, capped metrics, warn-on-drift

- **Scan strategy (pinned): full `GROUP BY`, no pagination, no incremental
  cursor.** Java's `reconcile()` runs one `countInFlightByFairnessKey`
  (`WHERE status IN ('DISPATCHED','COMMITTED') GROUP BY fairness_key`) per
  run, inside a single `@Transactional` with the repair loop, drift
  measure/publish, and `cms.rebuild`. Our `Transact` covers repair; publish
  + rebuild stay post-commit (window (2), structural — same split as the
  dispatcher). Aiding index: `idx_tasks_status_priority (status, priority)`
  from V1; there is no `(status, fairness_key)` covering index in V1..V5,
  and adding one is a schema change outside parity — measure first.
- **Loop budget (pinned): single-digit seconds at 10× current scale or the
  interval moves.** Back-of-envelope: 5M in-flight rows through the status
  index is hundreds of ms, not minutes. 3b ships a stress path (seeded
  large `tasks` table, timed reconcile) that either confirms the 5-minute
  interval or forces a documented ceiling + pagination — evidence, not
  assumption. This is the external review's GROUP BY item, answered by
  measurement.
- **Alerting threshold (pinned): none for repair, warn-log on any drift.**
  Java repairs every mismatch unconditionally and logs an info summary; it
  has no error threshold. Parity = thresholdless repair. Addition (safe,
  observability-only): log at warn when `maxDrift != 0` — drift is always
  a bug, and the log line is what EQLX-6 alerting keys off alongside the
  metric. No behavior change, no threshold-gated repair.
- **Metric emission (pinned): per-key but capped.** Java's
  `CmsDriftReport.of(drift, drift-metric-max-keys, …)` keeps the top-N
  drifters by |drift| with `fairnessKey`+`layer` tags, plus aggregates —
  never all keys. Our `PublishDrift(fullReport)` + adapter-side truncation
  (already in the port doc) matches exactly. At 100K keys the cap, not the
  scan, is what protects Prometheus cardinality.

### 2. Timeout sweep + startup: one code path, not two (CORRECTION-2)

- **Recovery = first timeout sweep.** There is no distinct recovery
  operation in Java — no threshold, no state target, no lock, no startup
  scan beyond CMS warm-up. Our 3a scope §7 (old-only threshold,
  lock-guarded job, `timeout+60s`, reject-the-combo validation) was
  over-engineered against an operation Java doesn't have. It is replaced
  by: **CMS warm-up at startup** (parity: `CmsWarmUpListener` → read-only
  snapshot + rebuild, no drift publish) **plus the timeout sweep running
  from its first tick**, which TIMEOUTs anything already stuck.
- **Cadence correction:** Java's `TaskTimeoutScheduler` runs at
  `dispatcher-interval` (50ms), batch `worker-poll-size` — not the 30s my
  3a scope guessed ("parity unknown" was wrong; the scheduler annotation
  says otherwise). Pin: the Go timeout sweep ticks on `DispatcherInterval`,
  batch `WorkerPollSize`. `TimeoutSweepInterval` is deleted from `Config`
  (with its validation test) — one knob, Java parity.
- **Consequence for 3a scope §7:** superseded in full. `RecoveryEnabled`,
  the `timeout+60s` rule, and the reject-the-combo validation are removed
  in 3b implementation (they guard an operation that no longer exists).
  The `Locker("recovery")` concept goes with them; locks remain per-job
  (dispatcher/calculator/timeout/watchdog names).

### 3. Calculator-overflow mitigation: hybrid, metric-first

- **Mechanism (pinned): bound (exists) + gauge (new) + warn (new).**
  The 100ms tick reading `worker_poll_size` rows is already bounded; what
  is missing is visibility when ingestion persistently outruns it. Addition:
  `received_queue_depth` gauge (`COUNT(*) WHERE status='RECEIVED'`,
  indexed by `idx_tasks_status_created_at`) sampled when a tick fills its
  batch, plus warn-level log after 5 consecutive saturated ticks. No
  ingestion-side rejection — queue depth must not become client-visible
  errors (review inclination adopted as-is).
- **Port cost: one narrow method.** `TaskRepository.CountReceived` (or
  `CountByStatus`; name pinned in implementation) + pgx one-liner. The
  gauge query runs only on saturated ticks, never on the hot path.
- Recorded as NOTE in §13 on landing (observability addition, no behavior
  change — Java has neither the gauge nor the warn).

### 4. Recovery state target: TIMEOUT, never re-queue

- **Pinned: stuck in-flight tasks become `TIMEOUT`** — the timeout path's
  existing target (`expire()` sets `TIMEOUT` + `lastError` + releases
  slots + blocks the sequential key). No re-queue to `QUEUED`.
- **Rationale (Java parity + safety):** Java never re-dispatches a stuck
  task; the executor may have completed it with a lost webhook, and
  re-queue would double-execute. Executor idempotency is never relied upon
  — the design avoids the question instead of answering it. Client retries
  by submitting a new task. `FAILED` (explicit failure) is wrong too: a
  timed-out task is *unknown*, not failed, and `TIMEOUT` preserves that
  distinction for operators.

---

## Failure-mode coverage (§11 items for 3b)

- **DB down during watchdog:** tick errors, `Transact` rolls back, Loop
  swallows (recoverable posture); next tick retries from phase 1. No
  partial repair is committable by construction (single tx). Test: kill
  the DB mid-run in the evidence script, restore, assert next reconcile
  completes and drift returns to zero.
- **Redis down during CMS rebuild:** local CMS unaffected (no network
  dependency by design). The shared-Redis adapter does not exist yet
  (EQLX-3-out) — its outage behavior is specified with the adapter, not
  here. Conditional, deferred, stated so nobody assumes coverage.
- **Concurrent warm-up on two instances:** read-only snapshot + local
  rebuild is idempotent; no lock needed. Two instances rebuilding
  simultaneously converge on the same snapshot. (Shared-Redis future:
  last-writer-wins with identical content — harmless; noted for the
  adapter's scope.)
- **Duplicate completion during sweep races:** B2 protocol + version
  conflict path already cover it (chi surface); the sweep's
  `isInFlight` recheck inside `expire()` is the second guard. Evidence
  script races a completion against a sweep tick.

---

## 3b run evidence (failure-mode scripts, not the 100-task scenario)

The 100-task shares scenario is 3a's evidence and stays 3a's. 3b proves
the failure paths, scripted pass/fail:

1. `kill -9` a dispatcher mid-tick → timeout sweep marks `TIMEOUT`,
   slots released (`counts` 0, CMS 0), sequential key blocked.
2. Inject CMS drift (direct `client_counts` update bypassing CMS) →
   watchdog repairs counts, drift published (top-N + aggregates), rebuild
   zeroes drift on the next run.
3. DB outage mid-tick → error logs with streaks, no crash; restore →
   next watchdog/timeout ticks complete, drift back to zero.
4. Duplicate completion raced against a sweep tick → exactly-once slot
   release (B2 protocol under concurrency).
5. Saturated calculator (ingestion burst > 100ms×batch) →
   `received_queue_depth` gauge rises, warn fires after 5 saturated ticks,
   no ingestion rejection, drain catches up when burst ends.

---

## Non-blocking / out of scope

- `TimeoutSweepInterval` removal + `RecoveryEnabled` removal + validation
  updates (mechanical, follows pin 2).
- `CountReceived` port method + pgx impl (follows pin 3).
- Stress path for the GROUP BY scan (follows pin 1; seeded table, timed).
- Hierarchical dispatch, Kafka, Redis CMS, adaptive RPS, migrate-on-startup:
  unchanged, later phases.
