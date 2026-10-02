# equalix-go behavioral specification

This document is the authoritative behavioral reference for equalix-go.
It is extracted from the Java Equalix implementation and its design documents.

Sources:

- Java design: `docs/design.md` (§ references per section)
- Java config: `docs/configuration.md` (defaults match `src/main/resources/application.yml`)
- Java REST: `docs/api-reference.md` (see also `docs/api.md` in this repo)
- Java code: `src/main/java/org/synanton/equalix/...` (paths recorded per section)
- Java DDL: `src/main/resources/db/migration/V1..V5` (see `docs/schema.sql`)

Extraction rules:

- Algorithms are stated formally (inputs, state, formula, invariants).
- The Java source is cited for every claim.
- Unverified or ambiguous behavior is marked `[GAP]` and listed in §13.
- Where Java docs and Java code disagree, the code is authoritative; the discrepancy is recorded explicitly.

---

## 1. Scheduling model

A **fairness key** is a string identifying the logical group for which fairness is enforced (tenant, client, user, project). All scheduling decisions are made at fairness-key level. One tenant normally maps to one key; hierarchical mode (§6.1) maps path keys (`acme/sales`) onto a tree. The `client_counts` table name is historical and retained for schema compatibility.

A **task** is one unit of work: opaque binary `payload` plus scheduling metadata (`fairness_key`, `weight`, priority state). `weight` is a positive decimal (default `1.0`, `@Positive` on ingest, `DECIMAL(10,4)`); higher weight receives a larger dispatch share. Effective weight: `weight == null || <= 0 ? 1.0 : weight` (`domain/model/Task.effectiveWeight`).

### 1.1 Task lifecycle

States (`domain/model/TaskStatus`):

```text
RECEIVED → QUEUED → DISPATCHED → COMMITTED → SUCCEEDED
                                             → FAILED
                                             → TIMEOUT
```

| Status | Meaning |
|---|---|
| `RECEIVED` | Ingested; awaiting priority calculation. `priority` is NULL. |
| `QUEUED` | Priority assigned; ready for dispatch. |
| `DISPATCHED` | Slot allocated; CMS +1 and `client_counts` +1; sent to remote executor. |
| `COMMITTED` | Remote executor acknowledged (HTTP 2xx via `DispatchAckService.markCommitted`, only from `DISPATCHED`). Awaiting completion callback. |
| `SUCCEEDED` | Terminal. CMS −1, counts −1, `result` populated. |
| `FAILED` | Terminal. Retries exhausted, business failure, block-recovery force-fail, or failed dependency. |
| `TIMEOUT` | Terminal. In-flight older than `task-timeout-ms`; treated as `FAILED` for accounting. |

Predicates: `isInFlight() = DISPATCHED || COMMITTED`; `isTerminal() = SUCCEEDED || FAILED || TIMEOUT`.

Transition rules:

- `RECEIVED → QUEUED`: priority calculator only, one batch per tick (§5 calculator).
- `QUEUED → DISPATCHED`: dispatcher only, under `SELECT ... FOR UPDATE SKIP LOCKED`.
- `DISPATCHED → COMMITTED`: executor adapter on HTTP 2xx only; errors are logged and do not throw.
- In-flight → terminal: completion webhook (validates in-flight, ignores terminal duplicates), timeout service, or sequential block recovery.
- Illegal transitions (e.g. completing a `QUEUED` task) throw `IllegalArgumentException` → `400 BAD_REQUEST`.
- `retry_count` is set to `0` at creation (`CreateTaskUseCase`) and never incremented by the scheduler; the remote system owns retry policy. `version` (`@Version`) provides optimistic locking on concurrent status transitions.

Sources: `domain/model/TaskStatus.java`, `domain/CreateTaskUseCase.java:66`, `domain/service/CompletionHandlerService.java`, `domain/service/SequentialCompletionHandlerService.java`, `domain/service/DispatchAckService.java`, `domain/service/TaskTimeoutService.java`.

### 1.2 Fairness key

The fairness key is the tenant-supplied `fairnessKey` string (Kafka path: record key, null → `"default"`). One key = one scheduling principal in flat mode. Keys must be unambiguous in hierarchical mode: no empty segments, and a key must not be both a leaf and a parent (e.g. both `acme` and `acme/sales`).

Sources: `adapter/in/rest/dto/CreateTaskRequest.java`, `adapter/in/kafka/TaskIngestionKafkaConsumer.java:32`, `docs/configuration.md` (hierarchical).

---

## 2. Virtual time

Self-clocked fair queueing over persistent state (`domain/service/VirtualTimeService.java`, `adapter/out/database/ClientVirtualTimeJpaRepository.java`).

### 2.1 Per-client state

`client_virtual_time(fairness_key PK, virtual_time DOUBLE, virtual_finish DOUBLE, updated_at)`:

- `virtual_time` (T_k): service received, advanced on **dispatch** to the highest dispatched tag of the key.
- `virtual_finish`: tag of the last queued task, advanced on **queueing** (tag reservation).

All updates are monotonic `GREATEST(...)` upserts, safe under concurrent instances and restarts.

### 2.2 Global virtual time V

`scheduler_virtual_clock` singleton row (`id = 1`): system virtual time V, the highest dispatched tag (more precisely the highest aged position `tag − A(W)` so aging-promoted tasks don't drag V ahead of the backlog). Read once per priority-calc batch; advanced per dispatch batch. Migration V4 seeds V at `MAX(priority)` over pre-existing `QUEUED` tasks so old epoch-millisecond priorities drain first.

Starting new work at `max(virtual_finish, V)` stops an idle key from banking credit and bursting later.

### 2.3 Finish tag

Reservation (atomic, `ClientVirtualTimeJpaRepository`):

```sql
INSERT INTO client_virtual_time (fairness_key, virtual_time, virtual_finish, updated_at)
VALUES (:key, :systemVirtualTime, :systemVirtualTime + :increment, now())
ON CONFLICT (fairness_key)
DO UPDATE SET virtual_finish = GREATEST(client_virtual_time.virtual_finish, :systemVirtualTime) + :increment,
              updated_at = now()
RETURNING virtual_finish
```

with `increment = quantum * UNIT_TASK_COST / effectiveWeight`, `UNIT_TASK_COST = 1.0`, `quantum` default `1000` (`app.queue.virtual-time.quantum`). Computed at **queueing** time (priority calculator), persisted per task in `tasks.virtual_finish`, one `save` together with `status = QUEUED` and `priority`.

Dispatch advance per key: `virtual_time = virtual_finish = GREATEST(..., highestDispatchedTag)`; system: `V = GREATEST(V, highestServedPosition)`.

**Invariants:**

- [ ] `virtual_finish` per key is monotonically non-decreasing (GREATEST upsert).
- [ ] V is monotonically non-decreasing.
- [ ] An idle key's next tag starts at ≥ V (no banked credit).

**Source:** `domain/service/VirtualTimeService.java:43-84`, `adapter/out/database/ClientVirtualTimeJpaRepository.java:12-23`, `docs/design.md` §5.5.

---

## 3. Priority calculation

Runs every `priority-calc-interval` (default 100 ms) over a batch (`worker-poll-size`, default 100) of `RECEIVED` tasks in creation order, so each key's tags follow submission order. Source: `domain/service/PriorityCalculatorService.java:28-88`.

```text
P = F + p × F̂_k / w
```

- `F`: persistent weighted virtual finish tag from §2.3 (`finishTag`).
- `F̂_k = cms.estimateCount(fairnessKey)`: CMS in-flight estimate at calc time (§4).
- `p = penaltyFactor = adaptiveRpsController.getPenaltyFactor()` (§7).
- `w = task.effectiveWeight()`.
- Stored as `priority = Math.round(F) + (long)(F̂_k × p / w)` (`BIGINT`, NULL until `QUEUED`).

Sequential tasks add a sequence boost and a blocked penalty (§6.4): `+ (sequenceNumber − lastCompletedSequence) × 100`, `+ 10000` if the key is blocked (`SEQUENCE_BOOST_FACTOR = 100`, `BLOCKED_PENALTY = 10_000`).

`priority` is both a DB column (flat dispatcher `ORDER BY`) and, with aging enabled, re-ranked in memory as `priority − A(W)` (§6.3). In hierarchical mode the stored priority orders per-key heads only; cross-key choice uses node virtual times (§6.1).

**Invariants:**

- [ ] Priority is deterministic given `(finishTag, inFlightEstimate, penaltyFactor, weight, sequenceState)`.
- [ ] Higher in-flight count never lowers a task's priority (monotonic in `F̂_k`).

---

## 4. Count-Min Sketch

Chain (`config/CmsConfig.java`): raw adapter (`local` | `redis`) → `TransactionAwareCmsProvider` → optional `HierarchicalCmsProvider`. Sources: `adapter/out/cms/*`, `docs/design.md` §7.

### 4.1 Parameters

`width` default `65536` (`ε = 2/w`), `depth` default `5` (`δ = (1/2)^d`). Memory `w × d × 8` bytes (~2.6 MB at defaults). `depth > 5` shows diminishing returns. Error depends on tasks currently in flight N, never underestimates, overestimates by ≤ `2N/w` with probability `1 − 2^-d`.

Hashing (`CmsKeyHasher`, `LAYOUT_VERSION = 2`): 64-bit FNV-1a over key UTF-8 bytes + SplitMix64 finalizer, per-row mixing `mix(keyHash + (row+1) × 0x9E3779B97F4A7C15)`, cell `floorMod(mix, width)`. Any depth supported.

### 4.2 Operations

- `add(key, delta)`: every row's cell `+= delta`; `total += delta`. Called **on dispatch (+1)** and **on completion/timeout (−1)** — never on enqueue. Local adapter is `synchronized`.
- `estimateCount(key)`: `max(0, min over d cells)`. Called in priority calculation and hierarchical selection. The `max(0, …)` guard prevents negatives from decrement collisions.
- `totalInFlight()`: `max(0, total)`; hierarchical mode reads the root `""` estimate instead.

### 4.3 Decay

There is **no time-based decay**. Counts change only via `add`/`rebuild`. Staleness is bounded by watchdog reconciliation (§8), not by decay. (Discrepancy note: early design prose mentions "decay"; the code has none — code is authoritative.)

### 4.4 Transactional buffering

`TransactionAwareCmsProvider`: inside a transaction `add` only buffers `pendingDeltas().merge(key, delta, sum)`; deltas apply to the delegate **after commit** and are **discarded on rollback**. A rolled-back dispatch/completion leaves the sketch unchanged. Reads and `rebuild` bypass the buffer. Crash between DB commit and after-commit flush loses the delta — this is exactly the drift the watchdog repairs.

### 4.5 Warm-up

`CmsWarmUpListener` on `ApplicationReadyEvent` → `watchdogService.warmUpCms()`: `SELECT fairness_key, COUNT(*) ... WHERE status IN (DISPATCHED, COMMITTED) GROUP BY fairness_key`, then `cms.rebuild(actual)` — no drift publication (unlike periodic reconcile).

### 4.6 Redis-backed CMS

Selected by `app.queue.cms.mode = redis` (default `local`). Layout: one hash `{namespace}:v2` (namespace default `equalix:cms`) with fields `r{row}:c{col}`, plus `{namespace}:v2:total`. Layout version in the key isolates rolling deploys. `add` is one atomic Lua `EVAL`:

```lua
local hash      = KEYS[1]
local total_key = KEYS[2]
local delta     = tonumber(ARGV[1])
for i = 2, #ARGV do
    redis.call('HINCRBY', hash, ARGV[i], delta)
end
redis.call('INCRBY', total_key, delta)
return 1
```

called with `KEYS = [hashKey, totalKey]`, `ARGV = [delta, field0..field_depth-1]`. `estimateCount` uses one `HMGET` (multiGet) over the `d` cells. Priority-calculator batch reads are pipelined into a single round-trip. `rebuild` does `DEL hashKey` + pipelined `HINCRBY` per cell + `SET totalKey`. No TTLs. If Redis is unavailable and `fallback-to-local: true` (default), the adapter serves from the local sketch for the outage duration; watchdog DB reconciliation bounds drift to the interval.

Recommendation (design §7.7): single instance → `local`; multi-instance with strict cross-instance fairness → `redis`; else `local` with watchdog drift accepted.

### 4.7 Drift and reconciliation

Drift per key: `drift_k = cms.estimateCount(k) − actualInFlight(k)`, measured by the watchdog **before** each rebuild (§8). Expected value 0. Published per key (non-zero only, top-N by |drift|, cap `drift-metric-max-keys` default 100) and as aggregates (max/min/absolute/keys/timestamp, plus per-layer in hierarchical mode).

---

## 5. Dispatcher

### 5.1 Selection query

Flat mode, every `dispatcher-interval` (default 50 ms). First: promote starved tasks (`created_at < now − max-queued-time-ms`, batch `worker-poll-size`) by setting `priority = 0` — bypassing quota checks. Then (`DispatcherService`):

```text
freeSlots = max(0, maxTasksInProcess − globalInFlight)
if adaptive RPS enabled: freeSlots = min(freeSlots, max(1, ceil(currentRps × intervalSeconds)))
```

`maxPerClient = max-per-client-quota > 0 ? quota : NULL` (NULL disables the ceiling).

```sql
SELECT t.*
FROM tasks t
LEFT JOIN client_counts cc
    ON t.fairness_key = cc.fairness_key
WHERE t.status = 'QUEUED'
  AND t.is_sequential = false
  AND (
        :maxPerClient IS NULL
     OR cc.in_flight_count < :maxPerClient
     OR cc.in_flight_count IS NULL
  )
ORDER BY t.priority ASC NULLS LAST, t.created_at ASC, t.id ASC
LIMIT :limit
FOR UPDATE OF t SKIP LOCKED
```

With aging enabled the dispatcher locks a candidate pool instead — `LIMIT max(freeSlots, candidate-pool-size)` by priority **plus** the same limit by `(created_at, id)` — and keeps the best `freeSlots` by aged rank (§6.3). Hierarchical mode delegates to the planner (§6.1); sequential tasks use `SequentialDispatcherService` (§6.4).

Per selected task (same transaction): `status = DISPATCHED`, `client_counts +1` (atomic `GREATEST(0, count + delta)`), then after commit CMS `+1` (§4.4); `RemoteExecutorPort.send(id, payload, previousResult)`.

### 5.2 Transaction boundaries

Status change + `client_counts` increment commit together. CMS updates and the executor HTTP call happen **after** commit (CMS via after-commit hook; executor send after save). Executor errors are logged, never thrown: the task stays `DISPATCHED` until completion, timeout, or recovery. `DispatchAckService` moves `DISPATCHED → COMMITTED` on HTTP 2xx.

Executor envelope (`HttpRemoteExecutorAdapter`): binary POST to `{base-url}/tasks/{id}/execute`: `[16 bytes UUID][4 bytes payload len][payload][4 bytes prev len][previousResult]`; timeouts `connect-timeout-ms` 2000 / `read-timeout-ms` 5000.

### 5.3 Duplicate completion

Terminal duplicates ignored (no double-decrement). Two instances cannot dispatch the same row: `FOR UPDATE SKIP LOCKED` partitions candidates. Lost-update races on status transitions are guarded by `@Version` optimistic locking.

### 5.4 Timeouts

`TaskTimeoutService` (when `task-timeout-ms > 0`, default 300000):

```sql
SELECT * FROM tasks
WHERE status IN ('DISPATCHED', 'COMMITTED')
  AND updated_at < now() - (:olderThanMs || ' milliseconds')::interval
ORDER BY updated_at ASC
LIMIT :limit
```

Each row → `TIMEOUT`, `lastError = "Exceeded task timeout of <ms>ms"`, `completedAt/updatedAt = now`, CMS −1, counts decrement (floored at 0). Sequential timed-out tasks also set `client_sequence_state.is_blocked = true, blocked_at = now, current_executing_task_id = taskId`.

### 5.5 Retry

No scheduler-side retry counter or backoff exists in code (`retry_count` stays `0`; the `max-retries`/`backoff` keys from design v1.0 were removed in v1.1 as having no consumer). Failed executor calls and `success=false` completions record `lastError` and terminal state; the remote system owns retry policy; re-submission is a new task with the same fairness key.

Sources: `domain/service/DispatcherService.java`, `adapter/out/database/TaskJpaRepository.java` (all SQL above), `domain/service/SequentialDispatcherService.java`, `domain/service/TaskTimeoutService.java`, `adapter/out/executor/HttpRemoteExecutorAdapter.java`, `docs/design.md` §5.6, §11.

---

## 6. Fairness features

### 6.1 Hierarchical fairness

Enabled by `app.queue.fairness-mode = hierarchical` (default `flat`). Keys are paths (`acme/sales`) split by `separator` (default `/`) onto `layers` (default `organization → department`); extra segments fold into the leaf; one-segment keys hang directly under root `""`. Leaf weight = task weight; internal nodes use layer `default-weight` (default `1.0`) unless `hierarchical.weights` overrides (`"[acme]": 2.0`). Switching modes starts the other mode's history from scratch.

Per tick (`HierarchicalDispatchPlanner`):

1. `findQueuedLeaves()`: per-key `(queued, promoted = priority ≤ 0 count, maxWeight, inFlight)` via `idx_tasks_queued_by_key`.
2. Load `hierarchy_node` rows for every node on those paths.
3. `HierarchicalSelector.plan` (pure): promoted leaves first (by key); then descend root → leaf, each level taking the backlogged child with least `τ + q/w + p·F̂/w` (node virtual time + quantum share + CMS pressure), charging `q/w` along the path; capacity per leaf `min(queued, max(0, maxPerClient − inFlight))`.
4. `findAndLockQueuedHeads(json [{key, limit}])`: one `LATERAL` query locking planned heads per key by `(priority, created_at, id)` `SKIP LOCKED`.
5. `recordDispatch`: charge each node `τ = max(τ, floor) + Σ q/w` on its path; raise parents' children floors (monotonic upserts, same transaction). Sequential dispatches charge through the same path.

Aging is ignored in hierarchical mode (warning logged); `max-queued-time-ms` promotion still applies. CMS is wrapped in `HierarchicalCmsProvider`, counting every internal node (`acme/`) and root (`""`) so per-layer pressure and global total come from the sketch. Metrics: `equalix.hierarchy.dispatches{layer, node}` up to `metrics-depth` (default 1).

Sources: `domain/service/HierarchicalDispatchPlanner.java`, `domain/service/HierarchicalSelector.java`, `adapter/out/cms/HierarchicalCmsProvider.java`, `docs/design.md` §9.5.

### 6.2 Per-tenant quotas

`max-per-client-quota` (default 500; `0` = disabled) is a hard per-key in-flight ceiling enforced in the dispatcher's SQL (`cc.in_flight_count < :maxPerClient`, NULL counts row = no row = admitted). Global cap `max-tasks-in-process` (default 5000) bounds `freeSlots`. Starvation promotion bypasses quota. CMS pressure still deprioritizes heavy keys when quota is disabled.

### 6.3 Anti-starvation aging

`app.queue.aging`: `policy none|linear|log|power` (default `none`), `lambda` 1000, `gamma` 2.0, `candidate-pool-size` 200. Effective priority `P − A(W)`, `W` = seconds since `created_at`:

| Policy | `A(W)` |
|---|---|
| `none` | `0` (only `max-queued-time-ms` promotion applies) |
| `linear` | `λ·W` |
| `log` | `λ·ln(1+W)` |
| `power` | `λ·W^γ` |

One weight-1 task ≈ `quantum` priority units; calibrate λ as credit at a target wait (e.g. `A(30s) = 10 × quantum` → λ = 333 linear / 2912 log / 11.1 power γ=2). `max-queued-time-ms` (default 60000) is the hard backstop for every policy (promotion to priority 0 at tick top). V advances to the highest **aged** position so promoted tasks don't drag V forward (§2.2).

Invocation (EQLX-1 review finding): aging is **opt-in** — enabled iff `policy != none` (default `none` takes the flat `ORDER BY priority` path, zero aging cost). When enabled, the dispatcher runs the aging path **every tick**: it locks up to `2 × max(freeSlots, candidate-pool-size)` candidates (best-by-priority pool plus oldest pool) and re-ranks them in memory. The `BenchmarkRankByAging` baseline (~67µs for a 400-candidate pool) is therefore the per-tick cost of the aging path, not an occasional backstop — relevant for EQLX-3 dispatch-loop budgets. Quota filtering applies before ranking (see CORRECTION-1).

Sources: `domain/service/AgingService.java`, `domain/model/AgingPolicy.java`, `docs/configuration.md`.

### 6.4 Sequential execution mode

Opt-in per task (`is_sequential = true`, `sequence_number` required). One task in flight per fairness key, dispatched in `sequence_number` order; different keys run in parallel (Kafka-partition analogy). State: `client_sequence_state(fairness_key, last_completed_sequence, last_dispatched_sequence, current_executing_task_id, is_blocked, blocked_at)`.

- Ingest: `findOrCreate` state row so the first task can dispatch.
- Priority: base + `(sequenceNumber − lastCompletedSequence) × 100` boost; `+10000` while blocked (keeps blocked keys parked).
- Dispatch (`SequentialDispatcherService`, own cadence default 50 ms): per unblocked ready key, `next = lastCompletedSequence + 1`, lock that `QUEUED` task; if `requires_previous_result && dependsOnTaskId != null`, attach predecessor `result` as `previous_result` (defer if unavailable); `DISPATCHED`, cursor `currentExecutingTaskId = id, lastDispatchedSequence = next`, CMS +1, counts +1, send with previous result.
- Completion routes on `is_sequential` (`CompleteTaskUseCase`): success advances `lastCompletedSequence`, clears executing/blocked, and immediately dispatches the next in sequence; failure sets `is_blocked = true, blocked_at = now` (key halts until recovery).
- `ClientBlockRecoveryService` (default every 10 s): keys blocked longer than `client-block-timeout-ms` (default 60000) are force-unblocked — in-flight task → `FAILED` (`"Force-unblocked..."`), CMS/counts released, `blocked = false, currentExecutingTaskId = NULL, lastCompletedSequence + 1`.
- `ResultPassthroughRecoveryService` (default every 60 s): tasks with `requiresPreviousResult && previousResult IS NULL && dependsOnTaskId IS NOT NULL` (QUEUED) get predecessor `result` attached on predecessor `SUCCEEDED`, or are marked `FAILED` (`"Dependency failed: ..."`) on predecessor `FAILED`/`TIMEOUT`.

Manual escape hatch: `UPDATE client_sequence_state SET is_blocked = false, current_executing_task_id = NULL, last_completed_sequence = last_completed_sequence + 1 WHERE fairness_key = ?`.

Sources: `domain/service/SequentialDispatcherService.java`, `domain/service/SequentialCompletionHandlerService.java`, `domain/service/ClientBlockRecoveryService.java`, `domain/service/ResultPassthroughRecoveryService.java`, `docs/design.md` §14.

---

## 7. Adaptive RPS controller

Global capacity control (`domain/service/AdaptiveRpsController.java`, `docs/design.md` §8). Sliding window of the last `window-size` (default 100) completions; evaluation at most every `adjustment-interval-ms` (default 2000); needs `min-samples` (default 10).

- `latency`: mean duration of completions **since the previous evaluation** (with `interval = 0`, whole-window mean — the pre-EQX-6 behavior), smoothed `smoothed = α·mean + (1−α)·smoothed`, `α = latency-ema-alpha` (default 0.7; `1` = off).
- `error_rate`: failures+timeouts fraction over the window.

| Condition | Action |
|---|---|
| `errorRate > error-threshold` (0.05) | `RPS = max(minRps, RPS × 0.5)` — emergency brake, never dampened (still one step per interval) |
| `smoothed > target × (1 + 0.2)` | `RPS = max(minRps, RPS × 0.9)` |
| `smoothed < target × (1 − 0.2)` AND `errorRate < 0.01` | `RPS = min(maxRps, RPS × 1.05)` |
| otherwise (dead band) | no change; reversal counter reset |

Reversing direction needs `direction-change-confirmations` (default 3) consecutive agreeing evaluations — the dead-band dampener. Bounds `min-rps` 1 / `max-rps` 100, start `initial-rps` 1 (ramp to 25 ≈ 66 increases ≈ 2 min at 2 s intervals; raise `initial-rps`/`increase-factor` for faster cold start).

Exposed as `getCurrentRps()` (dispatcher budget cap §5.1; Prometheus gauge) and `getPenaltyFactor() = 1000 / currentRps` (priority pressure §3). With adaptive RPS disabled, `recordCompletion` clears state and returns.

---

## 8. Watchdog reconciliation

Every `watchdog.interval-minutes` (default 5), `WatchdogService.reconcile()` — two-phase, in this order:

1. Repair `client_counts`: aggregate `SELECT fairness_key, COUNT(*) FROM tasks WHERE status IN ('DISPATCHED','COMMITTED') GROUP BY fairness_key`; upsert every disagreeing key (including zeroing keys with no in-flight rows).
2. Measure drift **before** rebuilding (`drift_k = estimate − actual` per key, ancestors + root added in hierarchical mode), publish (`CmsDriftReport`, cap `drift-metric-max-keys` 100, largest |drift| first, non-zero keys only; aggregates always full), then `cms.rebuild(actual snapshot)` (hierarchical provider expands ancestors).

Single reconciliation restores both layers because the CMS rebuilds from the just-reconciled snapshot. Startup warm-up (`warmUpCms`) rebuilds without publishing drift.

Sources: `domain/service/WatchdogService.java`, `domain/service/CmsErrorRecorder.java`, `docs/design.md` §5.9.

---

## 9. Cross-instance coordination

All scheduled jobs take a ShedLock (`shedlock` table, V2; `@EnableSchedulerLock(defaultLockAtMostFor = "10m")`):

| Job (lock name) | Cadence prop | `lockAtMostFor` / `lockAtLeastFor` |
|---|---|---|
| `priorityCalculator` | `priority-calc-interval` 100 ms | 5 s / 50 ms |
| `dispatcher` | `dispatcher-interval` 50 ms | 5 s / 25 ms |
| `sequentialDispatcher` | `sequential.dispatcher-interval` 50 ms | 5 s / 25 ms |
| `taskTimeout` | `task-timeout-ms` driven | 5 s / 25 ms |
| `clientBlockRecovery` | `block-recovery-interval` 10 s | 5 m / 1 s |
| `resultPassthroughRecovery` | `result-passthrough-interval` 60 s | 5 m / 1 s |
| `watchdog` | `interval-minutes` 5 m | 10 m / 1 m |

Concurrent dispatchers additionally partition via `SKIP LOCKED`. **equalix-go replaces ShedLock with PostgreSQL advisory locks** (`pg_advisory_lock`, one lock per job name; exact scope decided in EQLX-3). CMS cross-instance consistency comes from the Redis adapter (§4.6) or watchdog-bounded drift with local sketches.

Sources: `adapter/in/schedule/*.java`, `config/ShedLockConfig.java`, `docs/design.md` §10.

---

## 10. Configuration

Full table with types, defaults, and semantics: Java `docs/configuration.md` (defaults = `src/main/resources/application.yml`). Key defaults referenced normatively by this spec:

```yaml
app.queue.max-tasks-in-process: 5000
app.queue.max-per-client-quota: 500          # 0 = disabled
app.queue.priority-calc-interval: 100        # ms
app.queue.dispatcher-interval: 50            # ms
app.queue.worker-poll-size: 100
app.queue.max-queued-time-ms: 60000
app.queue.task-timeout-ms: 300000            # 0 = off
app.queue.max-payload-bytes: 1048576
app.queue.fairness-mode: flat
app.queue.virtual-time.quantum: 1000
app.queue.aging.policy: none
app.queue.aging.lambda: 1000
app.queue.aging.gamma: 2.0
app.queue.aging.candidate-pool-size: 200
app.queue.cms.mode: local
app.queue.cms.width: 65536
app.queue.cms.depth: 5
app.queue.cms.redis.key-namespace: equalix:cms
app.queue.cms.redis.fallback-to-local: true
app.queue.cms.error-sampling.enabled: false  # load-test only (GROUP BY per sample)
app.queue.cms.error-sampling.interval-ms: 1000
app.queue.sequential.enabled: true
app.queue.sequential.client-block-timeout-ms: 60000
app.queue.sequential.dispatcher-interval: 50
app.queue.sequential.block-recovery-interval: 10000
app.queue.sequential.result-passthrough-interval: 60000
app.adaptive-rps.enabled: true
app.adaptive-rps.initial-rps: 1
app.adaptive-rps.min-rps: 1
app.adaptive-rps.max-rps: 100
app.adaptive-rps.target-latency-ms: 200
app.adaptive-rps.latency-threshold: 0.2
app.adaptive-rps.error-threshold: 0.05
app.adaptive-rps.window-size: 100
app.adaptive-rps.min-samples: 10
app.adaptive-rps.emergency-factor: 0.5
app.adaptive-rps.decrease-factor: 0.9
app.adaptive-rps.increase-factor: 1.05
app.adaptive-rps.increase-error-threshold: 0.01
app.adaptive-rps.adjustment-interval-ms: 2000  # 0 = every completion (pre-EQX-6)
app.adaptive-rps.latency-ema-alpha: 0.7        # 1 = off
app.adaptive-rps.direction-change-confirmations: 3  # 1 = off
app.hierarchical.separator: /
app.hierarchical.layers: [organization/1.0, department/1.0]
app.hierarchical.weights: {}
app.hierarchical.metrics-depth: 1
app.watchdog.interval-minutes: 5
app.watchdog.drift-metric-max-keys: 100
app.executor.base-url: http://localhost:9090
app.executor.connect-timeout-ms: 2000
app.executor.read-timeout-ms: 5000
app.security.api-key: ${EQUALIX_API_KEY:changeme}
app.scheduling.enabled: true  # false = ingest + completions only
```

equalix-go may rename keys (see `README.md`) but **defaults must mirror these** unless a divergence is documented here. Overridable by env in Spring via relaxed binding (e.g. `APP_ADAPTIVE_RPS_TARGET_LATENCY_MS`).

---

## 11. Failure modes and recovery

Expected behavior per scenario (sources in parentheses):

- [ ] **Executor returns 5xx / explicit `success=false`:** task stays `DISPATCHED` (send error path) or → `FAILED` with `lastError`; CMS/counts released only on terminal transition. (`HttpRemoteExecutorAdapter`, `CompletionHandlerService`)
- [ ] **Executor times out:** send fails silently (logged); timeout service → `TIMEOUT` after `task-timeout-ms`; sequential key blocked. (§5.4)
- [ ] **Executor unreachable:** same as timeout; dispatch budget keeps being consumed until RPS controller brakes on error rate.
- [ ] **PostgreSQL unavailable during dispatch:** tick fails; `SKIP LOCKED` + ShedLock/advisory lock mean another instance picks up next tick; no partial dispatch (single transaction).
- [ ] **PostgreSQL unavailable during completion:** webhook fails (5xx); executor retries delivery; terminal-duplicate rule keeps redelivery safe.
- [ ] **Redis unavailable (CMS):** fallback-to-local serves estimates; watchdog DB reconciliation bounds drift; shared-sketch accuracy restored on recovery via rebuild. (§4.6)
- [ ] **Instance crashes mid-dispatch:** committed rows stay `DISPATCHED` (picked up by timeout/completion); uncommitted rows roll back and stay `QUEUED`.
- [ ] **Instance crashes between DB commit and CMS flush:** CMS undercounts by the lost delta until watchdog rebuild (§4.4, §8).
- [ ] **Two instances dispatch the same task:** prevented by `FOR UPDATE SKIP LOCKED` + `@Version`; quota double-admit bounded by atomic `GREATEST` increments + watchdog repair.
- [ ] **Completion webhook delivered twice:** second delivery ignored (terminal-state check). (§5.3)
- [ ] **Clock skew between instances:** scheduling uses DB `now()` in SQL intervals and monotonic virtual-time upserts (not wall-clock ordering), except aging wait `W` (per-instance `now`) and timeout scans — skew shifts promotion/timeout timing, not fairness order. [GAP-4]
- [ ] **Watchdog runs concurrently on two instances:** prevented by `watchdog` ShedLock (10 m/1 m); Go advisory-lock equivalent in EQLX-3.

---

## 12. Observability

Java Micrometer names (`docs/design.md` §12) that equalix-go mirrors (Go names TBD in EQLX-6, semantics fixed here):

| Metric | Type | Labels | Incremented / sampled where |
|---|---|---|---|
| `equalix.task.duration` | Timer | `success` | `MicrometerPerformanceMonitorAdapter` per completion |
| `equalix.task.errors` | Counter | — | per failed completion |
| `equalix.adaptive.rps` | Gauge | — | `getCurrentRps()` |
| `equalix.cms.estimation.drift` | Gauge | `fairnessKey`, `layer` | watchdog, before rebuild (§8) |
| `equalix.cms.estimation.drift.{max,min,absolute,keys,keys.sampled,timestamp}` | Gauge | — | watchdog aggregates |
| `equalix.cms.estimation.drift.layer.absolute` | Gauge | `layer` | watchdog, hierarchical |
| `equalix.cms.estimation.error`, `….magnitude` | Summary | `direction` | opt-in `cms.error-sampling` (GROUP BY per sample; load tests only) |
| `equalix.hierarchy.dispatches` | Counter | `layer`, `node` | hierarchical dispatcher, up to `metrics-depth` |
| `jvm.*`, `process.*`, `system.*`, `http.server.requests` | various | Micrometer/Spring defaults | runtime (Go: `process.*` + `http.server.requests` equivalents) |

---

## 13. Gaps and open questions

- [ ] **GAP-1 (closed in this spec):** sequential execution state machine — extracted from `SequentialDispatcherService`, `SequentialCompletionHandlerService`, `ClientBlockRecoveryService`, `ResultPassthroughRecoveryService` (§6.4). No longer a gap.
- [ ] **GAP-2 (closed in this spec):** Redis Lua script — reproduced verbatim (§4.6) from `RedisCMSAdapter`.
- [ ] **GAP-3 (closed in this spec):** watchdog two-phase rebuild + drift publication — extracted from `WatchdogService`/`CmsDriftReport` (§8).
- [ ] **GAP-4 (open, non-blocking for Phase 1):** clock-skew handling has no explicit Java mechanism; §11 records the analysis (DB-time SQL + monotonic upserts limit exposure to aging/timeout timing). Revisit in EQLX-3 with a fault-injection test.
- [ ] **GAP-5 (open, deferred to EQLX-6):** `/healthz` vs `/readyz` semantics (what fails readiness: DB? Redis? both?) — Java only has actuator health/info. Decide during operability work; record in `docs/runbook.md`.
- [x] **DECISION-1 (EQLX-1, closed): CMS decay.** Replicate code behavior (no decay), not design prose. Rationale: code is authoritative per the extraction rules; adding decay would change fairness dynamics and break differential parity. Decay, if ever wanted, is a new feature with its own spec section — not parity work. Implemented in `pkg/cms` (no decay paths).
- [x] **DECISION-2 (EQLX-1, closed): `retry_count`.** Keep the column and the API field (`Task.RetryCount`, `retryCount` in `TaskStatusResponse`), but invent no retry semantics: the scheduler never increments it (Java parity — `CreateTaskUseCase` sets 0, nothing else writes). Retry policy stays with the remote executor; resubmission is a new task.
- [x] **CORRECTION-1 (EQLX-1 review, fixed): aging candidate selection omitted quota.** The initial Go `RankByAging` ranked without `maxPerClient` filtering — a parity bug, not a judgment call: both Java candidate queries (`findAndLockDispatchable`, `findAndLockOldestDispatchable`) take `maxPerClient`, so keys at quota never enter the aging pool. Fixed in `a3287e7` with `TestRankByAgingRespectsQuota`. Recorded here (not as DECISION-N) to keep "we chose" distinct from "we got wrong and fixed".
- [ ] **GAP-6 (open, deferred to EQLX-2):** Go migration tooling (goose vs golang-migrate) and whether V2/shedlock artifacts appear in any form — deferred because the EQLX-1 module skeleton needs no migrations yet; decide with the first Postgres adapter.

---

## 14. Parity checklist

Before Phase 5 differential testing, every item must have a Go implementation and a conformance test:

- [ ] §2 virtual time (reserve/advance, V seeding, monotonicity)
- [ ] §3 priority (formula, sequential boost/penalty, determinism)
- [ ] §4 CMS (params, add/estimate, no-decay, buffering, warm-up, Redis Lua, drift)
- [ ] §5 dispatcher (SQL, tx boundaries, duplicates, timeouts, no scheduler retry)
- [ ] §6 fairness features (hierarchical, quotas, aging, sequential)
- [ ] §7 adaptive RPS (EMA, brake, dampener, state machine, dispatcher coupling)
- [ ] §8 watchdog (two-phase, drift metrics)
- [ ] §9 cross-instance coordination (advisory locks, SKIP LOCKED partitioning)
- [ ] §11 failure modes (all scenarios)
- [ ] §12 observability (metric semantics)
