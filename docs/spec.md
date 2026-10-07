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

#### Charge (vocabulary for the hierarchy path — new in EQLX-9, absent from the flat model)

Charge is the per-node accumulator tracking pending virtual-time
advance attributable to descendants not yet applied to the node's
own V. At dispatch, every node on the dispatched task's path
(except root) advances `τ += q/w`
(`HierarchicalSelector.charge`, selector lines 162–174:
`node.virtualTime += quantum / node.weight` for `node != root`);
at persist, the node's V is set to `GREATEST(current_V, floor) +
delta` (`HierarchyNodeJpaRepository.chargeVirtualTime`, adapter
lines 14–19). Consequence: sibling subtrees never see stale V from
each other's in-flight charges — each level reads max-persisted
state, never a partially-applied sibling write. Related but not
identical to flat-path in-flight pressure (`p·F̂/w`, a scheduling
input computed from CMS at selection time): pressure biases WHO is
picked next, charge records WHAT service was received for future
picks. Conflating them misreads both halves of the algorithm.

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

- [x] **ORACLE DETERMINATION (recorded, top-level scoping fact): the behavioral reference is Java `main`, which contains the virtual-time scheduler.** Verified 2026-10 against `main` @ `fc0d668`: `PriorityCalculatorService` computes `P = round(F) + inflight×penalty/w` from `VirtualTimeService.assignFinishTag` (persistent `client_virtual_time`, system `V`), `docs/design.md` §5.5 describes the same model, migrations V1–V6 are present, and the Sep-25 release jar's `PriorityCalculatorService.class` references `assignFinishTag` (7 call sites via `javap`). The EQLX-0 extraction read this content while it lived on `EQX-7-hierarchical-virtual-time`; it has since merged to `main`, so extraction source and production oracle are the same code — no re-extraction, no dual-oracle handling. The smoke comparison (Go vs that jar) therefore means what it looked like: virtual-time Go against virtual-time Java. Rationale for `main` over any branch: it is the production system, the release artifact builds from it, and differential parity is only meaningful against what actually serves traffic. If a future feature branch changes scheduling semantics, this entry is amended with the new branch + SHA before any comparison runs against it — never silently.
- [ ] **GAP-1 (closed in this spec):** sequential execution state machine — extracted from `SequentialDispatcherService`, `SequentialCompletionHandlerService`, `ClientBlockRecoveryService`, `ResultPassthroughRecoveryService` (§6.4). No longer a gap.
- [ ] **GAP-2 (closed in this spec):** Redis Lua script — reproduced verbatim (§4.6) from `RedisCMSAdapter`.
- [ ] **GAP-3 (closed in this spec):** watchdog two-phase rebuild + drift publication — extracted from `WatchdogService`/`CmsDriftReport` (§8).
- [ ] **GAP-4 (open, non-blocking for Phase 1):** clock-skew handling has no explicit Java mechanism; §11 records the analysis (DB-time SQL + monotonic upserts limit exposure to aging/timeout timing). Revisit in EQLX-3 with a fault-injection test.
- [x] **GAP-5 (closed in EQLX-6):** `/healthz` is liveness-only (200 while the mux serves, 200 through drain — k8s must not restart a draining pod); `/readyz` is readiness (DB ping, advisory-lock probe, migrations-applied check; 503 on any failure and from SIGTERM until exit). Docker HEALTHCHECK targets `/readyz` as a readiness proxy; k8s uses livenessProbe `/healthz` + readinessProbe `/readyz`. Runbook carries operator guidance.
- [x] **DECISION-1 (EQLX-1, closed): CMS decay.** Replicate code behavior (no decay), not design prose. Rationale: code is authoritative per the extraction rules; adding decay would change fairness dynamics and break differential parity. Decay, if ever wanted, is a new feature with its own spec section — not parity work. Implemented in `pkg/cms` (no decay paths).
- [x] **DECISION-2 (EQLX-1, closed): `retry_count`.** Keep the column and the API field (`Task.RetryCount`, `retryCount` in `TaskStatusResponse`), but invent no retry semantics: the scheduler never increments it (Java parity — `CreateTaskUseCase` sets 0, nothing else writes). Retry policy stays with the remote executor; resubmission is a new task.
- [x] **CORRECTION-1 (EQLX-1 review, fixed): aging candidate selection omitted quota.** The initial Go `RankByAging` ranked without `maxPerClient` filtering — a parity bug, not a judgment call: both Java candidate queries (`findAndLockDispatchable`, `findAndLockOldestDispatchable`) take `maxPerClient`, so keys at quota never enter the aging pool. Fixed in `a3287e7` with `TestRankByAgingRespectsQuota`. Recorded here (not as DECISION-N) to keep "we chose" distinct from "we got wrong and fixed".
- [x] **DECISION-3 (EQLX-2-pgx, closed): pool-vs-tx without an interface pair.** Neither alternative from the port review: one concrete store per aggregate, constructed with a narrow `querier` interface (`Query`/`QueryRow`/`Exec`) satisfied by both `*pgxpool.Pool` and `pgx.Tx`. Pool-bound instances are the only ones handed out; `Transact` builds tx-bound instances internally and never returns them, so misuse is impossible by construction (no second type to misuse, no runtime guard). Rationale: Java's `@Transactional` proxying has no Go equivalent, but hiding the tx inside `Transact` recovers the same property — callers cannot name a wrong connection because only one type exists.
- [x] **DECISION-4 (EQLX-2-pgx, closed): migration tooling is goose.** SQL migrations live in `/migrations`, replaying Java V1+V3+V4+V5 in order; V2 (`shedlock`) is dropped (advisory locks replace it, spec §9). Goose chosen for: plain SQL files (reviewable against `docs/schema.sql`), single-binary embedding, no ORM coupling. Migrate-on-startup wiring belongs to the service phase (EQLX-6); adapter tests apply the SQL directly.
- [x] **NOTE (EQLX-2-pgx, recorded): `domain.Task.Version` is the one domain change an adapter PR made.** The ports PR framed version as "the adapter's responsibility," but optimistic locking needs a counter on the struct itself — so `Version int64` lives on `domain.Task` (inert in the domain core; incremented by `TaskStore.Save`, surfaced as `port.ErrVersionConflict` on a lost race). Covered by `TestSaveVersionConflict` (integration, real Postgres) rather than a domain unit test, because the increment/conflict path is adapter behavior. Future domain changes from adapter PRs need the same treatment: field + rationale here + the test that pins it.
- [x] **NOTE (EQLX-2-chi, recorded): `domain.Task.UpdatedAt` + `port.TaskRepository.ListByKey`.** Two more adapter-driven additions under the same rule. `ListByKey` exists because the contract's `GET /tasks?fairnessKey=&status=` had no port path; the pgx implementation is a plain ordered select (no locking). `UpdatedAt` lives on the struct because completion latency feeds the RPS EMA, and the writer/clock-start semantics are:
  - **Writer:** the database, via the `set_updated_at` trigger (migration 00005 / Java V6) — not the adapter, not the domain. `Save` reads the DB-assigned value back via `RETURNING`, so the struct stays truthful without sending a timestamp.
  - **Clock start:** the last status transition, which is the dispatch save in the normal flow (`QUEUED → DISPATCHED` bumps it; a 5-minute queue followed by 100ms execution reports 100ms, not 5m100s). Java parity: the controller reads `now − updatedAt` at completion, and JPA bumps `updatedAt` on every entity save — same point (pre-unify wording; see single-clock NOTE below).
  - Caveat (accepted): `UpdatedAt` moves at the status *write*, so executor-side time between `DISPATCHED`-commit and HTTP arrival is excluded, and any non-status `Save` restarts the clock. Both match Java; differential tests compare the same quantity on both sides.
  - EQLX-4 cross-reference (§7.1 latency EMA): the RPS controller reads `now − UpdatedAt` as *executor* latency, so any non-status `Save` between dispatch and completion (retry bookkeeping, error annotation) reports latency *lower* than actual executor time. During error-heavy periods — exactly when the error brake matters most — real latency can hide behind restarted clocks. Not a bug (parity), but the controller scope must decide whether to read a dispatch-stamped field instead of the generic `UpdatedAt`.
  Pins: `TestSaveFindRoundTrip` (updated-at maintained), `TestListByKeyWithStatusFilter` (integration).
- [x] **NOTE (clock-unify, recorded): single clock — all `updated_at` writes are DB-assigned.** `set_updated_at()` trigger (Go 00005 / Java V6) stamps every `updated_at` on all six tables; app code on both sides sends no timestamp and reads it back (`RETURNING` / entity refresh). The old mixed clock (process-stamped writes vs SQL-`now()` reads) is gone — no skew class, no NTP requirement on app hosts (DB host only). Explicitly out: `created_at` stays app-stamped on insert (DEFAULT `now()` when omitted; ordering/aging thresholds only, skew exposure bounded by clock offset on promotion timing, not correctness), `completed_at`/`blocked_at` stay semantic event stamps (display/state-machine use, self-consistent within each repo). Latency stays app-side `now − updatedAt` on both sides (Java `CompletionHandlerService` shape preserved) — a deliberate parity choice over SQL-computed latency (JPA has no clean `RETURNING`; changing the oracle's duration flow risks differential drift in the exact quantity EQLX-4 tunes). Consequence, stated plainly: `updatedAt` is DB-clock but the completion instant is process-clock, so latency error is bounded by app-host-vs-DB-host skew δ. At the 200ms default target, δ=10ms is 5% — acceptable. At a 50ms LLM-serving target it would be 20% — not acceptable; such deployments must either tighten NTP or revisit SQL-computed latency. App hosts need NTP after all (narrowed from the earlier claim); the DB host needs it absolutely.
- [x] **NOTE (EQLX-2-chi, recorded): inert `retryCount` is an expected EQLX-5 divergence.** DECISION-2 keeps the field present-but-inert on the Go side (always 0; the scheduler never mutates it). If the Java oracle ever mutates `retry_count`, differential tests comparing `TaskStatusResponse.retryCount` will diverge by design — encode it as "expected divergence," not a mismatch failure.
- [x] **NOTE (EQLX-2-chi-notes, recorded): completion has two non-atomic windows, different sizes.** The webhook handler runs `Save` → `Decrement` → `CMS.Add` → metrics as sequential calls: (1) `Save` → `Decrement` is DB-only and closable today via `Transact` (DECISION-3 already supports it) — the chi handler predates the jobs layer that will own this path; EQLX-3 closes it on takeover, and until then a crash here leaves an over-reported slot for the watchdog. (2) DB commit → CMS flush is cross-store and fundamentally unclosable — a sketch cannot join a pgx transaction; this and only this is the §8 drift window. Earlier drafts conflated the two; they are not the same problem.
- [x] **NOTE (EQLX-2-chi-notes, recorded): two 4xx codes for client errors, by Java parity.** Schema violations (blank key, bad weight, null/undecodable payload, missing sequence number) → `VALIDATION_FAILED`; use-case rule violations (payload over cap, non-in-flight completion) → `BAD_REQUEST` (Java `IllegalArgumentException` path). Same class of "client error," different origin — the envelope code tells the caller whether the *shape* or the *state* was wrong.
- [x] **NOTE (EQLX-7, recorded): harness state reproducibility.** Any state the harness depends on (databases, containers, binaries, result directories) must be producible from committed artifacts (`test/differential/docker-compose.yml` + `pg-init/`, `Makefile`, the binary built from the tree, the embedded migration set). Ad-hoc local state has twice destroyed evidence or forced mid-run reconstruction (wiped `/tmp` result history; a hand-made database container that vanished with the evidence of which schema revision it carried). The differential compose file mounts `pg-init/` for roles + empty DBs; Java Flyway owns its schema on boot; the harness `ensureSchema` (embedded goose, Go sides only) heals fresh databases before `ResetDB`. New harness dependencies are not "done" until they are declarative — same shape as the collision NOTE: one paragraph now, three debugging sessions saved later.
- [x] **NOTE (EQLX-7, recorded): Write-Audit rule for adapter writes.** For any Save/Update/Insert, the written column list must match the caller's intent — verified by reading, not assumed. The rule exists because full-row writes are the fifth instance of the silent-default class (a path touching more than it should, producing a plausible value that hides a real mechanism): `TaskStore.Save` is a full-row UPDATE including `priority`, which is safe ONLY because (a) `FindByID` loads the full column list including priority with `HasPriority = priority.Valid`, and (b) no completion path mutates priority between load and save (verified: `markCommitted` + webhook touch status/completed only). The audit that closes the question for the whole project today: pgx `Save` is the only full-row writer in the tree, and its round-trip is faithful. Any new writer (or any caller that starts mutating priority) re-opens this NOTE.
- [x] **NOTE (EQLX-8, recorded): port grew `AddBatch` + `SetCMSDegraded`.** Third port extension under the EQLX-6 rule (change with recorded rationale, not scope creep): `AddBatch` because the buffering decorator flushes N keys and per-key remote round trips multiply RTT by batch size (local sketch loops at zero cost; callers must not assume cross-key atomicity — pipelined, fire-and-forget per key); `SetCMSDegraded` because the fail-open policy needs an observable transition (edge-triggered by the caller, hot path never pays a set per operation). Fakes updated; wire names for the degraded gauge live in the metrics adapter's constant block like all others. `PromoteStarved` (calculator + dispatcher backstop) is the sole writer of priority 0. `CalculatePriority` yields ≥1 for real workloads (`round(F)` + non-negative pressure; fresh-task finish tags sit ~143, not 0). Any other code path setting priority to 0 breaks the promoted-count metric (`WHERE priority <= 0`) across all traces — grep this string before refactoring the loader, the tagger, or the NULL/zero default. A `has_priority BOOLEAN NOT NULL DEFAULT false` column would make the invariant structural instead of conventional; deferred as schema churn the working metric does not justify.
- [x] **Write-Audit ledger (EQLX-7, recorded — mechanical pass over every adapter write, all clean):** `TaskStore.Save` full-row UPDATE — callers (calculator tag, both backstops, dispatch move, timeout expire, markCommitted, webhook, ingest) all load-then-save full rows; round-trip faithful. `TaskStore.insert` — explicit list, `updated_at` DB-owned by design. `VirtualTimeStore` upserts — GREATEST-guarded monotonic advance, intent matches. `CountsStore.add/Set` — single-column intent; `Set` clamps negatives with an in-code note. `SequenceStore.Save` — struct-is-the-row, 1:1 mapping. No partial-struct Saves, no unwritten-intent columns, no second full-row writer. Next audit due when a writer is added, not on a schedule.
- [x] **NOTE (EQLX-5, recorded): startup tag interleaving propagates into virtual time.** Tasks tagged before the first full priority-calculator tick can receive tags influenced by transient V: e.g. Java tagged its 2nd c-task (finish 285.71) while one task was already in flight, yielding priority 428 = 286 + 1×1000/7, where Go tagged the same-shaped task at empty in-flight for 286. Both follow the formula exactly; the inputs (in-flight at tag time, V trajectory) differ by tick/ingest interleave. It affects any comparison that doesn't quiesce, is present in both implementations (parity), and washes out at aggregate scale but not at task-level ordering in the startup window. Comparators must quiesce or filter the startup window. Do not treat startup-window ordering divergence as a parity failure.
- [x] **NOTE (EQLX-3a, recorded): `SendPool.FailedSends` is reserved, not dead.** An atomic counter written by the dispatcher send pool and read by nothing — yet. With async post-commit sends, executor failures surface only via missing webhooks; the EQLX-4 error brake would be blind to the failure mode it exists to catch without an independent failure signal. This counter is that signal's future input. Semantics fixed by DECISION-5 below. Same pattern as `Task.Version`: field + rationale; do not remove as dead code, do not duplicate in EQLX-4.
- [x] **DECISION-5 (EQLX-3a, closed; EQLX-4 finding appended): `FailedSends` counts non-2xx from the executor — superset of Java's completion-window `error_rate`.** Rationale: earlier signal on the same failure mode; Java sees declined sends only after the timeout sweep turns them into timeout completions. Everything Java counts, Go also counts; Go additionally counts the window between send and timeout. Parity caveat: fed identical workloads, Go's brake reacts sooner. EQLX-4 must tune α and threshold against this timing difference, not against Java's completion-window rate. Byte-parity (defer the count to the timeout path) was rejected: it discards the timing signal that makes the brake useful. Referenced from `SendPool.FailedSends` in code.
  - **Live evidence (EQLX-5 smoke, 2026-10): Java's cold-start RPS floor at 1 is visible in dispatch tags — penalty holds at 1000 until the controller's first non-seed latency sample. Confirms the scoped floor behavior under real ingest interleave.**
  - **EQLX-4 operational finding (appended): one-shot failure injection dampens away; sustained failures accumulate.** A single transient error (one failed send among healthy completions) is absorbed by the reversal dampener and never moves the throttle; a persistently sick executor drives the brake to the floor. Verified in `TestSendFailuresEnterErrorRate` (accumulating counter) vs the debug run that preceded it (one-shot delta, correctly absorbed). Operational property for EQLX-5 parity comparison: single transient error does not move either throttle; sustained error rate does. Test both sides of that boundary, not just the brake path.
- [x] **NOTE (EQLX-3b, recorded): clock sources — Go uses DB time on both halves, Java mixes.** The pg adapter stamps `updated_at` with SQL `now()` on every `Save`, and the sweep compares it against SQL `now()` — one clock, no skew class for the threshold itself. Java mixes JVM-clock writes (`Instant.now(clock)` / `@UpdateTimestamp`) with SQL-`now()` reads, so a skewed JVM shifts its threshold. Observable behavior matches under synchronized clocks; under skew Go stays self-consistent while Java shifts early/late proportional to skew. `CreatedAt`/`CompletedAt` remain process-clock stamps (ordering/display only — never threshold inputs). Recorded so EQLX-5 reads any skew-dependent divergence as known, not anomalous.
- [x] **CORRECTION-2 (EQLX-3b, fixed): there is no recovery service.** `docs/eqlx-3-jobs-scope.md` §7 specified a startup recovery job (old-only threshold, lock-guarded, `timeout+60s`, reject-the-combo validation) reasoned from first principles — a crashed process's tasks need cleanup. Java has no such service: `CmsWarmUpListener` rebuilds the sketch at startup (read-only, no drift publish) and `TaskTimeoutService` (ticking at `dispatcher-interval`, 50ms) owns stuck tasks from its first tick via the ordinary `TIMEOUT` path. The correction matters more than CORRECTION-1 because it removes reviewed-and-merged scope, not code: §7 was deleted and `RecoveryEnabled`/`TimeoutSweepInterval` removed from `jobs.Config` in the same commit. The pipeline caught it before code, which is what the scope process is for. Cite: `TaskTimeoutScheduler` cadence (`dispatcher-interval`), absence of any `*RecoveryService` outside sequential-mode block recovery.
- [x] **NOTE (EQLX-5, recorded): cold-start promotion asymmetry — deterministic per implementation, ramp-speed-driven, fairness-core-exact.** Promotion counts (rows with `priority <= 0`) do not overlap across implementations on w2000: Go 780–782 across five runs, Java 987–1014 across four. Same mechanism both sides (starvation backstop past `max-queued-time-ms` 60s); different sensitivity because Java's RPS ramps slower (tops ~20.6 vs Go ~23–25), so more of Java's backlog ages past the deadline. Not a fairness-core difference: run totals are exactly 200/400/1400 on both sides every run — only within-window distribution is affected, and whether it trips the ±2 window bound in a given run is a second-order timing effect. Differential acceptance therefore uses matched workload classes (cold burst vs warmed: ~500 throwaway tasks, RPS gate >15, then the measured 2000) with per-class rate comparison — never JvG-warm against GvG-cold. The cold-class rate is reported characterization (2/5 JvG fails vs 0/5 GvG fails is the predicted Java-ramp signature, not a gate failure); the warm-class rate is the parity gate (JvG fails ≤ GvG fails + 1 at N=10; same slack at N=5 with the stated resolution caveat). Every comparison declares its workload class in `results.json:methodology`. The runtime asymmetry is measurable directly: spawn → readiness → first-dispatch timings are recorded per side in `results.json:startup` (harness T0/T1/T2; service stdout milestones give the internal phase breakdown). Java's context build + Flyway + client init dominate its phase breakdown; Go's spawn-to-ready is effectively binary startup. Runtime characteristics, not parity-constrained. Cold-vs-warm JVM cache must be declared alongside any Java cell (page-cache-warm starts run 2–3x faster; CI runners are warm).
- [x] **NOTE (EQLX-5, recorded): completion-driven RPS evaluation — a drained backlog freezes the controller.** The adaptive controller evaluates RPS on task completions (5%/2s per evaluation while completions flow). A drained backlog therefore freezes the controller at its last computed value: it is reporting, not stalling. Diagnostic signature: RPS pinned at some low value (observed Java 2.2 and 4.3) with an empty backlog — distinguish from "controller dead" (0 or sentinel). Consequence for test design: a controller gate that waits for post-drain stabilization never trips; warm-up workloads must gate during submission (trickle-until-floor) or keep the backlog non-empty during measurement. Applies to any future controller with the same evaluation trigger (Micronaut column included).
- [x] **NOTE (EQLX-6, recorded): Prometheus metric naming.** Wire names use the `equalix_<domain>_<unit>_seconds` convention for time-valued metrics and `equalix_<domain>_total` for counters. EQLX-4's scope used bare logical names (`dispatch_decision_latency`, …); those were logical descriptions, not wire names. The adapter (`internal/adapter/metrics`) is the sole source of wire-name strings in code — constants referenced by registration, observation, and tests; no `equalix_*` literals in code outside it, not even in port comments (README mirrors the table for operators, marked as mirror). Canonical table: `equalix_tasks_dispatched_total{tenant}`, `equalix_tasks_completed_total{tenant,result}`, `equalix_dispatch_decision_latency_seconds`, `equalix_timeout_detection_latency_seconds`, `equalix_watchdog_reconciliation_duration_seconds`, `equalix_cms_warmup_duration_seconds`, `equalix_rps_current`, `equalix_received_queue_depth`, `equalix_cms_drift_estimate{tenant}`, `equalix_metrics_cardinality_exceeded_total{metric}`. Deferred for lack of a port source: `rps_target`, brake/deadband state gauges. Future additions follow the convention; renames break scrapers and require a NOTE here.
- [x] **NOTE (EQLX-8, recorded): Go 1.22 dependency pins (consolidated).** Three dependencies pinned below current major for Go 1.22 compatibility: pgx v5.7.0, prometheus/client_golang v1.19.1 (v1.24.x needs Go ≥ 1.25), redis/go-redis v9.5.5 (v9.6+ needs Go ≥ 1.25). One entry, not three — a future maintainer greps here, not three places. Upgrading any of them requires bumping the Go directive, the CI setup-go pin, and retesting every adapter built against the newer module: consolidate to one upgrade pass when the toolchain moves, never piecemeal (piecemeal upgrades are how the 1.25-directive churn happened twice).
- [x] **NOTE (EQLX-6, recorded): timeout-sweep race — Go skips, Java aborts.** A task that completes between the timed-out scan and the sweep write is a lost race the mover won. Go re-reads in-tx and skips (`expireOne` returns done=false): the expired count, CMS release, and timeout-latency observation cover only actually-expired tasks. Java has the same latent shape and handles it worse: `TaskTimeoutService.expire` checks only the scanned entity, saves unconditionally, and logs the scan size — on a race the JPA `@Version` check throws `OptimisticLockException`, aborting the whole sweep batch (single `@Transactional`), which the next tick retries in full. Observable difference, race-window only: log counts (scan size vs actual) and one-tick batch retry on the Java side; no steady-state divergence (the next tick converges both). Caller audit (EQLX-6): `expireOne`'s only caller is the sweep loop (same file; no test callers) — the `(bool, error)` change breaks no other package boundary.
- [x] **NOTE (EQLX-6, recorded): YAML config loader added in EQLX-6-docker.** Prior scopes referenced `/etc/equalix/config.yaml` and a YAML pattern that didn't exist in code — no loader, no flag, no dep. The Docker PR added the minimal surface matching the deployment shape (`--config`, default path, eight knobs, pointer-typed YAML so absent ≠ zero). Precedence pinned and tested: explicit flags > env > file > built-ins; missing file fatal only when explicitly requested; file ints strictly positive (loud), env keeps historical degrade-to-default. Otherwise the next reader of the EQLX-3 scope (which mentioned config) assumes the loader predates this PR.
- [x] **NOTE (EQLX-6, recorded): timeout-detection rate divergence under concurrent load — expected, not novel.** Companion to the sweep-race NOTE above: Go's sweep is skip-and-continue (whole batch processes every tick, expired count is the true count); Java aborts the whole batch on version conflict (that batch waits a tick, expired count depressed for that tick). Terminal states converge next tick, but per-tick expired counts — and therefore `timeout_detection_latency` window aggregates — diverge under completion-heavy load. Differential comparisons that catch this in a window must classify it as expected divergence, not a parity failure; a mismatch report here eats a round trip if it gets investigated as novel.
- [x] **CORRECTION-4 (EQLX-6, recorded): rps_target / brake_active / deadband_active have no port source.** EQLX-4's scope listed them as emitted controller telemetry, but no port method carries setpoint or brake/dampener state — the controller never exposed them, on either side. Scope over-specified, not implementation drift. The adapter emits what the interface sources (`rps_current` via `SetRPS`); the three gauges stay absent until a controller extension provides a source, at which point wire names plus a NOTE land together. Until then the maturity table's metrics row covers exactly the canonical table in the naming NOTE above.
- [x] **CORRECTION-6 (EQLX-7, recorded): "below floor" mistook a bound for a state.** The idle below-floor runs were read as "controller stayed at floor" with tolerance [1.0, 2.0] fitted to the observed 1.98 max. But code defines the floor as exactly `min_rps = 1.0` on both sides — Java `AdaptiveRpsProperties` (`min-rps: 1`, `application.yml`), Go `adaptive.DefaultConfig` (`MinRPS: 1`, `internal/adaptive/controller.go`) — a minimum constraint, not a state the controller holds: once completions arrive, ramp is expected (a dozen 5% UP steps over the run is correct behavior, not drift). The fitted tolerance was circular. Reframed: the regime claim is separation (below-floor max 1.98 vs above-floor operating peaks 10.9–63, an order of magnitude), with falsification at 20% of the above-floor minimum peak — derived from the comparison being made, not the observation. Distinct from CORRECTION-5 (rule inheritance): this is constraint interpretation, a new taxonomy class.
- [x] **CORRECTION-5 (EQLX-7, recorded): idle-tenant residual runs cold, not warm — rule inherited without derivation.** The scope prescribed warm-class by inheritance from the EQLX-5 parity gate, not by derivation from the clamp mechanism. Warm is correct for the parity gate (that decision stands); warm is inapplicable here because a warmed controller at ceiling RPS produces no backlog, V does not meaningfully advance past stale finishes under uncontended replay, and the clamp is never exercised — proven empirically by byte-identical file-mix replay at RPS 100 on both sides. The failure mode is the class to name: inheriting a rule from a different mechanism's requirements without checking the current mechanism's regime. It applies to future residuals the same way (the CMS curve faced it next: distribution, not rate, is what exercises sketch error). Cold keeps the return inside the ramp (budget < arrival), the regime the clamp needs. Scope text corrected; the warm negative control retained as evidence, not deleted.
- [x] **NOTE (EQLX-5, recorded): startup characterization row — phases, not totals.** The matrix records spawn → ready → first-dispatch per side in `results.json:startup` (harness T0/T1/T2; service stdout milestones supply the internal breakdown). Phase definitions are fixed across runtime columns so Micronaut plugs in as data entry: spawn→main (process/JVM load), main→context (Spring scan+proxy vs Micronaut AOT), context→ready (migrations, scheduler beans, client init), ready→first-dispatch. First cells (page-cache-warm host): Go spawn→ready ~0.5s; Java (Spring Boot) spawn→ready ~5.6s — ~11×, context build dominating per the internal milestones. Java cold-start cell pending (fresh-host number expected 2–3× the warm cell); do not cite the warm cell as the Java number. Informational, never gated.
- [x] **NOTE (EQLX-5, recorded): exact-order parity is flaky-by-construction — gate on shares, log orders.** Go-vs-Go control (identical binaries, same workload, separate DBs): exact dispatch orders agree on some runs and diverge on others, while shares converge exactly every run. Cause: tick/ingest interleaving differs per process, virtual-time histories diverge with it, shares still converge (ergodic fairness). Consequence: per-task positions are recorded evidence, never an acceptance criterion — gating on them would fail identical implementations, Java-vs-Java included. Tie-group diagnostics stay as specified.
- [x] **NOTE (EQLX-5, recorded): harness HTTP calls must fail loudly — silent defaults are a recurring trap class.** Three instances: (1) unknown `spring.task.scheduling.enabled` silently no-op'd Java scheduling; (2) `fetchRPS` decoded a 401 empty body as RPS 0.0, reading as "controller flatlined"; (3) the `-1` no-reading sentinel pooled with live reads in first→last summaries, reading as "RPS pinned at -1" (ctl-jg1: 57/57 Java misses with no recorded cause). Sibling class, same poison: two writers sharing one namespace with silent overwrite — pointer-aliasing in fakes, stale-checkout reads, and the CI results-dir collision (both legs wrote one dir; the control deleted the comparison's artifact every run). Namespace sharing between writers is guilty until proven isolated. Rule: every harness HTTP call errors on non-2xx and decode failure with endpoint + status (no zero/empty defaults); the tracer persists per-side first-fetch-errors and warmup prefixes in `results.json`; trace summaries report the live-read range, never raw endpoints (sequential runs put pre-boot/post-stop -1s at the endpoints by construction).
- [x] **GAP-6 (closed by DECISION-4 in EQLX-2-pgx):** migration tooling is goose; `/migrations` replays V1+V3+V4+V5, V2 dropped.
- [x] **NOTE (EQLX-9, recorded): hierarchy oracle answers — no CORRECTION needed.** The EQLX-9 oracle read verified, with citations: (1) opt-in via `app.queue.fairness-mode` (`flat` shipped default, `application.yml:51`) — EQLX-5 parity preserved, no restatement; (2) SEPARATE dispatch paths (`DispatcherService` branches on `fairnessHierarchy.isEnabled()` — flat parity was validated against the flat path, not hierarchy-reduced-to-one-level); Go mirrors with its own separate path, flat untouched. (3) Single-stage `max(parent_V, child_V, global_V) + quantum/weight` is a natural-but-wrong generalization — coherent, review-passing, and NOT what the code (nested walk-down per `HierarchicalSelector`) or spec §6.1 (already correct, no rewrite needed) describes. The trap demonstrably catches readers: it caught this scope's first draft, which misattributed the hint to §6.1 (corrected in `docs/eqlx-9-scope.md`). (4) Per-level clamp is to the PARENT floor (`applyFloors` uses the parent's stored `childrenVirtualTime`, not global V) — Go composes `max(stale, parentFloor)`, with global convergence emerging from the raising rule. (5) Weights independent (layer default/override, never sum-of-children); depth config-bounded with fold-in; sibling shares per layer by own weights. No CORRECTION entry: the spec was right throughout — the only error was the scope draft's, and it is corrected at the source.
- [x] **NOTE (EQLX-7, recorded): stub executor latency is a load-shaping parameter with a narrow usable band.** 5 ms completions race the dispatch transaction commit and are correctly rejected with 400 (task not yet visible — not-in-flight), polluting throughput with rejections on every implementation; 2000 ms completions read as executor slowness and collapse the adaptive RPS (backlog latencies 10–60 s); 100 ms sits under the RPS deadband (target 200 ms ± 20%) so RPS holds steady and latencies measure dispatch mechanics. Observed on the Micronaut column first, applies to any stub against any implementation: pick the latency for the regime under test, never the smallest round number, and record it with the run.
- [x] **NOTE (EQLX-7, recorded): harness-side failures present as scheduler-side asymmetries — attribute the harness first.** Two confirmed instances: async test assertions landing mid-dispatch (read as per-tick unfairness) and stub-side accept-loop starvation plus client keep-alive reuse artifacts (read as a Micronaut drain-timeout asymmetry; steady-state traces showed in-flight tracking the stuck-send count exactly, and the drops vanished against a well-behaved server). Attribution rule: before attributing any cross-implementation divergence to the scheduler, verify the harness paths — stub threading model, client pool reuse, negotiated HTTP version, timing of sends relative to commits. Any asymmetry visible only at n=2–3 gets request-level tracing before scheduler-level conclusions. Same trap class as the load-shaping NOTE above; this one is about attribution, that one about parameters.

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
