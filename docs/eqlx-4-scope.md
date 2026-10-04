# EQLX-4 Scope — adaptive RPS controller

Live throttle replacing the fixed `penalty_factor`: sliding-window latency
EMA + error-rate emergency brake + dead-band dampener, driving dispatcher
budget and priority pressure. Scope doc first; implementation after
green-light. All formulas cited from Java
`domain/service/AdaptiveRpsController.java` (EQX-6 revision) — nothing
below is inferred. Defaults from `application.yml` (spec §10).

---

## Blocking

### 1. EMA formula, α, and cold-start behavior

- **Signal:** per evaluation, `mean` = average latency of completions *since
  the previous evaluation* (`freshLatencySum / freshSamples`; with
  `adjustment-interval-ms: 0`, the whole-window mean — pre-EQX-6 signal).
  Smoothed: `smoothed = α·mean + (1−α)·smoothed`, `α = latency-ema-alpha`
  (default 0.7; `α = 1` disables smoothing).
- **Seed (pinned): `smoothedLatency` starts NaN; the first evaluation
  assigns it directly** (`Double.isNaN → smoothed = windowLatency`). The
  EMA carries no zero-drag: the first observed mean becomes the baseline
  unbiased. Cold-start behavior is therefore set entirely by the RPS seed,
  not the latency seed.
- **Cold start (external review's risk, answered):** `currentRps` seeds at
  `initial-rps` (default 1) and ramps ×`increase-factor` (1.05) per
  adjustment interval while latency stays in-band — ~2 minutes from 1 to
  25 RPS at defaults. The controller **never reports 0**: every decrease
  floors at `min-rps` (default 1, validated > 0). A 30–60s LLM/GPU warm-up
  reads as sustained high latency → repeated ×0.9 decreases pinned at the
  floor, then normal ramp on recovery. Slow, never stuck, never zero.
  Pouring concrete: no cold-start special case; the floor *is* the
  cold-start handling, and the scope records that explicitly so nobody
  adds one later.
- **Validation parity:** mirror Java's constructor checks
  (`initial/min/max-rps > 0`, `max ≥ initial`, `0 < latency-threshold < 1`,
  `0 ≤ error-threshold ≤ 1`, `α ∈ (0,1]`, `interval ≥ 0`,
  `confirmations ≥ 1`, `window/min-samples > 0`, `target > 0`). Reject at
  construction, same messages where cheap.

### 2. DECISION-5 brake timing — quantified, not hand-waved

- Java evaluates **only on `recordCompletion`** (webhook arrivals), gated
  by `min-samples` (10) and one adjustment per `adjustment-interval-ms`
  (2000). Go's `FailedSends` fires **at send time**.
- **Read path (pinned): narrow interface, not Metrics, not a new port.**
  The controller takes `Failures interface{ FailedSends() uint64 }` and
  the dispatcher hands it the `*SendPool` (which already exposes exactly
  that method). Rationale: mirroring the counter into Metrics would route
  a control-plane input through an observability port, and a new port
  method would widen the hexagonal boundary for one consumer. The narrow
  interface keeps the dependency visible (`ControllerDeps.Failures`) and
  fakes trivial (atomic counter stub). Recorded here so the implementation
  doesn't silently pick an alternative EQLX-5 would have to rediscover.
- **Visibility analysis (pinned):** differences below one adjustment
  interval are invisible — both sides quantize to 2s evaluation steps, so
  a send-failure seen 200ms "early" lands in the same evaluation bucket
  as the completion that would have reported it. The timing gap becomes
  visible at **timeout-scale latencies**: an executor hang surfaces in Go
  at send-failure time but in Java only when the timeout sweep converts
  it to a completion minutes later. At 200ms-scale executor errors the two
  are indistinguishable; at multi-second hangs Go reacts up to one
  `task_timeout` sooner.
- **Tuning consequence:** α and the decrease threshold tune against the
  *bucketed* signal, so defaults transfer unchanged (α 0.7,
  threshold 0.2). The brake's extra earliness lives entirely in the
  emergency path (error-rate, never dampened), which is threshold-gated,
  not EMA-gated — so no α retune is needed for DECISION-5. Recorded here
  so EQLX-4 implementation doesn't "correct" α for a difference that
  quantizes away.

### 3. Dead-band width — cited, and coupled to the brake

- **Width (pinned): `latency-threshold` (default 0.2)** — no action while
  `smoothed ∈ target×(1±0.2)`; evaluations inside the band *reset* the
  reversal counter. Increase additionally requires
  `errorRate < increase-error-threshold` (0.01).
- **Dampener (pinned): `direction-change-confirmations` (default 3)** —
  reversing direction needs 3 consecutive agreeing evaluations; the
  emergency brake bypasses it but stays interval-limited (one step per
  2000ms). First evaluation after `NONE` applies immediately.
- **Coupling (pinned):** the dampener exists to absorb exactly the class
  of extra sensitivity DECISION-5 adds — a faster-firing brake without a
  dead-band flaps. The two numbers ship as a pair; tuning one without the
  other is a scope violation, noted here so a future "reduce flapping"
  change doesn't halve the threshold while leaving confirmations at 3.

### 4. Latency field and contract — documented mixed (decided)

- **Pinned: `durationMs = completionTime − task.UpdatedAt`,** where
  `UpdatedAt` is the last status-write stamp (≈ dispatch time in the
  normal flow). This is the existing `Task.UpdatedAt` NOTE's quantity:
  Java computes the same difference at completion (`Instant.now(clock)`
  minus the entity's `updatedAt`, read before the completion save bumps
  it). **Not** `CreatedAt` (queueing time would poison the signal with
  backlog waits — the NOTE's whole point).
- **Contract (decided, not deferred): documented mixed-clock computation.**
  `updatedAt` is DB-clock but the completion instant is process-clock on
  both sides — a deliberate parity choice over SQL-computed latency (JPA
  has no clean `RETURNING`; changing the oracle's duration flow risks
  differential drift in the exact quantity this controller tunes). Error
  bound: app-host-vs-DB-host skew δ. At the 200ms default target, δ=10ms
  is 5% — acceptable. At a 50ms LLM-serving target it would be 20% — not
  acceptable; such deployments tighten NTP or revisit SQL latency. **App
  hosts need NTP after all** (narrowed claim); the DB host needs it
  absolutely. Cites the clock-unify NOTE; states the bound numerically so
  EQLX-5 knows what "parity" tolerates.

### 5. Failure surface — degrade to fixed, never to zero

- **Disabled controller:** `recordCompletion` clears the window + resets
  stability and returns; `getCurrentRps` stays frozen (initial value if
  never enabled). Go parity: disabled → fixed `penalty_factor` config
  (already the pre-EQLX-4 behavior — the flag path already exists).
- **Degradation targets, per source (pinned):**
  - *Redis down:* N/A until the Redis CMS adapter exists (EQLX-3-out).
    The local sketch cannot fail; when the Redis adapter lands it carries
    the specified `fallback-to-local` behavior, and the controller never
    sees the difference (it reads estimates, not connections).
  - *CMS estimate unavailable (error return):* treat the sample as
    latency-only — record duration, skip pressure input for that
    completion, hold last RPS. Estimates feed priority pressure, not
    throttle state; a missing estimate must not move the throttle.
  - *Metrics emit fails:* log and continue. Telemetry is write-only
    observability; it never feeds back into throttle decisions, so its
    failure cannot change RPS by construction.
  - *Catch-all:* any input error → hold last RPS. The only RPS mutations
    are the evaluated adjustments (§§1–3); nothing else in the controller
    assigns the value.
- **Degenerate configs** are rejected at construction (§1 validation),
  so runtime needs no `NaN`/divide-by-zero guards beyond the NaN seed,
  which is the defined initial state, not an error.
- **What the controller never does:** return 0, block, allocate per
  completion beyond the bounded window (ArrayDeque capped at
  `window-size`), or consult any clock except evaluation gating
  (`clock.millis()` for the interval check only).

---

## Non-blocking / out of scope

- `RPSReader` already exposes `CurrentRPS()`; the controller implements it
  (EQLX-4 wires the fixed stub out in `main`).
- Dispatcher budget coupling (`ceil(currentRps × interval)`) and
  `penaltyFactor = 1000/currentRps` already exist as call sites; EQLX-4
  swaps the source from config to controller. No call-site changes.
- Per-tenant latency attribution: explicitly out (Java has one global
  controller; per-layer pressure stays in CMS/hierarchy as today).
- Metrics `SetRPS` already exists; brake/decrease/increase events log at
  info/debug exactly as Java (`Emergency RPS brake`, `RPS decreased…`,
  `RPS increased`, dampened at debug).
- gRPC, Kafka, migrate-on-startup: unchanged, later phases.

---

## Metrics emitted (separate numbers, not one scheduler number)

EQLX-5 benchmarks the service as composed signals, never as one opaque
number. EQLX-4 emits, with fairness evidence attached at measurement time:

- `dispatch_decision_latency` — hot-path selection cost (priority compute
  + select, no I/O).
- `timeout_detection_latency` — dispatch-to-TIMEOUT delta (sweep
  responsiveness).
- `watchdog_reconciliation_duration` — tick duration (the GROUP BY gate
  evidence: 20k rows → ~190ms measured in 3b).
- `cms_warmup_duration` — startup rebuild time.
- `rps_target`, `rps_current` — controller setpoint vs actual.
- `brake_active`, `deadband_active` — 0/1 state gauges so throttle
  behavior is visible without inferring it from RPS steps.

---

## Testing strategy (time-series controller, FakeClock-driven)

Unit tests inject latency/error samples against a `FakeClock` and assert
throttle *state transitions* (not wall-clock behavior). Fault-injection
points, mirroring 3b's pattern:

- Inject sustained high latency → RPS steps down ×0.9 per interval;
  assert monotonic decrease to the new equilibrium.
- Inject error rate above threshold → emergency brake ×0.5 per interval
  to floor; assert no confirmation delay (brake bypasses the dampener).
- Restore healthy latency → RPS ramps ×1.05 back; assert recovery.
- Oscillate latency ±30% around target → RPS stays within one step
  (dead-band absorbs; reversal counter resets in-band).
- Cold start: fresh controller + 2×-target latency from sample zero →
  pinned at floor, never zero; ramp on restore.
- Disabled controller: samples recorded nowhere, RPS frozen, no panic on
  empty window (NaN seed is a defined state).

---

## 4a run evidence (controller characterization, not fairness)

Fairness shares stay 3a's evidence. EQLX-4 proves the throttle, scripted:

1. **Step-latency injection:** healthy executor → +200ms step → RPS steps
   down ×0.9 per interval to the new equilibrium; remove step → ramps
   ×1.05 back. Assert monotonic decrease then monotonic recovery, no
   oscillation beyond one dampened reversal.
2. **Error spike:** 10% failures for 30s → emergency brake ×0.5 per
   interval to floor; assert floor hit and no dampening delay on the
   brake (brake bypasses confirmations by design).
3. **Flap resistance:** alternating ±30% latency around target across 10
   intervals → RPS stays within one step of start (dead-band absorbs).
4. **Cold start:** fresh controller, slow executor (2× target) from tick
   zero → RPS pinned at floor, no zero, recovery ramp on latency restore.
