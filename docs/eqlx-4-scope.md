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

### 4. Latency field — dispatch-stamped, citing the NOTE

- **Pinned: `durationMs = completionTime − task.UpdatedAt`,** where
  `UpdatedAt` is the last status-write stamp (≈ dispatch time in the
  normal flow). This is the existing `Task.UpdatedAt` NOTE's quantity:
  Java computes the same difference at completion (`Instant.now(clock)`
  minus the entity's `updatedAt`, read before the completion save bumps
  it). **Not** `CreatedAt` (queueing time would poison the signal with
  backlog waits — the NOTE's whole point).
- The mixed-clock NOTE applies verbatim: Go stamps/sweeps in DB time on
  both halves where Java mixes JVM/DB. Same observable under sync,
  self-consistent under skew. No new handling; cited, not re-solved.

### 5. Failure surface — degrade to fixed, never to zero

- **Disabled controller:** `recordCompletion` clears the window + resets
  stability and returns; `getCurrentRps` stays frozen (initial value if
  never enabled). Go parity: disabled → fixed `penalty_factor` config
  (already the pre-EQLX-4 behavior — the flag path already exists).
- **Input errors (CMS estimate fails, metrics emit fails):** the tick
  logs and holds last RPS — controller inputs are advisory, never fatal
  to the dispatch loop. No error propagates out of `recordCompletion`.
- **Degenerate configs** are rejected at construction (§1 validation),
  so runtime needs no `NaN`/divide-by-zero guards beyond the NaN seed,
  which is the defined initial state, not an error.
- **What the controller never does:** return 0, block, allocate per
  completion beyond the bounded window (ArrayDeque capped at
  `window-size`), orConsult any clock except evaluation gating
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
