# EQLX-5 Scope — differential validation methodology

EQLX-5 proves the Go implementation behaviorally equivalent to the Java
oracle. This scope is a **methodology contract, not a set of design
decisions**: it defines what is measured, how, and what counts as a match
*before any measurement runs*. No target numbers appear below — a result
that sets its acceptance bar after seeing the data is not evidence.

Status dependency: the differential harness does not exist yet
(`make test-differential` fails honestly; `test/differential` absent).
Its construction is §0, the first blocking item — not a prerequisite someone
else provides.

---

## Blocking

### 0. Build the differential harness (gating everything)

- **Topology:** one harness process driving three endpoints — Java service,
  Go service, stub executor — against **separate databases** (no shared PG:
  shared state would entangle `client_counts`, advisory locks, and the
  tasks table across two schedulers). The stub executor is a shared
  *protocol* both schedulers speak (dispatch receive → webhook complete);
  scheduler-side code never knows it is a fixture.
- **Single harness-owned stub (pinned):** one stub instance serves a run;
  the next run constructs a fresh instance. No per-language stubs (nothing
  to keep identical across repos), no reset endpoint, no run tags —
  identical stub behavior by construction, and the unified dispatch log is
  exactly what the comparator consumes. Reset boundary is instance exit:
  [start stub] → [start scheduler] → [ingest] → [drain] → [teardown,
  capturing the log]. The wire contract lives in `test/differential`
  (`PROTOCOL.md` section of `protocol.go`): both schedulers implement
  against the written contract.
- **Falsification first — and on every run, not once:** the harness's
  first passing test runs a known-bad dispatcher and must report mismatch.
  The fixture: `FirstQueuedDispatcher` — a `port.TaskRepository` decorator
  (or config flag on the test binary) that ignores weights and priority
  entirely and returns the oldest `QUEUED` row per tick. Against a 1:2:7
  workload it yields ~1:1:1 shares; the harness must report deviation
  beyond the §4 bound and exit non-zero. A measuring instrument never
  calibrated against a known-bad case is an assertion, not an instrument.
  **Calibration is pre-flight, not history:** every comparison run opens
  by replaying the calibration fixtures against both sides and confirming
  the expected mismatches, then resets and runs the real workload. A
  comparator regression that silently passes everything (e.g. a filter
  swallowing non-matching tasks) would otherwise go undetected
  indefinitely — the fixtures gate every run, not just the first.
- Until §0 ships, every other section is specification, not procedure.

### 1. Dimension table (13 rows; mode per row, not per run)

| # | Dimension | Mode | Acceptance |
|---|---|---|---|
| 1 | Throughput (dispatches/s at saturation) | fixed-rate | report with config; no parity bound (impls differ honestly) |
| 2 | CPU efficiency (dispatches/core-s) | fixed-rate | report; no bound |
| 3 | Alloc efficiency (bytes/dispatch) | fixed-rate | report; no bound |
| 4 | Memory steady-state (RSS at N in-flight) | fixed-rate | report; no bound |
| 5 | Latency percentiles (ingest→dispatch, dispatch→complete) | fixed-rate | shape parity: same distribution family, p50 within 20% |
| 6 | Startup time (process start → first dispatch) | as-fast-as-possible | report; Go expected faster, no bound |
| 7 | Warmup duration (to steady-state rate) | fixed-rate | report per §3 discipline |
| 8 | Fairness shares (per-tenant dispatch fraction) | as-fast-as-possible | §4 bound |
| 9 | Starvation (max continuous zero-dispatch windows per tenant) | as-fast-as-possible | §5 bound |
| 10 | Recovery (kill-9 → TIMEOUT + slot release, timed) | as-fast-as-possible | both complete; durations reported, no bound |
| 11 | Scalability (shares at 2× tenants) | as-fast-as-possible | §4 bound holds |
| 12 | Stability (24h soak: drift, RSS growth, RPS variance) | fixed-rate | zero unrepaired drift; RSS slope ≈ 0 |
| 13 | Dispatch ordering evidence (per-task position) | as-fast-as-possible | recorded, never gated (control-proven flaky) |

Rows 1–7, 12 are *report-only*: Go and Java differ honestly in runtime
characteristics, and a parity bound there would be either vacuous or
dishonest. Rows 8, 9, 11 carry bounds — those are the scheduler
semantics. Row 10 is completion-gated (both finish) with reported timing.
Row 13 is evidence-only per the match criterion above.

### 2. Workload definition — full task specification, seeded executor

- **Workload file (JSONL, checked in under `test/differential/workloads/`):**
  one record per task with **deterministic `id` (UUIDv5 from
  `tenant:seq`), `tenant`, `weight`, `created_at_offset_ms` (relative to
  run start — never absolute timestamps), `payload_bytes`**. Rationale:
  the tie-break comparator is
  `priority ASC, created_at ASC, id ASC`. UUID-generated-at-ingest IDs
  would differ between runs, and every tie group would false-mismatch —
  not a scheduler bug on either side, a harness design flaw. The file
  specifies everything the server would otherwise generate, so both runs
  consume byte-identical inputs.
- **Offsets, not absolutes (pinned):** absolute `created_at` values couple
  the workload to wall-clock — a Go run starting an hour after the Java
  run would ingest hour-old tasks, and the timeout dimension would measure
  different things on each side (immediate timeouts vs none). The harness
  materializes absolute timestamps at ingest as `run_start + offset`.
  Offsets must be ≥ 0 — a negative offset (task arriving before the
  run-start marker) rejects the file at load, loudly. Synthesizing a
  pre-marker ingest would invent timeline both schedulers then share
  unknowingly; rejection keeps the file total and honest.
- **Run-start marker (pinned):** the first ingest call accepted by the
  service under test. Both timelines align on their own first ingest, so
  JVM warmup duration never leaks into Go's timeline or vice versa.
- **Match criterion (pinned, revised by the Go-vs-Go control): aggregate
  fairness gates; ordering is evidence, never verdict.** Within a tie group
  (equal priority *and* equal created_at), order may differ without failing
  the run; tie-group divergence is reported, not gated. Exact dispatch
  positions do not gate even outside tie groups: the Go-vs-Go control
  (identical binaries, same workload, separate DBs) agrees exactly on some
  runs and diverges on others — tick/ingest interleaving differs per
  process, virtual-time histories diverge with it, and shares still
  converge exactly. Exact-order parity across independently-ticking
  processes is therefore flaky-by-construction, including Java-vs-Java;
  gating on it would fail identical implementations. Row 13's bound is
  restated accordingly: per-task positions are recorded evidence for
  debugging, not an acceptance criterion.
- **Arrival discipline:** the file lists tasks in submission order with
  `submitted_at` offsets; the driver replays them on a fixed schedule
  (fixed-rate mode) or as fast as the service accepts (as-fast-as-possible
  mode) — see §3 for which mode serves which row.
- **Stub executor latency (pinned default): fixed 100ms, no jitter.**
  Completion timing determines in-flight counts → CMS behavior → scheduling
  decisions, so the stub is a first-class input, not background detail.
  Default is deterministic fixed latency (seed N/A); `uniform` and
  `lognormal` shapes with explicit `seed` + parameters are supported, and
  **both runs use byte-identical parameters from the same workload file**.
  If the schedulers disagree under identical stub parameters, the
  difference is scheduler behavior — the alternative (per-run jitter)
  makes "the schedulers disagree" indistinguishable from "the stubs
  differed." Non-default distributions are reported alongside results.

### 3. Time strategy + warmup discipline (coupled, pinned together)

- **Fixed-rate** (wall-clock controlled, task counts may differ): rows
  1–7, 12. Both services run the same RPS for the same duration.
- **As-fast-as-possible** (task counts controlled, durations may differ):
  rows 8–11, 13. Both services process all N tasks; timing measurements
  from these runs are discarded, not compared.
- **Warmup, fixed-rate mode:** discard the first 20% of the run window,
  then assert the JVM reached steady state by checking the second 20%
  against the third (rate slope ≈ 0). If the slope check fails, extend
  warmup and re-run — do not compare a warming JVM against a warm Go
  binary and call the difference "+20–50%."
- **Warmup, as-fast-as-possible mode:** process the first M=1000 tasks,
  discard their measurements, start recording at M+1. Throughput is not
  derived from these runs at all (see above).
- The two modes are reported separately, never blended into one number.

### 4. Fairness deviation definition (acceptance bound)

- **Windows:** fixed 1000-dispatch windows, aligned on dispatch sequence
  number (not wall-clock — alignment by time confounds rate differences
  with share differences).
- **Bound (pinned): no tenant more than 2 tasks off its weighted share in
  any window** — Java's stated result, adopted verbatim, not loosened.
  Windows counted: all complete windows in the run (partial tail window
  reported, not gated).
- If Java's bound needs revisiting (e.g. hierarchical mode has a
  different stated result), that is a spec change first, argued from the
  Java side — never a quiet bound relaxation in the harness.

### 5. Starvation definition (spec gap, closed here)

- Java states no starvation bound (fairness shares imply it loosely, but
  nothing testable). This scope defines it rather than leaving the
  harness to invent it per run: **no tenant with continuous backlog goes
  more than K consecutive zero-dispatch 100-windows, K=3.** Rationale:
  shares could technically hold while one tenant starves briefly and
  gorges after; the K-window rule forbids exactly that shape.
- **Window-scale refinement (pinned): starvation runs at 100, not the
  §4 1000.** An earlier draft used the same 1000-windows for both gates;
  implementation proved that un-isolatable: a 400-wide gap needs catch-up
  density that trips any tight quota bound (pigeonhole — the excess lives
  in some window at every scale). At 100-scale with compensated catch-up,
  per-window density stays within quota bound while 4 consecutive zeros
  still fire starvation. Different window scales per gate is what makes
  the calibration matrix satisfiable, not a relaxation.
- [x] **CORRECTION-3 (recorded): there is no Java starvation window to
  match.** Verified against Java main: anti-starvation is an aging-credit
  formula plus a `max-queued-time-ms: 60000` promotion deadline — a TIME
  threshold, not a dispatch-window count. No K-consecutive-windows concept
  exists anywhere in the Java tree (grep: no windowed starvation check in
  domain services, docs, or config). The 100-window K=3 gate is therefore
  harness-original, chosen on sensitivity (catches real starvation shapes
  at operator-relevant horizons) and constructibility (isolatable per the
  pigeonhole analysis above) — NOT on Java parity, and the earlier framing
  that implied a parity confirmation is withdrawn. Closest Java analog is
  the 60s promotion deadline, a different dimension (time, not dispatch
  count); their relationship is workload-rate-dependent (400 dispatches ≈
  60s only at ~7 dispatches/s) and is documented as such, not equated.
- **Tighter-wins, stated explicitly:** the scope's definition is the
  acceptance criterion for both sides. If the harness discovers Java
  tolerating K=5 on some workload while the scope says K=3, that is
  reported as **a finding about Java**, not a Go failure and not a reason
  to loosen the bound. Go passing K=3 proves Go meets the stated claim;
  Java failing it is information about the oracle. The comparator never
  relaxes the bound to match observed behavior on either side.

### 6. Falsification test (harness's first passing test)

- Fixture: `FirstQueuedDispatcher` — ignores weights/priority, returns
  oldest `QUEUED` per tick. Workload: the standard 1:2:7 file.
- **Must report:** shares ≈1:1:1 against expected 10/20/70, deviation
  beyond the §4 bound in multiple windows, non-zero exit. If the harness
  reports parity here, the comparator is broken and no other result from
  it is admissible.
- **Timeout detection granularity floor (pinned):** detection latency is
  bounded below by the sweep cadence on each side (Go: dispatcher-interval
  per CORRECTION-2; Java: its scheduler cadence). Deltas smaller than
  `max(sweep intervals)` measure the sweep clocks, not the timeout
  mechanism — only larger deltas are meaningful comparisons. The harness
  reports raw detection latencies and gates only on
  `|go − java| > max_interval_floor`.
- Second calibration (cheap, high value): inverted weights (7:2:1 file
  against a 1:2:7 expectation) must fail in the opposite direction,
  proving the comparator reads the file rather than the code.
- **Calibration family (pinned): falsification is one point, not the
  whole curve.** `FirstQueuedDispatcher` catches mis-weighted aggregates;
  two more fixtures cover the adjacent failure classes, each a small
  decorator in the same shape:
  - `StarvingDispatcher` — weights applied, aging disabled, sustained
    backlog. Correct shares in short windows; violates the K=3 starvation
    rule for the lowest-weight tenant over long ones. Confirms the
    starvation gate fires (and only it — shares must still pass).
  - `QuotaIgnoringDispatcher` — weights applied, `maxPerClient` ignored:
    a single-tenant burst dispatches disproportionately. Correct averages
    over long windows, wrong inside the burst. Confirms the fairness gate
    is window-sensitive (fails short windows, passes long ones).
- **Isolation mechanism (pinned): compensation within one window +
  asymmetric quota bound.** A burst that breaks a 100-window necessarily
  perturbs its enclosing 1000-window, so strict isolation needs
  construction, not luck: each fixture keeps per-1000-window totals exact
  (fairness passes) while confining the anomaly to 100-scale shape —
  starvation via zero-dispatch gap with catch-up spread evenly in-window,
  quota via front-loaded burst with light-but-nonzero remainder. The quota
  bound is over-only (+10 at 100-scale): suppression is fairness's
  jurisdiction (±2 at 1000-scale), so catch-up never fires it. Gate
  scales: fairness 1000/±2, starvation 100-windows/K=3 consecutive zeros,
  quota 100-windows/+10. Each fixture asserts its own gate fires, the
  other two pass, and `RequireGate` fails the fixture loudly if run below
  one full window — fixtures cannot be configured into vacuity.
- **Scope qualifier:** the construction above proves calibration is
  satisfiable for the current four-gate matrix (order, fairness,
  starvation, quota — one fixture each, complete as of this writing), not
  in general. A future fixture against a differently-sized gap or bound
  may need a different compensation shape or may be un-isolatable; that is
  a per-fixture proof obligation, not an inherited property.

### 7. Output format — methodology, not target numbers

- Every result publishes as a directory: `methodology.md` (scope version
  hash + workload file hash + stub parameters + mode + window definition),
  `results.json` (per-dimension numbers + bounds + pass/fail **plus, on
  any mismatch, a classification block:** which dimension diverged, which
  gate the run was exercising vs which gate actually fired, the specific
  tasks/windows of divergence, and both sides' evidence — raw logs,
  dispatch sequence, window breakdown — side by side), the Java
  commit SHA and Go commit SHA the result was measured against, and the raw
  dispatch logs for both sides. A result without its configuration is not
  publishable — re-running the same workload file with different stub
  parameters is a different experiment, and the format makes that
  unrepresentable as the same result. The two SHAs attribute every result
  to specific builds: a result at T2 is distinguishable from T1 without
  re-running. The classification block exists so the first real mismatch
  points at the failed boundary instead of producing "they disagree
  somewhere" and a debugging session from zero.
- Differential runs stay behind the `differential` build tag, out of
  default per-push CI (oracle checkout + two databases + minutes-to-hours
  runtime is not a per-push gate). **Intent, stated so the harness cannot
  rot:** a separate scheduled workflow (nightly or manual dispatch)
  runs differential against a pinned Java commit. Scope states the intent;
  the workflow file lands with the harness. "We have differential testing"
  must always name a cadence, never just a build tag.
- The scope doc this file lives in states the principle explicitly:
  **EQLX-5 defines how equivalence is demonstrated; it does not set
  performance targets.** Go may be faster, slower, or equal per dimension;
  only the bounded rows (§§4–5) gate, everything else reports.

---

## Non-blocking / out of scope

- Harness language: Go test binary driving both REST APIs (no new
  dependencies beyond the module; Java side needs no harness code — its
  REST contract is the interface).
- Separate databases + separate ports per run; port/DB allocation owned
  by the harness, never hardcoded.
- EQLX-6 Prometheus work may later replace log-scraped numbers with
  metric reads; the dimension table is metric-source-agnostic by design
  (each row names what is measured, not how it is scraped).
- Differential runs are excluded from `go test ./...` by default
  (build tag `differential`, explicit `make test-differential` with
  `JAVA_EQUALIX_PATH`); CI runs them only where the oracle is checked out.
