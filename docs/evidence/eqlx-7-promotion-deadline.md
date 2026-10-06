# EQLX-7 evidence: promotion-deadline 3-point sweep (cold class)

Workload: `test/differential/workloads/w-promo.jsonl` (200/400/1400,
1:2:7, deterministic `promo-<tenant>-<seq>` IDs, all offsets zero —
pacing is an invocation parameter, not a file property). File
parameterizes TOTAL rate; CORRECTION-3's ≈7/s is per-tenant (≈21/s
total for 3 tenants; both numbers stated here so no run misreads the
target). Driver: `EQUALIX_SUBMIT_PACE` (tasks/s; unset = historical
ASAP burst). Class: cold (no warm-up — the deadline only fires when
ingest persistently exceeds dispatch capacity during the ramp; a
warmed controller at ceiling RPS leaves nothing for the 60s deadline
to bite). Points: 10 / 21 / 35 total/s × (JvG + GvG), plus one
warm-paced negative control. Shares gate: 1000-windows, ±2 bound.
Run totals exact (200/400/1400) on every side of every run.

## The band (empirical edges)

| point | JvG verdict | GvG verdict | java prom | go prom | RPS (java → / go →) |
|---|---|---|---|---|---|
| warm 21/s (negative control) | FAIL, identical degenerate both sides, byte-identical steady orders | — | 0 | 0 | →100 / →100 |
| cold 10/s | FAIL {100,200,300} both sides | FAIL, same shape both sides | 0 | 0 / 0 | →100 / →100 |
| cold 21/s | FAIL (java {48,94,142}, go {100,162,262}) | FAIL (go {100,162,262} both) | 9 | 0 / 0 | →17.8 / →11.5 |
| cold 35/s | FAIL (java exact {0,0,0}, go {13,26,39}) | FAIL (go {13,27,40} / {13,26,39}) | 366 | 71 / 69,71 | →18.7 / →11.5,~11 |

Reading:

- **Below ~15/s: uncontended tag-order.** Dispatch capacity (ramping
  to ceiling) always exceeds arrival; every task dispatches in tag
  order with no backlog to mix windows. The 10/s point proves it in
  the extreme: both 1000-windows deviate exactly {100,200,300} on all
  four cold-lo sides, constraining each window to all-c or
  {200,400,400} — pure tag-order drain, in lockstep. Promotions 0
  everywhere. The deadline is irrelevant here; so is the gate (below).
- **~21/s: transitional.** Partial contention (RPS 11–18, never
  ceiling). c-heavy windows on all sides; Java's window shape differs
  from Go's ({48,94,142} vs {100,162,262}) and Java promotes 9 tasks
  against Go's 0 — the deadline starts biting Java first (slower
  ramp), the known asymmetry at its onset edge.
- **~35/s: deadline-dominant.** Sustained contention, mass promotion
  on both sides (Java 366, Go ~70) — the backstop mechanism is shared.
  Java's windows come out EXACT while Go carries a systematic
  {13,26,39} skew replicated across all three Go runs (JvG-go-side,
  go1, go2 agree to ±1). Totals exact everywhere. Java-exact is N=1
  (unknown if systematic); Go-skewed is N=3 (systematic). Reported
  without attribution — below the sample floor for a mechanism claim.

## Regime-validity finding (control-proven, all three points)

The shares gate fails on at least one pair at EVERY sweep point —
including GvG at all three points, with identical binaries producing
identical degenerate distributions. Paced ingest without backlog
mixing measures tag order, not fairness: the gate is regime-invalid
here, exactly as exact-order parity was flaky-by-construction. Window
verdicts in paced regimes are uninterpretable as parity evidence (for
either pair); the mappable deadline signals are promoted-vs-rate and
run totals, which is what the table above records. Burst ingest
(w2000 cold/warm) remains the only regime where window shares mix and
gate.

## Negative control (warm-paced 21/s)

JvG FAIL with byte-identical steady orders, exact totals, 0/0
promotions, both controllers at ceiling RPS. Included so nobody
re-tries warm pacing: warming first guarantees the uncontended
regime, which guarantees gate failure. The contention requirement is
structural, not a tuning miss.
