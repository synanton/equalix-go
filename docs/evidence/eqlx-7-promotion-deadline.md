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

Reading, corrected (the first version of this section overclaimed —
see the regime split below):

- **10/s: gate regime-invalid.** Uncontended tag-order drain both
  pairs (RPS→ceiling, 0 promotions, identical {100,200,300}
  degeneracy). No weighted-fair scheduler produces 1:2:7 here; the
  gate asks a question the regime cannot answer.
- **21/s: gate fires — real, unattributed JvG divergence.** Partial
  contention (RPS 11–18, never ceiling). GvG reproduces Go's shape
  exactly ({100,162,262} both sides = the noise floor: this
  implementation produces this distribution every run); Java differs
  ({48,94,142}) with 9 promotions against Go's 0. Same class as the
  cold-start avalanche finding: runtime-timing interaction at a
  specific regime, not a gate failure.
- **35/s: gate fires — real, unattributed.** Sustained contention,
  mass promotion both sides (Java 366, Go ~70). Go's {13,26,39} skew
  replicates across all three Go runs (noise floor established);
  Java exact is N=1. Same disposition as 21/s.

## Regime-validity finding, corrected scope (control-proven at 10/s only)

The first version claimed the gate regime-invalid at all sweep points
because GvG fails everywhere. That overreaches: at 21/s and 35/s the
readings show contention and promotion — not tag-order regimes — so a
GvG failure there establishes the noise floor (Go reproduces its own
shape), and the JvG failure on top establishes divergence (Java
differs). Both informative; discarding them as "regime-invalid"
throws away the finding. Corrected split: 10/s regime-invalid (gate
asks an unanswerable question); 21/s and 35/s gate fires with real,
unattributed JvG divergence. The warm-paced negative control stands
unchanged (structural impossibility, arithmetic proof intact).

## RPS trajectories (labeling verified clean)

JvG tracer reads by binary identity (java→Java proc port,
go→Go proc port — verified in code; the GvG "go1 in java_* slots"
mapping is Resolved-artifact-only). The paced-point inversion vs
cold-burst history (Go 11.5 < Java ~18 here; Go 23–25 > Java 20.6 on
burst) is therefore real trajectories, not swapped labels: both sides
ramp 1.1→ in lockstep early, then plateau rate-independently (mid and
hi plateaus identical per side: Java ~18, Go ~11.5 at both 21 and
35/s). Java promotes MORE at HIGHER RPS (366 at 18.7 vs Go 70 at
11.5) — backwards from the naive "higher RPS clears backlog" reading,
because promotions track AGING (queue-wait distribution), and Java's
dispatch order leaves a longer tail. Mechanism unattributed (N=1
Java at hi); the trajectory table is what a replicate compares
against.

## Zero-tag refutation (the promoted query is clean here)

Both implementations CAN tag priority 0 at empty in-flight
(`round(F)` with small F, zero pressure) — so `priority <= 0` mixes
starvation promotions with zero-tags in principle. Trajectory shape
refutes the pollution in practice: promoted stays 0 through the
entire dispatch phase on both sides (e.g. hi Java (149,0) → (1200,0)
→ (2000,0)) and climbs only post-drain ((2000,0) → (2000,236) →
(2000,366)) — the QUEUED-drain phase, exactly when aged tasks hit the
backstop. Early zero-tagging would read as nonzero promoted
mid-dispatch; it reads 0. The EQLX-5 promotion narrative (cold
asymmetry, avalanche NOTE) rests on true starvation counts, not tag
artifacts — verified, not assumed.

## Negative control (warm-paced 21/s)

JvG FAIL with byte-identical steady orders, exact totals, 0/0
promotions, both controllers at ceiling RPS. Included so nobody
re-tries warm pacing: warming first guarantees the uncontended
regime, which guarantees gate failure. The contention requirement is
structural, not a tuning miss.
