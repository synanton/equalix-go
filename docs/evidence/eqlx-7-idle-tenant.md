# EQLX-7 evidence: idle-tenant virtual-time clamp

Mechanism under test (named): whether max(stale_finish, V) clamps
virtual time forward on return from idle. Clamp works → A's first
returning tasks tag at V + quantum/weight like any waiting tenant
(~10% share at 1:2:7). Clamp missing → A's tags sit at the stale
finish below V and A bursts until tags catch up. Observable
(primary): A's dispatch share in the first 1000 dispatches created
at/after the return boundary. N=1 pair per regime — parity
interpretation below the rate floor, never a gate.

Workloads: `w-idle.jsonl` (2000 tasks; A active 0–5s, idle 5–25s,
returns 25–62s; above floor) and `w-idle-small.jsonl` (150 tasks,
A idle ~24s; below floor). Driver honors file offsets
(`EQUALIX_HONOR_OFFSETS=1`); record-only verdict with the window
persisted in `results.json:idle` (`EQUALIX_IDLE_TENANT=a`,
`EQUALIX_IDLE_RETURN_MS` per file).

## Above floor (cold, return @25s into the ramp)

| side | return window (1000 dispatches from return boundary) | A share | promoted | RPS |
|---|---|---|---|---|
| java (JvG) | {a:100 b:200 c:700} | 10.0% (exact) | 108 | 1.1→17.8 |
| go (JvG) | {a:103 b:206 c:691} | 10.3% | 24 | 1.1→11.5 |
| go1 (GvG) | {a:102 b:206 c:692} | 10.2% | 25 | 1.1→10.9 |
| go2 (GvG) | {a:103 b:206 c:691} | 10.3% | 25 | 1.1→11.5 |

A gets its weighted share on all four sides (100–103 vs 100
expected) — no burst. The clamp holds identically on both
implementations: four-way agreement within ±3 tasks. Promoted counts
(108/24/25/25) are the known cold-contention asymmetry, not idle
behavior — the idle observable is the window, and it matches.

Clamp-absent counterfactual (what falsifies the claim): without
`max(stale_finish, V)`, A's first returning task would tag at
stale_finish + quantum/weight ≈ 40k + 1000/1.0 ≈ 41k (Go numbers),
below the then-current V ≈ 109k — so A would dispatch on nearly
every tick until its tags caught up, producing a first-window share
≫10% (bounded by the 7:1 weight ratio toward ~70% in early windows),
not the observed 10%. The observed 10% rules this out; without this
sentence the doc would report a positive result without excluding
the alternative.

Clamp-exercise verification (not assumed): Go system V at the return
boundary ≈109k vs A's stale finish ≤ ~40k (phase-0 tags at V ≤ 36k +
quantum); Java V ≈ 51k vs stale ≤ ~16k. V advanced 3–10× past stale
finishes on both sides — a missing clamp would have shown A
dominating the return window. It shows 10%.

## Below floor (cold, return @26s, RPS pinned ~2)

| side | return window | A share |
|---|---|---|
| java (JvG) | {a:9 b:6 c:20} (35 dispatches) | — (file-mix replay) |
| go (JvG) | {a:9 b:6 c:20} | — |
| go1/go2 (GvG) | {a:9 b:6 c:20} / {a:9 b:6 c:20} | — |

Byte-identical across all four sides. RPS never leaves the floor
(1.1→1.8–2.0 on every side — pinned, never zero), all 150 tasks
terminal (DB-verified SUCCEEDED on the JvG pair), promotions 0
everywhere. The floor assertions matter: this run proves the floor
dispatches (nonzero count), holds (never drops to zero), and
terminates everything — without them it would be vacuous on both
axes (no clamp question AND no floor evidence), i.e. a null result
rather than a control. It never stands without its above-floor
sibling (which is why the two ship as one residual).

## Warm negative control (JvG, return @80s, RPS 100 both sides)

Return windows java {140,70,190} / go {140,70,191} — the phase-2
file mix replayed byte-identically (differing by one c-task).
Uncontended flow at ceiling RPS answers nothing about the clamp
(same structural reason the warm-paced promo point failed). Recorded
so nobody re-tries warm for this question; the cold redesign above
is the correction, with the anti-pattern named in the generator
(`Do not "simplify" back to warm`).

## Reading

The clamp holds on both sides where it is observable (above floor),
is vacuous below floor (by design — the control), and unobservable
warm (negative control). No burst-after-idle on any side in any
regime. Four-way agreement at ±3 tasks on the above-floor window is
the parity result; N=1 pair each keeps it characterization, per the
rate-floor rule.
