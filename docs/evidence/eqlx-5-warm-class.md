# EQLX-5 evidence: w2000 differential matrix (20 runs)

Citable record for the README maturity table's warm-green claim. Raw
`results.json` per run is ephemeral (`/tmp/ctl-*`, lost on reboot); this
file is the permanent summary. CI uploads `results.json` per run as a
standing retention property (see `.github/workflows/differential.yml`).

Workload: `test/differential/workloads/w2000.jsonl` (200/400/1400,
1:2:7), fixed-100ms stub, as-fast-as-possible ingest. Shares gate:
1000-windows, ±2 bound. Run totals exact (200/400/1400) on both sides in
all 20 runs — the fairness core matches everywhere; only within-window
distribution varies (cold class).

Artifact schema evolved across the campaign: `warmup` persisted from
jg3/gg2 on; `status_fetch` + `startup` from gg2/jgw3 on; `prephase`
from jgw4/ggw2 on. `n/a` below means "predates the field", not "zero".

## Cold class (characterization, not a gate)

| run | verdict | detail | java prom | go prom | java RPS | go RPS | java→ready | go→ready | java SHA | go SHA |
|---|---|---|---|---|---|---|---|---|---|---|
| jg1 | FAIL | window 0 avalanche a:8 b:2 c:6; RPS unattributed (pre-hardening) | 1008 | 782 | n/a (all -1) | -1→25.0 | n/a | n/a | fc0d668 | pre-58866d6 |
| jg2 | PASS | — | 987 | 780 | 1.1→20.6 | 1.1→23.8 | n/a | n/a | fc0d668 | 8b6963c |
| jg3 | PASS | — | 1002 | 780 | 1.1→20.6 | 1.1→21.6 | n/a | n/a | fc0d668 | df4efb3 |
| jg4 | FAIL | windows 0+1 a:12 b:3 c:9 | 1014 | 780 | 1.1→20.6 | 1.1→25.0 | n/a | n/a | fc0d668 | df4efb3 |
| jg5 | PASS | — | 988 | 787 | 1.1→20.6 | 1.1→26.3 | n/a | n/a | fc0d668 | 1db2679 |
| gg1 | PASS | orders diverge pos 11 (diagnostic) | — (781/781) | — | 1.1→25.0 | 1.0→23.8 | n/a | n/a | aa6f736 | aa6f736 |
| gg2 | PASS | orders diverge pos 11 | 788 | 793 | 1.1→21.6 | 1.1→23.8 | — | 502ms | 48f900c | 48f900c |
| gg3 | PASS | orders diverge pos 11 | 786 | 787 | 1.1→23.8 | 1.1→23.8 | — | 501/502ms | cb88f48 | cb88f48 |
| gg4 | PASS | orders diverge pos 12 | 788 | 793 | 1.1→23.8 | 1.1→23.8 | — | 501/502ms | cb88f48 | cb88f48 |
| gg5 | PASS | orders diverge pos 0 | 792 | 780 | 1.1→25.0 | 1.1→25.0 | — | 502/503ms | cb88f48 | cb88f48 |

Cold rate: JvG 2/5 fails vs GvG 0/5 — the predicted Java-ramp signature
(slower ramp × 60s promotion deadline), reported as characterization.
Promoted ranges do not overlap: Go 780–793, Java 987–1014.

## Warm class (parity gate, warm-500-p8-rps15)

| run | verdict | java prom | go prom | java RPS | go RPS | java→ready | go→ready | prephase | java SHA | go SHA |
|---|---|---|---|---|---|---|---|---|---|---|
| jgw1 | FAIL (harness) | — | — | 2.2 frozen | — | — | — | — | a15f43e | pre-trickle |
| jgw2 | FAIL (harness) | — | — | 4.3 frozen | — | — | — | — | a15f43e | paced, post-drain gate |
| jgw3 | PASS | 0 | 0 | 1.1→63.3 | 1.1→52.0 | 5598ms | 502ms | n/a (pre-field) | a15f43e | 99a6f08 |
| jgw4 | PASS | 0 | 0 | 1.1→63.3 | 1.0→52.0 | 5592ms | 501ms | 928/928 | a15f43e | 7aeea77 |
| jgw5 | PASS | 0 | 0 | 1.1→63.3 | 1.0→52.0 | 5570ms | 501ms | 928/936 | a15f43e | 7aeea77 |
| jgw6 | PASS | 0 | 0 | 1.1→63.3 | 1.1→52.0 | 5571ms | 502ms | 920/928 | a15f43e | 7aeea77 |
| jgw7 | PASS | 0 | 0 | 1.1→63.3 | 1.0→52.0 | 5604ms | 503ms | 928/928 | a15f43e | 7aeea77 |
| ggw1 | PASS | — (0/0) | — | 1.1→49.6 | 1.0→49.6 | — | 502/503ms | 928/928 | c17a62a | c17a62a |
| ggw2 | PASS | — (0/0) | — | 1.1→49.6 | 1.0→52.0 | — | 503ms | 928/928 | 7aeea77 | 7aeea77 |
| ggw3 | PASS | — (0/0) | — | 1.1→52.0 | 1.0→49.6 | — | 502/503ms | 928/928 | 7aeea77 | 7aeea77 |
| ggw4 | PASS | — (0/0) | — | 1.1→52.0 | 1.0→49.6 | — | 502ms | 928/920 | 7aeea77 | 7aeea77 |
| ggw5 | PASS | — (0/0) | — | 1.1→49.6 | 1.0→52.0 | — | 501/504ms | 928/928 | 7aeea77 | 7aeea77 |

Warm rate: JvG 0/5 fails vs GvG 0/5 fails — within one-run slack. Gate
met. jgw1/jgw2 are harness failures (burst/post-drain gating vs a
completion-driven controller), excluded from the rate; the trickle gate
is the corrected methodology. Promoted 0/0 on both sides in all 10
passing warm runs: the avalanche disappears when the ramp isn't
dominant. Orders diverge every run on both pairs (diagnostic-only, also
on GvG — flaky-by-construction confirmed in both classes).

## Startup row (from `results.json:startup`)

- Go spawn→ready: 501–504ms across 11 runs (warm- and cold-class).
- Java spawn→ready: 5570–5604ms across 4 warm runs, page-cache-warm
  host. Java cold-start cell pending (fresh-host number expected 2–3×).
- ready→first-dispatch ≈ 117s on warm runs measures to the pre-phase's
  first dispatch, not measurement start (class-dependent definition).
