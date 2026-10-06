# Benchmarks

Hardware provenance for every number below: i9-12900T, 24 threads,
linux/amd64, Go 1.22, 2026-10-06, `make bench`
(`go test -run=NONE -bench=. -benchmem ./pkg/cms/... ./internal/domain/...`).
Re-run on release hardware before citing beyond order-of-magnitude.

## Hot path (per-dispatch costs)

| Benchmark | ns/op | allocs/op | Notes |
|---|---|---|---|
| CMS Add | 18.5 | 0 | one cell per row, flat allocation |
| CMS EstimateCount | 18.4 | 0 | min over 5 rows |
| Virtual-time Reserve | 18.0 | 0 | atomic upsert path (in-memory store) |
| CalculatePriority | 2.2 | 0 | formula only |
| SelectBatch | 2297 | 3 | sort-dominated; scales with candidate pool |
| RankByAging | 65582 (~66µs) | 5 | 400-candidate pool; per-tick cost of the aging path |

The aging path (~66µs/tick at 400 candidates) is the per-tick cost
callers budget against, not an occasional backstop (spec §13).

## Scheduler-level timing (live, from differential traces)

- Dispatch decision (selection query): µs–low-ms; histogram
  `equalix_dispatch_decision_latency_seconds` on `/metrics`.
- Watchdog reconcile: ~13ms idle tick (empty DB); 20k rows → ~190ms
  (EQLX-3b GROUP BY gate measurement).
- CMS warm-up: ~6ms empty DB at startup.
- Timeout detection: ~sweep interval under light race load.
- RPS trajectories: cold ramp 1→20s; operating 20–65 depending on
  load and implementation (Java ~20/63, Go ~25/52 on w2000 warm).

## CMS error envelope

See `docs/evidence/eqlx-7-cms-curve.md` (table + raw JSON at
`pkg/cms/testdata/cms-curve.json`): exact through 10k keys both
distributions; uniform-100k degrades per load-factor arithmetic;
zero underestimates everywhere (one-sidedness holds).
