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

## DB write budget (integration, `TestDbLoadBenchmark`)

400 tasks, single tenant, Testcontainers PostgreSQL 16, jobs-driven
ingest → calc → dispatch → ack → complete. Exact per-phase statement counts
(counting `TxBeginner` wrapper — same Transact path the jobs use, production
untouched); wall-clock ranges over repeated runs on shared hardware:

| Phase (400 tasks) | Before (stmts) | After (stmts) | Before (wall) | After (wall) |
|---|---|---|---|---|
| Ingest | 800 (UPDATE-miss + INSERT) | 400 (`Insert`) | 56–137 ms | 61–83 ms |
| Priority calc | 1204 (V SELECT + upsert + full-row Save) | 805 (batch V + upsert + targeted UPDATE) | 77–289 ms | 97–200 ms |
| Dispatch | 868 | 94 (bulk UPDATE + 1 upsert/key) | 73–183 ms | 26–82 ms |
| Executor ack | 800 | 400 (guarded UPDATE) | 50–90 ms | 56–151 ms |
| Completion | 1200 | 1200 (already 1 UPDATE; narrower WAL) | 68–104 ms | 152–321 ms, see noise note |
| **Total** | **4872** | **2899 (−40%)** | **~445–650 ms** | **~440–790 ms** |

What moved the counts: bulk dispatch UPDATE (400 full-row UPDATEs → 1),
`AddBatch` counts (400 upserts → 1), bulk promotion (400 → 1),
`ReserveAt` batch-V (800 → 401 statements on calc), `Insert` (800 → 400),
guarded ack (800 → 400). Completion is unchanged in counts — Go's `Save`
was already one UPDATE (no ORM merge to eliminate, unlike the JVM); the
win there is narrower WAL plus atomic guards.

Noise notes (same discipline as the char runs):

- Wall-clock on shared CI hardware swings ±3× per statement across runs;
  statement counts are deterministic (identical across all runs) and are the
  primary evidence. Totals agree with the counts.
- The complete-phase wall leans patched-side-slow (+50–250 ms) with identical
  counts in 5/5 pairs, including a same-hour parallel A/B run — cause not
  isolated (suspected autovacuum/analyze placement on the fresh table, not the
  UPDATE shape, which touches fewer columns). No load regression either way;
  follow-up: repeat with autovacuum off and larger N.
- Calc wall is inconclusive (host noise swamps a −33% count delta).

## CMS error envelope

See `docs/evidence/eqlx-7-cms-curve.md` (table + raw JSON at
`pkg/cms/testdata/cms-curve.json`): exact through 10k keys both
distributions; uniform-100k degrades per load-factor arithmetic;
zero underestimates everywhere (one-sidedness holds).
