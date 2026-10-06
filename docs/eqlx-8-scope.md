# EQLX-8 scope — Redis-backed CMS (shared cross-instance sketch)

Phase goal: two scheduler instances share one in-flight view, so the
fairness the warm class validated on one instance holds across the
fleet. Domain core untouched — warm-class parity stays valid by
construction. EQLX-9 (hierarchy) follows separately; its opt-in
decision is recorded here so neither phase invalidates EQLX-5.

## Blocking choices (decided before implementation)

### 1. Fallback policy: fail-open with degradation signal

Redis down → dispatch continues against the local sketch; `/readyz`
reflects degradation; `equalix_cms_redis_degraded` gauge fires (1
while degraded, 0 otherwise); runbook documents the fairness drift
that occurs during degradation (two instances schedule as if
independent). Rationale: matches EQLX-4's never-block adaptive
invariants — a Redis outage must not stop the scheduler. Fail-closed
(reject dispatch, `/readyz` 503) makes Redis a hard dispatch
dependency; rejected as an operator opt-in at most, never the
default. Fail-open is the default; fail-closed is not implemented
in this phase (no `redis.fallback_policy` knob — one policy, no
configuration surface for a choice operators should not have to
make under pressure).

### 2. Watchdog cross-instance coordination: advisory lock

With a shared CMS, two watchdogs rebuilding concurrently race (one
rebuilds, the other overwrites mid-rebuild). Decision: the existing
Postgres advisory-lock pattern serializes rebuilds (same lock family
as migrate-on-startup; bounded by statement_timeout, documented per
the migrate review). Last-writer-wins rejected: silent double-write
with no serialization is the namespace-collision class wearing a
different hat.

### 3. Warm-up: bounded, then degrade

On startup, seed Redis from `client_counts` (DB authoritative).
Timeout 30s: on expiry, proceed with the local sketch + degraded
gauge + warn log — warm-up failure degrades, never blocks (same
philosophy as fallback). `/readyz` does NOT wait for warm-up: the
DB-ping/lock/migrations checks already gate serving, and holding
readiness on a cache-fill would conflate "not servable" with "not
yet warm". Warm-up duration observed via `cms_warmup_duration`
(histogram already exists — now measures the Redis seed path).

### 4. Batch interface: port gains AddBatch (+ spec entry)

The buffering decorator flushes N keys at commit; per-key round
trips would multiply Redis RTT by batch size. Decision: extend
`port.CMSStore` with `AddBatch(map[string]int64) error` + spec NOTE
(same pattern as the EQLX-6 Metrics extension — port change with
recorded rationale, not scope creep). Redis adapter pipelines
EVALSHA per key (atomicity is per-key — each script execution is
atomic; batching saves round trips, not cross-key atomicity, and
the scope does not claim more). Local sketch implements AddBatch
as a loop (zero cost, keeps the port uniform). Test fakes updated.

### 5. Key naming: namespaced, no TTL

Keys `equalix:cms:v1:{tenant}` (version segment for future format
changes). No TTL: eviction is watchdog-driven (SCAN namespace +
DEL keys absent from DB actuals on rebuild), not time-driven —
a TTL would silently drop slow tenants' counts mid-backlog, which
is the silent class with a timer on it. Memory bound: tenant cap
policy from EQLX-6 applies to series, not sketch keys; sketch
memory is width×depth fixed (~2.6 MB) regardless of key count.

### Opt-in defaults (parity preservation, pinned)

- `redis.enabled=false` (unchanged default): local CMS, behavior
  byte-identical to the validated build. `=true` selects shared
  mode — intentionally different behavior, validated independently
  (evidence below), never compared against local-mode runs.
- Hierarchy: `scheduler.fairness_mode=flat` default (unchanged).
  EQLX-9 adds the hierarchical path behind the existing flag;
  flat-path behavior frozen. Neither phase invalidates EQLX-5's
  warm-class claim while defaults hold — and defaults hold unless
  an operator flips them, which is then that deployment's
  validation burden, documented in the runbook.

## Test strategy

- Integration: testcontainers Redis, full `CMSStore` contract
  (including the new `AddBatch`), Lua scripts by SHA.
- Cross-instance: two schedulers, one Redis, shared 1:2:7 workload —
  in-flight estimate shared (assert via estimate reads, not timing).
- Fallback: kill Redis mid-run → dispatch continues locally,
  degraded gauge fires, behavior matches fail-open policy.
- Warm-up: fresh Redis + populated PG → estimate converges within
  the 30s bound; expiry path → local + degraded + warn (fault
  injection, not a real outage).

## Evidence

`docs/evidence/eqlx-8-cross-instance.md` — two instances, 1:2:7
workload, shares hold across the fleet. This is what the README's
scaling posture has been gesturing at; the evidence makes it a
measured claim instead.

## Sequence

1. Adapter + Lua + AddBatch (+ spec NOTE) + unit/integration tests.
2. Warm-up + watchdog lock + fallback + degraded gauge.
3. Cross-instance evidence run.
