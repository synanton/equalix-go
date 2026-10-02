# Contributing to equalix-go

This document explains how to set up, develop, test, and submit changes to `equalix-go`. It also defines the two acceptance gates (training and production) that every contribution should be aware of.

Java reference: [Equalix](https://github.com/synanton/equalix) — `docs/design.md` (algorithm), `docs/configuration.md` (defaults), `docs/api-reference.md` (REST). The Java code is the behavioral oracle; Go code must be idiomatic, not a class-by-class translation.

---

## Prerequisites

- **Go 1.22+**
- **Docker** and **Docker Compose** (for integration tests)
- **PostgreSQL 14+** and **Redis 6+** (provided by Docker Compose)
- **golangci-lint** (install via `go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest`)
- **Java 21 + Maven** (only for differential testing against the Java reference)
- The Java reference repository cloned locally if you intend to run differential tests

---

## Setup

```bash
git clone https://github.com/synanton/equalix-go.git
cd equalix-go

# Install tools
make tools

# Start local dependencies
docker compose up -d

# Run unit tests
make test

# Run integration tests (needs Docker; pulls postgres:16-alpine on first run)
make test-integration
```

---

## Repository Layout

```text
cmd/equalix-go/       # main entrypoint
internal/
  domain/             # pure domain logic (no I/O)
  port/               # interfaces (ports)
  adapter/
    postgres/         # pgx adapter
    redis/            # go-redis adapter
    http/             # chi REST adapter
    executor/         # HTTP executor client
    metrics/          # prometheus adapter
  jobs/               # dispatcher, watchdog, priority calculator, recovery
  adaptive/           # adaptive RPS controller
  config/             # configuration loading
pkg/                  # reusable public packages (e.g. cms)
docs/                 # architecture, API, configuration, implementation
configs/              # example YAML configs
migrations/           # PostgreSQL migrations (mirrors Java db/migration V1..V5)
test/
  integration/        # testcontainers-based tests
  conformance/        # fairness invariants
  differential/       # Java vs Go oracle comparison
```

**Rule:** `internal/domain/` must not import any package that performs I/O. If you find yourself reaching for `database/sql`, `net/http`, or `go-redis` inside domain, define a port instead.

---

## Development Workflow

1. Pick a task from the current phase in `docs/IMPLEMENTATION.md`.
2. Write the spec first. If the behavior is not already documented in `docs/spec.md`, add it before implementing. Ground it in the Java `docs/design.md` section and note any intentional divergence.
3. Write tests first for domain logic (table-driven) or integration tests for adapters.
4. Implement.
5. Run `make test lint` (and `go test -race ./...`) before committing.
6. Open a PR with a clear description linking the spec section and the epic ticket. Prefix branches with `EQLX-` (e.g. `EQLX-domain-priority`).
7. Merging is manual — a maintainer merges after review. Do not merge your own PR without approval.

---

## Branch Naming

All branches use the `EQLX` prefix:

```text
EQLX-<short-scope>        e.g. EQLX-docs-foundation
EQLX-<phase>-<topic>      e.g. EQLX-domain-cms, EQLX-adapter-postgres
```

Do not push directly to `main`.

---

## Commit Conventions

Use conventional commits:

```text
feat(domain): add virtual-time finish tag calculation
fix(postgres): handle duplicate completion idempotently
test(conformance): add 1:2:7 fairness invariant
docs(spec): clarify CMS decay semantics
chore: initialize module and repo skeleton
```

---

## Testing Requirements

| Layer        | Tool                          | Required Coverage        |
| ------------ | ----------------------------- | ------------------------ |
| Domain core  | `go test`, table-driven       | 90%+                     |
| Adapters     | `testcontainers-go`           | 80%+                     |
| Jobs         | Integration + fault injection | Critical paths           |
| Conformance  | Custom harness                | All fairness invariants  |
| Differential | Java + Go side-by-side        | Dispatch decision parity |
| Load         | `k6` or `vegeta`              | Target throughput        |

Every PR that changes domain logic must include unit tests. Every PR that changes an adapter must include integration tests. Every PR that changes scheduling behavior must update the conformance suite.

Key conformance reference: Java measured 1 : 2 : 7 weights → 10% / 20% / 70% shares over 10,000 dispatches, worst case 2 tasks off weighted share in any window. Go must reproduce this within the same tolerance.

---

## Training Gate

A contribution satisfies the training gate when it demonstrates:

- [ ] Idiomatic Go (no Java-isms: no getter/setter boilerplate, no inheritance simulation, no Spring-style DI)
- [ ] Clear use of interfaces at port boundaries
- [ ] Concurrency correctness (race detector clean: `go test -race ./...`)
- [ ] Table-driven tests for domain logic
- [ ] Integration tests for any new adapter
- [ ] Benchmarks for any new hot-path code
- [ ] Documentation update in `docs/` for any new behavior
- [ ] `golangci-lint run` clean

The training gate is about craft. It is the minimum bar for any PR.

---

## Production Gate

A release satisfies the production gate when evidence exists for:

- [ ] Fairness under sustained load
- [ ] Fairness under bursty load
- [ ] Tenant starvation resistance
- [ ] Scheduler correctness after restart
- [ ] Executor failure handling
- [ ] Database failure handling
- [ ] Redis failure handling
- [ ] Duplicate completion handling
- [ ] Network partition / timeout handling
- [ ] Clock skew effects
- [ ] Recovery semantics (watchdog, CMS warm-up)
- [ ] Horizontal scaling (two instances, shared DB + Redis)
- [ ] Backpressure stability
- [ ] Queue growth behavior
- [ ] Memory behavior under load
- [ ] Operational limits documented in `docs/runbook.md`

The production gate is about evidence. It is required before tagging a release as stable.

---

## Code Review Checklist

Reviewers should verify:

- [ ] Does the change match the spec in `docs/spec.md` (and Java `docs/design.md` where applicable)?
- [ ] Is the domain core free of I/O dependencies?
- [ ] Are new ports defined as interfaces, not concrete types?
- [ ] Are errors wrapped with context (`fmt.Errorf("...: %w", err)`)?
- [ ] Are goroutines properly cancellable via `context.Context`?
- [ ] Are channels closed by the sender only?
- [ ] Are tests deterministic (no `time.Sleep` in tests; use fake clocks)?
- [ ] Is the race detector clean?
- [ ] Are new metrics registered and documented?
- [ ] Is the change reflected in `CHANGELOG.md` if user-visible?
- [ ] Do defaults match the Java reference (`docs/configuration.md`) unless explicitly justified?

---

## Reporting Issues

Open an issue with:

- The phase and task from `docs/IMPLEMENTATION.md`
- A minimal reproduction (config + input + observed vs expected)
- Whether the Java reference exhibits the same behavior

---

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
