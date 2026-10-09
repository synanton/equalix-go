# equalix-go documentation

Developer documentation for equalix-go, the spec-first Go implementation of the
Equalix eventually-fair weighted-fair scheduler.

equalix-go is an **independent reimplementation** of
[Equalix](https://github.com/synanton/equalix): the Java implementation is the
behavioral reference (the oracle), and this codebase must reproduce its
scheduling semantics — verified by conformance suites, benchmarks, and the
three-way differential matrix against the Spring Boot oracle and the Micronaut
port.

## How this book is organized

- **Using equalix-go** — the HTTP API, benchmarks, and the operator runbook.
- **Architecture** — system architecture, the behavioral specification
  (`spec.md`, the source of truth for every parity decision), and the schema
  reference.
- **Differential validation** — the three-way methodology, the EQLX-5 scope
  behind it, and the recorded evidence (warm-class runs plus the per-pair
  methodology notes).
- **Developing** — the implementation guide, per-phase scopes, and the
  original epic ticket.

Differential evidence lives here, in `equalix-go/docs/evidence/` — including
the Micronaut-involved pairs. The sibling repos cite it by repo + path +
commit, never by copy (see the methodology chapter).

The full repository (code, harness, and these docs) is at
[github.com/synanton/equalix-go](https://github.com/synanton/equalix-go).
