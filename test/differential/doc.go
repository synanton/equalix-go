//go:build differential

// Package differential is the EQLX-5 oracle-comparison harness (scope §0).
// It drives a workload file against the Java and Go services (separate
// databases, shared stub-executor protocol) and compares dispatch behavior.
// Files here compile only under the differential tag; default builds and
// unit CI never touch them.
package differential
