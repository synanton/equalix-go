package port

import "errors"

// Sentinel errors let callers distinguish domain outcomes from transport
// failures without inventing adapter-specific strings. Adapters must wrap
// (not replace) these so errors.Is keeps working across layers.
var (
	// ErrNotFound is returned when a task ID names no row (completion of
	// an unknown task → 404, spec docs/api.md).
	ErrNotFound = errors.New("port: task not found")
	// ErrVersionConflict is returned when a Save loses optimistic locking
	// (concurrent dispatcher/completion on the same row). The caller
	// re-reads via FindByID to decide: terminal → treat as duplicate
	// success; otherwise retry or surface 409/400 per the API contract.
	ErrVersionConflict = errors.New("port: version conflict")
)
