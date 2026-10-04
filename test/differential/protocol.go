//go:build differential

// Protocol between schedulers and the harness-owned stub executor.
//
// Both schedulers (Java Equalix, equalix-go) implement against this
// written contract — never against each other's observed behavior.
// The stub is the single harness-owned instance both sides target
// (identical behavior by construction); no per-language stub exists.
//
// Dispatch (scheduler → stub):
//
//	POST {stub}/tasks/{id}/execute
//	Content-Type: application/octet-stream (body: opaque task payload)
//	→ 200 OK on receipt. 405 on wrong method, 404 on malformed id.
//
// Completion (stub → scheduler, after the configured latency):
//
//	POST {scheduler}/api/v1/tasks/{id}/complete
//	Content-Type: application/json
//	X-API-Key: <run key>
//	{"success": true} | {"success": false, "error": "<reason>"}
//
// Run lifecycle (reset boundary = stub process exit):
//
//	[start stub] → [start scheduler] → [ingest] → [drain] →
//	[teardown stub, capturing the log] → [next run's fresh stub]
//
// The dispatch log is obtainable only via Close: capture-after-teardown
// ordering is enforced by API, so one run's traffic can never contaminate
// the next.Wall-clock in the log is translated to per-side offsets
// (ToOffsets) before comparison — never compared directly.
package differential
