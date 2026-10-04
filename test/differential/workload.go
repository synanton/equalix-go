//go:build differential

package differential

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
)

// Task is one workload record (scope §2). Every field the server would
// otherwise generate is specified, so both runs consume byte-identical
// inputs: deterministic id (UUIDv5 tenant:seq recommended), explicit
// offsets (never absolute timestamps), fixed payload size.
type Task struct {
	// ID is deterministic across runs (UUIDv5 recommended).
	ID string `json:"id"`
	// Tenant is the fairness key.
	Tenant string `json:"tenant"`
	// Weight is the tenant's scheduling weight (> 0).
	Weight float64 `json:"weight"`
	// CreatedAtOffsetMs is relative to the run-start marker (first
	// accepted ingest). Must be >= 0 — negatives reject the file.
	CreatedAtOffsetMs int64 `json:"created_at_offset_ms"`
	// SubmittedAtOffsetMs is the driver's submit schedule, relative to
	// run start. Must be >= 0 and non-decreasing down the file.
	SubmittedAtOffsetMs int64 `json:"submitted_at_offset_ms"`
	// PayloadBytes fixes the opaque payload size.
	PayloadBytes int `json:"payload_bytes"`
}

// Load reads and validates a JSONL workload file. Validation failures
// (negative offsets, empty id/tenant, non-positive weight, decreasing
// submit schedule) reject the file loudly — a malformed workload must
// never silently compare two runs against different inputs.
func Load(path string) ([]Task, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("differential: open workload: %w", err)
	}
	defer f.Close()
	var out []Task
	seen := map[string]struct{}{}
	var lastSubmitted int64 = -1
	first := true
	line := 0
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		line++
		raw := sc.Bytes()
		if len(raw) == 0 {
			continue
		}
		var t Task
		if err := json.Unmarshal(raw, &t); err != nil {
			return nil, fmt.Errorf("differential: line %d: %w", line, err)
		}
		if t.ID == "" {
			return nil, fmt.Errorf("differential: line %d: empty id", line)
		}
		if _, dup := seen[t.ID]; dup {
			return nil, fmt.Errorf("differential: line %d: duplicate id %q", line, t.ID)
		}
		seen[t.ID] = struct{}{}
		if t.Tenant == "" {
			return nil, fmt.Errorf("differential: line %d: empty tenant", line)
		}
		if t.Weight <= 0 {
			return nil, fmt.Errorf("differential: line %d: non-positive weight", line)
		}
		if t.CreatedAtOffsetMs < 0 {
			return nil, fmt.Errorf("differential: line %d: negative created_at offset (arrives before run-start marker)", line)
		}
		if t.SubmittedAtOffsetMs < 0 {
			return nil, fmt.Errorf("differential: line %d: negative submitted_at offset", line)
		}
		// A created_at in the future relative to submit would schedule a
		// wait inside SubmitTask (correct behavior, not a hang) and skew
		// the run; created_at must not lead submission.
		if t.CreatedAtOffsetMs > t.SubmittedAtOffsetMs {
			return nil, fmt.Errorf("differential: line %d: created_at offset after submitted_at offset", line)
		}
		if !first && t.SubmittedAtOffsetMs < lastSubmitted {
			return nil, fmt.Errorf("differential: line %d: submit schedule decreases", line)
		}
		first = false
		lastSubmitted = t.SubmittedAtOffsetMs
		if t.PayloadBytes < 0 {
			return nil, fmt.Errorf("differential: line %d: negative payload size", line)
		}
		out = append(out, t)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("differential: read workload: %w", err)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("differential: workload %q has no tasks", path)
	}
	return out, nil
}
