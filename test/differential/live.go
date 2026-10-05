//go:build differential

package differential

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5"
)

// SideResult is one scheduler's observed run: dispatch log plus the
// run-start marker (first accepted ingest's server stamp) plus the warmup
// prefix length (dispatches that fired before calculator quiescence —
// startup transient, excluded from ordering comparison, included in
// shares per the scope's window rule).
type SideResult struct {
	Log    RunLog
	Marker time.Time
	Warmup int
}

// waitQuiesced polls until no RECEIVED rows remain (calculator drained)
// via direct DB read — uniform across both schedulers, no API dependency.
// Returns the number of stub dispatches observed so far: the warmup prefix.
func waitQuiesced(ctx context.Context, dsn string, stub *Stub, timeout time.Duration) (int, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return 0, fmt.Errorf("differential: quiescence connect: %w", err)
	}
	defer conn.Close(ctx)
	deadline := time.Now().Add(timeout)
	for {
		var n int
		if err := conn.QueryRow(ctx,
			`SELECT COUNT(*) FROM tasks WHERE status = 'RECEIVED'`).Scan(&n); err != nil {
			return 0, fmt.Errorf("differential: quiescence poll: %w", err)
		}
		if n == 0 {
			return stub.Count(), nil
		}
		if time.Now().After(deadline) {
			return 0, fmt.Errorf("differential: RECEIVED did not drain in %v", timeout)
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// RunSide drives one scheduler through a full workload against an already
// started stub. It takes ownership of the stub and Closes it at the end,
// returning the captured log — capture-after-teardown enforced by API, so
// the caller (one fresh stub per run) cannot leak traffic across runs.
func RunSide(ctx context.Context, svc SideConfig, apiKey string, workload []Task, stub *Stub, drainTimeout time.Duration) (SideResult, error) {
	var out SideResult
	client := &http.Client{Timeout: 10 * time.Second}
	created := map[string]int64{}
	weights := map[string]float64{}
	// Both schedulers mint their own IDs at ingest; the harness tracks
	// workload-ID → service-ID and maps back everywhere (status polls,
	// priority reads, stub-log join). No API carries the harness ID.
	svcIDs := map[string]string{}
	svcToWorkload := map[string]string{}
	var marker time.Time
	first := true
	for _, t := range workload {
		weights[t.Tenant] = t.Weight
		stamp, svcID, err := SubmitTask(ctx, client, svc.BaseURL, apiKey, t, time.Now())
		if err != nil {
			return out, err
		}
		if first {
			marker = stamp
			first = false
		}
		created[t.ID] = stamp.UnixMilli()
		svcIDs[t.ID] = svcID
		svcToWorkload[svcID] = t.ID
	}
	// Quiescence: wait until the calculator tagged everything. Dispatches
	// observed so far form the warmup prefix (startup transient).
	// Empty DSN skips the wait (fakes/backends without SQL visibility);
	// live runs must pass a DSN or the transient is unmeasured.
	warmup := 0
	if svc.DSN != "" {
		var err error
		warmup, err = waitQuiesced(ctx, svc.DSN, stub, drainTimeout)
		if err != nil {
			return out, err
		}
	}
	// Drain: every submitted task terminal (or deadline → loud failure,
	// never a silent partial comparison).
	deadline := time.Now().Add(drainTimeout)
	for {
		done, err := allTerminalSvc(ctx, client, svc, apiKey, svcIDs)
		if err != nil {
			return out, err
		}
		if done {
			break
		}
		if time.Now().After(deadline) {
			return out, fmt.Errorf("differential: side %s did not drain in %v", svc.Name, drainTimeout)
		}
		select {
		case <-ctx.Done():
			return out, ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	entries, err := stub.Close(ctx)
	if err != nil {
		return out, err
	}
	var order []DispatchRecord
	seq := 0
	for _, e := range entries {
		wid, ok := svcToWorkload[e.ID]
		if !ok {
			return out, fmt.Errorf("differential: stub dispatched unknown id %q", e.ID)
		}
		tenant := ""
		for _, t := range workload {
			if t.ID == wid {
				tenant = t.Tenant
				break
			}
		}
		order = append(order, DispatchRecord{Seq: seq, TaskID: wid, Tenant: tenant})
		seq++
	}
	// Priorities: read back per task (feeds tie groups; absent priorities
	// degrade tie detection toward "everything ties", which can only
	// suppress ordering verdicts, never invent them — but a read failure
	// is still loud, not a default, per the harness no-silent-default rule.
	priorities, err := fetchPrioritiesSvc(ctx, client, svc, apiKey, svcIDs)
	if err != nil {
		return out, err
	}
	for i := range order {
		order[i].Priority = priorities[order[i].TaskID]
	}
	out = SideResult{Log: RunLog{Weights: weights, Order: order, Created: created}, Marker: marker, Warmup: warmup}
	return out, nil
}

func allTerminalSvc(ctx context.Context, client *http.Client, svc SideConfig, apiKey string, svcIDs map[string]string) (bool, error) {
	for _, id := range svcIDs {
		st, err := fetchStatus(ctx, client, svc, apiKey, id)
		if err != nil {
			return false, err
		}
		switch st {
		case "SUCCEEDED", "FAILED", "TIMEOUT":
		default:
			return false, nil
		}
	}
	return true, nil
}

func fetchStatus(ctx context.Context, client *http.Client, svc SideConfig, apiKey, id string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", svc.BaseURL+"/api/v1/tasks/"+id, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("X-API-Key", apiKey)
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("differential: side %s status %s: %w", svc.Name, id, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("differential: side %s status %s: HTTP %d", svc.Name, id, resp.StatusCode)
	}
	var body struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", err
	}
	return body.Status, nil
}

func fetchPrioritiesSvc(ctx context.Context, client *http.Client, svc SideConfig, apiKey string, svcIDs map[string]string) (map[string]int64, error) {
	out := map[string]int64{}
	for wid, id := range svcIDs {
		req, err := http.NewRequestWithContext(ctx, "GET", svc.BaseURL+"/api/v1/tasks/"+id, nil)
		if err != nil {
			return nil, fmt.Errorf("differential: side %s priority request %s: %w", svc.Name, id, err)
		}
		req.Header.Set("X-API-Key", apiKey)
		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("differential: side %s priority %s: %w", svc.Name, id, err)
		}
		var body struct {
			Priority *int64 `json:"priority"`
		}
		derr := json.NewDecoder(resp.Body).Decode(&body)
		resp.Body.Close()
		// Non-2xx and decode failures are loud, never defaults: missing
		// priorities inflate every task into one giant tie group, which
		// neuters ordering diagnostics silently. Same class as the RPS
		// 401-to-zero bug — plausible-looking defaults instead of errors.
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("differential: side %s priority %s: HTTP %d", svc.Name, id, resp.StatusCode)
		}
		if derr != nil {
			return nil, fmt.Errorf("differential: side %s priority %s decode: %w", svc.Name, id, derr)
		}
		if body.Priority != nil {
			out[wid] = *body.Priority
		}
	}
	return out, nil
}

// ResetDB truncates scheduler tables and zeroes system virtual time so
// repeated runs start from identical state. Residue from a prior run would
// otherwise pollute shares (same keys) and tagging (persisted V) —
// repeated runs would measure history, not the workload. Test-only by
// construction: it takes a DSN, never a repository.
func ResetDB(ctx context.Context, dsn string) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("differential: reset connect: %w", err)
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, `TRUNCATE tasks, client_counts, client_sequence_state, client_virtual_time`)
	if err != nil {
		return fmt.Errorf("differential: reset truncate: %w", err)
	}
	_, err = conn.Exec(ctx, `UPDATE scheduler_virtual_clock SET virtual_time = 0`)
	if err != nil {
		return fmt.Errorf("differential: reset clock: %w", err)
	}
	return nil
}

// TracePoint is one 5s sample of scheduler state during a live run.
// Three series per side (RPS, cumulative dispatched, cumulative promoted)
// attribute divergence to ramp timing vs scheduling: if RPS trajectories
// match and shares still diverge, the residual is a real difference.
type TracePoint struct {
	At         time.Time `json:"at"`
	RPS        float64   `json:"rps"`
	Dispatched int       `json:"dispatched"`
	Promoted   int       `json:"promoted"`
}

// Result is the published artifact for one comparison.
type Result struct {
	Resolved Resolved `json:"resolved"`
	Method   string   `json:"methodology"`
	// Traces holds per-side sample series keyed by side name. Empty when
	// tracing was disabled; presence is what makes timing-attribution
	// possible after the fact.
	Traces      map[string][]TracePoint `json:"traces,omitempty"`
	Calibration []string                `json:"calibration"`
	Pass        bool                    `json:"pass"`
	Mismatch    *Mismatch               `json:"mismatch,omitempty"`
}

// WriteResult publishes results.json plus methodology.md into dir: dual
// SHAs attribute the build, resolved config attributes the run, traces
// attribute timing (empty map when tracing was disabled).
func WriteResult(dir, methodology string, resolved *Resolved, javaSHA, goSHA string, calibration []string, traces map[string][]TracePoint, mm *Mismatch) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	res := Result{Resolved: *resolved, Method: methodology, Traces: traces, Calibration: calibration, Pass: mm == nil, Mismatch: mm}
	raw, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "results.json"), raw, 0o600); err != nil {
		return err
	}
	doc := "# Methodology\n\n" + methodology + "\n\nJava: " + javaSHA + "\nGo: " + goSHA + "\n"
	return os.WriteFile(filepath.Join(dir, "methodology.md"), []byte(doc), 0o600)
}
