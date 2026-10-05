//go:build differential

package differential

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

// WarmupConfig is the warm workload class: N throwaway tasks bring the RPS
// controller up before measurement starts. Tasks == 0 is the cold class —
// identical to every run before the warm variant existed (no pre-phase,
// measurement from stub entry 0). Classes are never cross-compared: the
// cold avalanche is a workload-shape effect, and JvG-warm vs GvG-cold
// would measure the workload, not the implementations.
//
// Env: EQUALIX_WARMUP_TASKS (default 0), EQUALIX_WARMUP_RPS (default 15),
// EQUALIX_WARMUP_TIMEOUT (default 180s).
type WarmupConfig struct {
	Tasks   int
	RPS     float64
	Timeout time.Duration
}

// WarmupFromEnv reads the warm-up class knobs. Unset or unparseable means
// cold (Tasks 0) — a mistyped knob degrades to the historical behavior,
// never to a half-warmed run.
func WarmupFromEnv() WarmupConfig {
	cfg := WarmupConfig{RPS: 15, Timeout: 180 * time.Second}
	if v := os.Getenv("EQUALIX_WARMUP_TASKS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.Tasks = n
		}
	}
	if v := os.Getenv("EQUALIX_WARMUP_RPS"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			cfg.RPS = f
		}
	}
	if v := os.Getenv("EQUALIX_WARMUP_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			cfg.Timeout = d
		}
	}
	return cfg
}

// Class names the workload class for methodology strings and result dirs.
func (c WarmupConfig) Class() string {
	if c.Tasks <= 0 {
		return "cold"
	}
	return fmt.Sprintf("warm-%d-rps%.0f", c.Tasks, c.RPS)
}

// RunWarmup submits cfg.Tasks throwaway tasks (synthetic IDs, workload
// tenants/weights cycled 1:2:7-style), waits for the controller to reach
// cfg.RPS, drains them to terminal, and returns the stub dispatch count —
// the measurement offset RunSide slices from. The pre-phase exists to move
// the RPS ramp out of the measurement window: cold runs intermittently
// avalanche window 0 (spec NOTE, promotion asymmetry); warm runs measure
// the controller at operating RPS on both sides.
func RunWarmup(ctx context.Context, client *http.Client, svc SideConfig, apiKey string, workload []Task, stub *Stub, cfg WarmupConfig) (int, error) {
	tenants := []string{}
	weights := map[string]float64{}
	for _, t := range workload {
		if _, ok := weights[t.Tenant]; !ok {
			tenants = append(tenants, t.Tenant)
			weights[t.Tenant] = t.Weight
		}
	}
	if len(tenants) == 0 {
		return 0, fmt.Errorf("differential: side %s warmup needs workload tenants", svc.Name)
	}
	svcIDs := map[string]string{}
	for i := 0; i < cfg.Tasks; i++ {
		tenant := tenants[i%len(tenants)]
		wt := Task{ID: fmt.Sprintf("warm-%s-%d", svc.Name, i), Tenant: tenant, Weight: weights[tenant]}
		_, svcID, err := SubmitTask(ctx, client, svc.BaseURL, apiKey, wt, time.Now())
		if err != nil {
			return 0, err
		}
		svcIDs[wt.ID] = svcID
	}
	// RPS gate: the controller must report at/above floor before the
	// measurement starts. fetchRPS errors here are loud (unlike the
	// tracer, which tolerates a downed service): a warm-up that cannot
	// read RPS cannot claim to be warm.
	side := Side{Name: svc.Name, BaseURL: svc.BaseURL, APIKey: apiKey}
	deadline := time.Now().Add(cfg.Timeout)
	for {
		rps, err := fetchRPS(side)
		if err != nil {
			return 0, err
		}
		if rps >= cfg.RPS {
			break
		}
		if time.Now().After(deadline) {
			return 0, fmt.Errorf("differential: side %s warmup RPS %.1f < floor %.0f in %v", svc.Name, rps, cfg.RPS, cfg.Timeout)
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(time.Second):
		}
	}
	drain, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	for {
		done, err := allTerminalSvc(drain, client, svc, apiKey, svcIDs)
		if err != nil {
			return 0, err
		}
		if done {
			return stub.Count(), nil
		}
		select {
		case <-drain.Done():
			return 0, fmt.Errorf("differential: side %s warmup did not drain in %v", svc.Name, cfg.Timeout)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// SideResult is one scheduler's observed run: dispatch log plus the
// run-start marker (first accepted ingest's server stamp) plus the warmup
// prefix length (dispatches that fired before calculator quiescence —
// startup transient, excluded from ordering comparison, included in
// shares per the scope's window rule). SpawnedAt/ReadyAt are the harness
// T0/T1 (process spawn, first readiness-URL response); FirstDispatch is
// T2 (first stub dispatch receive time, zero when nothing dispatched).
type SideResult struct {
	Log    RunLog
	Marker time.Time
	Warmup int
	SpawnedAt time.Time
	ReadyAt   time.Time
	FirstDispatch time.Time
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
// measureFrom slices the stub log: entries below it are the warm-up
// pre-phase (RunWarmup's return), excluded from order, marker, and warmup
// counts. Zero is the cold class — the whole log is the measurement.
func RunSide(ctx context.Context, svc SideConfig, apiKey string, workload []Task, stub *Stub, drainTimeout time.Duration, measureFrom int) (SideResult, error) {
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
		// waitQuiesced counts all stub dispatches including the pre-phase;
		// the measurement's warmup prefix starts at measureFrom.
		warmup -= measureFrom
		if warmup < 0 {
			warmup = 0
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
	// Slice the pre-phase: warm-up dispatches carry synthetic IDs absent
	// from the workload map, so slicing (not lookup-skipping) keeps the
	// unknown-id fatal below a true invariant for the measurement.
	if measureFrom < 0 {
		measureFrom = 0
	}
	if measureFrom > len(entries) {
		return out, fmt.Errorf("differential: side %s measure offset %d beyond %d stub entries", svc.Name, measureFrom, len(entries))
	}
	entries = entries[measureFrom:]
	var order []DispatchRecord
	// T2 rides the final literal below (a fresh SideResult would clobber a
	// field set here): capture the first receive time in a local.
	var firstDispatch time.Time
	if len(entries) > 0 {
		firstDispatch = entries[0].Received
	}
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
	out.FirstDispatch = firstDispatch
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

// StartupInfo is one side's spawn → ready → first-dispatch record: the
// matrix's runtime-characterization row (informational, never gated).
// T0/T1 are harness-measured (process spawn, first readiness-URL HTTP
// response); T2 is the first stub dispatch receive time. Service-side
// stdout milestones (main entry, context built, client wired) give the
// internal phase breakdown when a cell needs drill-down; the harness does
// not parse them (interleaved child stdout is not a data channel).
// Cold-vs-warm JVM cache must be declared alongside any Java cell:
// page-cache-warm starts run 2–3x faster, and CI runners are warm.
type StartupInfo struct {
	Spawn        time.Time `json:"spawn"`
	Ready        time.Time `json:"ready"`
	FirstDispatch time.Time `json:"first_dispatch"`
	// Derived phase durations, milliseconds.
	SpawnToReadyMs       float64 `json:"spawn_to_ready_ms"`
	ReadyToFirstDispatchMs float64 `json:"ready_to_first_dispatch_ms"`
}

// NewStartup builds the record plus its phase durations. Zero times (side
// never became ready / never dispatched) yield zero durations, not
// garbage: a missing phase must read as missing.
func NewStartup(spawn, ready, first time.Time) StartupInfo {
	out := StartupInfo{Spawn: spawn, Ready: ready, FirstDispatch: first}
	if !spawn.IsZero() && !ready.IsZero() {
		out.SpawnToReadyMs = float64(ready.Sub(spawn).Milliseconds())
	}
	if !ready.IsZero() && !first.IsZero() {
		out.ReadyToFirstDispatchMs = float64(first.Sub(ready).Milliseconds())
	}
	return out
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
	// StatusFetch pins the per-side /status fetch record (miss count, first
	// and most recent causes). A trace full of -1s without this names no
	// cause (ctl-jg1 run1: 57/57 Java misses, cause unrecoverable post-hoc);
	// with it the verdict reader sees pre-boot refused vs mid-run 401 vs
	// decode directly, with counts.
	StatusFetch map[string]fetchStat `json:"status_fetch,omitempty"`
	// Warmup records the quiescence-prefix length per side: ordering
	// diagnostics without it cannot separate transient from steady state.
	Warmup map[string]int `json:"warmup,omitempty"`
	// Startup holds the per-side spawn → ready → first-dispatch record:
	// runtime characterization for the matrix, never an acceptance input.
	Startup map[string]StartupInfo `json:"startup,omitempty"`
	Pass    bool                   `json:"pass"`
	Mismatch *Mismatch            `json:"mismatch,omitempty"`
}

// Artifact is the full WriteResult input: eleven positional params proved
// to be a readability cliff, so the published-artifact fields travel as
// one struct. Callers fill what their leg measures.
type Artifact struct {
	Dir         string
	Method      string
	Resolved    *Resolved
	JavaSHA     string
	GoSHA       string
	Calibration []string
	Traces      map[string][]TracePoint
	Fetch       map[string]fetchStat
	Warmup      map[string]int
	Startup     map[string]StartupInfo
	MM          *Mismatch
}

// WriteResult publishes results.json plus methodology.md into dir: dual
// SHAs attribute the build, resolved config attributes the run, traces
// attribute timing (empty map when tracing was disabled).
func WriteResult(a Artifact) error {
	if err := os.MkdirAll(a.Dir, 0o755); err != nil {
		return err
	}
	res := Result{Resolved: *a.Resolved, Method: a.Method, Traces: a.Traces, Calibration: a.Calibration, StatusFetch: a.Fetch, Warmup: a.Warmup, Startup: a.Startup, Pass: a.MM == nil, Mismatch: a.MM}
	raw, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(a.Dir, "results.json"), raw, 0o600); err != nil {
		return err
	}
	doc := "# Methodology\n\n" + a.Method + "\n\nJava: " + a.JavaSHA + "\nGo: " + a.GoSHA + "\n"
	return os.WriteFile(filepath.Join(a.Dir, "methodology.md"), []byte(doc), 0o600)
}
