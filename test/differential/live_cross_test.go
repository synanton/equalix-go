//go:build differential

package differential

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	pgadapter "github.com/synanton/equalix-go/internal/adapter/postgres"
	redisadapter "github.com/synanton/equalix-go/internal/adapter/redis"
)

// crossMetrics is the minimal port.Metrics for the accuracy probe:
// only construction needs it (no observations asserted here).
type crossMetrics struct{}

func (crossMetrics) RecordDispatch(string)                  {}
func (crossMetrics) RecordCompletion(string, string, int64) {}
func (crossMetrics) ObserveDispatchLatency(float64)         {}
func (crossMetrics) ObserveTimeoutLatency(float64)          {}
func (crossMetrics) ObserveWatchdogReconciliation(float64)  {}
func (crossMetrics) ObserveCMSWarmup(float64)               {}
func (crossMetrics) SetRPS(float64)                         {}
func (crossMetrics) SetQueueDepth(int)                      {}
func (crossMetrics) PublishDrift(map[string]int64)          {}
func (crossMetrics) SetCMSDegraded(bool)                    {}

// TestLiveCrossInstance is the EQLX-8 evidence run: two IDENTICAL Go
// binaries share one Postgres database and one Redis, ingesting once
// (via A) and dispatching competitively (SKIP LOCKED). It gates what
// the README's scaling posture needs:
//
//  1. Fleet-aggregate shares: combined dispatches from both stubs hit
//     1:2:7 (per-instance shares are reported, never gated — two
//     instances each see ~half the workload and sample noisily).
//  2. No double-dispatch: every workload task dispatched exactly once
//     across both stubs (same DB rows contested via SKIP LOCKED +
//     version guard; stub entries carry DB UUIDs, joinable to the
//     ingest map — an unknown stub ID fails loudly, proving the join).
//  3. CMS estimate accuracy (supporting number, recorded not gated):
//     post-drain shared estimates per tenant.
//
// Requires: EQUALIX_LIVE_COMPARE=1, EQUALIX_GO_BIN, EQUALIX_GO_DSN
// (shared database — same value both sides by design), a Redis on
// EQUALIX_REDIS_ADDR (default 127.0.0.1:6379), w2000 workload, cold
// ASAP burst (mixing regime, same as the EQLX-5 cold class).
func TestLiveCrossInstance(t *testing.T) {
	if os.Getenv("EQUALIX_LIVE_COMPARE") != "1" {
		t.Skip("live comparison needs EQUALIX_LIVE_COMPARE=1")
	}
	for _, k := range []string{"EQUALIX_GO_BIN", "EQUALIX_GO_DSN"} {
		if os.Getenv(k) == "" {
			t.Fatalf("missing env %s", k)
		}
	}
	apiKey := "live-compare-key"
	ctx := context.Background()
	workload, err := Load(filepath.Join("workloads", "w2000.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	dsn := os.Getenv("EQUALIX_GO_DSN")
	redisAddr := os.Getenv("EQUALIX_REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "127.0.0.1:6379"
	}

	startSide := func(name, svcURL string, svcPort, stubPort int) (*Proc, *Stub) {
		stub, err := NewStub(StubConfig{
			Port: stubPort, Latency: DefaultLatency(),
			CompleteBase: svcURL, APIKey: apiKey,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := stub.Start(); err != nil {
			t.Fatalf("side %s stub: %v", name, err)
		}
		proc, err := Launch(ctx, ProcSpec{
			Name: name, Bin: os.Getenv("EQUALIX_GO_BIN"),
			Args: []string{
				"--dsn", dsn,
				"--addr", "127.0.0.1:" + itoa(svcPort), "--api-key", apiKey,
				"--executor-base-url", "http://127.0.0.1:" + itoa(stubPort),
				"--redis-enabled", "--redis-url", redisAddr,
			},
			Env:            map[string]string{},
			ReadyURL:       svcURL + "/api/v1/status",
			StartupTimeout: 60 * time.Second,
		})
		if err != nil {
			t.Fatalf("side %s: %v", name, err)
		}
		return proc, stub
	}
	procA, stubA := startSide("goA", "http://127.0.0.1:18087", 18087, 18097)
	defer procA.Stop()
	procB, stubB := startSide("goB", "http://127.0.0.1:18088", 18088, 18098)
	defer procB.Stop()
	// One shared database: schema healed once, residue cleared once
	// (both sides see the same rows from here on).
	if err := pgadapter.EnsureSchema(ctx, dsn); err != nil {
		t.Fatalf("shared schema: %v", err)
	}
	if err := ResetDB(ctx, dsn); err != nil {
		t.Fatalf("shared reset: %v", err)
	}

	// Ingest once, via A: one row per workload task, contested by both
	// dispatchers from here on.
	client := &http.Client{Timeout: 10 * time.Second}
	svcIDs := map[string]string{}
	weights := map[string]float64{}
	for _, task := range workload {
		weights[task.Tenant] = task.Weight
		_, svcID, err := SubmitTask(ctx, client, "http://127.0.0.1:18087", apiKey, task, time.Now())
		if err != nil {
			t.Fatalf("ingest %s: %v", task.ID, err)
		}
		svcIDs[task.ID] = svcID
	}
	// Drain: every task terminal (either service may complete any row).
	deadline := time.Now().Add(180 * time.Second)
	svc := SideConfig{Name: "fleet", BaseURL: "http://127.0.0.1:18087", DSN: dsn, APIKey: apiKey}
	for {
		done, err := allTerminalSvc(ctx, client, svc, apiKey, svcIDs)
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("differential: fleet did not drain in 180s")
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
	entriesA, err := stubA.Close(ctx)
	if err != nil {
		t.Fatal(err)
	}
	entriesB, err := stubB.Close(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Join stub entries (DB UUIDs, both stubs) back to workload tenants,
	// ordered by stub receive time (same host clock on both stubs —
	// stated, not hidden). Unknown IDs fail loudly: the
	// no-double-dispatch proof depends on the join being total.
	toWorkload := map[string]string{}
	for wid, sid := range svcIDs {
		toWorkload[sid] = wid
	}
	byTenant := map[string]string{}
	for _, wt := range workload {
		byTenant[wt.ID] = wt.Tenant
	}
	type timedEntry struct {
		at time.Time
		id string
	}
	var all []timedEntry
	for _, e := range append(append([]DispatchEntry{}, entriesA...), entriesB...) {
		wid, ok := toWorkload[e.ID]
		if !ok {
			t.Fatalf("differential: stub dispatched unknown id %q (join broken)", e.ID)
		}
		all = append(all, timedEntry{at: e.Received, id: wid})
	}
	sort.SliceStable(all, func(i, j int) bool { return all[i].at.Before(all[j].at) })
	seen := map[string]int{}
	var order []DispatchRecord
	for i, e := range all {
		seen[e.id]++
		order = append(order, DispatchRecord{Seq: i, TaskID: e.id, Tenant: byTenant[e.id]})
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("differential: task %s dispatched %d times (SKIP LOCKED violated)", id, n)
		}
	}
	if len(seen) != len(workload) {
		t.Fatalf("differential: %d unique dispatches for %d tasks", len(seen), len(workload))
	}
	t.Logf("fleet dispatches=%d unique=%d (no double-dispatch)", len(order), len(seen))
	perSide := map[string]int{"goA": len(entriesA), "goB": len(entriesB)}
	t.Logf("per-side dispatches (reported, never gated): %v", perSide)

	// Fleet-aggregate shares gate. Bound ±6, control-derived (NOT the
	// single-instance ±2): five identical fleet runs deviate at most 5
	// per window, shapes varying run to run while staying symmetric
	// across both windows within each run (suspected startup
	// phase-lock between the racers — persistent within a run, random
	// across runs; hypothesis, not established). Widening follows the
	// EQLX-5 precedent (bound from control agreement); the derivation,
	// not the number, is what makes it honest. N=5 to date; re-derive
	// if the fleet grows beyond two racers.
	fleet := RunLog{Weights: weights, Order: order, Created: map[string]int64{}}
	results, mm := CompareShares(fleet, 1000, 6)
	for _, w := range results {
		if w.Full {
			t.Logf("fleet window %d deviations: %v (bound ±2)", w.Window, w.Deviations)
		}
	}
	// CMS accuracy, supporting number: post-drain shared estimates per
	// tenant (everything terminal — near-zero expected, collisions aside).
	probe, err := redisadapter.New(ctx, redisadapter.Config{Addr: redisAddr, Timeout: 5 * time.Second}, 5, 65536, crossMetrics{})
	if err != nil {
		t.Fatalf("accuracy probe: %v", err)
	}
	defer probe.Close()
	accuracy := map[string]int64{}
	for _, tenant := range []string{"a", "b", "c"} {
		v, err := probe.EstimateCount(ctx, tenant)
		if err != nil {
			t.Fatalf("accuracy %s: %v", tenant, err)
		}
		accuracy[tenant] = v
	}
	t.Logf("post-drain shared estimates (supporting): %v", accuracy)

	outDir := os.Getenv("EQUALIX_RESULTS_DIR")
	if outDir == "" {
		outDir = "results-live-xfleet01"
	}
	artifact := map[string]interface{}{
		"methodology":  "EQLX-8 cross-instance: two identical Go binaries, shared PG + shared Redis, w2000 burst cold, fleet-aggregate gate",
		"dispatches":   len(order),
		"unique":       len(seen),
		"per_side":     perSide,
		"accuracy":     accuracy,
		"pass":         mm == nil,
		"java_sha":     "n/a (go-only leg)",
		"go_sha":       shaOr("EQUALIX_GO_SHA", "go-unrecorded"),
		"workload_sha": shaOr("EQUALIX_WORKLOAD_SHA", "workload-unrecorded"),
	}
	if mm != nil {
		artifact["mismatch"] = mm.Detail
	}
	raw, err := json.MarshalIndent(artifact, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outDir, "results.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if mm != nil {
		t.Fatalf("FLEET DIVERGENCE: %v", mm)
	}
}
