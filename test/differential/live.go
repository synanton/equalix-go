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
)

// SideResult is one scheduler's observed run: dispatch log plus the
// run-start marker (first accepted ingest's server stamp).
type SideResult struct {
	Log    RunLog
	Marker time.Time
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
	// Priorities: best-effort read-back per task (feeds tie groups; absent
	// priorities degrade tie detection toward "everything ties", which can
	// only suppress ordering verdicts, never invent them).
	priorities := fetchPrioritiesSvc(ctx, client, svc, apiKey, svcIDs)
	for i := range order {
		order[i].Priority = priorities[order[i].TaskID]
	}
	out = SideResult{Log: RunLog{Weights: weights, Order: order, Created: created}, Marker: marker}
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

func fetchPrioritiesSvc(ctx context.Context, client *http.Client, svc SideConfig, apiKey string, svcIDs map[string]string) map[string]int64 {
	out := map[string]int64{}
	for wid, id := range svcIDs {
		req, err := http.NewRequestWithContext(ctx, "GET", svc.BaseURL+"/api/v1/tasks/"+id, nil)
		if err != nil {
			continue
		}
		req.Header.Set("X-API-Key", apiKey)
		resp, err := client.Do(req)
		if err != nil {
			continue
		}
		var body struct {
			Priority *int64 `json:"priority"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&body)
		resp.Body.Close()
		if body.Priority != nil {
			out[wid] = *body.Priority
		}
	}
	return out
}

// Result is the published artifact for one comparison.
type Result struct {
	Resolved Resolved  `json:"resolved"`
	Method   string    `json:"methodology"`
	Pass     bool      `json:"pass"`
	Mismatch *Mismatch `json:"mismatch,omitempty"`
}

// WriteResult publishes results.json plus methodology.md into dir: dual
// SHAs attribute the build, resolved config attributes the run.
func WriteResult(dir, methodology string, resolved *Resolved, javaSHA, goSHA string, mm *Mismatch) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	res := Result{Resolved: *resolved, Method: methodology, Pass: mm == nil, Mismatch: mm}
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
