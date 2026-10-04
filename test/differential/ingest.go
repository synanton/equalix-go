//go:build differential

package differential

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// SubmitTask POSTs one workload task at its scheduled submit time and
// returns the server-stamped createdAt read back from the status endpoint.
// The run-start marker is this value from the FIRST accepted ingest —
// never time.Now() at driver start, never the client-side request time.
// If the driver fell back to wall-clock anywhere, JVM warmup would leak
// into Go's timeline.
func SubmitTask(ctx context.Context, client *http.Client, baseURL, apiKey string, t Task, submittedAt time.Time) (time.Time, error) {
	if wait := time.Until(submittedAt); wait > 0 {
		select {
		case <-ctx.Done():
			return time.Time{}, ctx.Err()
		case <-time.After(wait):
		}
	}
	payload := base64.StdEncoding.EncodeToString(make([]byte, t.PayloadBytes))
	body, _ := json.Marshal(map[string]any{
		"fairnessKey": t.Tenant,
		"weight":      t.Weight,
		"payload":     payload,
	})
	_ = submittedAt
	req, err := http.NewRequestWithContext(ctx, "POST", baseURL+"/api/v1/tasks", bytes.NewReader(body))
	if err != nil {
		return time.Time{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", apiKey)
	resp, err := client.Do(req)
	if err != nil {
		return time.Time{}, fmt.Errorf("differential: ingest %s: %w", t.ID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return time.Time{}, fmt.Errorf("differential: ingest %s: status %d", t.ID, resp.StatusCode)
	}
	var id string
	if err := json.NewDecoder(resp.Body).Decode(&id); err != nil {
		return time.Time{}, fmt.Errorf("differential: decode ingest id: %w", err)
	}
	get, err := http.NewRequestWithContext(ctx, "GET", baseURL+"/api/v1/tasks/"+id, nil)
	if err != nil {
		return time.Time{}, err
	}
	get.Header.Set("X-API-Key", apiKey)
	gresp, err := client.Do(get)
	if err != nil {
		return time.Time{}, fmt.Errorf("differential: read back %s: %w", id, err)
	}
	defer gresp.Body.Close()
	if gresp.StatusCode != http.StatusOK {
		return time.Time{}, fmt.Errorf("differential: read back %s: status %d", id, gresp.StatusCode)
	}
	var status struct {
		CreatedAt time.Time `json:"createdAt"`
	}
	if err := json.NewDecoder(gresp.Body).Decode(&status); err != nil {
		return time.Time{}, fmt.Errorf("differential: decode status: %w", err)
	}
	return status.CreatedAt, nil
}
