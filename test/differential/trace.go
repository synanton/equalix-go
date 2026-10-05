//go:build differential

package differential

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// Tracer samples RPS + dispatched + promoted per side at a fixed interval.
// Started before the runs, stopped after drain; the series go into
// results.json so divergence is attributable to ramp timing (all three
// series move together) vs scheduling (shares diverge while series match).
type Tracer struct {
	mu     sync.Mutex
	points map[string][]TracePoint
	cancel context.CancelFunc
	done   chan struct{}
}

// StartTracer begins 5s sampling for each side until ctx ends or Stop is
// called. RPS comes from GET /status currentRps; counts come straight from
// each side's database (uniform SQL, no API dependency).
func StartTracer(ctx context.Context, sides map[string]Side, apiKey string) *Tracer {
	ctx, cancel := context.WithCancel(ctx)
	t := &Tracer{points: map[string][]TracePoint{}, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(t.done)
		t.sample(sides, apiKey)
		tick := time.NewTicker(5 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				t.sample(sides, apiKey)
			}
		}
	}()
	return t
}

// Side is the per-side wiring a tracer needs: status endpoint, DB, key.
type Side struct {
	Name    string
	BaseURL string
	DSN     string
	APIKey  string
}

func (t *Tracer) sample(sides map[string]Side, _ string) {
	now := time.Now()
	for name, s := range sides {
		p := TracePoint{At: now}
		p.RPS = fetchRPS(s)
		p.Dispatched, p.Promoted = fetchCounts(s)
		t.mu.Lock()
		t.points[name] = append(t.points[name], p)
		t.mu.Unlock()
	}
}

func fetchRPS(s Side) float64 {
	client := &http.Client{Timeout: 5 * time.Second}
	req, err := http.NewRequest("GET", s.BaseURL+"/api/v1/status", nil)
	if err != nil {
		return 0
	}
	// Both schedulers gate /status behind X-API-Key; without it Java
	// answers 401 with an empty body, which decodes to a silent 0.0 —
	// a missing-auth bug that reads exactly like "RPS flatlined at zero".
	req.Header.Set("X-API-Key", s.APIKey)
	req.Header.Set("X-API-Key", s.APIKey)
	resp, err := client.Do(req)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	var body struct {
		CurrentRPS *float64 `json:"currentRps"`
		RPS        *float64 `json:"rps"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body.CurrentRPS != nil {
		return *body.CurrentRPS
	}
	if body.RPS != nil {
		return *body.RPS
	}
	return 0
}

func fetchCounts(s Side) (dispatched, promoted int) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, s.DSN)
	if err != nil {
		return 0, 0
	}
	defer conn.Close(ctx)
	_ = conn.QueryRow(ctx, `SELECT COUNT(*) FROM tasks WHERE status <> 'RECEIVED'`).Scan(&dispatched)
	_ = conn.QueryRow(ctx, `SELECT COUNT(*) FROM tasks WHERE priority <= 0`).Scan(&promoted)
	return dispatched, promoted
}

// Stop ends sampling and returns the series. Call after drain, before
// WriteResult; the map is a copy, safe to marshal.
func (t *Tracer) Stop() map[string][]TracePoint {
	t.cancel()
	<-t.done
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[string][]TracePoint, len(t.points))
	for k, v := range t.points {
		cp := make([]TracePoint, len(v))
		copy(cp, v)
		out[k] = cp
	}
	return out
}
