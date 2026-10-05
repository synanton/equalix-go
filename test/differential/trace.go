//go:build differential

package differential

import (
	"context"
	"encoding/json"
	"fmt"
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
	// fetchStat pins the /status fetch record per side. First-error-only
	// proved insufficient: on sequential runs the first miss is always the
	// pre-boot refused, which says nothing about mid-run misses (ctl-jg1:
	// 57/57 Java misses — firstErr alone would still have read
	// "connection refused" and closed the case wrongly). Failed counts
	// every miss; Last names the most recent cause.
	fetchStat map[string]*fetchStat
	cancel    context.CancelFunc
	done      chan struct{}
}

// fetchStat is the per-side /status fetch record: miss count plus first
// and most recent causes. Published in results.json beside the traces.
type fetchStat struct {
	Failed int    `json:"failed"`
	First  string `json:"first,omitempty"`
	Last   string `json:"last,omitempty"`
}

// StartTracer begins 5s sampling for each side until ctx ends or Stop is
// called. RPS comes from GET /status currentRps; counts come straight from
// each side's database (uniform SQL, no API dependency).
func StartTracer(ctx context.Context, sides map[string]Side, apiKey string) *Tracer {
	ctx, cancel := context.WithCancel(ctx)
	t := &Tracer{points: map[string][]TracePoint{}, fetchStat: map[string]*fetchStat{}, cancel: cancel, done: make(chan struct{})}
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
		p := TracePoint{At: now, RPS: -1}
		if rps, err := fetchRPS(s); err == nil {
			p.RPS = rps
		} else {
			t.mu.Lock()
			st := t.fetchStat[name]
			if st == nil {
				st = &fetchStat{}
				t.fetchStat[name] = st
			}
			st.Failed++
			if st.First == "" {
				st.First = err.Error()
			}
			st.Last = err.Error()
			t.mu.Unlock()
		}
		// RPS -1 = no reading (not zero — zero is a real throttle floor
		// value and must never be confused with a missed sample).
		p.TotalDispatched, p.Promoted = fetchCounts(s)
		t.mu.Lock()
		t.points[name] = append(t.points[name], p)
		t.mu.Unlock()
	}
}

func fetchRPS(s Side) (float64, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	req, err := http.NewRequest("GET", s.BaseURL+"/api/v1/status", nil)
	if err != nil {
		return 0, fmt.Errorf("differential: side %s status request: %w", s.Name, err)
	}
	// Both schedulers gate /status behind X-API-Key; without it Java
	// answers 401 with an empty body, which decodes to a silent 0.0 —
	// a missing-auth bug that reads exactly like "RPS flatlined at zero".
	req.Header.Set("X-API-Key", s.APIKey)
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("differential: side %s status: %w", s.Name, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("differential: side %s status: HTTP %d", s.Name, resp.StatusCode)
	}
	var body struct {
		CurrentRPS *float64 `json:"currentRps"`
		RPS        *float64 `json:"rps"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return 0, fmt.Errorf("differential: side %s status decode: %w", s.Name, err)
	}
	if body.CurrentRPS != nil {
		return *body.CurrentRPS, nil
	}
	if body.RPS != nil {
		return *body.RPS, nil
	}
	return 0, fmt.Errorf("differential: side %s status: no RPS field", s.Name)
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

// FetchStats returns the per-side /status fetch record (miss count, first
// and most recent causes). Logged by the live tests and persisted beside
// the traces so a dark sampler names both its count and its causes.
func (t *Tracer) FetchStats() map[string]fetchStat {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[string]fetchStat, len(t.fetchStat))
	for k, v := range t.fetchStat {
		out[k] = *v
	}
	return out
}

// LiveStats summarizes the successful /status reads in pts: -1 is "no
// reading" (service down, or fetch failed — see FetchStats), never a
// throttle value. Reporting first→last over raw endpoints misleads on
// sequential runs — the second side's early -1s are pre-boot, the first
// side's trailing -1s post-stop (ctl-jg2: both sides read "-1→live"
// while mid-run reads on both were healthy, 1.1→20+). The live range is
// the signal; endpoints are scheduling artifacts of the harness itself.
func LiveStats(pts []TracePoint) (live int, first, last float64) {
	for _, p := range pts {
		if p.RPS == -1 {
			continue
		}
		if live == 0 {
			first = p.RPS
		}
		last = p.RPS
		live++
	}
	return live, first, last
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
