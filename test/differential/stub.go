//go:build differential

package differential

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// DispatchEntry is one observed dispatch: task ID in arrival order with
// the stub's wall-clock receive time. Wall-clock is translated to offsets
// (ToOffsets) before comparison — never compared directly.
type DispatchEntry struct {
	ID       string
	Received time.Time
}

// StubConfig parameterizes one stub instance. A single instance serves one
// run; the next run gets a fresh instance (reset boundary = process exit,
// unambiguous — no shared state, no reset endpoint, no run tags).
type StubConfig struct {
	// Port to listen on; 0 selects a free port (tests). Production runs
	// pass an explicit parameterized port, collision-checked pre-start.
	Port int
	// Latency draws per-task completion delays (single shared value across
	// runs — identical stub behavior by construction).
	Latency LatencyConfig
	// ErrorRate in [0,1] of completions reported as failures, drawn from
	// a dedicated stream (seed+1) so failure injection never perturbs the
	// latency sequence.
	ErrorRate float64
	// CompleteBase is the scheduler's base URL receiving webhooks at
	// POST {base}/api/v1/tasks/{id}/complete.
	CompleteBase string
	// APIKey authenticates webhook callbacks.
	APIKey string
}

// Stub is the single harness-owned executor both schedulers target
// (PROTOCOL.md). One instance per run; the dispatch log is obtainable
// only via Close, so capture-after-teardown ordering is enforced by API:
// [start stub] → [start scheduler] → [ingest] → [drain] → [teardown] →
// [capture log]. Reading mid-run or across runs is unrepresentable.
type Stub struct {
	cfg      StubConfig
	sampler  *Sampler
	errRng   *rand.Rand
	server   *http.Server
	listener net.Listener

	mu      sync.Mutex
	entries []DispatchEntry
	closed  bool
}

// NewStub validates config and builds an unstarted stub.
func NewStub(cfg StubConfig) (*Stub, error) {
	if err := cfg.Latency.Validate(); err != nil {
		return nil, err
	}
	if cfg.ErrorRate < 0 || cfg.ErrorRate > 1 {
		return nil, fmt.Errorf("differential: error rate must be in [0,1]: %v", cfg.ErrorRate)
	}
	if cfg.CompleteBase == "" {
		return nil, fmt.Errorf("differential: stub needs a scheduler complete base URL")
	}
	sampler, err := NewSampler(cfg.Latency)
	if err != nil {
		return nil, err
	}
	// Error decisions ride a separate stream (seed+1) so failure
	// injection never perturbs the latency sequence: same seed always
	// yields the same completion timeline regardless of error rate.
	return &Stub{cfg: cfg, sampler: sampler, errRng: rand.New(rand.NewSource(cfg.Latency.Seed + 1))}, nil
}

// Start binds and serves. Fails fast naming the port (a squat is a config
// error, never a hang). Startup is a bare Listen — no warmup, no readiness
// window — so no startup timeout is needed on the stub itself.
func (s *Stub) Start() (addr string, err error) {
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", s.cfg.Port))
	if err != nil {
		return "", fmt.Errorf("differential: stub bind :%d: %w", s.cfg.Port, err)
	}
	s.listener = ln
	mux := http.NewServeMux()
	mux.HandleFunc("/tasks/", s.handleExecute)
	s.server = &http.Server{Handler: mux}
	go func() { _ = s.server.Serve(ln) }()
	return ln.Addr().String(), nil
}

func (s *Stub) handleExecute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/tasks/"), "/execute")
	if id == "" || strings.Contains(id, "/") || len(id) > 256 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	now := time.Now()
	s.mu.Lock()
	s.entries = append(s.entries, DispatchEntry{ID: id, Received: now})
	s.mu.Unlock()
	w.WriteHeader(http.StatusOK)

	delay := s.sampler.Next()
	failed := s.cfg.ErrorRate > 0 && s.nextErr()
	go func() {
		time.Sleep(time.Duration(delay) * time.Millisecond)
		s.complete(id, !failed)
	}()
}

// nextErr draws under lock: the error stream must be deterministic across
// identical dispatch orders.
func (s *Stub) nextErr() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.errRng.Float64() < s.cfg.ErrorRate
}

func (s *Stub) complete(id string, success bool) {
	body, _ := json.Marshal(map[string]any{"success": success})
	if !success {
		body, _ = json.Marshal(map[string]any{"success": false, "error": "stub injected failure"})
	}
	url := fmt.Sprintf("%s/api/v1/tasks/%s/complete", s.cfg.CompleteBase, id)
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", s.cfg.APIKey)
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
}

// Close tears down the server and returns the captured dispatch log. The
// log exists only here — after Close the instance is spent. Next run
// constructs a fresh Stub: reset boundary is process (instance) exit.
func (s *Stub) Close(ctx context.Context) ([]DispatchEntry, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, fmt.Errorf("differential: stub already closed")
	}
	s.closed = true
	out := make([]DispatchEntry, len(s.entries))
	copy(out, s.entries)
	s.mu.Unlock()
	if err := s.server.Shutdown(ctx); err != nil {
		return nil, fmt.Errorf("differential: stub shutdown: %w", err)
	}
	return out, nil
}
