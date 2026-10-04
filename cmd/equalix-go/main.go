// Command equalix-go runs the Equalix fair scheduler service.
//
// First wiring commit (EQLX-3a): config from flags/env, Postgres pool,
// readiness probes (fail → exit non-zero), chi HTTP surface, runner with
// placeholder jobs, graceful shutdown on SIGINT/SIGTERM. Dispatcher and
// calculator jobs land next; until then ingested tasks persist as RECEIVED.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	chiadapter "github.com/synanton/equalix-go/internal/adapter/http"
	pgadapter "github.com/synanton/equalix-go/internal/adapter/postgres"
	"github.com/synanton/equalix-go/internal/domain"
	"github.com/synanton/equalix-go/internal/jobs"
	"github.com/synanton/equalix-go/internal/port"
	"github.com/synanton/equalix-go/pkg/cms"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "equalix-go:", err)
		os.Exit(1)
	}
}

// localCMS is a mutex-guarded in-memory CMSStore (single-instance mode).
type localCMS struct {
	mu sync.Mutex
	s  *cms.Sketch
}

func (c *localCMS) Add(_ context.Context, k string, d int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.s.Add(k, d)
	return nil
}

func (c *localCMS) EstimateCount(_ context.Context, k string) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.s.EstimateCount(k), nil
}

func (c *localCMS) Total(_ context.Context) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.s.Total(), nil
}

func (c *localCMS) Rebuild(_ context.Context, m map[string]int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.s.Rebuild(m)
	return nil
}

// memMetrics records telemetry in memory until EQLX-6 Prometheus.
type memMetrics struct {
	mu          sync.Mutex
	dispatches  map[string]int
	completions map[string]int
	rps         float64
}

func (m *memMetrics) RecordDispatch(t string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dispatches[t]++
}

func (m *memMetrics) RecordCompletion(t, r string, _ int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.completions[t+"/"+r]++
}

func (m *memMetrics) ObserveDispatchLatency(_ float64) {}
func (m *memMetrics) SetRPS(r float64)                 { m.mu.Lock(); m.rps = r; m.mu.Unlock() }
func (m *memMetrics) PublishDrift(_ map[string]int64)  {}
func (m *memMetrics) SetQueueDepth(_ int)              {}

// TODO(EQLX-4): placeholder — reports a fixed 1.0, not a measured rate.
// Wire to the adaptive controller when it lands; until then GET /status
// currentRps is a constant, not a metric.
type fixedRPS struct{ rps float64 }

func (f fixedRPS) CurrentRPS() float64 { return f.rps }

func toInt64(m map[string]int) map[string]int64 {
	out := make(map[string]int64, len(m))
	for k, v := range m {
		out[k] = int64(v)
	}
	return out
}

func run() error {
	var (
		dsn        = flag.String("dsn", os.Getenv("EQUALIX_DSN"), "PostgreSQL DSN (or EQUALIX_DSN)")
		addr       = flag.String("addr", ":8080", "HTTP listen address")
		apiKey     = flag.String("api-key", os.Getenv("EQUALIX_API_KEY"), "API key, prefer EQUALIX_API_KEY env (flag value is visible in ps)")
		maxPayload = flag.Int("max-payload-bytes", 1048576, "ingest payload cap (Java app.queue.max-payload-bytes)")
	)
	flag.Parse()

	cfg := jobs.DefaultConfig()
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	if *dsn == "" {
		return fmt.Errorf("missing --dsn (or EQUALIX_DSN)")
	}
	if *apiKey == "" {
		return fmt.Errorf("missing --api-key (or EQUALIX_API_KEY)")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, *dsn)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()
	stores := pgadapter.NewStores(pool)

	locker, err := pgadapter.NewLocker(ctx, *dsn)
	if err != nil {
		return fmt.Errorf("locker: %w", err)
	}
	defer locker.Close()

	metrics := &memMetrics{dispatches: map[string]int{}, completions: map[string]int{}}
	cmsketch := &localCMS{s: cms.New(65536, 5)}
	handler := chiadapter.NewRouter(chiadapter.Deps{
		Tasks: stores.Tasks, Counts: stores.Counts, Sequences: stores.Sequences,
		CMS: cmsketch, Metrics: metrics,
		RPS: fixedRPS{rps: 1}, Clock: domain.SystemClock{},
		APIKey: *apiKey, MaxPayloadBytes: *maxPayload,
	})

	probes := []func(ctx context.Context) error{
		func(ctx context.Context) error {
			if err := pool.Ping(ctx); err != nil {
				return fmt.Errorf("readiness: postgres ping: %w", err)
			}
			return nil
		},
		func(ctx context.Context) error {
			ok, release, err := locker.Lock(ctx, "readiness-probe")
			if err != nil {
				return fmt.Errorf("readiness: lock probe: %w", err)
			}
			if !ok {
				return fmt.Errorf("readiness: lock probe denied")
			}
			release()
			return nil
		},
	}

	// Probes run BEFORE the listener binds: no connection is accepted
	// until readiness passes. (The runner also accepts probes, but the
	// pre-bind gate here is what keeps fast clients off a not-ready
	// backend; the runner gets the already-passing set for supervision.)
	pctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for _, probe := range probes {
		if err := probe(pctx); err != nil {
			return err // exit non-zero; orchestrator retries
		}
	}

	// CMS warm-up (Java parity: CmsWarmUpListener): rebuild the sketch
	// from in-flight rows WITHOUT publishing drift — an empty sketch at
	// startup would otherwise read as underestimate for every key.
	if actual, err := stores.Tasks.CountInFlight(ctx); err != nil {
		return fmt.Errorf("warm-up snapshot: %w", err)
	} else if err := cmsketch.Rebuild(ctx, toInt64(actual)); err != nil {
		return fmt.Errorf("warm-up rebuild: %w", err)
	} else {
		slog.Info("cms warmed up", "keys", len(actual))
	}

	// TODO(executor): wire the dispatcher once a port.Executor adapter
	// exists. Until then dispatch runs only in tests (TickForTest).
	runner := jobs.NewRunner(slog.Default(), nil,
		jobs.NewCalculator(jobs.CalculatorDeps{
			Tasks: stores.Tasks, Sequences: stores.Sequences,
			VT: stores.VirtualTime, CMS: cmsketch,
			Metrics: metrics, Config: cfg,
		}),
		jobs.NewWatchdog(jobs.WatchdogDeps{
			Tasks: stores.Tasks, Counts: stores.Counts, CMS: cmsketch,
			Metrics: metrics, Config: cfg,
		}),
		jobs.NewTimeout(jobs.TimeoutDeps{
			Tx: stores, Tasks: stores.Tasks, Counts: stores.Counts,
			Sequences: stores.Sequences, CMS: cmsketch,
			Config: cfg, Clock: domain.SystemClock{},
		}),
	)

	server := &http.Server{Addr: *addr, Handler: handler}
	serverErr := make(chan error, 1)
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serverErr <- err
			stop()
		}
	}()

	runErr := make(chan error, 1)
	go func() { runErr <- runner.Run(ctx) }()

	select {
	case err := <-serverErr:
		// HTTP dead: stop jobs, drain, report the server failure.
		stop()
		<-runErr
		return fmt.Errorf("http server: %w", err)
	case err := <-runErr:
		// Runner failed (readiness or startup): stop HTTP, report.
		stop()
		grace, cancel := context.WithTimeout(context.Background(), cfg.ShutdownGrace)
		defer cancel()
		_ = server.Shutdown(grace)
		return err
	case <-ctx.Done():
		// Signal: strictly sequential drain — (1) stop accepting + wait
		// for in-flight HTTP within grace, (2) then runner drain. The two
		// phases share the grace budget sequentially, never in parallel:
		// a webhook accepted during HTTP drain must find the stores (and
		// CMS flush path) still owned by a live runner, not torn down
		// underneath it. Runner normalizes shutdown cancels to nil.
		grace, cancel := context.WithTimeout(context.Background(), cfg.ShutdownGrace)
		defer cancel()
		if err := server.Shutdown(grace); err != nil {
			return fmt.Errorf("http shutdown: %w", err)
		}
		return <-runErr
	}
}

var _ port.Metrics = (*memMetrics)(nil)
