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
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	executor "github.com/synanton/equalix-go/internal/adapter/executor"
	chiadapter "github.com/synanton/equalix-go/internal/adapter/http"
	promadapter "github.com/synanton/equalix-go/internal/adapter/metrics"
	pgadapter "github.com/synanton/equalix-go/internal/adapter/postgres"
	"github.com/synanton/equalix-go/internal/adaptive"
	"github.com/synanton/equalix-go/internal/domain"
	"github.com/synanton/equalix-go/internal/jobs"
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

// toInt64 converts the CMS warm-up snapshot to the sketch's input type.
func toInt64(m map[string]int) map[string]int64 {
	out := make(map[string]int64, len(m))
	for k, v := range m {
		out[k] = int64(v)
	}
	return out
}

// envOr reads a string env with fallback; envIntOr the same for ints
// (unparseable falls back — a mistyped metrics knob degrades to the
// default, never to a zero cap that would drop every labeled sample).
func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envIntOr(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

func run() error {
	// Startup milestones feed the differential matrix's
	// runtime-characterization row (informational, never gated):
	// main-entry, config-loaded, readiness-ok, serving. slog already
	// stamps RFC3339 times; the phase key is what drill-down greps for.
	milestone := func(phase string) {
		slog.Info("startup milestone", "phase", phase)
	}
	milestone("main-entry")
	var (
		dsn         = flag.String("dsn", os.Getenv("EQUALIX_DSN"), "PostgreSQL DSN (or EQUALIX_DSN)")
		addr        = flag.String("addr", ":8080", "HTTP listen address")
		apiKey      = flag.String("api-key", os.Getenv("EQUALIX_API_KEY"), "API key, prefer EQUALIX_API_KEY env (flag value is visible in ps)")
		maxPayload  = flag.Int("max-payload-bytes", 1048576, "ingest payload cap (Java app.queue.max-payload-bytes)")
		execBase    = flag.String("executor-base-url", os.Getenv("EQUALIX_EXECUTOR_BASE_URL"), "executor base URL receiving POST /tasks/{id}/execute (or EQUALIX_EXECUTOR_BASE_URL)")
		metricsPath = flag.String("metrics-path", envOr("EQUALIX_METRICS_PATH", "/metrics"), "Prometheus exposition path on the service mux (operators: do not expose publicly)")
		tenantCap   = flag.Int("metrics-tenant-cap", envIntOr("EQUALIX_METRICS_TENANT_CAP", 1000), "distinct tenants per tenant-labeled metric; beyond the cap samples drop and count as cardinality-exceeded")
		driftKeys   = flag.Int("metrics-drift-max-keys", envIntOr("EQUALIX_METRICS_DRIFT_MAX_KEYS", 1000), "per-report drift series cap (sorted truncation)")
	)
	flag.Parse()

	cfg := jobs.DefaultConfig()
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	milestone("config-loaded")
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

	metrics, err := promadapter.New(promadapter.Config{TenantCap: *tenantCap, DriftMaxKeys: *driftKeys})
	if err != nil {
		return fmt.Errorf("metrics: %w", err)
	}
	cmsketch := &localCMS{s: cms.New(65536, 5)}
	// Throttle owns its own send pool as the FailedSends source
	// (DECISION-5 wiring: pool → controller, no wrapper).
	sendpool := jobs.NewSendPool(cfg.DispatchWorkers)
	controller, err := adaptive.New(adaptive.DefaultConfig(), domain.SystemClock{}, sendpool)
	if err != nil {
		return fmt.Errorf("adaptive controller: %w", err)
	}
	handler := chiadapter.NewRouter(chiadapter.Deps{
		Tasks: stores.Tasks, Counts: stores.Counts, Sequences: stores.Sequences,
		CMS: cmsketch, Metrics: metrics,
		MetricsPath: *metricsPath, MetricsHandler: metrics.Handler(),
		RPS: controller, Throttle: controller, Clock: domain.SystemClock{},
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
	warmStart := time.Now()
	if actual, err := stores.Tasks.CountInFlight(ctx); err != nil {
		return fmt.Errorf("warm-up snapshot: %w", err)
	} else if err := cmsketch.Rebuild(ctx, toInt64(actual)); err != nil {
		return fmt.Errorf("warm-up rebuild: %w", err)
	} else {
		slog.Info("cms warmed up", "keys", len(actual))
	}
	metrics.ObserveCMSWarmup(time.Since(warmStart).Seconds())
	milestone("readiness-ok")

	// Dispatcher needs an executor to send to. Without --executor-base-url
	// it stays unwired by explicit decision (dispatch logic stays covered
	// by TickForTest); with one, the full pipeline runs.
	allJobs := []jobs.Job{
		jobs.NewCalculator(jobs.CalculatorDeps{
			Tasks: stores.Tasks, Sequences: stores.Sequences,
			VT: stores.VirtualTime, CMS: cmsketch,
			Metrics: metrics, Throttle: controller, Config: cfg,
		}),
		jobs.NewWatchdog(jobs.WatchdogDeps{
			Tasks: stores.Tasks, Counts: stores.Counts, CMS: cmsketch,
			Metrics: metrics, Config: cfg,
		}),
		jobs.NewTimeout(jobs.TimeoutDeps{
			Tx: stores, Tasks: stores.Tasks, Counts: stores.Counts,
			Sequences: stores.Sequences, CMS: cmsketch,
			Metrics: metrics,
			Config:  cfg, Clock: domain.SystemClock{},
		}),
	}
	if *execBase != "" {
		allJobs = append(allJobs, jobs.NewDispatcher(jobs.DispatcherDeps{
			Tx: stores, Tasks: stores.Tasks, Counts: stores.Counts,
			CMS:      cmsketch,
			Executor: executor.NewHTTPExecutor(*execBase, 5*time.Second),
			Metrics:  metrics, Pool: sendpool, Throttle: controller,
			Config: cfg,
		}))
	} else {
		slog.Warn("no executor configured; dispatcher unwired (ingest + tagging only)")
	}
	runner := jobs.NewRunner(slog.Default(), nil, allJobs...)

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
	milestone("serving")

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
