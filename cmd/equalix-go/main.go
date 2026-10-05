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
	"gopkg.in/yaml.v3"
)

func main() {
	// healthcheck subcommand (Docker HEALTHCHECK in shell-less images,
	// k8s exec probes): liveness probe, exits 0/1. Default path is
	// /healthz (process alive, unauthenticated — liveness, never
	// readiness: a DB outage must not restart the container). Repoint
	// to /readyz with --path once GAP-5 lands the readiness endpoint.
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(runHealthcheck(os.Args[2:]))
	}
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "equalix-go:", err)
		os.Exit(1)
	}
}

// version is stamped at link time (-X main.version=... in the
// Dockerfile); dev builds report "dev".
var version = "dev"

func runHealthcheck(args []string) int {
	fs := flag.NewFlagSet("healthcheck", flag.ContinueOnError)
	probeAddr := "127.0.0.1:8080"
	if v := os.Getenv("EQUALIX_ADDR"); v != "" {
		probeAddr = v
	}
	addr := fs.String("addr", probeAddr, "service address to probe")
	path := fs.String("path", "/healthz", "mux path expecting HTTP 200 (liveness only; use /readyz once GAP-5 lands it)")
	if err := fs.Parse(args); err != nil {
		return 1
	}
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get("http://" + *addr + *path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "healthcheck:", err)
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintln(os.Stderr, "healthcheck: status", resp.Status)
		return 1
	}
	return 0
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

// fileConfig is the YAML config file surface: the same eight knobs as
// flags/env, pointers so absent keys are distinguishable from zero
// values (a zero tenant cap must never sneak in as "unset").
type fileConfig struct {
	DSN              *string `yaml:"dsn"`
	Addr             *string `yaml:"addr"`
	APIKey           *string `yaml:"api-key"`
	MaxPayloadBytes  *int    `yaml:"max-payload-bytes"`
	ExecutorBaseURL  *string `yaml:"executor-base-url"`
	MetricsPath      *string `yaml:"metrics-path"`
	MetricsTenantCap *int    `yaml:"metrics-tenant-cap"`
	MetricsDriftKeys *int    `yaml:"metrics-drift-max-keys"`
	MigrateOnStartup *bool   `yaml:"migrate-on-startup"`
	MigrationsDir    *string `yaml:"migrations-dir"`
}

// settings is the resolved runtime configuration.
type settings struct {
	DSN, Addr, APIKey    string
	MaxPayload           int
	ExecBase             string
	MetricsPath          string
	TenantCap, DriftKeys int
	MigrateOnStartup     bool
	MigrationsDir        string
}

func loadFileConfig(path string, explicit bool) (fileConfig, error) {
	var fc fileConfig
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) && !explicit {
			return fc, nil // default path doubles as "no file"
		}
		return fc, fmt.Errorf("config file %s: %w", path, err)
	}
	if err := yaml.Unmarshal(raw, &fc); err != nil {
		return fc, fmt.Errorf("config file %s: %w", path, err)
	}
	return fc, nil
}

func strVal(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// envBool parses a truthy env (1/true/yes, case-insensitive); anything
// else is false — an opt-in knob must never enable on a typo.
func envBool(v string) bool {
	switch v {
	case "1", "true", "TRUE", "True", "yes", "YES", "Yes":
		return true
	}
	return false
}

// resolveSettings applies the pinned precedence: explicit flags > env >
// file > built-ins. set names the flags passed on the command line.
func resolveSettings(set map[string]bool, fl settings, getenv func(string) string, fc fileConfig) (settings, error) {
	str := func(flagName, flagVal, envKey, fileVal, def string) string {
		if set[flagName] {
			return flagVal
		}
		if v := getenv(envKey); v != "" {
			return v
		}
		if fileVal != "" {
			return fileVal
		}
		return def
	}
	num := func(flagName string, flagVal int, envKey string, fileVal *int, def int) (int, error) {
		if set[flagName] {
			return flagVal, nil
		}
		if v := getenv(envKey); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				return n, nil
			}
			return def, nil // historical degrade-to-default for env
		}
		if fileVal != nil {
			if *fileVal <= 0 {
				return 0, fmt.Errorf("config file %s must be > 0, got %d", flagName, *fileVal)
			}
			return *fileVal, nil
		}
		return def, nil
	}
	out := settings{
		DSN:           str("dsn", fl.DSN, "EQUALIX_DSN", strVal(fc.DSN), ""),
		Addr:          str("addr", fl.Addr, "EQUALIX_ADDR", strVal(fc.Addr), ":8080"),
		APIKey:        str("api-key", fl.APIKey, "EQUALIX_API_KEY", strVal(fc.APIKey), ""),
		ExecBase:      str("executor-base-url", fl.ExecBase, "EQUALIX_EXECUTOR_BASE_URL", strVal(fc.ExecutorBaseURL), ""),
		MetricsPath:   str("metrics-path", fl.MetricsPath, "EQUALIX_METRICS_PATH", strVal(fc.MetricsPath), "/metrics"),
		MigrationsDir: str("migrations-dir", fl.MigrationsDir, "EQUALIX_MIGRATIONS_DIR", strVal(fc.MigrationsDir), ""),
	}
	// Bool knob, strict precedence (an explicit --migrate-on-startup=false
	// vetoes env/file — unlike strings, absence and false differ, so each
	// layer is tested for presence, not truthiness).
	if set["migrate-on-startup"] {
		out.MigrateOnStartup = fl.MigrateOnStartup
	} else if v := getenv("EQUALIX_MIGRATE_ON_STARTUP"); v != "" {
		out.MigrateOnStartup = envBool(v)
	} else if fc.MigrateOnStartup != nil {
		out.MigrateOnStartup = *fc.MigrateOnStartup
	}
	var err error
	if out.MaxPayload, err = num("max-payload-bytes", fl.MaxPayload, "EQUALIX_MAX_PAYLOAD_BYTES", fc.MaxPayloadBytes, 1048576); err != nil {
		return out, err
	}
	if out.TenantCap, err = num("metrics-tenant-cap", fl.TenantCap, "EQUALIX_METRICS_TENANT_CAP", fc.MetricsTenantCap, 1000); err != nil {
		return out, err
	}
	if out.DriftKeys, err = num("metrics-drift-max-keys", fl.DriftKeys, "EQUALIX_METRICS_DRIFT_MAX_KEYS", fc.MetricsDriftKeys, 1000); err != nil {
		return out, err
	}
	return out, nil
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
	slog.Info("equalix-go starting", "version", version)
	// Precedence (pinned): explicit flags > env > config file >
	// built-ins. Flags default unset so provenance is detectable
	// (flag.Visit); env keeps the historical degrade-to-default for
	// strings; the file is strict (an operator-authored file with a
	// non-positive int cap fails loud instead of silently halving
	// observability to a zero cap that drops every labeled sample).
	var (
		dsn              = flag.String("dsn", "", "PostgreSQL DSN (or EQUALIX_DSN)")
		addr             = flag.String("addr", "", "HTTP listen address (default :8080)")
		apiKey           = flag.String("api-key", "", "API key, prefer EQUALIX_API_KEY env (flag value is visible in ps)")
		maxPayload       = flag.Int("max-payload-bytes", 0, "ingest payload cap (default 1048576, Java app.queue.max-payload-bytes)")
		execBase         = flag.String("executor-base-url", "", "executor base URL receiving POST /tasks/{id}/execute (or EQUALIX_EXECUTOR_BASE_URL)")
		metricsPath      = flag.String("metrics-path", "", "Prometheus exposition path on the service mux (default /metrics; operators: do not expose publicly)")
		tenantCap        = flag.Int("metrics-tenant-cap", 0, "distinct tenants per tenant-labeled metric (default 1000); beyond the cap samples drop and count as cardinality-exceeded")
		driftKeys        = flag.Int("metrics-drift-max-keys", 0, "per-report drift series cap, sorted truncation (default 1000)")
		migrateOnStartup = flag.Bool("migrate-on-startup", false, "apply pending migrations before serving (or EQUALIX_MIGRATE_ON_STARTUP=true); default false — multi-instance deploys migrate as a separate step")
		migrationsDir    = flag.String("migrations-dir", "", "read goose files from disk instead of the embedded set (operators who inspect SQL first)")
		configPath       = flag.String("config", "/etc/equalix/config.yaml", "YAML config file (loaded when present; missing explicit path is fatal, missing default is ignored)")
	)
	flag.Parse()

	set := map[string]bool{}
	flag.Visit(func(f *flag.Flag) { set[f.Name] = true })
	// A missing file is fatal only when explicitly requested: the
	// default path doubles as "no file configured" on dev machines.
	// Explicitly requesting the default path still counts as explicit.
	explicitConfig := set["config"]
	fc, err := loadFileConfig(*configPath, explicitConfig)
	if err != nil {
		return err
	}
	s, err := resolveSettings(set,
		settings{DSN: *dsn, Addr: *addr, APIKey: *apiKey, MaxPayload: *maxPayload,
			ExecBase: *execBase, MetricsPath: *metricsPath,
			TenantCap: *tenantCap, DriftKeys: *driftKeys,
			MigrateOnStartup: *migrateOnStartup, MigrationsDir: *migrationsDir},
		os.Getenv, fc)
	if err != nil {
		return err
	}

	cfg := jobs.DefaultConfig()
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	milestone("config-loaded")
	if s.DSN == "" {
		return fmt.Errorf("missing --dsn (or EQUALIX_DSN)")
	}
	if s.APIKey == "" {
		return fmt.Errorf("missing --api-key (or EQUALIX_API_KEY)")
	}

	// Migrate-on-startup runs BEFORE any pool, probe, or listener:
	// migrated schema is a precondition of serving, never background
	// work. Fail-fast (exit 1) on any error — a half-migrated schema
	// must never serve traffic. Opt-in, default false: multi-instance
	// deploys migrate as a separate step to avoid lock-queue pileups.
	// The signal context is created first so SIGTERM cancels a stuck
	// migration instead of wedging startup past the orchestrator.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if s.MigrateOnStartup {
		milestone("migrating")
		if err := pgadapter.Migrate(ctx, s.DSN, s.MigrationsDir); err != nil {
			return err
		}
		milestone("migrated")
	}

	pool, err := pgxpool.New(ctx, s.DSN)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()
	stores := pgadapter.NewStores(pool)

	locker, err := pgadapter.NewLocker(ctx, s.DSN)
	if err != nil {
		return fmt.Errorf("locker: %w", err)
	}
	defer locker.Close()

	metrics, err := promadapter.New(promadapter.Config{TenantCap: s.TenantCap, DriftMaxKeys: s.DriftKeys})
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
		MetricsPath: s.MetricsPath, MetricsHandler: metrics.Handler(),
		RPS: controller, Throttle: controller, Clock: domain.SystemClock{},
		APIKey: s.APIKey, MaxPayloadBytes: s.MaxPayload,
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
	if s.ExecBase != "" {
		allJobs = append(allJobs, jobs.NewDispatcher(jobs.DispatcherDeps{
			Tx: stores, Tasks: stores.Tasks, Counts: stores.Counts,
			CMS:      cmsketch,
			Executor: executor.NewHTTPExecutor(s.ExecBase, 5*time.Second),
			Metrics:  metrics, Pool: sendpool, Throttle: controller,
			Config: cfg,
		}))
	} else {
		slog.Warn("no executor configured; dispatcher unwired (ingest + tagging only)")
	}
	runner := jobs.NewRunner(slog.Default(), nil, allJobs...)

	server := &http.Server{Addr: s.Addr, Handler: handler}
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
