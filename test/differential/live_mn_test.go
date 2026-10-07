//go:build differential

package differential

// Three-way differential legs involving the Micronaut implementation
// (methodology: docs/differential-methodology.md). Mirrors TestLiveJavaVsGo:
// same runOne shape (per-side stub, supervised proc, schema handling, reset,
// warmup, RunSide), same shares gate, same order diagnostic — plus the seams
// checklist the Micronaut pair owes (stuck-send counts per side, reported
// with the client stack, never gated).

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	pgadapter "github.com/synanton/equalix-go/internal/adapter/postgres"
)

// mnSide launches the Micronaut fat jar. All config is environment (same
// discipline as the Spring side): Micronaut maps MICRONAUT_SERVER_PORT and
// APP_EXECUTOR_BASE_URL onto dotted keys, EQUALIX_* carry the rest.
func mnSide(ctx context.Context, jar, jdbc, user, pass, apiKey, appPort, stubPort string) func() (*Proc, error) {
	return func() (*Proc, error) {
		return Launch(ctx, ProcSpec{
			Name: "micronaut", Bin: "java",
			Args: []string{"-jar", jar},
			Env: map[string]string{
				"EQUALIX_JDBC_URL":      jdbc,
				"EQUALIX_DB_USER":       user,
				"EQUALIX_DB_PASSWORD":   pass,
				"EQUALIX_API_KEY":       apiKey,
				"APP_EXECUTOR_BASE_URL": "http://127.0.0.1:" + stubPort,
				"MICRONAUT_SERVER_PORT": appPort,
			},
			ReadyURL:       "http://127.0.0.1:" + appPort + "/api/v1/status",
			StartupTimeout: envDuration("EQUALIX_MN_STARTUP_TIMEOUT", 120*time.Second),
		})
	}
}

func javaSide(ctx context.Context, jar, jdbc, user, pass, apiKey, appPort, stubPort string) func() (*Proc, error) {
	return func() (*Proc, error) {
		return Launch(ctx, ProcSpec{
			Name: "java", Bin: "java",
			Args: []string{"-jar", jar},
			Env: map[string]string{
				"SPRING_DATASOURCE_URL":      jdbc,
				"SPRING_DATASOURCE_USERNAME": user,
				"SPRING_DATASOURCE_PASSWORD": pass,
				"EQUALIX_API_KEY":            apiKey,
				"APP_EXECUTOR_BASE_URL":      "http://127.0.0.1:" + stubPort,
				"SERVER_PORT":                appPort,
			},
			ReadyURL:       "http://127.0.0.1:" + appPort + "/api/v1/status",
			StartupTimeout: envDuration("EQUALIX_JAVA_STARTUP_TIMEOUT", 120*time.Second),
		})
	}
}

func goSide(ctx context.Context, bin, dsn, addr, apiKey, stubPort string) func() (*Proc, error) {
	return func() (*Proc, error) {
		return Launch(ctx, ProcSpec{
			Name: "go", Bin: bin,
			Args: []string{
				"--dsn", dsn,
				"--addr", addr,
				"--api-key", apiKey,
				"--executor-base-url", "http://127.0.0.1:" + stubPort,
			},
			Env:            map[string]string{},
			ReadyURL:       "http://" + addr + "/api/v1/status",
			StartupTimeout: envDuration("EQUALIX_GO_STARTUP_TIMEOUT", 60*time.Second),
		})
	}
}

// workloadSHA pins the workload file for the evidence header: same file is
// asserted across invocations, not assumed from the filename.
func workloadSHA(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%x", sha256.Sum256(raw))
}

// stuckSends counts non-terminal (DISPATCHED/COMMITTED) rows post-run: sends
// the executor never answered. Client-stack diagnostic per the methodology
// doc — reported, never gated.
func stuckSends(ctx context.Context, dsn string) (int, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return 0, err
	}
	defer conn.Close(ctx)
	var n int
	err = conn.QueryRow(ctx,
		"SELECT COUNT(*) FROM tasks WHERE status IN ('DISPATCHED','COMMITTED')").Scan(&n)
	return n, err
}

// runPair runs two sides against one shared workload file and returns both
// results. Shape mirrors TestLiveJavaVsGo.runOne: per-side stub, supervised
// proc, schema handling (Go sides only — Flyway owns JVM schemas), reset,
// optional warmup, RunSide with drain.
func runPair(t *testing.T, ctx context.Context, apiKey string, workload []Task,
	warmCfg WarmupConfig, legs []pairLeg) map[string]SideResult {
	t.Helper()
	out := map[string]SideResult{}
	for _, leg := range legs {
		stub, err := NewStub(StubConfig{
			Port: leg.stubPort, Latency: DefaultLatency(),
			CompleteBase: leg.baseURL, APIKey: apiKey,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := stub.Start(); err != nil {
			t.Fatalf("side %s stub: %v", leg.name, err)
		}
		proc, err := leg.start()
		if err != nil {
			t.Fatalf("side %s: %v", leg.name, err)
		}
		func() {
			defer proc.Stop()
			if leg.ensureSchema {
				if err := pgadapter.EnsureSchema(ctx, leg.dsn); err != nil {
					t.Fatalf("side %s schema: %v", leg.name, err)
				}
			}
			if err := ResetDB(ctx, leg.dsn); err != nil {
				t.Fatalf("side %s reset: %v", leg.name, err)
			}
			measureFrom := 0
			prephase := 0
			if warmCfg.Tasks > 0 {
				m, err := RunWarmup(ctx, &http.Client{Timeout: 10 * time.Second},
					SideConfig{Name: leg.name, BaseURL: leg.baseURL, DSN: leg.dsn, APIKey: apiKey},
					apiKey, workload, stub, warmCfg)
				if err != nil {
					t.Fatalf("side %s warmup: %v", leg.name, err)
				}
				measureFrom = m
				n, err := countDispatched(ctx, leg.dsn)
				if err != nil {
					t.Fatalf("side %s prephase count: %v", leg.name, err)
				}
				prephase = n
			}
			svc := SideConfig{Name: leg.name, BaseURL: leg.baseURL, DSN: leg.dsn, APIKey: apiKey}
			res, err := RunSide(ctx, svc, apiKey, workload, stub, 180*time.Second, measureFrom)
			if err != nil {
				t.Fatalf("side %s: %v", leg.name, err)
			}
			res.SpawnedAt, res.ReadyAt = proc.SpawnedAt, proc.ReadyAt
			res.PrephaseDispatched = prephase
			out[leg.name] = res
		}()
	}
	return out
}

type pairLeg struct {
	name         string
	baseURL      string
	dsn          string
	stubPort     int
	ensureSchema bool
	start        func() (*Proc, error)
}

// gateShares applies the warm-class shares gate to every side and returns the
// first mismatch (recorded, never fatalf'd here — the artifact publishes even
// on divergence; the verdict fatalf's after WriteResult).
func gateShares(t *testing.T, results map[string]SideResult) *Mismatch {
	t.Helper()
	var mm *Mismatch
	for name, res := range results {
		results, m := CompareShares(res.Log, 1000, 2)
		if FullWindows(results) == 0 {
			t.Logf("%s shares recorded (no full window — gate not applied): %s", name, summarize(res.Log))
			continue
		}
		if m != nil && mm == nil {
			mm = m
		}
		for _, w := range results {
			if w.Full {
				t.Logf("%s window %d deviations: %v (bound ±2)", name, w.Window, w.Deviations)
			}
		}
	}
	return mm
}

func orderDiagnostic(t *testing.T, results map[string]SideResult, a, b string) {
	t.Helper()
	ra, rb := results[a], results[b]
	steady := func(r SideResult) RunLog {
		o := r.Log.Order[min(r.Warmup, len(r.Log.Order)):]
		return RunLog{Weights: r.Log.Weights, Order: o, Created: r.Log.Created}
	}
	sa, sb := steady(ra), steady(rb)
	if len(sa.Order) != len(sb.Order) {
		t.Logf("ORDER DIAGNOSTIC (non-gating): steady lengths differ %s=%d %s=%d (warmup %d vs %d)",
			a, len(sa.Order), b, len(sb.Order), ra.Warmup, rb.Warmup)
	} else if note := OrderDivergence(sa, sb); note != "" {
		t.Logf("ORDER DIAGNOSTIC (non-gating): %s", note)
	} else {
		t.Logf("ORDER DIAGNOSTIC: steady orders identical")
	}
}
