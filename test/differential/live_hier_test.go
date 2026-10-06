//go:build differential

package differential

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	pgadapter "github.com/synanton/equalix-go/internal/adapter/postgres"
)

// TestLiveHierarchical is the EQLX-9 differential: Java and Go with
// hierarchy enabled, same tree config, w-hier workload, warm-class
// discipline. Two independent gates (never one flattened gate —
// flattening hides whichever level is wrong):
//
//  1. Parent level: aggregate dispatches per parent vs the parents'
//     weight ratio (p1:p2 = 1:2).
//  2. Within-parent level: child dispatches per parent vs 1:2:7,
//     gated separately per parent.
//
// A new parity claim, not a re-check of EQLX-5: flat-tenant parity is
// untouched (separate code path, flag off by default).
func TestLiveHierarchical(t *testing.T) {
	if os.Getenv("EQUALIX_LIVE_COMPARE") != "1" {
		t.Skip("live comparison needs EQUALIX_LIVE_COMPARE=1, built binaries, and two databases")
	}
	env := map[string]string{}
	for _, k := range []string{"EQUALIX_JAVA_JAR", "EQUALIX_GO_BIN", "EQUALIX_JAVA_JDBC", "EQUALIX_GO_DSN", "EQUALIX_PG_USER", "EQUALIX_PG_PASSWORD"} {
		v := os.Getenv(k)
		if v == "" {
			t.Fatalf("missing env %s", k)
		}
		env[k] = v
	}
	apiKey := "live-compare-key"
	ctx := context.Background()
	calibration := runCalibration(t)
	warmCfg := WarmupFromEnv()
	workload, err := Load(filepath.Join("workloads", "w-hier.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	javaYaml, err := filepath.Abs("hierarchical-java.yml")
	if err != nil {
		t.Fatal(err)
	}
	goYaml, err := filepath.Abs("hierarchical-go.yaml")
	if err != nil {
		t.Fatal(err)
	}
	tracer := StartTracer(ctx, map[string]Side{
		"java": {Name: "java", BaseURL: "http://127.0.0.1:18089", DSN: jdbcToPgx(env["EQUALIX_JAVA_JDBC"], env["EQUALIX_PG_USER"], env["EQUALIX_PG_PASSWORD"]), APIKey: apiKey},
		"go":   {Name: "go", BaseURL: "http://127.0.0.1:18090", DSN: env["EQUALIX_GO_DSN"], APIKey: apiKey},
	}, apiKey)

	runHier := func(name, svcURL string, stubPort int, dsn string, ensureSchema bool, start func() (*Proc, error)) SideResult {
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
		proc, err := start()
		if err != nil {
			t.Fatalf("side %s: %v", name, err)
		}
		defer proc.Stop()
		if ensureSchema {
			if err := pgadapter.EnsureSchema(ctx, dsn); err != nil {
				t.Fatalf("side %s schema: %v", name, err)
			}
		}
		if err := ResetDB(ctx, dsn); err != nil {
			t.Fatalf("side %s reset: %v", name, err)
		}
		measureFrom := 0
		if warmCfg.Tasks > 0 {
			m, err := RunWarmup(ctx, &http.Client{Timeout: 10 * time.Second}, SideConfig{Name: name, BaseURL: svcURL, DSN: dsn, APIKey: apiKey}, apiKey, workload, stub, warmCfg)
			if err != nil {
				t.Fatalf("side %s warmup: %v", name, err)
			}
			measureFrom = m
		}
		svc := SideConfig{Name: name, BaseURL: svcURL, DSN: dsn, HTTPPort: 0, APIKey: apiKey}
		res, err := RunSide(ctx, svc, apiKey, workload, stub, 180*time.Second, measureFrom)
		if err != nil {
			t.Fatalf("side %s: %v", name, err)
		}
		res.SpawnedAt, res.ReadyAt = proc.SpawnedAt, proc.ReadyAt
		return res
	}

	javaDSN := jdbcToPgx(env["EQUALIX_JAVA_JDBC"], env["EQUALIX_PG_USER"], env["EQUALIX_PG_PASSWORD"])
	java := runHier("java", "http://127.0.0.1:18089", 18099, javaDSN, false,
		func() (*Proc, error) {
			return Launch(ctx, ProcSpec{
				Name: "java", Bin: "java",
				Args: []string{"-jar", env["EQUALIX_JAVA_JAR"]},
				Env: map[string]string{
					"SPRING_DATASOURCE_URL":            env["EQUALIX_JAVA_JDBC"],
					"SPRING_DATASOURCE_USERNAME":       env["EQUALIX_PG_USER"],
					"SPRING_DATASOURCE_PASSWORD":       env["EQUALIX_PG_PASSWORD"],
					"EQUALIX_API_KEY":                  apiKey,
					"APP_EXECUTOR_BASE_URL":            "http://127.0.0.1:18099",
					"SERVER_PORT":                      "18089",
					"SPRING_CONFIG_ADDITIONALLOCATION": "file:" + javaYaml,
				},
				ReadyURL:       "http://127.0.0.1:18089/api/v1/status",
				StartupTimeout: envDuration("EQUALIX_JAVA_STARTUP_TIMEOUT", 120*time.Second),
			})
		})

	goRes := runHier("go", "http://127.0.0.1:18090", 18100, env["EQUALIX_GO_DSN"], true, func() (*Proc, error) {
		return Launch(ctx, ProcSpec{
			Name: "go", Bin: env["EQUALIX_GO_BIN"],
			Args: []string{
				"--dsn", env["EQUALIX_GO_DSN"],
				"--addr", "127.0.0.1:18090",
				"--api-key", apiKey,
				"--executor-base-url", "http://127.0.0.1:18100",
				"--fairness-mode", "hierarchical",
				"--config", goYaml,
			},
			Env:            map[string]string{},
			ReadyURL:       "http://127.0.0.1:18090/api/v1/status",
			StartupTimeout: envDuration("EQUALIX_GO_STARTUP_TIMEOUT", 60*time.Second),
		})
	})

	// Level 1: parent aggregates vs 1:2, each side, RequireGate each.
	var mm *Mismatch
	parentWeights := map[string]float64{"p1": 1.0, "p2": 2.0}
	for _, side := range []struct {
		name string
		log  RunLog
	}{{"java", java.Log}, {"go", goRes.Log}} {
		plog := ParentLog(side.log, "/", parentWeights)
		results, m := CompareShares(plog, 1000, 2)
		RequireGate(t, results, 1)
		if m != nil {
			t.Logf("%s PARENT DIVERGENCE: %v", side.name, m)
			if mm == nil {
				mm = m
			}
		}
		for _, w := range results {
			if w.Full {
				t.Logf("%s parent window %d deviations: %v (bound ±2)", side.name, w.Window, w.Deviations)
			}
		}
	}
	// Level 2: within-parent children vs 1:2:7, per parent per side.
	for _, parent := range []string{"p1", "p2"} {
		for _, side := range []struct {
			name string
			log  RunLog
		}{{"java", java.Log}, {"go", goRes.Log}} {
			clog := ChildLog(side.log, "/", parent, side.log.Weights)
			results, m := CompareShares(clog, 1000, 2)
			RequireGate(t, results, 1)
			if m != nil {
				t.Logf("%s/%s CHILD DIVERGENCE: %v", side.name, parent, m)
				if mm == nil {
					mm = m
				}
			}
			for _, w := range results {
				if w.Full {
					t.Logf("%s/%s child window %d deviations: %v (bound ±2)", side.name, parent, w.Window, w.Deviations)
				}
			}
		}
	}

	traces := tracer.Stop()
	firstErr := tracer.FetchStats()
	for side, pts := range traces {
		if len(pts) > 0 {
			live, first, last := LiveStats(pts)
			t.Logf("trace %s: %d samples (%d live, rps %.1f→%.1f), total dispatched %d (incl prephase), promoted %d",
				side, len(pts), live, first, last,
				pts[len(pts)-1].TotalDispatched, pts[len(pts)-1].Promoted)
		}
	}
	for side, st := range firstErr {
		t.Logf("trace %s status fetch: %d missed, first: %s, last: %s", side, st.Failed, st.First, st.Last)
	}
	startup := map[string]StartupInfo{
		"java": NewStartup(java.SpawnedAt, java.ReadyAt, java.FirstDispatch),
		"go":   NewStartup(goRes.SpawnedAt, goRes.ReadyAt, goRes.FirstDispatch),
	}
	outDir := os.Getenv("EQUALIX_RESULTS_DIR")
	if outDir == "" {
		outDir = "results-live-hier01"
	}
	resolved := &Resolved{
		JavaDSN: redact(env["EQUALIX_JAVA_JDBC"]), GoDSN: redact(env["EQUALIX_GO_DSN"]),
		JavaPort: 18089, GoPort: 18090, Stub: DefaultLatency(),
		MarkerJava: java.Marker, MarkerGo: goRes.Marker,
		SubmitPace: SubmitPacePerSec(),
	}
	method := "EQLX-9 hierarchical [warm-class]: w-hier.jsonl (p1:p2 1:2, children 1:2:7) fixed-100ms stub, burst"
	if err := WriteResult(Artifact{Dir: outDir, Method: method,
		Resolved: resolved, JavaSHA: shaOr("EQUALIX_JAVA_SHA", "java-unrecorded"), GoSHA: shaOr("EQUALIX_GO_SHA", "go-unrecorded"), Calibration: calibration, Traces: traces, Fetch: firstErr,
		Warmup:   map[string]int{"java": java.Warmup, "go": goRes.Warmup},
		Prephase: map[string]int{"java": java.PrephaseDispatched, "go": goRes.PrephaseDispatched},
		Startup:  startup, MM: mm}); err != nil {
		t.Fatal(err)
	}
	t.Logf("java shares: %s", summarize(java.Log))
	t.Logf("go shares: %s", summarize(goRes.Log))
	if mm != nil {
		t.Fatalf("HIERARCHICAL DIVERGENCE: %v", mm)
	}
}
