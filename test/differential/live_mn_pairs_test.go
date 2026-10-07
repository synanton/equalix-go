//go:build differential

package differential

// Pairwise differential legs involving Micronaut. Claim (methodology doc):
// "a direct port preserves oracle semantics across the integration seams."
// Shares gate at warm class (expected pass — same code); the seams checklist
// (stuck sends per side, order diagnostic) is what these runs actually validate.

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func mnThreeWayEnv(t *testing.T, keys ...string) map[string]string {
	t.Helper()
	env := map[string]string{}
	for _, k := range keys {
		v := os.Getenv(k)
		if v == "" {
			t.Fatalf("missing env %s", k)
		}
		env[k] = v
	}
	return env
}

func mnWorkload(t *testing.T) ([]Task, string, string) {
	t.Helper()
	wl := os.Getenv("EQUALIX_WORKLOAD")
	if wl == "" {
		wl = "w2000.jsonl"
	}
	path := filepath.Join("workloads", wl)
	workload, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return workload, wl, fmt.Sprintf("%x", sha256.Sum256(raw))
}

func TestLiveJavaVsMicronaut(t *testing.T) {
	if os.Getenv("EQUALIX_LIVE_COMPARE") != "1" {
		t.Skip("live comparison needs EQUALIX_LIVE_COMPARE=1, built binaries, and databases")
	}
	env := mnThreeWayEnv(t, "EQUALIX_JAVA_JAR", "EQUALIX_MN_JAR", "EQUALIX_JAVA_JDBC",
		"EQUALIX_MN_JDBC", "EQUALIX_PG_USER", "EQUALIX_PG_PASSWORD")
	apiKey := "live-compare-key"
	ctx := context.Background()
	calibration := runCalibration(t)
	warmCfg := WarmupFromEnv()
	workload, wl, wlSHA := mnWorkload(t)

	javaDSN := jdbcToPgx(env["EQUALIX_JAVA_JDBC"], env["EQUALIX_PG_USER"], env["EQUALIX_PG_PASSWORD"])
	mnDSN := jdbcToPgx(env["EQUALIX_MN_JDBC"], env["EQUALIX_PG_USER"], env["EQUALIX_PG_PASSWORD"])
	tracer := StartTracer(ctx, map[string]Side{
		"java":      {Name: "java", BaseURL: "http://127.0.0.1:18083", DSN: javaDSN, APIKey: apiKey},
		"micronaut": {Name: "micronaut", BaseURL: "http://127.0.0.1:18101", DSN: mnDSN, APIKey: apiKey},
	}, apiKey)

	results := runPair(t, ctx, apiKey, workload, warmCfg, []pairLeg{
		{name: "java", baseURL: "http://127.0.0.1:18083", dsn: javaDSN, stubPort: 18093,
			start: javaSide(ctx, env["EQUALIX_JAVA_JAR"], env["EQUALIX_JAVA_JDBC"],
				env["EQUALIX_PG_USER"], env["EQUALIX_PG_PASSWORD"], apiKey, "18083", "18093")},
		{name: "micronaut", baseURL: "http://127.0.0.1:18101", dsn: mnDSN, stubPort: 18111,
			start: mnSide(ctx, env["EQUALIX_MN_JAR"], env["EQUALIX_MN_JDBC"],
				env["EQUALIX_PG_USER"], env["EQUALIX_PG_PASSWORD"], apiKey, "18101", "18111")},
	})

	mm := gateShares(t, results)
	orderDiagnostic(t, results, "java", "micronaut")
	seamsReport(t, ctx, map[string]string{"java": javaDSN, "micronaut": mnDSN})

	outDir := os.Getenv("EQUALIX_RESULTS_DIR")
	if outDir == "" {
		outDir = "results-live-javamn01"
	}
	pace := SubmitPacePerSec()
	paceStr := "burst"
	if pace > 0 {
		paceStr = fmt.Sprintf("paced-%.0f/s", pace)
	}
	method := "EQLX-7 differential [warm]: " + wl + " fixed-100ms stub, " + paceStr
	traces := tracer.Stop()
	fetch := tracer.FetchStats()
	for side, pts := range traces {
		if len(pts) > 0 {
			live, first, last := LiveStats(pts)
			t.Logf("trace %s: %d samples (%d live, rps %.1f→%.1f)", side, len(pts), live, first, last)
		}
	}
	startup := map[string]StartupInfo{}
	for name, r := range results {
		startup[name] = NewStartup(r.SpawnedAt, r.ReadyAt, r.FirstDispatch)
		st := startup[name]
		t.Logf("startup %s: spawn→ready %.0fms, ready→first-dispatch %.0fms",
			name, st.SpawnToReadyMs, st.ReadyToFirstDispatchMs)
	}
	warmup := map[string]int{}
	prephase := map[string]int{}
	for name, r := range results {
		warmup[name] = r.Warmup
		prephase[name] = r.PrephaseDispatched
	}
	resolved := &Resolved{
		JavaDSN: redact(env["EQUALIX_JAVA_JDBC"]), MnDSN: redact(env["EQUALIX_MN_JDBC"]),
		JavaPort: 18083, MnPort: 18101, Stub: DefaultLatency(), WorkloadSHA: wlSHA,
		SubmitPace: pace,
	}
	for name, r := range results {
		switch name {
		case "java":
			resolved.MarkerJava = r.Marker
		case "micronaut":
			resolved.MarkerMn = r.Marker
		}
	}
	if err := WriteResult(Artifact{Dir: outDir, Method: method,
		Resolved: resolved, JavaSHA: shaOr("EQUALIX_JAVA_SHA", "java-unrecorded"),
		MnSHA: shaOr("EQUALIX_MN_SHA", "mn-unrecorded"), Calibration: calibration, Traces: traces,
		Fetch: fetch, Warmup: warmup, Prephase: prephase, Startup: startup, MM: mm}); err != nil {
		t.Fatal(err)
	}
	for name, r := range results {
		t.Logf("%s shares: %s", name, summarize(r.Log))
	}
	if mm != nil {
		t.Fatalf("DIVERGENCE: %v", mm)
	}
}

func TestLiveGoVsMicronaut(t *testing.T) {
	if os.Getenv("EQUALIX_LIVE_COMPARE") != "1" {
		t.Skip("live comparison needs EQUALIX_LIVE_COMPARE=1, built binaries, and databases")
	}
	env := mnThreeWayEnv(t, "EQUALIX_GO_BIN", "EQUALIX_MN_JAR", "EQUALIX_GO_DSN",
		"EQUALIX_MN_JDBC", "EQUALIX_PG_USER", "EQUALIX_PG_PASSWORD")
	apiKey := "live-compare-key"
	ctx := context.Background()
	calibration := runCalibration(t)
	warmCfg := WarmupFromEnv()
	workload, wl, wlSHA := mnWorkload(t)

	mnDSN := jdbcToPgx(env["EQUALIX_MN_JDBC"], env["EQUALIX_PG_USER"], env["EQUALIX_PG_PASSWORD"])
	goDSN := env["EQUALIX_GO_DSN"]
	tracer := StartTracer(ctx, map[string]Side{
		"go":        {Name: "go", BaseURL: "http://127.0.0.1:18084", DSN: goDSN, APIKey: apiKey},
		"micronaut": {Name: "micronaut", BaseURL: "http://127.0.0.1:18101", DSN: mnDSN, APIKey: apiKey},
	}, apiKey)

	results := runPair(t, ctx, apiKey, workload, warmCfg, []pairLeg{
		{name: "go", baseURL: "http://127.0.0.1:18084", dsn: goDSN, stubPort: 18094, ensureSchema: true,
			start: goSide(ctx, env["EQUALIX_GO_BIN"], env["EQUALIX_GO_DSN"],
				"127.0.0.1:18084", apiKey, "18094")},
		{name: "micronaut", baseURL: "http://127.0.0.1:18101", dsn: mnDSN, stubPort: 18111,
			start: mnSide(ctx, env["EQUALIX_MN_JAR"], env["EQUALIX_MN_JDBC"],
				env["EQUALIX_PG_USER"], env["EQUALIX_PG_PASSWORD"], apiKey, "18101", "18111")},
	})
	mm := gateShares(t, results)
	orderDiagnostic(t, results, "go", "micronaut")
	seamsReport(t, ctx, map[string]string{"go": goDSN, "micronaut": mnDSN})

	outDir := os.Getenv("EQUALIX_RESULTS_DIR")
	if outDir == "" {
		outDir = "results-live-gomn01"
	}
	pace := SubmitPacePerSec()
	paceStr := "burst"
	if pace > 0 {
		paceStr = fmt.Sprintf("paced-%.0f/s", pace)
	}
	method := "EQLX-7 differential [warm]: " + wl + " fixed-100ms stub, " + paceStr
	traces := tracer.Stop()
	fetch := tracer.FetchStats()
	for side, pts := range traces {
		if len(pts) > 0 {
			live, first, last := LiveStats(pts)
			t.Logf("trace %s: %d samples (%d live, rps %.1f→%.1f)", side, len(pts), live, first, last)
		}
	}
	startup := map[string]StartupInfo{}
	for name, r := range results {
		startup[name] = NewStartup(r.SpawnedAt, r.ReadyAt, r.FirstDispatch)
		st := startup[name]
		t.Logf("startup %s: spawn→ready %.0fms, ready→first-dispatch %.0fms",
			name, st.SpawnToReadyMs, st.ReadyToFirstDispatchMs)
	}
	warmup := map[string]int{}
	prephase := map[string]int{}
	for name, r := range results {
		warmup[name] = r.Warmup
		prephase[name] = r.PrephaseDispatched
	}
	resolved := &Resolved{
		GoDSN: redact(env["EQUALIX_GO_DSN"]), MnDSN: redact(env["EQUALIX_MN_JDBC"]),
		GoPort: 18084, MnPort: 18101, Stub: DefaultLatency(), WorkloadSHA: wlSHA,
		SubmitPace: pace,
	}
	for name, r := range results {
		switch name {
		case "go":
			resolved.MarkerGo = r.Marker
		case "micronaut":
			resolved.MarkerMn = r.Marker
		}
	}
	if err := WriteResult(Artifact{Dir: outDir, Method: method,
		Resolved: resolved, GoSHA: shaOr("EQUALIX_GO_SHA", "go-unrecorded"),
		MnSHA: shaOr("EQUALIX_MN_SHA", "mn-unrecorded"), Calibration: calibration, Traces: traces,
		Fetch: fetch, Warmup: warmup, Prephase: prephase, Startup: startup, MM: mm}); err != nil {
		t.Fatal(err)
	}
	for name, r := range results {
		t.Logf("%s shares: %s", name, summarize(r.Log))
	}
	if mm != nil {
		t.Fatalf("DIVERGENCE: %v", mm)
	}
}

// seamsReport logs the integration-seam signals the Micronaut pairs owe:
// stuck sends per side (client-stack diagnostic, never gated) with the stack
// named in the methodology doc, not inferred here.
func seamsReport(t *testing.T, ctx context.Context, dsns map[string]string) {
	t.Helper()
	for name, dsn := range dsns {
		n, err := stuckSends(ctx, dsn)
		if err != nil {
			t.Logf("seams %s: stuck-send count unavailable: %v", name, err)
			continue
		}
		t.Logf("seams %s: stuck sends (DISPATCHED/COMMITTED at end): %d", name, n)
	}
}
