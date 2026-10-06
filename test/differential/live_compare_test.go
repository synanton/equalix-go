//go:build differential

package differential

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	pgadapter "github.com/synanton/equalix-go/internal/adapter/postgres"
)

// shaOr reads a build SHA for results attribution. "Unrecorded" is an
// explicit marker, never a silent default — a result that cannot name its
// builds is not publishable per the scope's output contract.
func shaOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// runCalibration executes the four calibration fixtures as a subprocess
// and returns their names on success. The live comparison refuses to run
// unless every fixture fires as designed — a broken comparator must fail
// here, loudly, before any real workload is measured. Returns the fired
// gate list for the results artifact.
func runCalibration(t *testing.T) []string {
	t.Helper()
	fixtures := []struct {
		name string
		args []string
	}{
		{"FirstQueued", []string{"-run", "TestFalsificationFirstQueued"}},
		{"InvertedWeights", []string{"-run", "TestFalsificationInvertedWeights"}},
		{"Starving", []string{"-run", "TestStarvingIsolatesStarvationGate"}},
		{"QuotaIgnoring", []string{"-run", "TestQuotaIsolatesQuotaGate"}},
	}
	var fired []string
	for _, f := range fixtures {
		args := append([]string{"test", "-count=1", "-tags=differential"}, f.args...)
		args = append(args, "./test/differential/...")
		cmd := exec.Command("go", args...)
		cmd.Dir = moduleRoot(t)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("pre-flight calibration %s did not fire: %v\n%s", f.name, err, out)
		}
		fired = append(fired, f.name)
		t.Logf("pre-flight calibration fired: %s", f.name)
	}
	return fired
}

// moduleRoot locates the repo root (the directory holding go.mod) from the
// test working directory.
func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above working directory")
		}
		dir = parent
	}
}

// jdbcToPgx derives a pgx DSN from a JDBC URL + credentials (same database,
// driver-appropriate scheme) so the harness can poll quiescence directly.
func jdbcToPgx(jdbc, user, pass string) string {
	rest, ok := strings.CutPrefix(jdbc, "jdbc:postgresql://")
	if !ok {
		return ""
	}
	host, db, ok := strings.Cut(rest, "/")
	if !ok || db == "" {
		return ""
	}
	return "postgres://" + user + ":" + pass + "@" + host + "/" + db + "?sslmode=disable"
}

// TestLiveJavaVsGo is the first real comparison: Java and Go services,
// separate databases, one fresh stub per run, w127 workload, ComparePair
// verdict. Gated on EQUALIX_LIVE_COMPARE=1 plus env-provided binaries and
// databases — never runs in default CI. This is the test the whole harness
// exists to serve; everything else was scaffolding for this moment.
//
// Required env:
//
//	EQUALIX_JAVA_JAR  path to the equalix Java jar
//	EQUALIX_GO_BIN    path to the equalix-go binary
//	EQUALIX_JAVA_JDBC jdbc:postgresql://host:port/equalix_java (migrated by Flyway on boot)
//	EQUALIX_GO_DSN    postgres://.../equalix_go (migrated: apply migrations/*.sql Up sections)
//	EQUALIX_PG_USER / EQUALIX_PG_PASSWORD shared DB credentials
//	EQUALIX_RESULTS_DIR (optional) results destination
func TestLiveJavaVsGo(t *testing.T) {
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

	// Fresh state per run is established inside runOne after boot (tables
	// exist only post-migration); residue shares keys and persists V, so
	// repeated runs without reset would measure history, not the workload.

	wl := os.Getenv("EQUALIX_WORKLOAD")
	if wl == "" {
		wl = "w127.jsonl"
	}
	workload, err := Load(filepath.Join("workloads", wl))
	if err != nil {
		t.Fatal(err)
	}

	runOne := func(name, svcURL string, stubPort int, dsn string, ensureSchema bool, start func() (*Proc, error)) SideResult {
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
		// Reset AFTER boot: tables exist only post-migration (Flyway on
		// Java boot, manual apply on Go). Resetting before launch would
		// fail on missing relations; resetting here guarantees identical
		// fresh state per run regardless of prior runs.
		// Go sides additionally self-heal schema: a fresh database from
		// the compose pg-init path has roles + empty DBs but no tables —
		// EnsureSchema applies the embedded goose set iff tasks is
		// absent (no-op otherwise). Java sides never take this path:
		// Flyway owns that schema, and goose-migrating it would fork
		// the source and fight Flyway validation on boot.
		if ensureSchema {
			if err := pgadapter.EnsureSchema(ctx, dsn); err != nil {
				t.Fatalf("side %s schema: %v", name, err)
			}
		}
		if err := ResetDB(ctx, dsn); err != nil {
			t.Fatalf("side %s reset: %v", name, err)
		}
		measureFrom := 0
		prephase := 0
		if warmCfg.Tasks > 0 {
			m, err := RunWarmup(ctx, &http.Client{Timeout: 10 * time.Second}, SideConfig{Name: name, BaseURL: svcURL, DSN: dsn, APIKey: apiKey}, apiKey, workload, stub, warmCfg)
			if err != nil {
				t.Fatalf("side %s warmup: %v", name, err)
			}
			measureFrom = m
			// Pin the pre-phase DB size HERE (between warm-up drain and
			// measurement ingest): counting after RunSide would mix
			// measurement rows into the pre-phase number.
			n, err := countDispatched(ctx, dsn)
			if err != nil {
				t.Fatalf("side %s prephase count: %v", name, err)
			}
			prephase = n
		}
		svc := SideConfig{Name: name, BaseURL: svcURL, DSN: dsn, HTTPPort: 0, APIKey: apiKey}
		res, err := RunSide(ctx, svc, apiKey, workload, stub, 180*time.Second, measureFrom)
		if err != nil {
			t.Fatalf("side %s: %v", name, err)
		}
		res.SpawnedAt, res.ReadyAt = proc.SpawnedAt, proc.ReadyAt
		res.PrephaseDispatched = prephase
		return res
	}

	javaDSN := jdbcToPgx(env["EQUALIX_JAVA_JDBC"], env["EQUALIX_PG_USER"], env["EQUALIX_PG_PASSWORD"])
	goDSN := env["EQUALIX_GO_DSN"]
	tracer := StartTracer(ctx, map[string]Side{
		"java": {Name: "java", BaseURL: "http://127.0.0.1:18083", DSN: javaDSN, APIKey: apiKey},
		"go":   {Name: "go", BaseURL: "http://127.0.0.1:18084", DSN: goDSN, APIKey: apiKey},
	}, apiKey)

	java := runOne("java", "http://127.0.0.1:18083", 18093, javaDSN, false,
		func() (*Proc, error) {
			return Launch(ctx, ProcSpec{
				Name: "java", Bin: "java",
				Args: []string{"-jar", env["EQUALIX_JAVA_JAR"]},
				Env: map[string]string{
					"SPRING_DATASOURCE_URL":      env["EQUALIX_JAVA_JDBC"],
					"SPRING_DATASOURCE_USERNAME": env["EQUALIX_PG_USER"],
					"SPRING_DATASOURCE_PASSWORD": env["EQUALIX_PG_PASSWORD"],
					"EQUALIX_API_KEY":            apiKey,
					"APP_EXECUTOR_BASE_URL":      "http://127.0.0.1:18093",
					"SERVER_PORT":                "18083",
				},
				ReadyURL:       "http://127.0.0.1:18083/api/v1/status",
				StartupTimeout: envDuration("EQUALIX_JAVA_STARTUP_TIMEOUT", 120*time.Second),
			})
		})

	goRes := runOne("go", "http://127.0.0.1:18084", 18094, env["EQUALIX_GO_DSN"], true, func() (*Proc, error) {
		return Launch(ctx, ProcSpec{
			Name: "go", Bin: env["EQUALIX_GO_BIN"],
			Args: []string{
				"--dsn", env["EQUALIX_GO_DSN"],
				"--addr", "127.0.0.1:18084",
				"--api-key", apiKey,
				"--executor-base-url", "http://127.0.0.1:18094",
			},
			Env:            map[string]string{},
			ReadyURL:       "http://127.0.0.1:18084/api/v1/status",
			StartupTimeout: envDuration("EQUALIX_GO_STARTUP_TIMEOUT", 60*time.Second),
		})
	})

	// Shares gate the full run (transient included — it washed out exactly).
	// Ordering is evidence only (see ORDER DIAGNOSTIC below): the Go-vs-Go
	// control proved exact-order parity flaky-by-construction, so no
	// ordering verdict gates a live run.
	// Shares gate the full run ONLY when at least one complete window
	// exists. Below one window of volume the gate is vacuous (a partial
	// tail never fails) — shares are recorded, never claimed. Smoke runs
	// (21 tasks vs 1000-window) exercise the pipeline, not the bound.
	// Mismatches are RECORDED, not fatalf'd here: the artifact must publish
	// even on divergence (a mismatch with classification is the whole
	// point of results.json); the verdict fatalf's after WriteResult.
	// Idle-tenant runs (EQUALIX_IDLE_TENANT set) skip the shares gate
	// entirely: a phased workload starves a tenant by design, so
	// per-window shares vs 1:2:7 are meaningless. The observable is the
	// return window below — recorded, never gated (N=1 pair each).
	idleTenant := os.Getenv("EQUALIX_IDLE_TENANT")
	var idleReturnMs int64
	if idleTenant != "" {
		if v := os.Getenv("EQUALIX_IDLE_RETURN_MS"); v != "" {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil || n < 0 {
				t.Fatalf("EQUALIX_IDLE_RETURN_MS unparseable: %q", v)
			}
			idleReturnMs = n
		}
	}
	var mm *Mismatch
	var idle *IdleResult
	if idleTenant != "" {
		idle = &IdleResult{Tenant: idleTenant, ReturnMs: idleReturnMs, Window: map[string]map[string]int{}}
		for _, side := range []struct {
			name string
			res  SideResult
		}{{"java", java}, {"go", goRes}} {
			counts, seq := IdleWindow(side.res.Log, side.res.Marker, idleTenant, idleReturnMs, 1000)
			idle.Window[side.name] = counts
			t.Logf("idle %s: return window (from seq %d): %v", side.name, seq, counts)
		}
	} else {
		for _, side := range []struct {
			name string
			log  RunLog
		}{{"java", java.Log}, {"go", goRes.Log}} {
			results, m := CompareShares(side.log, 1000, 2)
			if FullWindows(results) == 0 {
				t.Logf("%s shares recorded (no full window — gate not applied): %s", side.name, summarize(side.log))
				continue
			}
			if m != nil && mm == nil {
				mm = m
			}
			// Numeric deviations per tenant, win or lose — "shares matched" is
			// not a result, numbers against the bound are.
			for _, w := range results {
				if w.Full {
					t.Logf("%s window %d deviations: %v (bound ±2)", side.name, w.Window, w.Deviations)
				}
			}
		}
	}
	steady := func(r SideResult) RunLog {
		o := r.Log.Order[min(r.Warmup, len(r.Log.Order)):]
		t.Logf("side warmup prefix: %d/%d dispatches", r.Warmup, len(r.Log.Order))
		return RunLog{Weights: r.Log.Weights, Order: o, Created: r.Log.Created}
	}
	// Ordering is diagnostic only: the Go-vs-Go control proved exact-order
	// parity flaky-by-construction across independently-ticking processes
	// (identical binaries agree exactly on some runs, diverge on others).
	// Gate on shares; log the ordering signal for analysis, never fail on it.
	// Steady slices can differ in length (warmup prefixes differ per run);
	// length mismatch there is expected, not a missing dispatch.
	sj, sg := steady(java), steady(goRes)
	if len(sj.Order) != len(sg.Order) {
		t.Logf("ORDER DIAGNOSTIC (non-gating): steady lengths differ java=%d go=%d (warmup %d vs %d)",
			len(sj.Order), len(sg.Order), java.Warmup, goRes.Warmup)
	} else if note := OrderDivergence(sj, sg); note != "" {
		t.Logf("ORDER DIAGNOSTIC (non-gating): %s", note)
	} else {
		t.Logf("ORDER DIAGNOSTIC: steady orders identical")
	}
	// Verdict recorded is shares-parity (the gated invariant); ordering is
	// evidence, logged above. mm flows into WriteResult below, then gates.
	outDir := os.Getenv("EQUALIX_RESULTS_DIR")
	if outDir == "" {
		outDir = "results-live-smoke01"
	}
	resolved := &Resolved{
		JavaDSN: redact(env["EQUALIX_JAVA_JDBC"]), GoDSN: redact(env["EQUALIX_GO_DSN"]),
		JavaPort: 18083, GoPort: 18084, Stub: DefaultLatency(),
		MarkerJava: java.Marker, MarkerGo: goRes.Marker,
		SubmitPace: SubmitPacePerSec(),
	}
	pace := SubmitPacePerSec()
	paceStr := "burst"
	if pace > 0 {
		paceStr = fmt.Sprintf("paced-%.0f/s", pace)
	}
	method := "EQLX-5 real01 [" + warmCfg.Class() + "]: " + wl + " (200/400/1400, 1:2:7) fixed-100ms stub, " + paceStr
	if idleTenant != "" {
		method += fmt.Sprintf(", idle-%s-return@%dms", idleTenant, idleReturnMs)
	}
	traces := tracer.Stop()
	fetch := tracer.FetchStats()
	for side, pts := range traces {
		if len(pts) > 0 {
			live, first, last := LiveStats(pts)
			t.Logf("trace %s: %d samples (%d live, rps %.1f→%.1f), total dispatched %d (incl prephase), promoted %d",
				side, len(pts), live, first, last,
				pts[len(pts)-1].TotalDispatched, pts[len(pts)-1].Promoted)
		}
	}
	for side, st := range fetch {
		t.Logf("trace %s status fetch: %d missed, first: %s, last: %s", side, st.Failed, st.First, st.Last)
	}
	startup := map[string]StartupInfo{
		"java": NewStartup(java.SpawnedAt, java.ReadyAt, java.FirstDispatch),
		"go":   NewStartup(goRes.SpawnedAt, goRes.ReadyAt, goRes.FirstDispatch),
	}
	for side, st := range startup {
		t.Logf("startup %s: spawn→ready %.0fms, ready→first-dispatch %.0fms",
			side, st.SpawnToReadyMs, st.ReadyToFirstDispatchMs)
	}
	if err := WriteResult(Artifact{Dir: outDir, Method: method,
		Resolved: resolved, JavaSHA: shaOr("EQUALIX_JAVA_SHA", "java-unrecorded"), GoSHA: shaOr("EQUALIX_GO_SHA", "go-unrecorded"), Calibration: calibration, Traces: traces, Fetch: fetch,
		Warmup:   map[string]int{"java": java.Warmup, "go": goRes.Warmup},
		Prephase: map[string]int{"java": java.PrephaseDispatched, "go": goRes.PrephaseDispatched},
		Startup:  startup, Idle: idle, MM: mm}); err != nil {
		t.Fatal(err)
	}
	t.Logf("java shares: %s", summarize(java.Log))
	t.Logf("go shares: %s", summarize(goRes.Log))
	if mm != nil {
		t.Fatalf("DIVERGENCE: %v", mm)
	}
}

// TestLiveGoVsGo is the control experiment for the ordering criterion: two
// IDENTICAL Go binaries, same workload, separate databases. If their exact
// dispatch orders diverge, exact-order parity is unachievable for ANY pair
// of independently-ticking schedulers — including Java-vs-Java — and the
// criterion (not either implementation) is wrong.
func TestLiveGoVsGo(t *testing.T) {
	if os.Getenv("EQUALIX_LIVE_COMPARE") != "1" {
		t.Skip("live comparison needs EQUALIX_LIVE_COMPARE=1")
	}
	apiKey := "live-compare-key"
	ctx := context.Background()
	calibration := runCalibration(t)
	warmCfg := WarmupFromEnv()
	wl := os.Getenv("EQUALIX_WORKLOAD")
	if wl == "" {
		wl = "w127.jsonl"
	}
	workload, err := Load(filepath.Join("workloads", wl))
	if err != nil {
		t.Fatal(err)
	}
	dsn2 := os.Getenv("EQUALIX_GO_DSN2")
	if dsn2 == "" {
		t.Skip("EQUALIX_GO_DSN2 not set")
	}
	goDSN := os.Getenv("EQUALIX_GO_DSN")
	if goDSN == "" {
		t.Skip("EQUALIX_GO_DSN not set")
	}
	// Traced like the Java-vs-Go leg: the control protocol compares
	// divergence RATES across pairs, which needs traces per run on both
	// legs — an untraced control run contributes no attributable evidence.
	tracer := StartTracer(ctx, map[string]Side{
		"go1": {Name: "go1", BaseURL: "http://127.0.0.1:18085", DSN: goDSN, APIKey: apiKey},
		"go2": {Name: "go2", BaseURL: "http://127.0.0.1:18086", DSN: dsn2, APIKey: apiKey},
	}, apiKey)
	// No pre-launch reset here: the GoVsGo sides reset inside runGo after
	// boot (same reason as above — tables exist only post-migration).
	runGo := func(name, svcURL string, svcPort, stubPort int, dsn string) SideResult {
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
		proc, err := Launch(ctx, ProcSpec{
			Name: name, Bin: os.Getenv("EQUALIX_GO_BIN"),
			Args: []string{
				"--dsn", dsn,
				"--addr", "127.0.0.1:" + itoa(svcPort), "--api-key", apiKey,
				"--executor-base-url", "http://127.0.0.1:" + itoa(stubPort),
			},
			Env:            map[string]string{},
			ReadyURL:       svcURL + "/api/v1/status",
			StartupTimeout: 60 * time.Second,
		})
		if err != nil {
			t.Fatalf("side %s: %v", name, err)
		}
		defer proc.Stop()
		// Go side: self-heal schema on fresh databases (see runOne —
		// same ensureSchema rationale; GvG legs are always Go).
		if err := pgadapter.EnsureSchema(ctx, dsn); err != nil {
			t.Fatalf("side %s schema: %v", name, err)
		}
		if err := ResetDB(ctx, dsn); err != nil {
			t.Fatalf("side %s reset: %v", name, err)
		}
		measureFrom := 0
		prephase := 0
		if warmCfg.Tasks > 0 {
			m, err := RunWarmup(ctx, &http.Client{Timeout: 10 * time.Second}, SideConfig{Name: name, BaseURL: svcURL, DSN: dsn, APIKey: apiKey}, apiKey, workload, stub, warmCfg)
			if err != nil {
				t.Fatalf("side %s warmup: %v", name, err)
			}
			measureFrom = m
			n, err := countDispatched(ctx, dsn)
			if err != nil {
				t.Fatalf("side %s prephase count: %v", name, err)
			}
			prephase = n
		}
		svc := SideConfig{Name: name, BaseURL: svcURL, DSN: dsn, HTTPPort: svcPort, APIKey: apiKey}
		res, err := RunSide(ctx, svc, apiKey, workload, stub, 180*time.Second, measureFrom)
		if err != nil {
			t.Fatalf("side %s: %v", name, err)
		}
		res.SpawnedAt, res.ReadyAt = proc.SpawnedAt, proc.ReadyAt
		res.PrephaseDispatched = prephase
		return res
	}
	g1 := runGo("go1", "http://127.0.0.1:18085", 18085, 18095, os.Getenv("EQUALIX_GO_DSN"))
	g2 := runGo("go2", "http://127.0.0.1:18086", 18086, 18096, dsn2)
	// Shares gate, same rule as the Java-vs-Go leg: full 1000-windows only,
	// mismatches recorded into the artifact, verdict after WriteResult.
	// Idle-tenant runs skip the gate like the JvG leg (record-only).
	idleTenant := os.Getenv("EQUALIX_IDLE_TENANT")
	var idleReturnMs int64
	if idleTenant != "" {
		if v := os.Getenv("EQUALIX_IDLE_RETURN_MS"); v != "" {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil || n < 0 {
				t.Fatalf("EQUALIX_IDLE_RETURN_MS unparseable: %q", v)
			}
			idleReturnMs = n
		}
	}
	var mm *Mismatch
	var idle *IdleResult
	if idleTenant != "" {
		idle = &IdleResult{Tenant: idleTenant, ReturnMs: idleReturnMs, Window: map[string]map[string]int{}}
		for _, side := range []struct {
			name string
			res  SideResult
		}{{"go1", g1}, {"go2", g2}} {
			counts, seq := IdleWindow(side.res.Log, side.res.Marker, idleTenant, idleReturnMs, 1000)
			idle.Window[side.name] = counts
			t.Logf("idle %s: return window (from seq %d): %v", side.name, seq, counts)
		}
	} else {
		for _, side := range []struct {
			name string
			log  RunLog
		}{{"go1", g1.Log}, {"go2", g2.Log}} {
			results, m := CompareShares(side.log, 1000, 2)
			if FullWindows(results) == 0 {
				t.Logf("%s shares recorded (no full window — gate not applied): %s", side.name, summarize(side.log))
				continue
			}
			if m != nil && mm == nil {
				mm = m
			}
			for _, w := range results {
				if w.Full {
					t.Logf("%s window %d deviations: %v (bound ±2)", side.name, w.Window, w.Deviations)
				}
			}
		}
	}
	t.Logf("go1 shares: %v warmup=%d", sharesOf(g1), g1.Warmup)
	t.Logf("go2 shares: %v warmup=%d", sharesOf(g2), g2.Warmup)
	if mm := ComparePair(g1.Log, g2.Log, 1000, 2, "fairness-shares"); mm != nil {
		t.Logf("GO-VS-GO DIVERGENCE (identical binaries): %v", mm)
	} else {
		t.Logf("GO-VS-GO: exact parity (shares and order)")
	}
	// Steady-state check: same comparison on post-warmup orders. If this
	// still diverges, histories never converge and exact-order parity is
	// unachievable even in steady state — not just a startup transient.
	steady := func(r SideResult) RunLog {
		o := r.Log.Order[min(r.Warmup, len(r.Log.Order)):]
		return RunLog{Weights: r.Log.Weights, Order: o, Created: r.Log.Created}
	}
	if mm := ComparePair(steady(g1), steady(g2), 1000, 2, "fairness-shares"); mm != nil {
		t.Logf("GO-VS-GO STEADY DIVERGENCE: %v", mm)
	} else {
		t.Logf("GO-VS-GO STEADY: exact parity")
	}
	// Retained artifact, same contract as the Java-vs-Go leg: the control
	// rate is computed across runs from results.json, so every control run
	// publishes — especially the diverged ones. Resolved reuses the
	// java_*/go_* slots for go1/go2 (methodology names the mapping).
	outDir := os.Getenv("EQUALIX_RESULTS_DIR")
	if outDir == "" {
		outDir = "results-live-gvg01"
	}
	resolved := &Resolved{
		JavaDSN: redact(goDSN), GoDSN: redact(dsn2),
		JavaPort: 18085, GoPort: 18086, Stub: DefaultLatency(),
		MarkerJava: g1.Marker, MarkerGo: g2.Marker,
		SubmitPace: SubmitPacePerSec(),
	}
	method := "EQLX-5 control [" + warmCfg.Class() + "]: go-vs-go (go1 in java_* slots) " + wl + " fixed-100ms stub, " + func() string {
		if p := SubmitPacePerSec(); p > 0 {
			return fmt.Sprintf("paced-%.0f/s", p)
		}
		return "burst"
	}()
	if idleTenant != "" {
		method += fmt.Sprintf(", idle-%s-return@%dms", idleTenant, idleReturnMs)
	}
	traces := tracer.Stop()
	fetch := tracer.FetchStats()
	for side, pts := range traces {
		if len(pts) > 0 {
			live, first, last := LiveStats(pts)
			t.Logf("trace %s: %d samples (%d live, rps %.1f→%.1f), total dispatched %d (incl prephase), promoted %d",
				side, len(pts), live, first, last,
				pts[len(pts)-1].TotalDispatched, pts[len(pts)-1].Promoted)
		}
	}
	for side, st := range fetch {
		t.Logf("trace %s status fetch: %d missed, first: %s, last: %s", side, st.Failed, st.First, st.Last)
	}
	goSHA := shaOr("EQUALIX_GO_SHA", "go-unrecorded")
	startup := map[string]StartupInfo{
		"go1": NewStartup(g1.SpawnedAt, g1.ReadyAt, g1.FirstDispatch),
		"go2": NewStartup(g2.SpawnedAt, g2.ReadyAt, g2.FirstDispatch),
	}
	for side, st := range startup {
		t.Logf("startup %s: spawn→ready %.0fms, ready→first-dispatch %.0fms",
			side, st.SpawnToReadyMs, st.ReadyToFirstDispatchMs)
	}
	if err := WriteResult(Artifact{Dir: outDir, Method: method,
		Resolved: resolved, JavaSHA: goSHA, GoSHA: goSHA, Calibration: calibration, Traces: traces, Fetch: fetch,
		Warmup:   map[string]int{"go1": g1.Warmup, "go2": g2.Warmup},
		Prephase: map[string]int{"go1": g1.PrephaseDispatched, "go2": g2.PrephaseDispatched},
		Startup:  startup, Idle: idle, MM: mm}); err != nil {
		t.Fatal(err)
	}
	if mm != nil {
		t.Fatalf("GO-VS-GO DIVERGENCE (recorded above): %v", mm)
	}
}

func sharesOf(r SideResult) map[string]int {
	m := map[string]int{}
	for _, d := range r.Log.Order {
		m[d.Tenant]++
	}
	return m
}

func itoa(n int) string {
	return strconv.Itoa(n)
}

// envDuration reads a Go duration from the environment, falling back to
// def on unset or unparseable values. Startup timeouts stay tunable per
// runner without code changes: a cold CI runner gets a generous value in
// the workflow, tightened later once baselines exist. A "service never
// came up" failure with these knobs untouched means what it says. No upper
// bound is enforced here by design — the workflow's job-level timeout is
// the outer backstop against a typo'd 5h value hanging CI.
func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return def
}
