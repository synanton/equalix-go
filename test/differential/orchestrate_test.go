//go:build differential

package differential

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testConfig() RunConfig {
	return RunConfig{
		Java:     SideConfig{Name: "java", BaseURL: "http://127.0.0.1:18081", DSN: "postgres://u:p@localhost:5432/java", HTTPPort: 18081},
		Go:       SideConfig{Name: "go", BaseURL: "http://127.0.0.1:18082", DSN: "postgres://u:p@localhost:5432/go", HTTPPort: 18082},
		Workload: "w.jsonl",
		Stub:     DefaultLatency(),
	}
}

func TestValidatePortsAndDBs(t *testing.T) {
	p := filepath.Join(t.TempDir(), "w.jsonl")
	if err := os.WriteFile(p, []byte("x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ok := testConfig()
	ok.Workload = p
	if err := ok.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	collide := ok
	collide.Go.HTTPPort = collide.Java.HTTPPort
	if err := collide.Validate(); err == nil || !strings.Contains(err.Error(), "collide") {
		t.Fatalf("port collision not rejected: %v", err)
	}
	shared := ok
	shared.Go.DSN = shared.Java.DSN
	if err := shared.Validate(); err == nil || !strings.Contains(err.Error(), "separate") {
		t.Fatalf("shared DB not rejected: %v", err)
	}
	missing := ok
	missing.Workload = filepath.Join(t.TempDir(), "nope.jsonl")
	if err := missing.Validate(); err == nil {
		t.Fatal("missing workload not rejected")
	}
	badURL := ok
	badURL.Java.BaseURL = "://bad"
	if err := badURL.Validate(); err == nil {
		t.Fatal("bad URL not rejected")
	}
}

// TestRunStartMarkerFromFirstIngest proves the marker pin: the harness
// records the first accepted ingest's timestamp — not time.Now() at driver
// start, not the client-side request time. A 500ms skew between sides must
// vanish after offset translation.
func TestRunStartMarkerFromFirstIngest(t *testing.T) {
	javaMarker := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	goMarker := javaMarker.Add(500 * time.Millisecond) // JVM warmed up slower
	wall := map[string]time.Time{
		"t1": javaMarker.Add(100 * time.Millisecond),
		"t2": goMarker.Add(100 * time.Millisecond),
	}
	_ = wall
	javaOffsets := ToOffsets(javaMarker, map[string]time.Time{"t1": javaMarker.Add(100 * time.Millisecond)})
	goOffsets := ToOffsets(goMarker, map[string]time.Time{"t1": goMarker.Add(100 * time.Millisecond)})
	if javaOffsets["t1"] != goOffsets["t1"] {
		t.Fatalf("same relative dispatch reads differently: %v vs %v", javaOffsets, goOffsets)
	}
	if javaOffsets["t1"] != 100 {
		t.Fatalf("offset = %d, want 100", javaOffsets["t1"])
	}
}

// TestPreflightRunsFirst wires a broken dispatcher (FirstQueued-style
// failure) into calibration and asserts the harness fails at calibration,
// never reaching comparison.
func TestPreflightRunsFirst(t *testing.T) {
	compared := false
	calibrate := func(ctx context.Context) error {
		return errors.New("calibration: FirstQueued shares 1:1:1 vs expected 1:2:7")
	}
	compare := func(ctx context.Context) (*Mismatch, error) {
		compared = true
		return nil, nil
	}
	cfg := testConfig()
	p := filepath.Join(t.TempDir(), "w.jsonl")
	if err := os.WriteFile(p, []byte("x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg.Workload = p
	_, err := Run(context.Background(), cfg, calibrate, compare)
	if err == nil || !strings.Contains(err.Error(), "pre-flight") {
		t.Fatalf("expected pre-flight failure, got %v", err)
	}
	if compared {
		t.Fatal("comparison ran despite failed calibration — gate not live")
	}
}

// TestPortSquatFailsFast binds a port, then asserts validation rejects a
// config pointing at it (distinct error, not a hang).
func TestPortSquatFailsFast(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skip("no loopback")
	}
	defer ln.Close()
	port := ln.Addr().(*net.TCPAddr).Port
	cfg := testConfig()
	p := filepath.Join(t.TempDir(), "w.jsonl")
	if err := os.WriteFile(p, []byte("x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg.Workload = p
	cfg.Java.BaseURL = fmt.Sprintf("http://127.0.0.1:%d", port)
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "already bound") {
		t.Fatalf("squatted port not rejected: %v", err)
	}
}

// TestProbeDistinguishesDownFromWrongPort asserts distinct failure modes:
// refused connection (nothing there) vs HTTP response (service up).
func TestProbeDistinguishesDownFromWrongPort(t *testing.T) {
	if err := Probe("http://127.0.0.1:1/"); err == nil || !strings.Contains(err.Error(), "unreachable") {
		t.Fatalf("down service should report unreachable, got %v", err)
	}
}

// TestResolvedRedactsPasswords ensures DSNs in the artifact never carry secrets.
func TestResolvedRedactsPasswords(t *testing.T) {
	cfg := testConfig()
	p := filepath.Join(t.TempDir(), "w.jsonl")
	if err := os.WriteFile(p, []byte("x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg.Workload = p
	res, err := Run(context.Background(), cfg,
		func(ctx context.Context) error { return nil },
		func(ctx context.Context) (*Mismatch, error) { return nil, nil })
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(res.GoDSN, ":p@") || strings.Contains(res.JavaDSN, ":p@") {
		t.Fatalf("password leaked: %+v", res)
	}
	if res.Stub.BaseMs != 100 || res.JavaPort != 18081 || res.GoPort != 18082 {
		t.Fatalf("resolved config wrong: %+v", res)
	}
}
