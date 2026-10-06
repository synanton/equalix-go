package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func getenvOf(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func intp(n int) *int { return &n }

func strp(s string) *string { return &s }

// TestResolvePrecedence pins flag > env > file > built-in per knob, plus
// the strict-file rule (non-positive file ints fail loud) and the
// historical env degrade-to-default.
func TestResolvePrecedence(t *testing.T) {
	fc := fileConfig{Addr: strp("file:1"), MetricsTenantCap: intp(50)}
	fl := settings{Addr: "flag:1"}
	got, err := resolveSettings(
		map[string]bool{"addr": true},
		fl, getenvOf(map[string]string{"EQUALIX_ADDR": "env:1"}), fc)
	if err != nil {
		t.Fatal(err)
	}
	if got.Addr != "flag:1" {
		t.Fatalf("flag did not win: %q", got.Addr)
	}
	if got.TenantCap != 50 {
		t.Fatalf("file cap not applied: %d", got.TenantCap)
	}
	// Env beats file when no flag is set.
	got, err = resolveSettings(map[string]bool{},
		settings{}, getenvOf(map[string]string{"EQUALIX_ADDR": "env:1"}), fc)
	if err != nil {
		t.Fatal(err)
	}
	if got.Addr != "env:1" {
		t.Fatalf("env did not win: %q", got.Addr)
	}
	// Built-ins when nothing is set anywhere.
	got, err = resolveSettings(map[string]bool{}, settings{}, getenvOf(nil), fileConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Addr != ":8080" || got.MetricsPath != "/metrics" || got.TenantCap != 1000 ||
		got.DriftKeys != 1000 || got.MaxPayload != 1048576 {
		t.Fatalf("built-ins wrong: %+v", got)
	}
	// Strict file: non-positive int cap fails loud.
	if _, err := resolveSettings(map[string]bool{}, settings{}, getenvOf(nil),
		fileConfig{MetricsTenantCap: intp(0)}); err == nil {
		t.Fatal("zero file cap accepted — would drop every labeled sample")
	}
	// Historical env leniency: garbage env falls back, never errors.
	got, err = resolveSettings(map[string]bool{}, settings{},
		getenvOf(map[string]string{"EQUALIX_METRICS_TENANT_CAP": "bogus"}), fileConfig{})
	if err != nil || got.TenantCap != 1000 {
		t.Fatalf("env fallback broken: %+v %v", got, err)
	}
}

func TestLoadFileConfig(t *testing.T) {
	dir := t.TempDir()
	// Missing default path is ignored (dev machines have no file).
	if _, err := loadFileConfig(filepath.Join(dir, "absent.yaml"), false); err != nil {
		t.Fatalf("missing default path errored: %v", err)
	}
	// Missing explicit path is fatal.
	if _, err := loadFileConfig(filepath.Join(dir, "absent.yaml"), true); err == nil {
		t.Fatal("missing explicit path accepted")
	}
	// Present file parses; absent keys stay nil (unset, not zero).
	p := filepath.Join(dir, "equalix.yaml")
	if err := os.WriteFile(p, []byte("addr: file:9\nmetrics-tenant-cap: 25\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	fc, err := loadFileConfig(p, true)
	if err != nil {
		t.Fatal(err)
	}
	if fc.Addr == nil || *fc.Addr != "file:9" {
		t.Fatalf("addr not parsed: %+v", fc)
	}
	if fc.DSN != nil {
		t.Fatalf("absent key not nil: %+v", fc)
	}
	got, err := resolveSettings(map[string]bool{}, settings{}, getenvOf(nil), fc)
	if err != nil {
		t.Fatal(err)
	}
	if got.Addr != "file:9" || got.TenantCap != 25 {
		t.Fatalf("file values not applied: %+v", got)
	}
	// Malformed YAML fails loud.
	bad := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(bad, []byte("addr: [unclosed"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadFileConfig(bad, true); err == nil {
		t.Fatal("malformed YAML accepted")
	}
}

func TestMigrateKnobPrecedence(t *testing.T) {
	// Default off everywhere: opt-in, never accidental. The full unit
	// suite runs flag-off, which is the parity-preservation evidence
	// (local CMS path unchanged); this asserts the default, not behavior.
	got, err := resolveSettings(map[string]bool{}, settings{}, getenvOf(nil), fileConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if got.RedisEnabled || got.RedisURL != "" {
		t.Fatalf("redis defaults wrong: %+v", got)
	}
	if got.MigrateOnStartup || got.MigrationsDir != "" {
		t.Fatalf("migrate defaults wrong: %+v", got)
	}
	// Env enables.
	got, err = resolveSettings(map[string]bool{}, settings{},
		getenvOf(map[string]string{"EQUALIX_MIGRATE_ON_STARTUP": "true"}), fileConfig{})
	if err != nil || !got.MigrateOnStartup {
		t.Fatalf("env did not enable: %+v %v", got, err)
	}
	// Explicit flag false vetoes env true AND file true simultaneously.
	fileTrue := true
	got, err = resolveSettings(map[string]bool{"migrate-on-startup": true},
		settings{MigrateOnStartup: false},
		getenvOf(map[string]string{"EQUALIX_MIGRATE_ON_STARTUP": "true"}),
		fileConfig{MigrateOnStartup: &fileTrue})
	if err != nil || got.MigrateOnStartup {
		t.Fatalf("explicit false did not veto all sources: %+v %v", got, err)
	}
	// File enables and carries the external dir.
	tr := true
	got, err = resolveSettings(map[string]bool{}, settings{}, getenvOf(nil),
		fileConfig{MigrateOnStartup: &tr, MigrationsDir: strp("/srv/sql")})
	if err != nil || !got.MigrateOnStartup || got.MigrationsDir != "/srv/sql" {
		t.Fatalf("file migrate keys not applied: %+v %v", got, err)
	}
}

// TestHealthcheck probes the subcommand against a fake mux: default
// /healthz path, explicit wrong path, and refused connection.
func TestHealthcheck(t *testing.T) {
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/readyz" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ok.Close()
	srvAddr := ok.Listener.Addr().String()
	// Default path is /healthz (liveness): no --path flag exercises it.
	if code := runHealthcheck([]string{"--addr", srvAddr}); code != 0 {
		t.Fatalf("healthy instance exit = %d, want 0", code)
	}
	if code := runHealthcheck([]string{"--addr", srvAddr, "--path", "/nope"}); code == 0 {
		t.Fatal("non-200 path exited 0")
	}
	// Shut the server down: refused connections exit non-zero, never hang
	// past the client timeout and never panic.
	ok.Close()
	if code := runHealthcheck([]string{"--addr", srvAddr}); code == 0 {
		t.Fatal("refused connection exited 0")
	}
}
