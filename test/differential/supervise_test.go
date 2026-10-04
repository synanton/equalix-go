//go:build differential

package differential

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestLaunchBadBinaryFailsNamingSide proves fail-fast: a missing binary
// reports which side, without hanging.
func TestLaunchBadBinaryFailsNamingSide(t *testing.T) {
	_, err := Launch(context.Background(), ProcSpec{
		Name: "java", Bin: "/nonexistent/binary-xyz",
		ReadyURL: "http://127.0.0.1:1/", StartupTimeout: 5 * time.Second,
	})
	if err == nil || !strings.Contains(err.Error(), "java") {
		t.Fatalf("expected side-naming error, got %v", err)
	}
}

// TestLaunchTimeoutNamesSide proves a process that never becomes ready
// fails with the side named (JVM-with-bad-classpath shape), not a hang.
func TestLaunchTimeoutNamesSide(t *testing.T) {
	_, err := Launch(context.Background(), ProcSpec{
		Name: "go", Bin: "/bin/sleep", Args: []string{"60"},
		ReadyURL: "http://127.0.0.1:1/", StartupTimeout: 2 * time.Second,
	})
	if err == nil || !strings.Contains(err.Error(), "go") {
		t.Fatalf("expected side-naming timeout, got %v", err)
	}
}

// TestLaunchReadyAndStop proves the happy path: ready endpoint observed,
// Stop reaps cleanly.
func TestLaunchReadyAndStop(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	p, err := Launch(context.Background(), ProcSpec{
		Name: "go", Bin: "/bin/sleep", Args: []string{"60"},
		ReadyURL: srv.URL, StartupTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if p.PID() <= 0 {
		t.Fatal("no child PID recorded for the artifact")
	}
	if err := p.Stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}
}
