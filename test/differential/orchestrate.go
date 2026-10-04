//go:build differential

package differential

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"time"
)

// SideConfig describes one scheduler under test. Ports are parameters,
// never defaults: the harness asserts JavaPort != GoPort before starting
// either side, so a squatted 8080 fails fast instead of flaking.
type SideConfig struct {
	Name     string
	BaseURL  string
	DSN      string
	HTTPPort int
	APIKey   string
	PID      int // filled at run time for the artifact
}

// RunConfig is the fully resolved comparison configuration. Stub is a
// single shared value (not two equal structs): both sides read the same
// LatencyConfig, so a future refactor cannot change one side's stub
// parameters without the other's. Verify by construction, not convention.
type RunConfig struct {
	Java     SideConfig
	Go       SideConfig
	Workload string
	Stub     LatencyConfig
}

// Resolved is the artifact block answering "did both sides run against
// the intended config?" DSN passwords are redacted.
type Resolved struct {
	JavaDSN     string        `json:"java_dsn"`
	GoDSN       string        `json:"go_dsn"`
	JavaPort    int           `json:"java_port"`
	GoPort      int           `json:"go_port"`
	JavaPID     int           `json:"java_pid"`
	GoPID       int           `json:"go_pid"`
	Stub        LatencyConfig `json:"stub"`
	MarkerJava  time.Time     `json:"marker_java"`
	MarkerGo    time.Time     `json:"marker_go"`
	WorkloadSHA string        `json:"workload_sha"`
}

// Validate rejects incoherent runs before anything starts: missing files,
// colliding ports, distinct databases, unreachable bases. Every failure is
// a distinct error — a hang on any of these is a bug, never patience.
func (c RunConfig) Validate() error {
	if _, err := os.Stat(c.Workload); err != nil {
		return fmt.Errorf("differential: workload: %w", err)
	}
	if c.Java.HTTPPort == c.Go.HTTPPort {
		return fmt.Errorf("differential: java and go HTTP ports collide on %d", c.Java.HTTPPort)
	}
	if c.Java.DSN == "" || c.Go.DSN == "" {
		return fmt.Errorf("differential: both sides need a DSN")
	}
	if c.Java.DSN == c.Go.DSN {
		return fmt.Errorf("differential: java and go must use separate databases (shared PG entangles counts, locks, tasks)")
	}
	if err := c.Stub.Validate(); err != nil {
		return err
	}
	for _, s := range []SideConfig{c.Java, c.Go} {
		u, err := url.Parse(s.BaseURL)
		if err != nil || u.Host == "" {
			return fmt.Errorf("differential: side %q has bad URL %q", s.Name, s.BaseURL)
		}
		if err := checkPortFree(u.Host); err != nil {
			return fmt.Errorf("differential: side %q: %w", s.Name, err)
		}
	}
	return nil
}

// checkPortFree dials briefly: a listener already there is a squat, not a
// service — fail fast instead of talking to a stranger.
func checkPortFree(hostport string) error {
	conn, err := net.DialTimeout("tcp", hostport, 500*time.Millisecond)
	if err == nil {
		conn.Close()
		return fmt.Errorf("port %s already bound", hostport)
	}
	return nil
}

// ToOffsets translates wall-clock dispatch times to per-side offsets from
// that side's run-start marker. Comparing absolute times across runs that
// started 500ms apart would read skew as latency on every measurement;
// offsets normalize it away. Explicit function, not implicit log shape.
func ToOffsets(marker time.Time, wall map[string]time.Time) map[string]int64 {
	out := make(map[string]int64, len(wall))
	for id, t := range wall {
		out[id] = t.Sub(marker).Milliseconds()
	}
	return out
}

// Calibrate runs the falsification fixtures and reports mismatch-or-clean.
// Injected (not hardcoded) so tests drive both outcomes; production wires
// the FirstQueuedDecorator comparison. Runs before any real comparison.
type CalibrateFunc func(ctx context.Context) error

// Run executes pre-flight calibration first, then the comparison.
// Calibration failure aborts before measurement — the gate is live, not
// decorative.
func Run(ctx context.Context, cfg RunConfig, calibrate CalibrateFunc, compare func(ctx context.Context) (*Mismatch, error)) (*Resolved, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	resolved := &Resolved{
		JavaDSN: redact(cfg.Java.DSN), GoDSN: redact(cfg.Go.DSN),
		JavaPort: cfg.Java.HTTPPort, GoPort: cfg.Go.HTTPPort,
		JavaPID: cfg.Java.PID, GoPID: cfg.Go.PID,
		Stub: cfg.Stub,
	}
	if err := calibrate(ctx); err != nil {
		return resolved, fmt.Errorf("differential: pre-flight calibration failed: %w", err)
	}
	mm, err := compare(ctx)
	if err != nil {
		return resolved, err
	}
	if mm != nil {
		return resolved, mm
	}
	return resolved, nil
}

func redact(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil || u.User == nil {
		return "unparseable-dsn"
	}
	u.User = url.User(u.User.Username())
	return u.String()
}

// Probe distinguishes "service down" from "service never started": a
// refused connection means nothing listens (not started / wrong port); any
// HTTP response, even 4xx, means the service is up. Callers map these to
// distinct errors so a down dependency never reads as a hang.
func Probe(url string) error {
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return fmt.Errorf("service at %s unreachable (not started or wrong port): %w", url, err)
	}
	defer resp.Body.Close()
	return nil
}
