//go:build integration

package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	redisctr "github.com/testcontainers/testcontainers-go/modules/redis"

	redisadapter "github.com/synanton/equalix-go/internal/adapter/redis"
)

// redisMetrics is the minimal port.Metrics for adapter tests: only the
// degraded gauge is exercised (the full metrics surface is covered by
// the prom adapter's own tests).
type redisMetrics struct{ degraded bool }

func (m *redisMetrics) RecordDispatch(string)                  {}
func (m *redisMetrics) RecordCompletion(string, string, int64) {}
func (m *redisMetrics) ObserveDispatchLatency(float64)         {}
func (m *redisMetrics) ObserveTimeoutLatency(float64)          {}
func (m *redisMetrics) ObserveWatchdogReconciliation(float64)  {}
func (m *redisMetrics) ObserveCMSWarmup(float64)               {}
func (m *redisMetrics) SetRPS(float64)                         {}
func (m *redisMetrics) SetQueueDepth(int)                      {}
func (m *redisMetrics) PublishDrift(map[string]int64)          {}
func (m *redisMetrics) SetCMSDegraded(d bool)                  { m.degraded = d }

func startRedis(t *testing.T) (string, func()) {
	t.Helper()
	rc, err := redisctr.Run(context.Background(), "redis:7-alpine")
	if err != nil {
		t.Skipf("redis container unavailable: %v", err)
	}
	endpoint, err := rc.ConnectionString(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// ConnectionString returns a URI; go-redis Addr wants host:port.
	endpoint = strings.TrimPrefix(endpoint, "redis://")
	return endpoint, func() {
		if err := rc.Terminate(context.Background()); err != nil {
			t.Logf("redis terminate: %v", err)
		}
	}
}

func redisAdapter(t *testing.T, addr string) (*redisadapter.Adapter, *redisMetrics) {
	t.Helper()
	m := &redisMetrics{}
	a, err := redisadapter.New(context.Background(), redisadapter.Config{
		Addr: addr, Timeout: 5 * time.Second,
	}, 5, 65536, m)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	return a, m
}

// TestRedisContract exercises the full CMSStore surface against real
// Redis: add/estimate/total, pipelined batch, rebuild replacing state
// (stale keys deleted), and missing-key reads as zero.
func TestRedisContract(t *testing.T) {
	addr, stop := startRedis(t)
	defer stop()
	ctx := context.Background()
	a, _ := redisAdapter(t, addr)

	if got, err := a.EstimateCount(ctx, "ghost"); err != nil || got != 0 {
		t.Fatalf("missing key = %d, %v; want 0, nil", got, err)
	}
	if err := a.Add(ctx, "a", 7); err != nil {
		t.Fatal(err)
	}
	if err := a.AddBatch(ctx, map[string]int64{"a": 3, "b": 5}); err != nil {
		t.Fatal(err)
	}
	if got, _ := a.EstimateCount(ctx, "a"); got != 10 {
		t.Fatalf("a = %d, want 10", got)
	}
	if got, _ := a.EstimateCount(ctx, "b"); got != 5 {
		t.Fatalf("b = %d, want 5", got)
	}
	if got, _ := a.Total(ctx); got != 15 {
		t.Fatalf("total = %d, want 15", got)
	}
	// Rebuild replaces: b vanishes (stale key deleted), c appears.
	if err := a.Rebuild(ctx, map[string]int64{"c": 4}); err != nil {
		t.Fatal(err)
	}
	if got, _ := a.EstimateCount(ctx, "b"); got != 0 {
		t.Fatalf("stale b = %d, want 0 (deleted on rebuild)", got)
	}
	if got, _ := a.EstimateCount(ctx, "c"); got != 4 {
		t.Fatalf("c = %d, want 4", got)
	}
	if got, _ := a.Total(ctx); got != 4 {
		t.Fatalf("total after rebuild = %d, want 4", got)
	}
}

// TestRedisCrossInstance proves the shared view: two adapters, one
// Redis — writes through one are readable through the other. This is
// the evidence the README's scaling posture needs.
func TestRedisCrossInstance(t *testing.T) {
	addr, stop := startRedis(t)
	defer stop()
	ctx := context.Background()
	a1, _ := redisAdapter(t, addr)
	a2, _ := redisAdapter(t, addr)

	if err := a1.Add(ctx, "shared", 10); err != nil {
		t.Fatal(err)
	}
	if got, _ := a2.EstimateCount(ctx, "shared"); got != 10 {
		t.Fatalf("cross-instance read = %d, want 10", got)
	}
	if err := a2.AddBatch(ctx, map[string]int64{"shared": 5, "other": 2}); err != nil {
		t.Fatal(err)
	}
	if got, _ := a1.EstimateCount(ctx, "shared"); got != 15 {
		t.Fatalf("cross-instance batch read = %d, want 15", got)
	}
}

// TestRedisScriptReload covers the NOSCRIPT path: flush the script
// cache (what a Redis restart does) and verify the next call reloads
// and succeeds instead of spuriously degrading.
func TestRedisScriptReload(t *testing.T) {
	addr, stop := startRedis(t)
	defer stop()
	ctx := context.Background()
	a, m := redisAdapter(t, addr)

	if err := a.Add(ctx, "k", 1); err != nil {
		t.Fatal(err)
	}
	// Flush the server-side script cache out from under the adapter
	// (what a Redis restart does) through a second connection —
	// production code paths never flush.
	raw := redis.NewClient(&redis.Options{Addr: addr})
	defer raw.Close()
	if err := raw.ScriptFlush(ctx).Err(); err != nil {
		t.Skipf("flush unavailable: %v", err)
	}
	if err := a.Add(ctx, "k", 1); err != nil {
		t.Fatal(err)
	}
	if got, _ := a.EstimateCount(ctx, "k"); got != 2 {
		t.Fatalf("k = %d after script flush, want 2", got)
	}
	if m.degraded {
		t.Fatal("degraded after successful reload — NOSCRIPT retry failed")
	}
}

// TestRedisFallbackKillsServer is the fail-open proof: with Redis gone,
// every operation serves from the local mirror, the degraded gauge
// fires, and estimates stay correct.
func TestRedisFallbackKillsServer(t *testing.T) {
	addr, stop := startRedis(t)
	defer stop()
	ctx := context.Background()
	a, m := redisAdapter(t, addr)

	if err := a.Add(ctx, "pre", 4); err != nil {
		t.Fatal(err)
	}
	stop() // kill Redis mid-run; no restart — recovery is covered by reload test
	if err := a.Add(ctx, "pre", 1); err != nil {
		t.Fatalf("fallback add errored (must be nil, fail-open): %v", err)
	}
	if got, _ := a.EstimateCount(ctx, "pre"); got != 5 {
		t.Fatalf("fallback estimate = %d, want 5 (local mirror)", got)
	}
	if got, _ := a.Total(ctx); got != 5 {
		t.Fatalf("fallback total = %d, want 5", got)
	}
	if !m.degraded {
		t.Fatal("degraded gauge not fired during outage")
	}
	if a.Degraded() != true {
		t.Fatal("Degraded() false during outage")
	}
}

// TestRedisWarmup seeds from a DB-shaped snapshot and converges:
// fresh Redis + populated counts → estimates match.
func TestRedisWarmup(t *testing.T) {
	addr, stop := startRedis(t)
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	a, _ := redisAdapter(t, addr)

	snapshot := map[string]int64{"a": 12, "b": 7}
	if err := a.Warmup(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	for k, want := range snapshot {
		if got, _ := a.EstimateCount(ctx, k); got != want {
			t.Fatalf("warmed %s = %d, want %d", k, got, want)
		}
	}
}
