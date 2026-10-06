// Package redis is the shared CMSStore: two scheduler instances see
// one in-flight view (EQLX-8). Opt-in only (redis.enabled); disabled
// means the local sketch with byte-identical behavior, and the warm
// class parity claim survives exactly while defaults hold.
//
// Layout: one HASH per tenant (equalix:cms:v1:{tenant}), one field
// per row (field = row index — each row touches exactly its cell, so
// HGETALL returns the row values and the client takes the min, same
// semantics as the local sketch). A global total key tracks the sum,
// and a known-keys set tracks membership for rebuilds. No TTLs:
// eviction is watchdog-driven (SCAN + DEL absent keys on rebuild);
// a TTL would silently drop slow tenants mid-backlog.
//
// Failure policy is fail-open, sole policy (scope): any Redis error
// falls back to the local sketch and fires the degraded gauge; no
// code path refuses to dispatch for lack of Redis. A local mirror
// absorbs every write (success or fallback) so degraded reads stay
// approximately right; post-recovery Redis undercounts missed
// outage writes until the next watchdog rebuild corrects it (same
// window class as the crash-between-commit-and-flush §8 drift).
package redis

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/synanton/equalix-go/internal/port"
	"github.com/synanton/equalix-go/pkg/cms"
)

// Key names. Single source like the metrics adapter: no key literals
// outside this file (tests reference the helpers, never raw strings).
const keyPrefix = "equalix:cms:v1:"

const totalKey = keyPrefix + "__total"
const knownSet = keyPrefix + "__keys"

func tenantKey(tenant string) string { return keyPrefix + tenant }

// addScript increments one row-cell, tracks membership, and bumps the
// global total atomically (KEYS[1] = tenant hash, KEYS[2] = total key,
// KEYS[3] = known set; ARGV = row, delta). The total moves only on row
// 0: one Add call fans out to depth script calls (one per row), and an
// unconditional INCRBY would count the delta depth times.
const addScript = `
redis.call('HINCRBY', KEYS[1], ARGV[1], ARGV[2])
redis.call('SADD', KEYS[3], KEYS[1])
if tonumber(ARGV[1]) == 0 then
  redis.call('INCRBY', KEYS[2], ARGV[2])
end
return 1
`

// estimateScript returns max(0, min over row fields), or 0 for a
// missing key. One round trip, atomic read.
const estimateScript = `
local vals = redis.call('HGETALL', KEYS[1])
local min = nil
for i = 2, #vals, 2 do
  local v = tonumber(vals[i])
  if min == nil or v < min then min = v end
end
if min == nil or min < 0 then return 0 end
return min
`

// Config parameterizes the adapter. Zero values rejected, not
// defaulted (same rule as the metrics adapter).
type Config struct {
	// Addr is host:port (redis.Options.Addr form).
	Addr string
	// Timeout bounds dial, read, and write.
	Timeout time.Duration
}

// Adapter implements port.CMSStore against Redis with a local-sketch
// fallback. It is safe for concurrent use.
type Adapter struct {
	client  *redis.Client
	scripts map[string]string // name -> sha, loaded once at startup
	local   *cms.Sketch
	metrics port.Metrics

	depth int
	width int

	scriptMu sync.RWMutex
	mu       sync.Mutex
	degraded atomic.Bool
}

var _ port.CMSStore = (*Adapter)(nil)

// New connects, loads both scripts (SCRIPT LOAD once; EVALSHA on the
// hot path), and returns ready. Connection failure here fails fast —
// a misconfigured URL must not boot into silent fallback. Runtime
// Redis loss goes through the degraded path instead.
func New(ctx context.Context, cfg Config, depth, width int, metrics port.Metrics) (*Adapter, error) {
	if cfg.Addr == "" {
		return nil, fmt.Errorf("redis: addr required")
	}
	if cfg.Timeout <= 0 {
		return nil, fmt.Errorf("redis: timeout must be positive")
	}
	if depth <= 0 || width <= 0 {
		return nil, fmt.Errorf("redis: depth and width must be positive")
	}
	client := redis.NewClient(&redis.Options{
		Addr:         cfg.Addr,
		DialTimeout:  cfg.Timeout,
		ReadTimeout:  cfg.Timeout,
		WriteTimeout: cfg.Timeout,
	})
	a := &Adapter{
		client:  client,
		scripts: map[string]string{},
		local:   cms.New(width, depth),
		metrics: metrics,
		depth:   depth,
		width:   width,
	}
	for name, src := range map[string]string{"add": addScript, "estimate": estimateScript} {
		sha, err := client.ScriptLoad(ctx, src).Result()
		if err != nil {
			_ = client.Close()
			return nil, fmt.Errorf("redis: load %s script: %w", name, err)
		}
		a.scripts[name] = sha
	}
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redis: ping: %w", err)
	}
	return a, nil
}

// Close releases the client. The local mirror is process memory and
// needs no cleanup.
func (a *Adapter) Close() error { return a.client.Close() }

// runScript executes a loaded script by SHA, reloading once on
// NOSCRIPT (Redis restart/flush wipes the script cache — without the
// retry the first call after any restart would spuriously degrade).
func (a *Adapter) runScript(ctx context.Context, name string, keys []string, args ...interface{}) (interface{}, error) {
	a.scriptMu.RLock()
	sha := a.scripts[name]
	a.scriptMu.RUnlock()
	res, err := a.client.EvalSha(ctx, sha, keys, args...).Result()
	if err != nil && isNoScript(err) {
		src := addScript
		if name == "estimate" {
			src = estimateScript
		}
		nsha, lerr := a.client.ScriptLoad(ctx, src).Result()
		if lerr != nil {
			return nil, lerr
		}
		a.scriptMu.Lock()
		a.scripts[name] = nsha
		a.scriptMu.Unlock()
		return a.client.EvalSha(ctx, nsha, keys, args...).Result()
	}
	return res, err
}

func isNoScript(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "NOSCRIPT") || strings.Contains(msg, "No matching script")
}

// degrade flips to local on first failure (edge-triggered: one log
// line per transition with the cause — an unreachable Redis and a
// warm-up timeout both read "degraded" on the gauge, but the operator
// needs to tell "Redis is down, check Redis" from "seed timed out,
// check client_counts size and lock status". The gauge stays binary
// (cause lives in logs, not labels — a per-cause series doubles
// cardinality for a diagnostic dimension).
func (a *Adapter) degrade(err error) {
	if a.degraded.CompareAndSwap(false, true) {
		slog.Warn("cms redis degraded, using local mirror", "err", err)
		a.metrics.SetCMSDegraded(true)
	}
}

func (a *Adapter) recover() {
	if a.degraded.CompareAndSwap(true, false) {
		a.metrics.SetCMSDegraded(false)
	}
}

// Degraded reports fallback state for tests and the runbook.
func (a *Adapter) Degraded() bool { return a.degraded.Load() }

func (a *Adapter) Add(ctx context.Context, key string, delta int64) error {
	for row := 0; row < a.depth; row++ {
		if _, err := a.runScript(ctx, "add",
			[]string{tenantKey(key), totalKey, knownSet}, row, delta); err != nil {
			a.degrade(fmt.Errorf("add %s: %w", key, err))
			a.mu.Lock()
			a.local.Add(key, delta)
			a.mu.Unlock()
			return nil // fail-open: local mirror absorbs the write
		}
	}
	a.recover()
	a.mu.Lock()
	a.local.Add(key, delta)
	a.mu.Unlock()
	return nil
}

// AddBatch pipelines one add-script call per key (scope decision:
// fire-and-forget per key, NO MULTI — a dropped connection mid-batch
// leaves partial writes, bounded and self-healing via the next
// watchdog rebuild, same window class as the §8 crash drift).
// NOSCRIPT mid-batch is NOT resent: commands before the miss applied,
// the miss failed, the rest never executed — resending the whole batch
// would double-count the head. The batch degrades as a unit (mirror
// absorbed everything up front), scripts reload best-effort for the
// next call. Single-key Add, by contrast, reloads and retries once —
// resending one idempotent increment is safe, resending a partial
// batch is not. The two paths deliberately differ; do not "unify"
// them without re-deriving this paragraph.
// Callers must not assume cross-key atomicity.
func (a *Adapter) AddBatch(ctx context.Context, deltas map[string]int64) error {
	if len(deltas) == 0 {
		return nil
	}
	_, err := a.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		a.scriptMu.RLock()
		sha := a.scripts["add"]
		a.scriptMu.RUnlock()
		for key, delta := range deltas {
			for row := 0; row < a.depth; row++ {
				pipe.EvalSha(ctx, sha,
					[]string{tenantKey(key), totalKey, knownSet}, row, delta)
			}
		}
		return nil
	})
	a.mu.Lock()
	for k, d := range deltas {
		a.local.Add(k, d)
	}
	a.mu.Unlock()
	if err != nil {
		// A flushed script cache surfaces here as NOSCRIPT for the
		// whole batch: reload best-effort so the NEXT call succeeds,
		// degrade for this one (the mirror already absorbed it).
		if isNoScript(err) {
			a.reloadScripts(ctx)
		}
		a.degrade(fmt.Errorf("addBatch: %w", err))
		return nil // fail-open: mirror already absorbed the batch
	}
	a.recover()
	return nil
}

// reloadScripts re-caches both scripts (post-restart recovery path).
func (a *Adapter) reloadScripts(ctx context.Context) {
	for name, src := range map[string]string{"add": addScript, "estimate": estimateScript} {
		if sha, err := a.client.ScriptLoad(ctx, src).Result(); err == nil {
			a.scriptMu.Lock()
			a.scripts[name] = sha
			a.scriptMu.Unlock()
		}
	}
}

func toInt64(v interface{}) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	default:
		return 0
	}
}

func (a *Adapter) EstimateCount(ctx context.Context, key string) (int64, error) {
	res, err := a.runScript(ctx, "estimate", []string{tenantKey(key)})
	if err != nil {
		a.degrade(fmt.Errorf("estimate %s: %w", key, err))
		a.mu.Lock()
		defer a.mu.Unlock()
		return a.local.EstimateCount(key), nil
	}
	a.recover()
	return toInt64(res), nil
}

func (a *Adapter) Total(ctx context.Context) (int64, error) {
	n, err := a.client.Get(ctx, totalKey).Int64()
	if err != nil {
		a.degrade(fmt.Errorf("total: %w", err))
		a.mu.Lock()
		defer a.mu.Unlock()
		return a.local.Total(), nil
	}
	a.recover()
	if n < 0 {
		return 0, nil
	}
	return n, nil
}

// Rebuild replaces Redis state from a snapshot (watchdog path — caller
// holds the rebuild advisory lock): overwrite each key's fields, DEL
// keys absent from the snapshot (stale keys must not survive — an
// absent key with an old count reads as live in-flight), reset the
// total. Concurrent dispatcher ADDs racing the rebuild can lose an
// increment (documented residual race, same class as the local crash
// window — the next rebuild converges it).
func (a *Adapter) Rebuild(ctx context.Context, counts map[string]int64) error {
	known, err := a.client.SMembers(ctx, knownSet).Result()
	if err != nil {
		return fmt.Errorf("redis: rebuild known keys: %w", err)
	}
	keep := map[string]bool{}
	_, err = a.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for key, v := range counts {
			if v == 0 {
				continue
			}
			hk := tenantKey(key)
			keep[hk] = true
			pipe.Del(ctx, hk)
			fields := make([]interface{}, 0, 2*a.depth)
			for row := 0; row < a.depth; row++ {
				fields = append(fields, row, v)
			}
			pipe.HSet(ctx, hk, fields...)
			pipe.SAdd(ctx, knownSet, hk)
		}
		for _, hk := range known {
			if !keep[hk] {
				pipe.Del(ctx, hk)
				pipe.SRem(ctx, knownSet, hk)
			}
		}
		var total int64
		for _, v := range counts {
			total += v
		}
		pipe.Set(ctx, totalKey, total, 0)
		return nil
	})
	if err != nil {
		return fmt.Errorf("redis: rebuild: %w", err)
	}
	a.mu.Lock()
	a.local.Rebuild(counts)
	a.mu.Unlock()
	return nil
}

// Warmup seeds Redis from a DB snapshot (startup path — caller holds
// the warm-up advisory lock and a 30s timeout). Same mechanics as
// Rebuild; separate name so call sites read as startup, not repair.
func (a *Adapter) Warmup(ctx context.Context, counts map[string]int64) error {
	return a.Rebuild(ctx, counts)
}
