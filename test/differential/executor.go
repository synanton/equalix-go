//go:build differential

package differential

import (
	"fmt"
	"math"
	"math/rand"
)

// LatencyShape selects the stub executor's completion-latency distribution.
// Completion timing drives in-flight counts → CMS behavior → scheduling
// decisions, so the distribution is a first-class harness input: both runs
// use byte-identical parameters from the same workload run config.
type LatencyShape string

const (
	// LatencyFixed completes every task after exactly BaseMs (default).
	LatencyFixed LatencyShape = "fixed"
	// LatencyUniform draws from [BaseMs-SpreadMs, BaseMs+SpreadMs].
	LatencyUniform LatencyShape = "uniform"
	// LatencyLognormal draws exp(N(log(BaseMs), Sigma)). BaseMs must be > 0.
	LatencyLognormal LatencyShape = "lognormal"
)

// LatencyConfig parameterizes the stub. Seed makes every shape
// reproducible; identical config on both runs is what makes "the
// schedulers disagree" distinguishable from "the stubs differed."
type LatencyConfig struct {
	Shape    LatencyShape
	BaseMs   int64
	SpreadMs int64
	Sigma    float64
	Seed     int64
}

// Validate rejects incoherent configs at harness startup.
func (c LatencyConfig) Validate() error {
	switch c.Shape {
	case LatencyFixed:
		if c.BaseMs < 0 {
			return fmt.Errorf("differential: fixed latency must be >= 0")
		}
	case LatencyUniform:
		if c.BaseMs < 0 || c.SpreadMs < 0 || c.SpreadMs > c.BaseMs {
			return fmt.Errorf("differential: uniform needs 0 <= spread <= base")
		}
	case LatencyLognormal:
		if c.BaseMs <= 0 || c.Sigma <= 0 {
			return fmt.Errorf("differential: lognormal needs base > 0, sigma > 0")
		}
	default:
		return fmt.Errorf("differential: unknown latency shape %q", c.Shape)
	}
	return nil
}

// DefaultLatency is the pinned default: fixed 100ms, no jitter.
func DefaultLatency() LatencyConfig {
	return LatencyConfig{Shape: LatencyFixed, BaseMs: 100, Seed: 42}
}

// Sampler draws completion latencies deterministically from the config.
// Determinism is cross-platform by construction: math/rand (v1) with an
// explicit Seed — never the global rand, never time-seeded, never
// math/rand/v2 (whose algorithm versioning differs). A sampler that
// diverged by platform would read as scheduler divergence.
type Sampler struct {
	cfg LatencyConfig
	rng *rand.Rand
}

// NewSampler builds a Sampler. Config must Validate.
func NewSampler(cfg LatencyConfig) (*Sampler, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Sampler{cfg: cfg, rng: rand.New(rand.NewSource(cfg.Seed))}, nil
}

// Next returns the next completion latency in milliseconds (>= 0).
func (s *Sampler) Next() int64 {
	switch s.cfg.Shape {
	case LatencyUniform:
		span := 2*s.cfg.SpreadMs + 1
		return s.cfg.BaseMs - s.cfg.SpreadMs + s.rng.Int63n(span)
	case LatencyLognormal:
		mu := math.Log(float64(s.cfg.BaseMs))
		return int64(math.Exp(mu + s.cfg.Sigma*s.rng.NormFloat64()))
	default:
		return s.cfg.BaseMs
	}
}
