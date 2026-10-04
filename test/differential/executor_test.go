//go:build differential

package differential

import (
	"testing"
)

func TestLatencyValidation(t *testing.T) {
	if err := DefaultLatency().Validate(); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []LatencyConfig{
		{Shape: "weird"},
		{Shape: LatencyFixed, BaseMs: -1},
		{Shape: LatencyUniform, BaseMs: 10, SpreadMs: 11},
		{Shape: LatencyLognormal, BaseMs: 0, Sigma: 1},
		{Shape: LatencyLognormal, BaseMs: 10, Sigma: 0},
	} {
		if _, err := NewSampler(bad); err == nil {
			t.Fatalf("config %+v should reject", bad)
		}
	}
}

func TestSamplerDeterministic(t *testing.T) {
	newSeq := func() []int64 {
		s, err := NewSampler(LatencyConfig{Shape: LatencyUniform, BaseMs: 100, SpreadMs: 20, Seed: 7})
		if err != nil {
			t.Fatal(err)
		}
		out := make([]int64, 50)
		for i := range out {
			out[i] = s.Next()
			if out[i] < 80 || out[i] > 120 {
				t.Fatalf("sample %d out of range", out[i])
			}
		}
		return out
	}
	a, b := newSeq(), newSeq()
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("same seed diverged at %d: %d != %d", i, a[i], b[i])
		}
	}
}

func TestSamplerFixedDefault(t *testing.T) {
	s, err := NewSampler(DefaultLatency())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if got := s.Next(); got != 100 {
			t.Fatalf("fixed latency = %d, want 100", got)
		}
	}
}

func TestSamplerLognormalNonNegative(t *testing.T) {
	s, err := NewSampler(LatencyConfig{Shape: LatencyLognormal, BaseMs: 100, Sigma: 1.5, Seed: 3})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		if got := s.Next(); got < 0 {
			t.Fatalf("negative sample %d", got)
		}
	}
}
