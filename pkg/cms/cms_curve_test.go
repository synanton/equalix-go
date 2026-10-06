package cms

import (
	"encoding/json"
	"flag"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

var updateCurve = flag.Bool("update-curve", false, "regenerate testdata/cms-curve.json")

// curveCase is one measured cell: per-key relative error distribution
// (p50/p99/max) plus mean absolute error. Relative error is undefined
// for zero-actual keys; the generator ensures every counted key has
// actual > 0 (zipfian tail keys with zero draws are excluded and
// their count reported as uncovered).
type curveCase struct {
	Cardinality   int     `json:"cardinality"`
	Distribution  string  `json:"distribution"`
	Total         int64   `json:"total"`
	KeysCovered   int     `json:"keys_covered"`
	KeysUncovered int     `json:"keys_uncovered"`
	P50Rel        float64 `json:"p50_rel"`
	P99Rel        float64 `json:"p99_rel"`
	MaxRel        float64 `json:"max_rel"`
	MeanAbs       float64 `json:"mean_abs"`
	MaxAbs        float64 `json:"max_abs"`
	Underest      int     `json:"underestimates"`
}

type curveDoc struct {
	Width  int         `json:"width"`
	Depth  int         `json:"depth"`
	Seed   int64       `json:"seed"`
	Cases  []curveCase `json:"cases"`
	Theory string      `json:"theory"`
}

// measureCurve runs the full matrix with a fixed seed (deterministic —
// same binary, same table, no flakiness possible by construction).
func measureCurve() curveDoc {
	const seed = 42
	const total = int64(1000000)
	doc := curveDoc{Width: 65536, Depth: 5, Seed: seed,
		Theory: "one-sided overestimate; abs error <= eps*total w.h.p., eps = 2/65536"}
	for _, n := range []int{100, 1000, 10000, 100000} {
		doc.Cases = append(doc.Cases,
			measureOne(n, "uniform", total, uniformKeys(n, total, seed)),
			measureOne(n, "zipfian-s1.5", total, zipfKeys(n, total, seed)))
	}
	return doc
}

func uniformKeys(n int, total int64, seed int64) map[string]int64 {
	r := rand.New(rand.NewSource(seed))
	out := map[string]int64{}
	per, rem := total/int64(n), total%int64(n)
	for i := 0; i < n; i++ {
		out[keyName(i)] = per
	}
	for i := int64(0); i < rem; i++ {
		out[keyName(r.Intn(n))]++
	}
	return out
}

func zipfKeys(n int, total int64, seed int64) map[string]int64 {
	r := rand.New(rand.NewSource(seed + 1))
	z := rand.NewZipf(r, 1.5, 1, uint64(n-1))
	out := map[string]int64{}
	for i := int64(0); i < total; i++ {
		out[keyName(int(z.Uint64()))]++
	}
	return out
}

func keyName(i int) string { return "key-" + itoa(i) }

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	return string(b[p:])
}

func measureOne(n int, dist string, total int64, actual map[string]int64) curveCase {
	s := New(65536, 5)
	for k, v := range actual {
		s.Add(k, v)
	}
	c := curveCase{Cardinality: n, Distribution: dist, Total: s.Total()}
	rels := make([]float64, 0, n)
	var absSum float64
	// Iterate the full key space (0..n-1), not just drawn keys: zipfian
	// tail keys with zero draws are uncovered, and silently dropping
	// them would flatter the curve exactly where it degrades.
	for i := 0; i < n; i++ {
		k := keyName(i)
		want := actual[k]
		if want <= 0 {
			c.KeysUncovered++
			continue
		}
		c.KeysCovered++
		got := s.EstimateCount(k)
		if got < want {
			c.Underest++
		}
		diff := float64(got - want)
		absSum += math.Abs(diff)
		rels = append(rels, math.Abs(diff)/float64(want))
	}
	sort.Float64s(rels)
	c.MeanAbs = absSum / float64(len(rels))
	c.P50Rel = rels[len(rels)*50/100]
	c.P99Rel = rels[len(rels)*99/100]
	c.MaxRel = rels[len(rels)-1]
	for i := 0; i < n; i++ {
		k := keyName(i)
		if want := actual[k]; want > 0 {
			if d := float64(s.EstimateCount(k) - want); d > c.MaxAbs {
				c.MaxAbs = d
			}
		}
	}
	return c
}

// TestErrorCurve pins the CMS error envelope: one-sidedness (never
// underestimates — the load-bearing invariant for in-flight counting),
// the theoretical bound, and the committed golden table. Regenerate
// with -update-curve after deliberate sketch changes only; a diff
// without one is a regression, not an update.
func TestErrorCurve(t *testing.T) {
	got := measureCurve()
	if *updateCurve {
		raw, err := json.MarshalIndent(got, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join("testdata", "cms-curve.json"), raw, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := os.ReadFile(filepath.Join("testdata", "cms-curve.json"))
	if err != nil {
		t.Fatalf("golden testdata missing (run with -update-curve once): %v", err)
	}
	var want curveDoc
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}
	wantJSON, _ := json.Marshal(want.Cases)
	gotJSON, _ := json.Marshal(got.Cases)
	if string(wantJSON) != string(gotJSON) {
		t.Fatal("curve drifted from golden testdata without -update-curve")
	}
	// Property assertions (independent of the golden — they pin the
	// invariants even if someone regenerates the file blindly).
	eps := 2.0 / 65536.0
	for _, c := range got.Cases {
		if c.Underest != 0 {
			t.Fatalf("%s n=%d: %d underestimates — one-sidedness broken",
				c.Distribution, c.Cardinality, c.Underest)
		}
		// Generous theoretical envelope: worst single-key absolute error
		// within an order of magnitude of eps*total (deterministic seed,
		// so no probabilistic flakiness — a breach is a code change).
		bound := 10 * eps * float64(c.Total)
		if c.MaxAbs > bound {
			t.Fatalf("%s n=%d: max abs error %v exceeds envelope %v",
				c.Distribution, c.Cardinality, c.MaxAbs, bound)
		}
		_ = bound
	}
}
