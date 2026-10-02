// Package cms implements a Count-Min Sketch for O(1) approximate in-flight
// counting per fairness key.
//
// Behavioral reference: Java Equalix CountMinSketchAdapter + CmsKeyHasher
// (see docs/spec.md §4). Key properties preserved:
//
//   - add(key, delta) touches one cell per row; estimateCount returns
//     max(0, min over rows) so decrement collisions never yield negatives.
//   - Key hashing is FNV-1a (64-bit) over the key's UTF-8 bytes finalized
//     with SplitMix64, mixed per row — identical cell layout to the Java
//     oracle for the same (width, depth).
//   - There is no time-based decay; counts change only via Add/Rebuild
//     (spec §4.3 decision: replicate code behavior, not design prose).
package cms

import (
	"math"
)

// Sketch is a depth × width matrix of counters. It is not safe for
// concurrent use; guard with a mutex at the call site (Java uses
// synchronized on the adapter).
type Sketch struct {
	width int
	depth int
	table [][]int64
	total int64
}

// New returns an empty sketch. Width controls error magnitude (ε = 2/width),
// depth controls error probability (δ = (1/2)^depth).
// Defaults mirroring Java: width 65536, depth 5 (~2.6 MB).
func New(width, depth int) *Sketch {
	if width <= 0 {
		panic("cms: width must be positive")
	}
	if depth <= 0 {
		panic("cms: depth must be positive")
	}
	table := make([][]int64, depth)
	for i := range table {
		table[i] = make([]int64, width)
	}
	return &Sketch{width: width, depth: depth, table: table}
}

// Width returns the number of columns per row.
func (s *Sketch) Width() int { return s.width }

// Depth returns the number of rows.
func (s *Sketch) Depth() int { return s.depth }

// Add increments (or decrements, for negative delta) the cells for key.
func (s *Sketch) Add(key string, delta int64) {
	h := hashKey(key)
	for row := 0; row < s.depth; row++ {
		s.table[row][cell(h, row, s.width)] += delta
	}
	s.total += delta
}

// EstimateCount returns max(0, min over rows) for key. The sketch never
// underestimates; it overestimates by at most 2N/width with probability
// 1 - 2^-depth, where N is the total in flight.
func (s *Sketch) EstimateCount(key string) int64 {
	h := hashKey(key)
	min := int64(math.MaxInt64)
	for row := 0; row < s.depth; row++ {
		if v := s.table[row][cell(h, row, s.width)]; v < min {
			min = v
		}
	}
	if min < 0 {
		return 0
	}
	return min
}

// Total returns max(0, total) of all deltas applied since creation or Rebuild.
func (s *Sketch) Total() int64 {
	if s.total < 0 {
		return 0
	}
	return s.total
}

// Rebuild discards all state and replays counts (watchdog / warm-up path).
func (s *Sketch) Rebuild(counts map[string]int64) {
	for _, row := range s.table {
		for i := range row {
			row[i] = 0
		}
	}
	s.total = 0
	for k, v := range counts {
		if v != 0 {
			s.Add(k, v)
		}
	}
}

// Drift reports estimate - actual per key over the union of tracked keys,
// mirroring CmsErrorRecorder (measured before rebuild).
func (s *Sketch) Drift(actual map[string]int64, keys []string) map[string]int64 {
	drift := make(map[string]int64, len(keys))
	for _, k := range keys {
		drift[k] = s.EstimateCount(k) - actual[k]
	}
	return drift
}

// hashKey is FNV-1a (64-bit) over the key bytes, finalized with SplitMix64.
// Mirrors Java CmsKeyHasher so cells agree with the oracle.
func hashKey(key string) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for i := 0; i < len(key); i++ {
		h ^= uint64(key[i])
		h *= prime64
	}
	return splitMix64(h)
}

// splitMix64 finalizer (same avalanche as Java's SplitMix64 usage).
func splitMix64(z uint64) uint64 {
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// cell mixes the key hash with a per-row constant and folds into [0, width).
// Mirrors Java: floorMod(mix(keyHash + (row+1) * 0x9E3779B97F4A7C15), width).
func cell(h uint64, row, width int) int {
	const golden = 0x9e3779b97f4a7c15
	m := h + uint64(row+1)*golden
	m ^= m >> 33
	m *= 0xff51afd7ed558ccd
	m ^= m >> 33
	return int(m % uint64(width))
}

// Epsilon returns the error-magnitude bound 2/width.
func (s *Sketch) Epsilon() float64 { return 2.0 / float64(s.width) }

// Delta returns the bound-exceed probability (1/2)^depth.
func (s *Sketch) Delta() float64 { return 1.0 / float64(uint64(1)<<uint(s.depth)) }

// CellOf exposes the cell index for key/row (testing interop with the oracle).
func (s *Sketch) CellOf(key string, row int) int {
	return cell(hashKey(key), row, s.width)
}
