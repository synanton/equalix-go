package domain

import "sync"

// Counts is an in-memory durable-counter equivalent: per-key in-flight
// counts with atomic increment and floor-at-zero decrement, mirroring
// client_counts (UPDATE ... SET in_flight_count = GREATEST(0, ... + delta)).
type Counts struct {
	mu sync.Mutex
	m  map[string]int
}

// NewCounts returns empty counts.
func NewCounts() *Counts { return &Counts{m: make(map[string]int)} }

// Increment adds one in-flight slot for key (dispatch path).
func (c *Counts) Increment(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[key]++
}

// Decrement releases one in-flight slot, floored at zero (completion path).
func (c *Counts) Decrement(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m[key] > 0 {
		c.m[key]--
	}
}

// Set overwrites the count (watchdog repair path).
func (c *Counts) Set(key string, n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n < 0 {
		n = 0
	}
	c.m[key] = n
}

// Get returns the count for key (0 when absent).
func (c *Counts) Get(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.m[key]
}

// Total returns the global in-flight count (dispatcher freeSlots input).
func (c *Counts) Total() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	total := 0
	for _, n := range c.m {
		total += n
	}
	return total
}

// Snapshot returns a copy of all counts (watchdog / CMS rebuild input).
func (c *Counts) Snapshot() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]int, len(c.m))
	for k, v := range c.m {
		out[k] = v
	}
	return out
}
