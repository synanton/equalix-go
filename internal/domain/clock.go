package domain

import "time"

// Clock abstracts time for the domain so tests use FakeClock instead of
// sleeping (spec: tests must be deterministic).
type Clock interface {
	Now() time.Time
}

// SystemClock is the production Clock.
type SystemClock struct{}

// Now returns the current wall-clock time.
func (SystemClock) Now() time.Time { return time.Now() }

// FakeClock is a manually advanced clock for tests.
type FakeClock struct {
	t time.Time
}

// NewFakeClock returns a FakeClock fixed at t.
func NewFakeClock(t time.Time) *FakeClock { return &FakeClock{t: t} }

// Now returns the fake time.
func (c *FakeClock) Now() time.Time { return c.t }

// Advance moves the fake time forward.
func (c *FakeClock) Advance(d time.Duration) { c.t = c.t.Add(d) }
