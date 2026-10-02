package jobs

import (
	"context"
	"log/slog"
	"sync/atomic"
)

// SendPool bounds concurrent executor sends. The dispatcher claims
// min(freeSlots, pool free capacity) per tick and submits exactly that
// many sends — claim-limited, no queue (scope §2). A submit therefore never
// blocks on capacity in correct use; if it would, that is a dispatcher bug,
// not backpressure.
type SendPool struct {
	sem    chan struct{}
	failed atomic.Uint64
	log    *slog.Logger
}

// NewSendPool returns a pool with capacity workers. Panic policy,
// stated once: send-path panics are contained, never fatal. A panic in fn
// is recovered, logged, and counted into FailedSends; the pool does NOT
// re-panic and does NOT propagate to the errgroup — a crashing executor
// client degrades into a visible counter (the EQLX-4 brake's future input),
// never into a dead process. This extends the scope's recoverable-tick
// posture to the send path.
func NewSendPool(workers int) *SendPool {
	return &SendPool{sem: make(chan struct{}, workers), log: slog.Default()}
}

// Free reports currently available send slots (dispatcher claim input).
func (p *SendPool) Free() int { return cap(p.sem) - len(p.sem) }

// Submit runs fn(ctx) in a pooled goroutine. Callers must have claimed
// capacity via Free first; Submit blocks rather than oversubscribing, so a
// block here means the claim accounting is wrong. ctx carries shutdown:
// the pool guarantees fn sees a cancelled context, not the absence of new
// work — fn must abort on ctx.Done. done is closed when fn returns;
// failures (including recovered panics) increment FailedSends.
func (p *SendPool) Submit(ctx context.Context, fn func(ctx context.Context) error) (done <-chan struct{}) {
	c := make(chan struct{})
	select {
	case p.sem <- struct{}{}:
	case <-ctx.Done():
		close(c)
		return c
	}
	go func() {
		defer close(c)
		defer func() { <-p.sem }()
		defer func() {
			if v := recover(); v != nil {
				p.log.Error("send panicked", "panic", v)
				p.failed.Add(1)
			}
		}()
		if err := fn(ctx); err != nil {
			p.failed.Add(1)
		}
	}()
	return c
}

// FailedSends counts failed executor sends since construction: transport
// errors and application-layer declines alike. This is deliberately a
// superset of Java's completion-window error_rate (which never observes
// send-level failures — a declined send just sits DISPATCHED until timeout
// in Java too). Semantics fixed by spec §13 DECISION-5: earlier signal on
// the same failure mode; EQLX-4 tunes α and threshold against this timing
// difference, not Java's rate.
func (p *SendPool) FailedSends() uint64 { return p.failed.Load() }
