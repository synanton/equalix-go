package jobs

import (
	"context"
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
}

// NewSendPool returns a pool with capacity workers.
func NewSendPool(workers int) *SendPool {
	return &SendPool{sem: make(chan struct{}, workers)}
}

// Free reports currently available send slots (dispatcher claim input).
func (p *SendPool) Free() int { return cap(p.sem) - len(p.sem) }

// Submit runs fn(ctx) in a pooled goroutine. Callers must have claimed
// capacity via Free first; Submit blocks rather than oversubscribing, so a
// block here means the claim accounting is wrong. ctx carries shutdown:
// fn must abort on ctx.Done. done is closed when fn returns; failures
// increment FailedSends.
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
		if err := fn(ctx); err != nil {
			p.failed.Add(1)
		}
	}()
	return c
}

// FailedSends counts failed executor sends since construction. Written by
// the pool, currently unwired, reserved for the EQLX-4 error brake (which
// would otherwise be blind to async send failures — the brake's input must
// not depend on webhooks that may never arrive). Do not remove as dead
// code; do not duplicate in EQLX-4.
func (p *SendPool) FailedSends() uint64 { return p.failed.Load() }
