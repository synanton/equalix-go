package jobs

import (
	"context"
	"log/slog"
	"time"

	"golang.org/x/sync/errgroup"
)

// Job is one scheduled loop. Run blocks until ctx is done or a startup
// error occurs; tick errors are swallowed inside (recoverable) and must
// never escape as Run errors — only startup failures fail the group.
type Job interface {
	Name() string
	Run(ctx context.Context) error
}

// Runner owns the job lifecycle: pre-launch readiness, errgroup
// supervision, graceful shutdown. Readiness failure returns an error for
// main to exit non-zero (orchestrator retries; the process never invents
// its own backoff).
type Runner struct {
	jobs      []Job
	readiness []func(ctx context.Context) error
	log       *slog.Logger
}

// NewRunner builds a Runner. Readiness probes run in order before any
// goroutine launches; the first failure aborts startup.
func NewRunner(log *slog.Logger, readiness []func(ctx context.Context) error, jobs ...Job) *Runner {
	if log == nil {
		log = slog.Default()
	}
	return &Runner{jobs: jobs, readiness: readiness, log: log}
}

// Run blocks until ctx is cancelled (then drains within the caller's grace)
// or a job returns a startup error. Transient tick errors never surface
// here by contract (see Job).
func (r *Runner) Run(ctx context.Context) error {
	for _, probe := range r.readiness {
		if err := probe(ctx); err != nil {
			return err
		}
	}
	g, ctx := errgroup.WithContext(ctx)
	for _, j := range r.jobs {
		j := j
		g.Go(func() error {
			r.log.Info("job started", "job", j.Name())
			err := j.Run(ctx)
			r.log.Info("job stopped", "job", j.Name(), "err", err)
			return err
		})
	}
	return g.Wait()
}

// Loop is the shared ticker helper: runs tick immediately, then per
// interval, until ctx is done. Tick errors increment a streak; the streak
// logs at error level past threshold and resets on success. ctx is checked
// between iterations only — never mid-tick.
func Loop(ctx context.Context, log *slog.Logger, name string, interval time.Duration, streakThreshold int, tick func(ctx context.Context) error) {
	t := time.NewTicker(interval)
	defer t.Stop()
	streak := 0
	run := func() bool {
		if err := tick(ctx); err != nil {
			streak++
			if streak >= streakThreshold {
				log.Error("job tick failed", "job", name, "streak", streak, "err", err)
			} else {
				log.Warn("job tick failed", "job", name, "streak", streak, "err", err)
			}
			return false
		}
		streak = 0
		return true
	}
	run()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// Drain backlog: a slow tick skips beats instead of piling up.
			select {
			case <-t.C:
			default:
			}
			run()
		}
	}
}
