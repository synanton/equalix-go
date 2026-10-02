package jobs

import (
	"context"
	"log/slog"
	"time"
)

// NoopJob is a placeholder for jobs whose bodies land later (watchdog and
// timeout in 3b). It registers with the runner and ticks at its interval
// returning no error — validating the multi-job framework, not job logic.
type NoopJob struct {
	name     string
	interval time.Duration
}

// NewNoopJob returns a placeholder job ticking at interval.
func NewNoopJob(name string, interval time.Duration) NoopJob {
	return NoopJob{name: name, interval: interval}
}

// Name implements Job.
func (j NoopJob) Name() string { return j.name }

// Run implements Job: tick forever, do nothing, never fail.
func (j NoopJob) Run(ctx context.Context) error {
	Loop(ctx, slog.Default(), j.name, j.interval, 5, func(ctx context.Context) error {
		return nil
	})
	return nil
}
