// Package jobs runs the scheduler's background jobs: dispatcher, priority
// calculator, watchdog, timeout sweep, and startup recovery. See
// docs/eqlx-3-jobs-scope.md for the blocking choices (errgroup shape,
// Transact-per-tick, claim-limited sends, thresholdless watchdog).
package jobs

import (
	"fmt"
	"time"
)

// Config carries every jobs knob. Defaults mirror the Java reference
// (spec §10) except the three new keys, which are marked NEW.
// YAML/env binding arrives in EQLX-6; until then this struct is the config.
type Config struct {
	// Dispatcher tick interval (Java app.queue.dispatcher-interval, 50ms).
	DispatcherInterval time.Duration
	// Calculator tick interval (Java priority-calc-interval, 100ms).
	CalculatorInterval time.Duration
	// Watchdog interval (Java watchdog.interval-minutes, 5m).
	WatchdogInterval time.Duration
	// MaxTasksInProcess caps global in-flight (Java 5000).
	MaxTasksInProcess int
	// MaxPerClientQuota caps per-key in-flight, 0 disables (Java 500).
	MaxPerClientQuota int
	// WorkerPollSize bounds calculator batches (Java 100).
	WorkerPollSize int
	// MaxQueuedTime bounds starvation promotion (Java max-queued-time-ms, 60s).
	MaxQueuedTime time.Duration
	// TaskTimeout bounds in-flight age, 0 disables (Java task-timeout-ms, 300s).
	TaskTimeout time.Duration
	// DispatchWorkers caps concurrent executor sends (NEW, default 32).
	DispatchWorkers int
	// ShutdownGrace bounds shutdown drain (NEW, default 30s).
	ShutdownGrace time.Duration
	// ErrorStreakThreshold counts consecutive tick failures before error
	// logging (NEW, default 5).
	ErrorStreakThreshold int
	// PenaltyFactor is the fixed in-flight pressure until EQLX-4 wires the
	// live controller (NEW, default 1000.0 = 1000/initial-rps Java parity).
	PenaltyFactor float64
}

// DefaultConfig returns Java defaults plus the new keys' defaults.
func DefaultConfig() Config {
	return Config{
		DispatcherInterval:   50 * time.Millisecond,
		CalculatorInterval:   100 * time.Millisecond,
		WatchdogInterval:     5 * time.Minute,
		MaxTasksInProcess:    5000,
		MaxPerClientQuota:    500,
		WorkerPollSize:       100,
		MaxQueuedTime:        60 * time.Second,
		TaskTimeout:          300 * time.Second,
		DispatchWorkers:      32,
		ShutdownGrace:        30 * time.Second,
		ErrorStreakThreshold: 5,
		PenaltyFactor:        1000.0,
	}
}

// Validate rejects incoherent combinations at startup (fail fast, never
// silent no-op). In particular it enforces the batch/grace relationship:
// bounded batches are what keep the worst-case tick under ShutdownGrace,
// so grace must cover a multiple of every job interval, and batches must
// stay within sane bounds.
func (c Config) Validate() error {
	// Timeout sweep ticks on DispatcherInterval (Java parity:
	// TaskTimeoutScheduler uses dispatcher-interval). No separate knob.
	if c.DispatcherInterval <= 0 || c.CalculatorInterval <= 0 ||
		c.WatchdogInterval <= 0 {
		return fmt.Errorf("jobs: intervals must be positive")
	}
	if c.WorkerPollSize < 1 || c.WorkerPollSize > 10000 {
		return fmt.Errorf("jobs: worker_poll_size %d out of [1, 10000]", c.WorkerPollSize)
	}
	if c.MaxTasksInProcess < 1 {
		return fmt.Errorf("jobs: max_tasks_in_process must be positive")
	}
	if c.DispatchWorkers < 1 {
		return fmt.Errorf("jobs: dispatch_workers must be positive")
	}
	if c.ErrorStreakThreshold < 1 {
		return fmt.Errorf("jobs: error_streak_threshold must be positive")
	}
	if c.PenaltyFactor <= 0 {
		return fmt.Errorf("jobs: penalty_factor must be positive")
	}
	// Drain budget, not interval multiple: grace must cover one in-flight
	// tick plus send-pool drain on the hot loop. Batch caps
	// (WorkerPollSize, freeSlots ≤ MaxTasksInProcess) bound work per tick;
	// grace covers a slow DB on top. The hot loop is the dispatcher, so
	// the rule keys off its interval; infrequent jobs (watchdog 5m,
	// timeout sweep) run batch-bounded single iterations measured in
	// seconds and fall under the 10s floor — their intervals correctly do
	// not enter the budget. Floor 10s keeps testing honest.
	if c.ShutdownGrace < 10*time.Second {
		return fmt.Errorf("jobs: shutdown_grace %v below 10s floor", c.ShutdownGrace)
	}
	if c.ShutdownGrace < 100*c.DispatcherInterval {
		return fmt.Errorf("jobs: shutdown_grace %v must cover 100x dispatcher interval %v",
			c.ShutdownGrace, c.DispatcherInterval)
	}
	if c.MaxQueuedTime <= 0 {
		// Zero would promote every task on the first tick, silently
		// destroying fairness; starvation backstop must be positive.
		return fmt.Errorf("jobs: max_queued_time must be positive")
	}
	// No recovery knob: Java has no recovery service (CORRECTION-2) —
	// startup does CMS warm-up, the timeout sweep owns stuck tasks from
	// its first tick. TaskTimeout <= 0 simply disables the sweep.
	return nil
}
