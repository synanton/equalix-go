// Package metrics is the Prometheus adapter for port.Metrics: the sole
// source of wire-name strings in the repo (spec §13 NOTE: Prometheus
// metric naming). Nothing outside this package names an equalix_*
// series — not port comments, not tests, not docs. Renames happen here,
// once, and break scrapers openly.
//
// Cardinality policy (pinned): tenant-labeled series admit the first
// TenantCap distinct tenants per metric, first-seen order; beyond the
// cap the sample drops and CardinalityExceeded{metric} increments, so
// the policy is observable instead of silent. Drift series additionally
// truncate to DriftMaxKeys sorted keys per report (deterministic), so a
// drift report never exceeds min(DriftMaxKeys, TenantCap) series; gauges
// for keys absent from a later report are deleted (no stale series).
//
// Registry scope: one prometheus.Registry per Adapter, never the
// process-global default — two test instances in one process must not
// collide on registration. No production cost.
package metrics

import (
	"fmt"
	"net/http"
	"sort"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/synanton/equalix-go/internal/port"
)

// Wire names. Referenced by registration, observation, and tests —
// never retyped as literals anywhere else.
const (
	TasksDispatched                = "equalix_tasks_dispatched_total"
	TasksCompleted                 = "equalix_tasks_completed_total"
	DispatchDecisionLatency        = "equalix_dispatch_decision_latency_seconds"
	TimeoutDetectionLatency        = "equalix_timeout_detection_latency_seconds"
	WatchdogReconciliationDuration = "equalix_watchdog_reconciliation_duration_seconds"
	CMSWarmupDuration              = "equalix_cms_warmup_duration_seconds"
	RPSCurrent                     = "equalix_rps_current"
	ReceivedQueueDepth             = "equalix_received_queue_depth"
	CMSDriftEstimate               = "equalix_cms_drift_estimate"
	CardinalityExceeded            = "equalix_metrics_cardinality_exceeded_total"
)

// Buckets per the EQLX-6 scope (histograms aggregate across instances;
// summaries would not — hence histograms everywhere a distribution
// crosses a process boundary).
var (
	dispatchDecisionBuckets  = []float64{1e-6, 5e-6, 1e-5, 5e-5, 1e-4, 5e-4, 1e-3}
	timeoutDetectionBuckets  = []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
	watchdogReconcileBuckets = []float64{0.1, 0.5, 1, 5, 10, 30, 60, 300}
	cmsWarmupBuckets         = []float64{0.01, 0.05, 0.1, 0.5, 1, 5, 30}
)

// Config parameterizes the adapter. Zero values are rejected, not
// defaulted — a metrics adapter with an accidental cap of 0 would drop
// every tenant-labeled sample, the silent class spec §13 tracks.
type Config struct {
	// TenantCap bounds distinct tenants per tenant-labeled metric.
	TenantCap int
	// DriftMaxKeys bounds per-report drift series (sorted truncation).
	DriftMaxKeys int
}

// DefaultConfig is the scoped default: 1000-tenant cap.
func DefaultConfig() Config {
	return Config{TenantCap: 1000, DriftMaxKeys: 1000}
}

func (c Config) validate() error {
	if c.TenantCap <= 0 {
		return fmt.Errorf("metrics: tenant cap must be > 0, got %d", c.TenantCap)
	}
	if c.DriftMaxKeys <= 0 {
		return fmt.Errorf("metrics: drift max keys must be > 0, got %d", c.DriftMaxKeys)
	}
	return nil
}

// Adapter implements port.Metrics against one private registry.
type Adapter struct {
	reg *prometheus.Registry
	cfg Config

	dispatched       *prometheus.CounterVec
	completed        *prometheus.CounterVec
	dispatchLatency  *prometheus.HistogramVec
	timeoutLatency   *prometheus.HistogramVec
	watchdogDuration *prometheus.HistogramVec
	cmsWarmup        *prometheus.HistogramVec
	rps              prometheus.Gauge
	queueDepth       prometheus.Gauge
	drift            *prometheus.GaugeVec
	exceeded         *prometheus.CounterVec

	mu        sync.Mutex
	admitted  map[string]map[string]bool // metric -> tenant set
	driftKeys map[string]bool            // tenants with a live drift gauge
}

var _ port.Metrics = (*Adapter)(nil)

// New builds and registers the full series set on a private registry.
func New(cfg Config) (*Adapter, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	reg := prometheus.NewRegistry()
	a := &Adapter{
		reg:       reg,
		cfg:       cfg,
		admitted:  map[string]map[string]bool{},
		driftKeys: map[string]bool{},
		dispatched: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: TasksDispatched,
			Help: "Dispatches by tenant.",
		}, []string{"tenant"}),
		completed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: TasksCompleted,
			Help: "Terminal completions by tenant and result (success, failed, timeout).",
		}, []string{"tenant", "result"}),
		dispatchLatency: newHistogram(DispatchDecisionLatency,
			"Dispatch-decision latency: selection cost, no I/O.", dispatchDecisionBuckets),
		timeoutLatency: newHistogram(TimeoutDetectionLatency,
			"Timeout-detection latency: deadline expiry to TIMEOUT marking.", timeoutDetectionBuckets),
		watchdogDuration: newHistogram(WatchdogReconciliationDuration,
			"Watchdog tick duration.", watchdogReconcileBuckets),
		cmsWarmup: newHistogram(CMSWarmupDuration,
			"Startup sketch-rebuild duration.", cmsWarmupBuckets),
		rps: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: RPSCurrent, Help: "Adaptive controller current RPS cap.",
		}),
		queueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: ReceivedQueueDepth, Help: "RECEIVED backlog size.",
		}),
		drift: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: CMSDriftEstimate, Help: "Watchdog per-key CMS drift.",
		}, []string{"tenant"}),
		exceeded: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: CardinalityExceeded,
			Help: "Samples dropped by the tenant cardinality cap, by metric.",
		}, []string{"metric"}),
	}
	reg.MustRegister(a.dispatched, a.completed, a.dispatchLatency,
		a.timeoutLatency, a.watchdogDuration, a.cmsWarmup,
		a.rps, a.queueDepth, a.drift, a.exceeded)
	return a, nil
}

func newHistogram(name, help string, buckets []float64) *prometheus.HistogramVec {
	return prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: name, Help: help, Buckets: buckets,
	}, []string{})
}

func (a *Adapter) cap() int      { return a.cfg.TenantCap }
func (a *Adapter) driftCap() int { return a.cfg.DriftMaxKeys }

// Handler serves the registry for mounting on the service mux.
func (a *Adapter) Handler() http.Handler {
	return promhttp.HandlerFor(a.reg, promhttp.HandlerOpts{})
}

// admit reports whether tenant may use a tenant-labeled series of the
// named metric, recording the over-cap drop when not.
func (a *Adapter) admit(metric, tenant string, cap int) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	set, ok := a.admitted[metric]
	if !ok {
		set = map[string]bool{}
		a.admitted[metric] = set
	}
	if set[tenant] {
		return true
	}
	if len(set) >= cap {
		a.exceeded.WithLabelValues(metric).Inc()
		return false
	}
	set[tenant] = true
	return true
}

func (a *Adapter) RecordDispatch(tenant string) {
	if !a.admit(TasksDispatched, tenant, a.cap()) {
		return
	}
	a.dispatched.WithLabelValues(tenant).Inc()
}

func (a *Adapter) RecordCompletion(tenant, result string, _ int64) {
	if !a.admit(TasksCompleted, tenant, a.cap()) {
		return
	}
	a.completed.WithLabelValues(tenant, result).Inc()
}

func (a *Adapter) ObserveDispatchLatency(seconds float64) {
	a.dispatchLatency.WithLabelValues().Observe(seconds)
}

func (a *Adapter) ObserveTimeoutLatency(seconds float64) {
	a.timeoutLatency.WithLabelValues().Observe(seconds)
}

func (a *Adapter) ObserveWatchdogReconciliation(seconds float64) {
	a.watchdogDuration.WithLabelValues().Observe(seconds)
}

func (a *Adapter) ObserveCMSWarmup(seconds float64) {
	a.cmsWarmup.WithLabelValues().Observe(seconds)
}

func (a *Adapter) SetRPS(rps float64) { a.rps.Set(rps) }

func (a *Adapter) SetQueueDepth(n int) { a.queueDepth.Set(float64(n)) }

func (a *Adapter) PublishDrift(drift map[string]int64) {
	keys := make([]string, 0, len(drift))
	for k := range drift {
		keys = append(keys, k)
	}
	sort.Strings(keys) // deterministic truncation, not map order
	if len(keys) > a.driftCap() {
		keys = keys[:a.driftCap()]
	}
	keep := map[string]bool{}
	for _, k := range keys {
		if !a.admit(CMSDriftEstimate, k, a.cap()) {
			continue
		}
		a.drift.WithLabelValues(k).Set(float64(drift[k]))
		keep[k] = true
	}
	a.mu.Lock()
	for k := range a.driftKeys {
		if !keep[k] {
			a.drift.DeleteLabelValues(k)
		}
	}
	a.driftKeys = keep
	a.mu.Unlock()
}
