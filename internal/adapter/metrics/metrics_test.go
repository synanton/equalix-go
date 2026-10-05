package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	dto "github.com/prometheus/client_model/go"
	"io"
)

// families gathers the registry by name for assertions. Tests reference
// the wire-name constants, never literals — a rename updates the const
// block and every assertion follows (the single-source rule, enforced by
// construction: there is no other string to update).
func families(t *testing.T, a *Adapter) map[string]*dto.MetricFamily {
	t.Helper()
	mfs, err := a.reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]*dto.MetricFamily{}
	for _, mf := range mfs {
		out[mf.GetName()] = mf
	}
	return out
}

func counterValue(t *testing.T, a *Adapter, name string, labels ...string) float64 {
	t.Helper()
	mf, ok := families(t, a)[name]
	if !ok {
		t.Fatalf("series %s not registered", name)
	}
	for _, m := range mf.GetMetric() {
		match := len(m.GetLabel()) == len(labels)
		for i, want := range labels {
			if !match || m.GetLabel()[i].GetValue() != want {
				match = false
				break
			}
		}
		if match {
			return m.GetCounter().GetValue()
		}
	}
	t.Fatalf("series %s labels %v not found", name, labels)
	return 0
}

func TestNewRejectsZeroConfig(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("zero config accepted — a zero tenant cap would drop every labeled sample silently")
	}
	if _, err := New(Config{TenantCap: 1, DriftMaxKeys: 0}); err == nil {
		t.Fatal("zero drift cap accepted")
	}
}

func TestAllWireNamesRegistered(t *testing.T) {
	// Small cap so the exceeded counter fires below too: every wire
	// name must appear after one pass over the observation paths.
	a, err := New(Config{TenantCap: 2, DriftMaxKeys: 10})
	if err != nil {
		t.Fatal(err)
	}
	// Untouched vecs gather to nothing, so exercise every observation
	// path once: registration is proven through the same calls
	// production makes, not through registry introspection of empties.
	a.RecordDispatch("t")
	a.RecordDispatch("u")
	a.RecordDispatch("overflow")
	a.RecordCompletion("t", "success", 1)
	a.ObserveDispatchLatency(0.001)
	a.ObserveTimeoutLatency(0.1)
	a.ObserveWatchdogReconciliation(0.1)
	a.ObserveCMSWarmup(0.1)
	a.SetRPS(1)
	a.SetQueueDepth(0)
	a.PublishDrift(map[string]int64{"t": 0})
	for _, name := range []string{
		TasksDispatched, TasksCompleted,
		DispatchDecisionLatency, TimeoutDetectionLatency,
		WatchdogReconciliationDuration, CMSWarmupDuration,
		RPSCurrent, ReceivedQueueDepth, CMSDriftEstimate,
		CardinalityExceeded,
	} {
		if _, ok := families(t, a)[name]; !ok {
			t.Fatalf("wire name %s not registered", name)
		}
	}
}

func TestTenantCapDropsAndCounts(t *testing.T) {
	a, err := New(Config{TenantCap: 2, DriftMaxKeys: 10})
	if err != nil {
		t.Fatal(err)
	}
	a.RecordDispatch("a")
	a.RecordDispatch("b")
	a.RecordDispatch("c") // over cap: dropped, counted
	if got := counterValue(t, a, TasksDispatched, "a"); got != 1 {
		t.Fatalf("a = %v, want 1", got)
	}
	// Admission is per metric: completions have their own cap budget.
	a.RecordCompletion("a", "success", 100)
	a.RecordCompletion("b", "success", 100)
	a.RecordCompletion("c", "success", 100) // over cap here too
	present := false
	if mf, ok := families(t, a)[TasksCompleted]; ok {
		for _, m := range mf.GetMetric() {
			if len(m.GetLabel()) == 2 && m.GetLabel()[0].GetValue() == "c" {
				present = true
			}
		}
	}
	if present {
		t.Fatal("c completed present — over-cap sample was not dropped")
	}
	if got := counterValue(t, a, CardinalityExceeded, TasksCompleted); got != 1 {
		t.Fatalf("exceeded{%s} = %v, want 1", TasksCompleted, got)
	}
	if got := counterValue(t, a, CardinalityExceeded, TasksDispatched); got != 1 {
		t.Fatalf("exceeded{%s} = %v, want 1 (the dropped dispatch)", TasksDispatched, got)
	}
	// Admitted tenants keep flowing after the cap fills.
	a.RecordDispatch("a")
	if got := counterValue(t, a, TasksDispatched, "a"); got != 2 {
		t.Fatalf("a = %v, want 2", got)
	}
}

func TestHistogramsObserve(t *testing.T) {
	a, err := New(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	a.ObserveDispatchLatency(0.0002)
	a.ObserveTimeoutLatency(0.3)
	a.ObserveWatchdogReconciliation(1.5)
	a.ObserveCMSWarmup(0.05)
	for name, want := range map[string]uint64{
		DispatchDecisionLatency: 1, TimeoutDetectionLatency: 1,
		WatchdogReconciliationDuration: 1, CMSWarmupDuration: 1,
	} {
		mf, ok := families(t, a)[name]
		if !ok {
			t.Fatalf("histogram %s missing", name)
		}
		if got := mf.GetMetric()[0].GetHistogram().GetSampleCount(); got != want {
			t.Fatalf("%s samples = %d, want %d", name, got, want)
		}
	}
}

func TestGauges(t *testing.T) {
	a, err := New(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	a.SetRPS(12.5)
	a.SetQueueDepth(42)
	mfs := families(t, a)
	if got := mfs[RPSCurrent].GetMetric()[0].GetGauge().GetValue(); got != 12.5 {
		t.Fatalf("rps = %v, want 12.5", got)
	}
	if got := mfs[ReceivedQueueDepth].GetMetric()[0].GetGauge().GetValue(); got != 42 {
		t.Fatalf("queue = %v, want 42", got)
	}
}

func TestDriftTruncationAndStaleDeletion(t *testing.T) {
	a, err := New(Config{TenantCap: 10, DriftMaxKeys: 2})
	if err != nil {
		t.Fatal(err)
	}
	a.PublishDrift(map[string]int64{"c": 3, "a": 1, "b": 2})
	mf := families(t, a)[CMSDriftEstimate]
	if len(mf.GetMetric()) != 2 {
		t.Fatalf("drift series = %d, want 2 (sorted truncation)", len(mf.GetMetric()))
	}
	// Sorted truncation keeps a, b — deterministic, not map order.
	seen := map[string]bool{}
	for _, m := range mf.GetMetric() {
		seen[m.GetLabel()[0].GetValue()] = true
	}
	if !seen["a"] || !seen["b"] || seen["c"] {
		t.Fatalf("drift kept %v, want {a b}", seen)
	}
	// A later report without b deletes its gauge (no stale series).
	a.PublishDrift(map[string]int64{"a": 9})
	mf = families(t, a)[CMSDriftEstimate]
	if len(mf.GetMetric()) != 1 {
		t.Fatalf("drift series after republish = %d, want 1", len(mf.GetMetric()))
	}
}

func TestSeparateRegistries(t *testing.T) {
	a, err := New(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	b, err := New(DefaultConfig())
	if err != nil {
		t.Fatal(err) // second registration must not collide
	}
	a.RecordDispatch("a")
	// b's registry must show no trace of a's sample: untouched vecs
	// gather to nothing, which is the isolation signal (a zero-valued
	// series would mean shared state).
	if mf, ok := families(t, b)[TasksDispatched]; ok {
		for _, m := range mf.GetMetric() {
			if len(m.GetLabel()) == 1 && m.GetLabel()[0].GetValue() == "a" {
				t.Fatal("registries share state: b saw a's dispatch")
			}
		}
	}
}

func TestHandlerServesExposition(t *testing.T) {
	a, err := New(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	a.RecordDispatch("a")
	a.SetRPS(7)
	srv := httptest.NewServer(a.Handler())
	defer srv.Close()
	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	buf, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	body := string(buf)
	for _, want := range []string{TasksDispatched, RPSCurrent} {
		if !strings.Contains(body, want) {
			t.Fatalf("exposition missing %s", want)
		}
	}
}
