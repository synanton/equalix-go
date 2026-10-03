package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/synanton/equalix-go/internal/domain"
	"github.com/synanton/equalix-go/internal/port"
)

// fakeTasks is a map-backed TaskRepository with version-checked Save.
type fakeTasks struct {
	mu    sync.Mutex
	tasks map[string]*domain.Task
}

func (f *fakeTasks) get(id string) (*domain.Task, bool) {
	t, ok := f.tasks[id]
	return t, ok
}

func (f *fakeTasks) FindReceived(_ context.Context, _ int) ([]*domain.Task, error) {
	return nil, nil
}

func (f *fakeTasks) FindByID(_ context.Context, id string) (*domain.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, ok := f.tasks[id]
	if !ok {
		return nil, port.ErrNotFound
	}
	cp := *t
	return &cp, nil
}

func (f *fakeTasks) Save(_ context.Context, t *domain.Task) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if cur, ok := f.tasks[t.ID]; ok {
		if cur.Version != t.Version {
			return port.ErrVersionConflict
		}
		t.Version++
	}
	cp := *t
	f.tasks[t.ID] = &cp
	return nil
}

func (f *fakeTasks) FindAndLockDispatchable(_ context.Context, _ int, _ int) ([]*domain.Task, error) {
	return nil, nil
}

func (f *fakeTasks) FindStarved(_ context.Context, _ time.Duration, _ int) ([]*domain.Task, error) {
	return nil, nil
}

func (f *fakeTasks) FindTimedOut(_ context.Context, _ time.Duration, _ int) ([]*domain.Task, error) {
	return nil, nil
}

func (f *fakeTasks) CountInFlight(_ context.Context) (map[string]int, error) {
	return map[string]int{}, nil
}

func (f *fakeTasks) FindNextSequential(_ context.Context, _ string, _ int64) (*domain.Task, error) {
	return nil, nil
}

func (f *fakeTasks) ListByKey(_ context.Context, key string, status *domain.Status) ([]*domain.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*domain.Task
	for _, t := range f.tasks {
		if t.FairnessKey != key {
			continue
		}
		if status != nil && t.Status != *status {
			continue
		}
		cp := *t
		out = append(out, &cp)
	}
	return out, nil
}

type fakeCounts struct {
	mu sync.Mutex
	m  map[string]int
}

func (f *fakeCounts) Increment(_ context.Context, k string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.m[k]++
	return nil
}

func (f *fakeCounts) Decrement(_ context.Context, k string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.m[k] > 0 {
		f.m[k]--
	}
	return nil
}

func (f *fakeCounts) Get(_ context.Context, k string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.m[k], nil
}

func (f *fakeCounts) Set(_ context.Context, k string, n int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.m[k] = n
	return nil
}

func (f *fakeCounts) All(_ context.Context) (map[string]int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]int{}
	for k, v := range f.m {
		out[k] = v
	}
	return out, nil
}

type fakeSeqs struct {
	mu sync.Mutex
	m  map[string]*domain.SequenceState
}

func (f *fakeSeqs) FindOrCreate(_ context.Context, k string) (*domain.SequenceState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	st, ok := f.m[k]
	if !ok {
		st = &domain.SequenceState{FairnessKey: k}
		f.m[k] = st
	}
	cp := *st
	return &cp, nil
}

func (f *fakeSeqs) Save(_ context.Context, st *domain.SequenceState) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := *st
	f.m[st.FairnessKey] = &cp
	return nil
}

type fakeCMS struct {
	mu sync.Mutex
	m  map[string]int64
}

func (f *fakeCMS) Add(_ context.Context, k string, d int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.m[k] += d
	return nil
}

func (f *fakeCMS) EstimateCount(_ context.Context, k string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.m[k], nil
}

func (f *fakeCMS) Total(_ context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var t int64
	for _, v := range f.m {
		t += v
	}
	return t, nil
}

func (f *fakeCMS) Rebuild(_ context.Context, _ map[string]int64) error { return nil }

type completion struct {
	tenant, result string
	durationMs     int64
}

type fakeMetrics struct {
	mu          sync.Mutex
	dispatches  []string
	completions []completion
	rps         float64
}

func (f *fakeMetrics) RecordDispatch(t string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dispatches = append(f.dispatches, t)
}

func (f *fakeMetrics) RecordCompletion(t, r string, d int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.completions = append(f.completions, completion{t, r, d})
}

func (f *fakeMetrics) ObserveDispatchLatency(_ float64) {}
func (f *fakeMetrics) SetRPS(r float64)                 { f.rps = r }
func (f *fakeMetrics) PublishDrift(_ map[string]int64)  {}
func (f *fakeMetrics) SetQueueDepth(_ int)              {}

type fakeRPS struct{ rps float64 }

func (f fakeRPS) CurrentRPS() float64 { return f.rps }

type fixture struct {
	router  http.Handler
	tasks   *fakeTasks
	counts  *fakeCounts
	seqs    *fakeSeqs
	cms     *fakeCMS
	metrics *fakeMetrics
	clock   *domain.FakeClock
}

func newFixture() *fixture {
	fx := &fixture{
		tasks:   &fakeTasks{tasks: map[string]*domain.Task{}},
		counts:  &fakeCounts{m: map[string]int{}},
		seqs:    &fakeSeqs{m: map[string]*domain.SequenceState{}},
		cms:     &fakeCMS{m: map[string]int64{}},
		metrics: &fakeMetrics{},
		clock:   domain.NewFakeClock(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)),
	}
	fx.router = NewRouter(Deps{
		Tasks: fx.tasks, Counts: fx.counts, Sequences: fx.seqs, CMS: fx.cms,
		Metrics: fx.metrics, RPS: fakeRPS{rps: 8.5}, Clock: fx.clock,
		APIKey: "changeme", MaxPayloadBytes: 1024,
	})
	return fx
}

func (fx *fixture) do(method, path, body string, key string) *httptest.ResponseRecorder {
	var reader *bytes.Buffer
	if body == "" {
		reader = bytes.NewBuffer(nil)
	} else {
		reader = bytes.NewBufferString(body)
	}
	req := httptest.NewRequest(method, path, reader)
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("X-API-Key", key)
	}
	rec := httptest.NewRecorder()
	fx.router.ServeHTTP(rec, req)
	return rec
}

func (fx *fixture) authed(method, path, body string) *httptest.ResponseRecorder {
	return fx.do(method, path, body, "changeme")
}

func TestAuthRequired(t *testing.T) {
	fx := newFixture()
	for _, tc := range []struct{ method, path string }{
		{"POST", "/api/v1/tasks"},
		{"GET", "/api/v1/tasks/x"},
		{"GET", "/api/v1/tasks"},
		{"POST", "/api/v1/tasks/x/complete"},
		{"GET", "/api/v1/status"},
	} {
		rec := fx.do(tc.method, tc.path, "", "")
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("%s %s: code = %d, want 401", tc.method, tc.path, rec.Code)
		}
		if rec.Body.String() != "" {
			t.Fatalf("%s %s: 401 body must be empty, got %q", tc.method, tc.path, rec.Body.String())
		}
		rec = fx.do(tc.method, tc.path, "", "wrong")
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("%s %s: wrong key code = %d, want 401", tc.method, tc.path, rec.Code)
		}
	}
}

func TestIngestValid(t *testing.T) {
	fx := newFixture()
	rec := fx.authed("POST", "/api/v1/tasks", `{"fairnessKey":"tenant-123","weight":1.0,"payload":"aGVsbG8="}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("code = %d, body %s", rec.Code, rec.Body.String())
	}
	var id string
	if err := json.Unmarshal(rec.Body.Bytes(), &id); err != nil || id == "" {
		t.Fatalf("body %q is not a UUID string", rec.Body.String())
	}
	stored, ok := fx.tasks.get(id)
	if !ok || stored.Status != domain.StatusReceived || stored.HasPriority {
		t.Fatalf("stored = %+v, want RECEIVED without priority", stored)
	}
	// Sequential ingest creates sequence state.
	rec = fx.authed("POST", "/api/v1/tasks", `{"fairnessKey":"s","payload":"eA==","sequential":true,"sequenceNumber":1}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("sequential code = %d, body %s", rec.Code, rec.Body.String())
	}
	if _, ok := fx.seqs.m["s"]; !ok {
		t.Fatal("sequential ingest did not findOrCreate sequence state")
	}
}

func TestIngestValidation(t *testing.T) {
	fx := newFixture()
	cases := []struct {
		name, body string
		field      string
	}{
		{"blank key", `{"fairnessKey":"","payload":"eA=="}`, "fairnessKey"},
		{"missing key", `{"payload":"eA=="}`, "fairnessKey"},
		{"bad weight", `{"fairnessKey":"a","weight":-1,"payload":"eA=="}`, "weight"},
		{"null payload", `{"fairnessKey":"a"}`, "payload"},
		{"bad base64", `{"fairnessKey":"a","payload":"!!!"}`, "payload"},
		{"seq without number", `{"fairnessKey":"a","payload":"eA==","sequential":true}`, "sequenceNumber"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := fx.authed("POST", "/api/v1/tasks", tc.body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("code = %d", rec.Code)
			}
			var env envelope
			if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
				t.Fatal(err)
			}
			if env.Code != "VALIDATION_FAILED" {
				t.Fatalf("code = %s, want VALIDATION_FAILED", env.Code)
			}
			found := false
			for _, fe := range env.FieldErrors {
				if fe.Field == tc.field {
					found = true
				}
			}
			if !found {
				t.Fatalf("fieldErrors %v lack %q", env.FieldErrors, tc.field)
			}
		})
	}
}

func TestIngestOversizeIsBadRequest(t *testing.T) {
	fx := newFixture()
	big := strings.Repeat("A", 2048) // valid base64, decodes > 1024 cap
	rec := fx.authed("POST", "/api/v1/tasks", fmt.Sprintf(`{"fairnessKey":"a","payload":%q}`, big))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code = %d", rec.Code)
	}
	var env envelope
	_ = json.Unmarshal(rec.Body.Bytes(), &env)
	if env.Code != "BAD_REQUEST" {
		t.Fatalf("code = %s, want BAD_REQUEST (use-case rule, not validation)", env.Code)
	}
}

func seedDispatched(fx *fixture, id, key string) {
	fx.tasks.tasks[id] = &domain.Task{
		ID: id, FairnessKey: key, Weight: 1, Status: domain.StatusDispatched,
		Priority: 10, HasPriority: true, CreatedAt: fx.clock.Now().Add(-time.Second),
		UpdatedAt: fx.clock.Now().Add(-time.Second),
	}
	fx.counts.m[key] = 1
	fx.cms.m[key] = 1
}

func TestCompleteSuccess(t *testing.T) {
	fx := newFixture()
	seedDispatched(fx, "t1", "a")
	rec := fx.authed("POST", "/api/v1/tasks/t1/complete", `{"success":true,"result":"b3V0cHV0"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d, body %s", rec.Code, rec.Body.String())
	}
	stored, _ := fx.tasks.get("t1")
	if stored.Status != domain.StatusSucceeded || stored.CompletedAt.IsZero() {
		t.Fatalf("stored = %+v", stored)
	}
	if fx.counts.m["a"] != 0 || fx.cms.m["a"] != 0 {
		t.Fatalf("slots not released: counts=%v cms=%v", fx.counts.m, fx.cms.m)
	}
	if len(fx.metrics.completions) != 1 {
		t.Fatalf("metrics = %+v", fx.metrics.completions)
	}
	c := fx.metrics.completions[0]
	if c.tenant != "a" || c.result != "success" || c.durationMs != 1000 {
		t.Fatalf("completion = %+v, want {a success 1000}", c)
	}
}

func TestCompleteDuplicateIgnored(t *testing.T) {
	fx := newFixture()
	seedDispatched(fx, "t1", "a")
	fx.authed("POST", "/api/v1/tasks/t1/complete", `{"success":false,"error":"boom"}`)
	rec := fx.authed("POST", "/api/v1/tasks/t1/complete", `{"success":true}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("duplicate code = %d", rec.Code)
	}
	if fx.counts.m["a"] != 0 || fx.cms.m["a"] != 0 {
		t.Fatalf("double decrement: counts=%v cms=%v", fx.counts.m, fx.cms.m)
	}
	if len(fx.metrics.completions) != 1 {
		t.Fatalf("duplicate recorded metrics: %+v", fx.metrics.completions)
	}
}

func TestCompleteErrors(t *testing.T) {
	fx := newFixture()
	// Unknown id → 404 envelope.
	rec := fx.authed("POST", "/api/v1/tasks/nope/complete", `{"success":true}`)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("code = %d", rec.Code)
	}
	var env envelope
	_ = json.Unmarshal(rec.Body.Bytes(), &env)
	if env.Code != "NOT_FOUND" {
		t.Fatalf("code = %s", env.Code)
	}
	// failure without error → 400 VALIDATION_FAILED.
	seedDispatched(fx, "t2", "a")
	rec = fx.authed("POST", "/api/v1/tasks/t2/complete", `{"success":false}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code = %d", rec.Code)
	}
	// Non-in-flight (QUEUED) → 400 BAD_REQUEST.
	fx.tasks.tasks["t3"] = &domain.Task{ID: "t3", FairnessKey: "a", Status: domain.StatusQueued}
	rec = fx.authed("POST", "/api/v1/tasks/t3/complete", `{"success":true}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("queued complete code = %d", rec.Code)
	}
}

func TestGetAndList(t *testing.T) {
	fx := newFixture()
	seedDispatched(fx, "t1", "a")
	rec := fx.authed("GET", "/api/v1/tasks/t1", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d", rec.Code)
	}
	var one taskStatusResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &one); err != nil {
		t.Fatal(err)
	}
	if one.Status != "DISPATCHED" || one.RetryCount != 0 || one.Priority == nil || *one.Priority != 10 {
		t.Fatalf("task = %+v", one)
	}
	// RECEIVED task carries explicit null priority.
	fx.tasks.tasks["t9"] = &domain.Task{ID: "t9", FairnessKey: "a", Status: domain.StatusReceived}
	rec = fx.authed("GET", "/api/v1/tasks/t9", "")
	var recv taskStatusResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &recv)
	if recv.Priority != nil || recv.CompletedAt != nil || recv.LastError != nil {
		t.Fatalf("nulls not null: %+v", recv)
	}
	rec = fx.authed("GET", "/api/v1/tasks/missing", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("code = %d", rec.Code)
	}
	// List: missing key → 400; bad status → 400; filter works.
	if rec := fx.authed("GET", "/api/v1/tasks", ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("no-key code = %d", rec.Code)
	}
	if rec := fx.authed("GET", "/api/v1/tasks?fairnessKey=a&status=BOGUS", ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad-status code = %d", rec.Code)
	}
	rec = fx.authed("GET", "/api/v1/tasks?fairnessKey=a&status=DISPATCHED", "")
	var list []taskStatusResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &list)
	if len(list) != 1 || list[0].ID != "t1" {
		t.Fatalf("list = %+v", list)
	}
}

func TestStatus(t *testing.T) {
	fx := newFixture()
	fx.cms.m["a"] = 12
	rec := fx.authed("GET", "/api/v1/status", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d", rec.Code)
	}
	var st systemStatusResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &st); err != nil {
		t.Fatal(err)
	}
	if st.InFlight != 12 || st.CurrentRPS != 8.5 {
		t.Fatalf("status = %+v", st)
	}
}
