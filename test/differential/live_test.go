//go:build differential

package differential

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeScheduler is a minimal REST surface (ingest/status/complete) that
// auto-dispatches ingested tasks to a stub executor URL in arrival order.
// It exercises the live-run pipeline without a real scheduler.
type fakeScheduler struct {
	mu     sync.Mutex
	tasks  map[string]*fakeTask
	stub   string
	apiKey string
}

func (f *fakeScheduler) setStub(url string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stub = url
}

type fakeTask struct {
	id       string
	tenant   string
	weight   float64
	created  time.Time
	status   string
	priority int64
}

func newFakeScheduler(stubURL, apiKey string) *fakeScheduler {
	return &fakeScheduler{tasks: map[string]*fakeTask{}, stub: stubURL, apiKey: apiKey}
}

func (f *fakeScheduler) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/tasks", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if r.Header.Get("X-API-Key") != f.apiKey {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		var body struct {
			FairnessKey string  `json:"fairnessKey"`
			Weight      float64 `json:"weight"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.mu.Lock()
		id := "task-" + strings.ReplaceAll(body.FairnessKey, " ", "") + "-" + string(rune('0'+len(f.tasks)))
		f.tasks[id] = &fakeTask{id: id, tenant: body.FairnessKey, weight: body.Weight, created: time.Now(), status: "RECEIVED"}
		f.mu.Unlock()
		go f.dispatch(id)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(id)
	})
	mux.HandleFunc("/api/v1/tasks/", func(w http.ResponseWriter, r *http.Request) {
		rest := strings.TrimPrefix(r.URL.Path, "/api/v1/tasks/")
		if strings.HasSuffix(rest, "/complete") && r.Method == http.MethodPost {
			id := strings.TrimSuffix(rest, "/complete")
			f.mu.Lock()
			if t, ok := f.tasks[id]; ok && (t.status == "DISPATCHED" || t.status == "COMMITTED") {
				t.status = "SUCCEEDED"
			}
			f.mu.Unlock()
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method == http.MethodGet {
			f.mu.Lock()
			t, ok := f.tasks[rest]
			var snap fakeTask
			if ok {
				snap = *t // copy under lock; dispatch mutates concurrently
			}
			f.mu.Unlock()
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id": snap.id, "fairnessKey": snap.tenant, "status": snap.status,
				"priority": snap.priority, "createdAt": snap.created.Format(time.RFC3339Nano),
			})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
	return mux
}

func (f *fakeScheduler) dispatch(id string) {
	time.Sleep(10 * time.Millisecond)
	f.mu.Lock()
	t, ok := f.tasks[id]
	stub := f.stub
	if ok && t.status == "RECEIVED" {
		t.status = "DISPATCHED"
		t.priority = 100
	}
	f.mu.Unlock()
	if !ok || stub == "" {
		return
	}
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Post(stub+"/tasks/"+id+"/execute", "application/octet-stream", nil)
	if err == nil {
		resp.Body.Close()
	}
}

// TestLiveRunAgainstFakes drives RunSide end to end with fakes standing in
// for a real scheduler: fresh stub, ingest with marker capture, drain to
// terminal, Close-captured log, offset translation. The live pipeline with
// none of the live processes.
func TestLiveRunAgainstFakes(t *testing.T) {
	ctx := context.Background()
	fake := newFakeScheduler("", "k")
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()

	stub, err := NewStub(StubConfig{
		Port: 0, Latency: LatencyConfig{Shape: LatencyFixed, BaseMs: 20, Seed: 1},
		CompleteBase: srv.URL, APIKey: "k",
	})
	if err != nil {
		t.Fatal(err)
	}
	stubAddr, err := stub.Start()
	if err != nil {
		t.Fatal(err)
	}
	fake.setStub("http://" + stubAddr)

	// Empty DSN: no SQL visibility in fakes, quiescence wait skipped.
	svc := SideConfig{Name: "fake", BaseURL: srv.URL, DSN: "", HTTPPort: 1, APIKey: "k"}
	workload := []Task{
		{ID: "t-a-0", Tenant: "a", Weight: 1, CreatedAtOffsetMs: 0, SubmittedAtOffsetMs: 0, PayloadBytes: 4},
		{ID: "t-b-0", Tenant: "b", Weight: 2, CreatedAtOffsetMs: 0, SubmittedAtOffsetMs: 0, PayloadBytes: 4},
	}
	res, err := RunSide(ctx, svc, "k", workload, stub, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if res.Marker.IsZero() {
		t.Fatal("no run-start marker captured")
	}
	if len(res.Log.Order) != 2 {
		t.Fatalf("dispatch log = %+v, want 2 dispatches", res.Log.Order)
	}
	if _, mm := CompareShares(res.Log, 1000, 2); mm != nil {
		t.Fatalf("fake shares diverged: %v", mm)
	}
	// Offsets normalize per-side markers: same relative timing, zero skew.
	offs := ToOffsets(res.Marker, map[string]time.Time{"t-a-0": res.Marker.Add(50 * time.Millisecond)})
	if offs["t-a-0"] != 50 {
		t.Fatalf("offset = %d, want 50", offs["t-a-0"])
	}
}
