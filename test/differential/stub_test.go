//go:build differential

package differential

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

type webhook struct {
	id      string
	success bool
}

func runStub(t *testing.T, cfg StubConfig, ids []string) (stub *Stub, addr string, hooks []webhook) {
	t.Helper()
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Success bool   `json:"success"`
			Error   string `json:"error"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		hooks = append(hooks, webhook{id: r.URL.Path, success: body.Success})
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	cfg.CompleteBase = srv.URL
	cfg.APIKey = "k"
	stub, err := NewStub(cfg)
	if err != nil {
		t.Fatal(err)
	}
	addr, err = stub.Start()
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 5 * time.Second}
	for _, id := range ids {
		resp, err := client.Post("http://"+addr+"/tasks/"+id+"/execute", "application/octet-stream", nil)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("execute status = %d", resp.StatusCode)
		}
	}
	return stub, addr, hooks
}

func waitHooks(t *testing.T, want int, get func() int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if get() >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d/%d webhooks arrived", get(), want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestStubRecordsDispatchOrder proves per-run capture: three executes in
// order yield three log entries in order on Close.
func TestStubRecordsDispatchOrder(t *testing.T) {
	stub, _, _ := runStub(t, StubConfig{Port: 0, Latency: DefaultLatency()}, []string{"a", "b", "c"})
	log, err := stub.Close(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(log) != 3 || log[0].ID != "a" || log[1].ID != "b" || log[2].ID != "c" {
		t.Fatalf("log = %+v", log)
	}
	// Double close fails loudly — a spent instance is not reusable.
	if _, err := stub.Close(context.Background()); err == nil {
		t.Fatal("second Close should fail")
	}
}

// TestStubIsolationPerRun proves the reset boundary: two instances share
// nothing; the second log contains only its own traffic.
func TestStubIsolationPerRun(t *testing.T) {
	s1, _, _ := runStub(t, StubConfig{Port: 0, Latency: DefaultLatency()}, []string{"x"})
	l1, err := s1.Close(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	s2, _, _ := runStub(t, StubConfig{Port: 0, Latency: DefaultLatency()}, []string{"y", "z"})
	l2, err := s2.Close(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(l1) != 1 || l1[0].ID != "x" {
		t.Fatalf("run 1 log = %+v", l1)
	}
	if len(l2) != 2 || l2[0].ID != "y" || l2[1].ID != "z" {
		t.Fatalf("run 2 log = %+v (cross-run contamination?)", l2)
	}
}

// TestStubCompletionLatencyBound proves the latency path end to end: fixed
// 100ms stub delivers both webhooks no earlier than ~100ms and well within
// the test deadline (generous bounds — timing-sensitive assertions use
// lower bounds only, never exact sleeps).
func TestStubCompletionLatencyBound(t *testing.T) {
	var mu sync.Mutex
	count := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		count++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	cfg := StubConfig{Port: 0, Latency: DefaultLatency(), CompleteBase: srv.URL, APIKey: "k"}
	stub, err := NewStub(cfg)
	if err != nil {
		t.Fatal(err)
	}
	addr, err := stub.Start()
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	client := &http.Client{Timeout: 5 * time.Second}
	for _, id := range []string{"a", "b"} {
		resp, err := client.Post("http://"+addr+"/tasks/"+id+"/execute", "application/octet-stream", nil)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
	waitHooks(t, 2, func() int {
		mu.Lock()
		defer mu.Unlock()
		return count
	})
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Fatalf("completions arrived in %v, faster than the 100ms stub latency", elapsed)
	}
	if _, err := stub.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// TestStubRejectsBadConfig pins fail-fast construction.
func TestStubRejectsBadConfig(t *testing.T) {
	for _, cfg := range []StubConfig{
		{Latency: DefaultLatency()},                                   // no complete base
		{Latency: DefaultLatency(), CompleteBase: "x", ErrorRate: -1}, // bad rate
		{Latency: LatencyConfig{Shape: "nope"}, CompleteBase: "x"},    // bad shape
	} {
		if _, err := NewStub(cfg); err == nil {
			t.Fatalf("config %+v should reject", cfg)
		}
	}
}
