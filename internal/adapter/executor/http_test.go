package executor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSendTrichotomy(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/tasks/ok/execute":
			w.WriteHeader(http.StatusOK)
		case "/tasks/slow/execute":
			time.Sleep(200 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer srv.Close()

	e := NewHTTPExecutor(srv.URL, 5*time.Second)
	ctx := context.Background()
	if ok, err := e.Send(ctx, "ok", []byte{1}, nil); !ok || err != nil {
		t.Fatalf("2xx = %v, %v; want committed", ok, err)
	}
	if ok, err := e.Send(ctx, "nope", nil, nil); ok || err != nil {
		t.Fatalf("503 = %v, %v; want declined without error", ok, err)
	}
	if _, err := e.Send(ctx, "ok", nil, []byte{9}); err != nil {
		t.Fatalf("previousResult must not break send: %v", err)
	}

	down := NewHTTPExecutor("http://127.0.0.1:1", time.Second)
	if ok, err := down.Send(ctx, "x", nil, nil); ok || err == nil {
		t.Fatalf("refused = %v, %v; want transport error", ok, err)
	}

	slow := NewHTTPExecutor(srv.URL, 50*time.Millisecond)
	if ok, err := slow.Send(ctx, "slow", nil, nil); ok || err == nil {
		t.Fatalf("timeout = %v, %v; want transport error", ok, err)
	}
}
