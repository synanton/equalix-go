//go:build differential

package differential

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestMarkerFromFirstIngestResponse wires a fake service returning a
// specific createdAt and asserts the marker equals exactly that timestamp.
// Any wall-clock fallback (time.Now at driver start, client request time)
// would surface here as a mismatch against the fixed server stamp.
func TestMarkerFromFirstIngestResponse(t *testing.T) {
	serverStamp := time.Date(2026, 5, 1, 12, 0, 0, 123000000, time.UTC)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/tasks":
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode("task-1")
		default:
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{"createdAt": serverStamp.Format(time.RFC3339Nano)})
		}
	}))
	defer srv.Close()

	before := time.Now()
	marker, err := SubmitTask(context.Background(), srv.Client(), srv.URL, "key",
		Task{ID: "t", Tenant: "a", Weight: 1, PayloadBytes: 4}, before.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if !marker.Equal(serverStamp) {
		t.Fatalf("marker = %v, want server stamp %v (wall-clock fallback?)", marker, serverStamp)
	}
	if !time.Now().After(before) {
		t.Fatal("clock did not advance — test is vacuous")
	}
}
