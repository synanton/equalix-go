// Package executor implements port.Executor over HTTP: POST the task
// payload to {base}/tasks/{id}/execute, per the harness protocol
// (test/differential protocol.go). 2xx → committed; anything else is a
// decline, never a throw — the caller leaves the task DISPATCHED.
package executor

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/synanton/equalix-go/internal/port"
)

// HTTPExecutor posts task payloads to a stub-compatible endpoint.
type HTTPExecutor struct {
	base   string
	client *http.Client
}

var _ port.Executor = (*HTTPExecutor)(nil)

// NewHTTPExecutor builds an executor targeting base (e.g. the harness stub
// or a real worker). Timeout bounds a slow downstream; it does not change
// the committed/declined/error trichotomy.
func NewHTTPExecutor(base string, timeout time.Duration) *HTTPExecutor {
	return &HTTPExecutor{base: base, client: &http.Client{Timeout: timeout}}
}

// Send posts the payload. Transport failure → (false, err); non-2xx →
// (false, nil) declined; 2xx → (true, nil) committed. previousResult is
// accepted for interface parity and forwarded as a header when present
// (sequential passthrough is stub-ignored; real workers may use it).
func (e *HTTPExecutor) Send(ctx context.Context, taskID string, payload, previousResult []byte) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, "POST",
		fmt.Sprintf("%s/tasks/%s/execute", e.base, taskID), bytes.NewReader(payload))
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	if len(previousResult) > 0 {
		req.Header.Set("X-Previous-Result-Len", fmt.Sprintf("%d", len(previousResult)))
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return false, fmt.Errorf("executor: send %s: %w", taskID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return false, nil
	}
	return true, nil
}
