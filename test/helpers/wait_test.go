package helpers

import (
	"testing"
	"time"
)

func TestWaitForImmediate(t *testing.T) {
	calls := 0
	WaitFor(t, time.Second, time.Millisecond, nil, func() bool {
		calls++
		return true
	})
	if calls != 1 {
		t.Fatalf("calls = %d, want 1 (no polling when already true)", calls)
	}
}

func TestWaitForPollsUntilTrue(t *testing.T) {
	n := 0
	WaitFor(t, 5*time.Second, time.Millisecond, nil, func() bool {
		n++
		return n >= 5
	})
	if n != 5 {
		t.Fatalf("n = %d, want 5", n)
	}
}
