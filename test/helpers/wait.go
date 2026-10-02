// Package helpers holds shared utilities for evidence tests.
package helpers

import (
	"testing"
	"time"
)

// WaitFor polls predicate every interval until true or deadline, then fails
// loudly with a state dump. The pattern for all quiescence waits: never
// assert mid-flight async state, never sleep-and-pray.
//
//	dump is called on deadline to describe the unreached state
//	(e.g. residual in-flight counts); pass nil for no dump.
func WaitFor(t *testing.T, timeout, interval time.Duration, dump func() string, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if predicate() {
			return
		}
		if time.Now().After(deadline) {
			state := ""
			if dump != nil {
				state = ": " + dump()
			}
			t.Fatalf("quiescence deadline exceeded%s", state)
			return
		}
		time.Sleep(interval)
	}
}
