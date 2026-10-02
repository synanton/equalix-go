// Command equalix-go is the entrypoint of the Equalix fair scheduler service.
//
// Pre-alpha: the domain core (internal/domain) and CMS (pkg/cms) are usable
// as libraries and exercised by the conformance suite. The service wiring
// (adapters, jobs, config) lands in later phases; this main only reports
// the build status until then.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "equalix-go: pre-alpha — service wiring not yet implemented (see docs/IMPLEMENTATION.md)")
	os.Exit(1)
}
