MODULE := github.com/synanton/equalix-go
GO := go
GOLANGCI := golangci-lint

.PHONY: build test test-integration test-conformance test-differential bench lint vet tools clean

build:
	$(GO) build ./...

test:
	$(GO) test -race ./...

test-integration:
	$(GO) test -race -tags=integration ./test/integration/...

test-conformance:
	$(GO) test -race -tags=conformance ./test/conformance/...

# Differential testing against the Java oracle (Phase 5).
# Usage: make test-differential JAVA_EQUALIX_PATH=/path/to/equalix
test-differential:
	$(GO) test -tags=differential ./test/differential/...

bench:
	$(GO) test -run=NONE -bench=. -benchmem ./pkg/cms/... ./internal/domain/...

vet:
	$(GO) vet ./...

lint:
	$(GOLANGCI) run ./...

tools:
	$(GO) install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

clean:
	rm -rf bin/
