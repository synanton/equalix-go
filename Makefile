MODULE := github.com/synanton/equalix-go
GO := go
GOLANGCI := golangci-lint

.PHONY: build test test-integration test-conformance test-differential bench lint vet tools clean

build:
	$(GO) build ./...

test:
	$(GO) test -race ./...

# Ryuk 0.8.1 (pinned by testcontainers-go v0.33.0) is not in every local
# registry cache; 0.12.0 is protocol-compatible and widely cached. Override
# only the reaper image — cleanup behavior is unchanged.
export TESTCONTAINERS_RYUK_CONTAINER_IMAGE ?= testcontainers/ryuk:0.12.0

test-integration:
	TESTCONTAINERS_RYUK_CONTAINER_IMAGE="$(TESTCONTAINERS_RYUK_CONTAINER_IMAGE)" \
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
