MODULE := github.com/synanton/equalix-go
GO := go
GOLANGCI := golangci-lint
# Single source of truth for the binary version (spec §13: version
# mechanics). Every build path stamps from here — make, Docker
# (--build-arg VERSION=$(cat VERSION)), release workflow. Missing file
# (source tarball without it, detached oddity) falls back to dev rather
# than failing the build; the release workflow asserts tag == content
# separately, so the fallback can never ship a release.
VERSION := $(shell cat VERSION 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: build test test-integration test-conformance test-differential bench lint vet tools clean

build:
	$(GO) build -ldflags "$(LDFLAGS)" ./...

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
# Fails with a clear message until test/differential exists.
test-differential:
	@if [ -d test/differential ]; then \
		$(GO) test -tags=differential ./test/differential/...; \
	else \
		echo "test/differential not yet built (Phase 5): no differential evidence exists"; exit 1; \
	fi

bench:
	$(GO) test -run=NONE -bench=. -benchmem ./pkg/cms/... ./internal/domain/...

# Regenerate the CMS error-curve golden (pkg/cms/testdata/cms-curve.json).
# Run ONLY when the sketch algorithm intentionally changes — a diff here
# without an accompanying algorithm change is a regression, not an
# update. Commit the regenerated file alongside the algorithm change.
regen-cms-curve:
	$(GO) test -count=1 -run TestErrorCurve ./pkg/cms/ -args -update-curve

vet:
	$(GO) vet ./...

lint:
	$(GOLANGCI) run ./...

tools:
	$(GO) install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

clean:
	rm -rf bin/
