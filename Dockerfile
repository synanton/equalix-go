# Equalix Go service — multi-stage, static binary on distroless.
#
# Build: docker build --build-arg VERSION=$(cat VERSION) .
# (VERSION file is the source of truth — never git-describe here; a
# dirty tree or a tarball build would stamp irreproducibly.)
# Run (env-only, no config file):
#   docker run --rm -p 8080:8080 \
#     -e EQUALIX_DSN='postgres://cmp:x@host:5432/equalix_go?sslmode=disable' \
#     -e EQUALIX_API_KEY=secret \
#     -e EQUALIX_EXECUTOR_BASE_URL=http://executor:9000 \
#     equalix-go
# Run (config file): docker run --rm -v ./equalix.yaml:/etc/equalix/config.yaml equalix-go
#
# Base choice (pinned by scope): distroless/static:nonroot — CA certs
# included (scratch lacks them), no shell to exploit, standard non-root
# user 65532. No HEALTHCHECK shell available, so the healthcheck
# subcommand (same binary) serves docker --health-cmd and k8s probes.
#
# Health signal (pinned): the HEALTHCHECK default path is /readyz — a
# readiness proxy for Docker/Swarm users (container shows unhealthy on
# DB outage, which is honest). k8s users ignore HEALTHCHECK entirely
# and use livenessProbe: /healthz + readinessProbe: /readyz.
# /healthz stays 200 through shutdown drain by construction
# (k8s-safe: liveness must not restart a deliberately-draining pod).

ARG GO_VERSION=1.22

FROM golang:${GO_VERSION}-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath \
	-ldflags "-s -w -X main.version=${VERSION}" \
	-o /out/equalix-go ./cmd/equalix-go

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/equalix-go /equalix-go
# Non-root is the image default (65532); stated explicitly so a base
# change that drops it fails review, not silently runs as root.
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/equalix-go"]
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
	CMD ["/equalix-go", "healthcheck"]
