# Equalix Go service — multi-stage, static binary on distroless.
#
# Build: docker build --build-arg VERSION=$(git rev-parse --short HEAD) .
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
