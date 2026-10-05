# syntax=docker/dockerfile:1
# One image definition for every service: docker build --build-arg SERVICE=collector .
FROM --platform=$BUILDPLATFORM golang:1.27-alpine AS build
ARG TARGETOS
ARG TARGETARCH
ARG SERVICE
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY gen ./gen
COPY internal ./internal
COPY cmd ./cmd
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    test -n "$SERVICE" && \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/$SERVICE && \
    mkdir -p /out/agent-state

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
# Named volumes inherit this ownership, so the non-root agent can write its spool.
COPY --from=build --chown=65532:65532 /out/agent-state /var/lib/logplat-agent
USER nonroot:nonroot
ENTRYPOINT ["/app"]
