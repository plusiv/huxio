# syntax=docker/dockerfile:1

# Build stage
FROM golang:1.26-alpine AS build
WORKDIR /src
RUN apk add --no-cache git ca-certificates

# Pinned so the cache mounts below cannot miss their target if a base image
# moves GOPATH or HOME.
ENV GOMODCACHE=/go/pkg/mod
ENV GOCACHE=/root/.cache/go-build

COPY go.mod go.sum ./
# Its own layer because cache mounts are builder-local and not exported by
# --cache-to, so a cold CI builder needs this one to hit.
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .
ARG VERSION=dev
# Keeps a one-file edit from recompiling every dependency and the stdlib.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/huxio ./cmd/huxio

# Runtime stage: one static binary, and Postgres is the only thing it needs.
FROM alpine:3.21
RUN apk add --no-cache ca-certificates tzdata && adduser -D -u 10001 huxio
COPY --from=build /out/huxio /usr/local/bin/huxio
USER huxio
EXPOSE 8080
ENTRYPOINT ["huxio"]
CMD ["serve", "--role=all"]
