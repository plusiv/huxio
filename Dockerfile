# Build stage
FROM golang:1.26-alpine AS build
WORKDIR /src
RUN apk add --no-cache git ca-certificates
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/huxio ./cmd/huxio

# Runtime stage: one static binary, and Postgres is the only thing it needs.
FROM alpine:3.21
RUN apk add --no-cache ca-certificates tzdata && adduser -D -u 10001 huxio
COPY --from=build /out/huxio /usr/local/bin/huxio
USER huxio
EXPOSE 8080
ENTRYPOINT ["huxio"]
CMD ["serve", "--role=all"]
