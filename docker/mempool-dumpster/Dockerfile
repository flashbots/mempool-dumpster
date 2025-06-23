# syntax=docker/dockerfile:1
FROM golang:1.24 AS builder
ARG VERSION
WORKDIR /build

# Cache for the modules
COPY go.mod ./
COPY go.sum ./
ENV GOCACHE=/root/.cache/go-build
RUN --mount=type=cache,target=/root/.cache/go-build go mod download

# Now adding all the code and start building
ADD . .
ENV GOCACHE=/root/.cache/go-build
RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-s -X github.com/flashbots/mempool-dumpster/common.Version=${VERSION}" -v -o ./bin/mempool-dumpster cmd/main.go

FROM alpine:latest
WORKDIR /app
RUN mkdir /mnt/data
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /build/bin/* /app/
CMD ["/app/mempool-dumpster", "collect"]
