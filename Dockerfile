# syntax=docker/dockerfile:1
FROM golang:1.23 AS builder
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
RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-s -X main.version=${VERSION}" -v -o ./bin/collect cmd/collect/*
RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-s -X main.version=${VERSION}" -v -o ./bin/merge cmd/merge/*
RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-s -X main.version=${VERSION}" -v -o ./bin/analyze cmd/analyze/*

FROM alpine:latest
WORKDIR /app
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /build/bin/* /app/
CMD ["/app/collect"]
