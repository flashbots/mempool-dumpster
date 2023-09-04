VERSION := $(shell git describe --tags --always --dirty="-dev")

all: build

v:
	@echo "Version: ${VERSION}"

clean-build:
	rm -rf build/

clean-dev:
	rm -rf out/ test/

.PHONY: build
build: clean-build
	@mkdir -p build
	go build -trimpath -ldflags "-X main.version=${VERSION}" -v -o ./build/collect cmd/collect/*
	go build -trimpath -ldflags "-X main.version=${VERSION}" -v -o ./build/merge cmd/merge/*
	go build -trimpath -ldflags "-X main.version=${VERSION}" -v -o ./build/analyze cmd/analyze/*

.PHONY: website
website:
	go run cmd/website/main.go -build -upload

website-dev:
	go run cmd/website/main.go -dev

test:
	go test ./...

test-race:
	go test -race ./...

lint:
	gofmt -d -s .
	gofumpt -d -extra .
	go vet ./...
	staticcheck ./...
	golangci-lint run

fmt:
	gofmt -s -w .
	gofumpt -extra -w .
	gci write .
	go mod tidy

lt: lint test

gofumpt:
	gofumpt -l -w -extra .

cover:
	go test -coverprofile=/tmp/go-sim-lb.cover.tmp ./...
	go tool cover -func /tmp/go-sim-lb.cover.tmp
	unlink /tmp/go-sim-lb.cover.tmp

cover-html:
	go test -coverprofile=/tmp/go-sim-lb.cover.tmp ./...
	go tool cover -html=/tmp/go-sim-lb.cover.tmp
	unlink /tmp/go-sim-lb.cover.tmp

docker-image:
	DOCKER_BUILDKIT=1 docker build --platform linux/amd64 --build-arg VERSION=${VERSION} . -t your-project
