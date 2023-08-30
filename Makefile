VERSION := $(shell git describe --tags --always --dirty="-dev")

all: clean build

v:
	@echo "Version: ${VERSION}"

clean:
	rm -rf build/ out/

clean-dev:
	rm -rf out/ test/

.PHONY: build
build:
	@mkdir ./build || true
	go build -trimpath -ldflags "-X main.version=${VERSION}" -v -o ./build/collector cmd/collector/main.go
	go build -trimpath -ldflags "-X main.version=${VERSION}" -v -o ./build/summerizer cmd/summarizer/main.go

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
