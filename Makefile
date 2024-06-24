.DEFAULT_GOAL := build

VERSION := $(shell git describe --tags --always --dirty="-dev")

##@ Help

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "Usage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

.PHONY: v
v: ## Show the current version
	@echo "Version: ${VERSION}"

##@ Building

.PHONY: build
build: clean-build  ## build the binaries
	@mkdir -p build
	go build -trimpath -ldflags "-X main.version=${VERSION}" -v -o ./build/collect cmd/collect/*
	go build -trimpath -ldflags "-X main.version=${VERSION}" -v -o ./build/merge cmd/merge/*
	go build -trimpath -ldflags "-X main.version=${VERSION}" -v -o ./build/analyze cmd/analyze/*
	go build -trimpath -ldflags "-X main.version=${VERSION}" -v -o ./build/website cmd/website/*

.PHONY: website
website: ## Build the website and upload to R2
	go run cmd/website/main.go -build -upload

.PHONY: docker-image
docker-image: ## Build the docker image
	docker build --platform linux/amd64 --build-arg VERSION=${VERSION} . -t mempool-dumpster

##@ Development

clean-build: ## Clean build files
	rm -rf build/

clean-dev: ## Clean dev files
	rm -rf out/ test/

website-dev: ## Run the website in dev mode (hot reloading templates)
	go run cmd/website/main.go -dev

test: ## Run tests
	go test ./...

test-race: ## Run tests with -race flag
	go test -race ./...

lint: ## Run all the linters
	gofmt -d -s .
	gofumpt -d -extra .
	go vet ./...
	staticcheck ./...
	golangci-lint run

fmt: ## Run formatters (updates code in place)
	gofmt -s -w .
	gofumpt -extra -w .
	gci write .
	go mod tidy

lt: fmt lint test ## Run fmt, lint and test

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
