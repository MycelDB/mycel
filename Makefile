.PHONY: test test-verbose test-watch build run-cli

BINARY ?= mycel

test:
	go test ./...

test-verbose:
	go test -v -count=1 -cover -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

test-watch:
	@command -v watchexec >/dev/null 2>&1 || (echo "watchexec is required. Install with: brew install watchexec" && exit 1)
	watchexec -e go -- "go test -v -count=1 -cover -coverprofile=coverage.out ./... && go tool cover -func=coverage.out"

build:
	go build -o bin/$(BINARY) ./cmd/mycel

run-cli:
	go run ./cmd/mycel
