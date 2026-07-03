.PHONY: test test-verbose test-watch build build-cli build-daemon run-cli run-daemon

CLI_BINARY ?= mycel
DAEMON_BINARY ?= myceld

test:
	go test ./...

test-verbose:
	go test -v -count=1 -cover -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

test-watch:
	@command -v watchexec >/dev/null 2>&1 || (echo "watchexec is required. Install with: brew install watchexec" && exit 1)
	watchexec -e go -- "go test -v -count=1 -cover -coverprofile=coverage.out ./... && go tool cover -func=coverage.out"

build: build-cli build-daemon

build-cli:
	go build -o bin/$(CLI_BINARY) ./cmd/mycel

build-daemon:
	go build -o bin/$(DAEMON_BINARY) ./cmd/myceld

run-cli:
	go run ./cmd/mycel

run-daemon:
	go run ./cmd/myceld
