.PHONY: test test-verbose test-watch build build-cli build-daemon run-cli run-daemon start stop generate-proto

CLI_BINARY ?= mycel
DAEMON_BINARY ?= myceld
MYCELD_DATA_DIR ?= $(HOME)/mycel_data
MYCELD_GRPC_ADDR ?= 127.0.0.1:9091
MYCELD_PID_FILE ?= $(MYCELD_DATA_DIR)/myceld.pid
MYCELD_STDOUT_LOG ?= $(MYCELD_DATA_DIR)/log/myceld.stdout.log

generate-proto:
	go run github.com/bufbuild/buf/cmd/buf@v1.50.1 generate

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

start: build-daemon
	@mkdir -p "$(MYCELD_DATA_DIR)/log"
	@if [ -f "$(MYCELD_PID_FILE)" ]; then \
		pid=$$(cat "$(MYCELD_PID_FILE)"); \
		if kill -0 "$$pid" 2>/dev/null; then \
			echo "myceld is already running with PID $$pid"; \
			exit 1; \
		fi; \
	fi
	@rm -f "$(MYCELD_PID_FILE)"
	@MYCELD_DATA_DIR="$(MYCELD_DATA_DIR)" MYCELD_GRPC_ADDR="$(MYCELD_GRPC_ADDR)" nohup bin/$(DAEMON_BINARY) > "$(MYCELD_STDOUT_LOG)" 2>&1 & echo $$! > "$(MYCELD_PID_FILE)"
	@sleep 0.2
	@pid=$$(cat "$(MYCELD_PID_FILE)"); \
	if ! kill -0 "$$pid" 2>/dev/null; then \
		echo "myceld failed to start; see $(MYCELD_STDOUT_LOG)"; \
		rm -f "$(MYCELD_PID_FILE)"; \
		exit 1; \
	fi; \
	echo "myceld started with PID $$pid"
	@echo "data dir: $(MYCELD_DATA_DIR)"
	@echo "gRPC addr: $(MYCELD_GRPC_ADDR)"
	@echo "stdout log: $(MYCELD_STDOUT_LOG)"
	@echo "daemon log: $(MYCELD_DATA_DIR)/log/myceld.log"

stop:
	@if [ ! -f "$(MYCELD_PID_FILE)" ]; then \
		echo "myceld is not running (missing PID file $(MYCELD_PID_FILE))"; \
		exit 0; \
	fi
	@pid=$$(cat "$(MYCELD_PID_FILE)"); \
	if ! kill -0 "$$pid" 2>/dev/null; then \
		echo "removing stale PID file for $$pid"; \
		rm -f "$(MYCELD_PID_FILE)"; \
		exit 0; \
	fi; \
	echo "stopping myceld PID $$pid"; \
	kill "$$pid"; \
	for i in $$(seq 1 50); do \
		if ! kill -0 "$$pid" 2>/dev/null; then \
			rm -f "$(MYCELD_PID_FILE)"; \
			echo "myceld stopped"; \
			exit 0; \
		fi; \
		sleep 0.1; \
	done; \
	echo "myceld did not stop after 5s; sending SIGKILL"; \
	kill -9 "$$pid" 2>/dev/null || true; \
	rm -f "$(MYCELD_PID_FILE)"; \
	echo "myceld stopped"
