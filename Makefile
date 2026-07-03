SHELL := /bin/sh

.PHONY: test test-verbose test-watch build build-cli build-daemon run-cli run-daemon start stop generate-proto

CLI_BINARY ?= mycel
DAEMON_BINARY ?= myceld
MYCELD_DATA_DIR = $(HOME)/mycel_data
MYCELD_GRPC_ADDR = 127.0.0.1:9091
MYCELD_PID_FILE = $(MYCELD_DATA_DIR)/myceld.pid
MYCELD_STDOUT_LOG = $(MYCELD_DATA_DIR)/log/myceld.stdout.log

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
	@set -eu; \
	datadir="$(MYCELD_DATA_DIR)"; \
	grpc_addr="$(MYCELD_GRPC_ADDR)"; \
	pidfile="$(MYCELD_PID_FILE)"; \
	stdout_log="$(MYCELD_STDOUT_LOG)"; \
	mkdir -p "$$datadir/log"; \
	if [ -f "$$pidfile" ]; then \
		pid=""; \
		read pid < "$$pidfile" || true; \
		if [ -n "$$pid" ] && kill -0 "$$pid" 2>/dev/null; then \
			echo "myceld is already running with PID $$pid"; \
			exit 1; \
		fi; \
	fi; \
	rm -f "$$pidfile"; \
	MYCELD_DATA_DIR="$$datadir" MYCELD_GRPC_ADDR="$$grpc_addr" nohup bin/$(DAEMON_BINARY) > "$$stdout_log" 2>&1 & \
	pid="$$!"; \
	echo "$$pid" > "$$pidfile"; \
	sleep 0.2; \
	if ! kill -0 "$$pid" 2>/dev/null; then \
		echo "myceld failed to start; see $$stdout_log"; \
		rm -f "$$pidfile"; \
		exit 1; \
	fi; \
	echo "myceld started with PID $$pid"; \
	echo "data dir: $$datadir"; \
	echo "gRPC addr: $$grpc_addr"; \
	echo "stdout log: $$stdout_log"; \
	echo "daemon log: $$datadir/log/myceld.log"

stop:
	@set -u; \
	pidfile="$(MYCELD_PID_FILE)"; \
	if [ ! -f "$$pidfile" ]; then \
		echo "myceld is not running (missing PID file $$pidfile)"; \
		exit 0; \
	fi; \
	pid=""; \
	read pid < "$$pidfile" || true; \
	if [ -z "$$pid" ] || ! kill -0 "$$pid" 2>/dev/null; then \
		echo "removing stale PID file for $$pid"; \
		rm -f "$$pidfile"; \
		exit 0; \
	fi; \
	echo "stopping myceld PID $$pid"; \
	kill "$$pid"; \
	for i in $$(seq 1 50); do \
		if ! kill -0 "$$pid" 2>/dev/null; then \
			rm -f "$$pidfile"; \
			echo "myceld stopped"; \
			exit 0; \
		fi; \
		sleep 0.1; \
	done; \
	echo "myceld did not stop after 5s; sending SIGKILL"; \
	kill -9 "$$pid" 2>/dev/null || true; \
	rm -f "$$pidfile"; \
	echo "myceld stopped"
