SHELL := /bin/sh

.PHONY: generate-proto generate-gql-parser generate-gql-parser-docker validate-gql-grammar antlr-jar check-daemon-only check-public-surface test test-verbose test-watch test-cluster-identity test-phase-a test-cluster-release-gate test-compose-cluster test-k3s-cluster coverage coverage-html daemon-coverage daemon-coverage-html coverage-clean build build-cli build-daemon run-cli run-daemon start stop reset api-info

CLI_BINARY ?= mycel
DAEMON_BINARY ?= myceld
COVERAGE_DIR ?= coverage
COVERAGE_OUT ?= $(COVERAGE_DIR)/coverage.out
COVERAGE_HTML ?= $(COVERAGE_DIR)/coverage.html
DAEMON_COVERAGE_OUT ?= $(COVERAGE_DIR)/daemon-coverage.out
DAEMON_COVERAGE_HTML ?= $(COVERAGE_DIR)/daemon-coverage.html
MYCELD_DATA_DIR = $(HOME)/mycel_data
MYCELD_GRPC_ADDR = 127.0.0.1:9091
MYCELD_PID_FILE = $(MYCELD_DATA_DIR)/myceld.pid
MYCELD_STDOUT_LOG = $(MYCELD_DATA_DIR)/log/myceld.stdout.log
ANTLR_VERSION ?= 4.13.1
ANTLR_JAR ?= bin/antlr-$(ANTLR_VERSION)-complete.jar
ANTLR_DOCKER_IMAGE ?= eclipse-temurin:17-jre
GQL_GRAMMAR_DIR = internal/query/gql/antlr
GQL_GENERATED_DIR = $(GQL_GRAMMAR_DIR)/generated

api-info:
	@echo "Protobuf definitions live in github.com/myceldb/mycel-api; daemon Go stubs are generated locally under internal/gen/."

generate-proto:
	./scripts/generate-proto.sh

antlr-jar:
	@mkdir -p bin
	@test -f $(ANTLR_JAR) || curl -L -o $(ANTLR_JAR) https://www.antlr.org/download/antlr-$(ANTLR_VERSION)-complete.jar

validate-gql-grammar: antlr-jar
	@command -v java >/dev/null 2>&1 || (echo "java is required to run ANTLR. Install a JRE/JDK and retry." && exit 1)
	@tmpdir=$$(mktemp -d); \
	cd $(GQL_GRAMMAR_DIR) && java -jar ../../../../$(ANTLR_JAR) -Dlanguage=Go -Werror -o $$tmpdir MycelGQL.g4; \
	rm -rf $$tmpdir

generate-gql-parser: antlr-jar
	@set -eu; \
	rm -rf $(GQL_GENERATED_DIR); \
	mkdir -p $(GQL_GENERATED_DIR); \
	if command -v java >/dev/null 2>&1 && java -version >/dev/null 2>&1; then \
		cd $(GQL_GRAMMAR_DIR) && java -jar ../../../../$(ANTLR_JAR) -Dlanguage=Go -visitor -no-listener -package generated -o generated MycelGQL.g4; \
	elif command -v docker >/dev/null 2>&1; then \
		docker run --rm -u "$$(id -u):$$(id -g)" -v "$$(pwd):/work" -w /work $(ANTLR_DOCKER_IMAGE) sh -c 'cd $(GQL_GRAMMAR_DIR) && java -jar ../../../../$(ANTLR_JAR) -Dlanguage=Go -visitor -no-listener -package generated -o generated MycelGQL.g4'; \
	else \
		echo "java or docker is required to run ANTLR. Install a JRE/JDK or Docker and retry."; \
		exit 1; \
	fi
	gofmt -w $(GQL_GENERATED_DIR)

generate-gql-parser-docker: antlr-jar
	@command -v docker >/dev/null 2>&1 || (echo "docker is required to run ANTLR generation without local Java." && exit 1)
	rm -rf $(GQL_GENERATED_DIR)
	mkdir -p $(GQL_GENERATED_DIR)
	docker run --rm -u "$$(id -u):$$(id -g)" -v "$$(pwd):/work" -w /work $(ANTLR_DOCKER_IMAGE) sh -c 'cd $(GQL_GRAMMAR_DIR) && java -jar ../../../../$(ANTLR_JAR) -Dlanguage=Go -visitor -no-listener -package generated -o generated MycelGQL.g4'
	gofmt -w $(GQL_GENERATED_DIR)

check-daemon-only:
	scripts/check-daemon-only.sh

check-public-surface:
	scripts/check-public-surface.sh

test: generate-proto generate-gql-parser check-daemon-only check-public-surface
	go test ./...

test-verbose: generate-proto generate-gql-parser check-daemon-only check-public-surface
	go test -v -count=1 -cover -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

test-watch:
	@command -v watchexec >/dev/null 2>&1 || (echo "watchexec is required. Install with: brew install watchexec" && exit 1)
	watchexec -e go,sh -- "make generate-proto generate-gql-parser && scripts/check-daemon-only.sh && scripts/check-public-surface.sh && go test -v -count=1 -cover -coverprofile=coverage.out ./... && go tool cover -func=coverage.out"

test-cluster-identity: generate-proto generate-gql-parser
	go test ./internal/clustering ./internal/clustering/consensus ./internal/daemon/app ./internal/daemon/api/admin ./internal/cli/cmd -count=1

test-phase-a: generate-proto generate-gql-parser
	go test ./internal/clustering ./internal/clustering/consensus ./internal/daemon/app ./internal/daemon/api/admin ./internal/daemon/api/client ./internal/daemon/config ./internal/daemon/runtime ./internal/daemon/server ./internal/graph/service ./internal/cli/cmd -count=1

test-cluster-release-gate: test test-compose-cluster test-k3s-cluster

test-compose-cluster:
	cd ../../knot_pkm/knot_pkm_server && MYCELD_CLUSTER_BACKEND_AUTH_TOKEN="$${MYCELD_CLUSTER_BACKEND_AUTH_TOKEN:-mycel-compose-cluster-token}" $(MAKE) compose-reset compose-up
	./scripts/validateComposeClusterIdentity.sh
	cd ../../knot_pkm/knot_pkm_server && MYCELD_CLUSTER_BACKEND_AUTH_TOKEN="$${MYCELD_CLUSTER_BACKEND_AUTH_TOKEN:-mycel-compose-cluster-token}" docker compose -f compose.dev.yml restart myceld-a myceld-b myceld-c
	cd ../../knot_pkm/knot_pkm_server && MYCELD_CLUSTER_BACKEND_AUTH_TOKEN="$${MYCELD_CLUSTER_BACKEND_AUTH_TOKEN:-mycel-compose-cluster-token}" docker compose -f compose.dev.yml up -d --wait myceld-a myceld-b myceld-c knot-pkm-server
	./scripts/validateComposeClusterIdentity.sh
	MYCEL_COMPOSE_VALIDATE_SOURCE=files ./scripts/validateComposeClusterIdentity.sh

test-k3s-cluster:
	./scripts/testK3sCluster.sh

coverage: generate-proto generate-gql-parser check-daemon-only check-public-surface
	mkdir -p $(COVERAGE_DIR)
	go test ./... -coverprofile=$(COVERAGE_OUT)
	go tool cover -func=$(COVERAGE_OUT)

coverage-html: coverage
	go tool cover -html=$(COVERAGE_OUT) -o $(COVERAGE_HTML)
	@echo "Coverage HTML written to $(COVERAGE_HTML)"

daemon-coverage: generate-proto generate-gql-parser
	mkdir -p $(COVERAGE_DIR)
	go test ./internal/daemon/... -coverprofile=$(DAEMON_COVERAGE_OUT)
	go tool cover -func=$(DAEMON_COVERAGE_OUT)

daemon-coverage-html: daemon-coverage
	go tool cover -html=$(DAEMON_COVERAGE_OUT) -o $(DAEMON_COVERAGE_HTML)
	@echo "Daemon coverage HTML written to $(DAEMON_COVERAGE_HTML)"

coverage-clean:
	rm -rf $(COVERAGE_DIR)

build: generate-proto generate-gql-parser check-daemon-only check-public-surface build-cli build-daemon

build-cli:
	go build -o bin/$(CLI_BINARY) ./cmd/mycel

build-daemon:
	go build -o bin/$(DAEMON_BINARY) ./cmd/myceld

run-cli: generate-proto
	go run ./cmd/mycel

run-daemon: generate-proto
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

reset: stop
	@set -eu; \
	datadir="$(MYCELD_DATA_DIR)"; \
	if [ -z "$$datadir" ] || [ "$$datadir" = "/" ]; then \
		echo "refusing to remove unsafe data dir: $$datadir"; \
		exit 1; \
	fi; \
	rm -rf "$$datadir"; \
	echo "removed data dir: $$datadir"
