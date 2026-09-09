SHELL := /bin/sh

COMPOSE_FILE ?= compose.yml
MYCEL_IMAGE ?= local/mycel:dev
MYCEL_PULL_POLICY ?= never
MYCELD_CLUSTER_BACKEND_AUTH_TOKEN ?= mycel-compose-cluster-token

.PHONY: compose-reset compose-up compose-down compose-logs

compose-reset:
	MYCEL_IMAGE=$(MYCEL_IMAGE) MYCEL_PULL_POLICY=$(MYCEL_PULL_POLICY) MYCELD_CLUSTER_BACKEND_AUTH_TOKEN=$(MYCELD_CLUSTER_BACKEND_AUTH_TOKEN) docker compose -f $(COMPOSE_FILE) down -v --remove-orphans

compose-up:
	MYCEL_IMAGE=$(MYCEL_IMAGE) MYCEL_PULL_POLICY=$(MYCEL_PULL_POLICY) MYCELD_CLUSTER_BACKEND_AUTH_TOKEN=$(MYCELD_CLUSTER_BACKEND_AUTH_TOKEN) docker compose -f $(COMPOSE_FILE) up -d --wait myceld-a myceld-b myceld-c

compose-down:
	MYCEL_IMAGE=$(MYCEL_IMAGE) MYCEL_PULL_POLICY=$(MYCEL_PULL_POLICY) MYCELD_CLUSTER_BACKEND_AUTH_TOKEN=$(MYCELD_CLUSTER_BACKEND_AUTH_TOKEN) docker compose -f $(COMPOSE_FILE) down --remove-orphans

compose-logs:
	docker compose -f $(COMPOSE_FILE) logs --tail=100 myceld-a myceld-b myceld-c
