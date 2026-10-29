SHELL := /bin/sh

COMPOSE_FILE ?= compose.yml
MYCEL_IMAGE ?= local/mycel:dev
MYCEL_PULL_POLICY ?= never
MYCELD_CLUSTER_BACKEND_AUTH_TOKEN ?= mycel-compose-cluster-token
MINIO_IMAGE ?= docker.io/myceldb/mycel-minio-test-helper@sha256:f33b406adb70891f722e0465ec085056b036f8f5494e4eff33317a9612cf24ff
MINIO_MC_IMAGE ?= docker.io/myceldb/mycel-mc-test-helper@sha256:9646d8b5d019fd20e8f4e1d782b29a56581d51c5a367e7c43156c0faac1a69f0
MINIO_ROOT_USER ?= mycelminio
MINIO_ROOT_PASSWORD ?= mycelminiosecret
MYCELD_BLOB_OBJECT_STORE_BUCKET ?= mycel-compose-blobs
AWS_ACCESS_KEY_ID ?= $(MINIO_ROOT_USER)
AWS_SECRET_ACCESS_KEY ?= $(MINIO_ROOT_PASSWORD)
AWS_REGION ?= us-east-1

COMPOSE_ENV = MYCEL_IMAGE=$(MYCEL_IMAGE) MYCEL_PULL_POLICY=$(MYCEL_PULL_POLICY) MYCELD_CLUSTER_BACKEND_AUTH_TOKEN=$(MYCELD_CLUSTER_BACKEND_AUTH_TOKEN) MINIO_IMAGE=$(MINIO_IMAGE) MINIO_MC_IMAGE=$(MINIO_MC_IMAGE) MINIO_ROOT_USER=$(MINIO_ROOT_USER) MINIO_ROOT_PASSWORD=$(MINIO_ROOT_PASSWORD) MYCELD_BLOB_OBJECT_STORE_BUCKET=$(MYCELD_BLOB_OBJECT_STORE_BUCKET) AWS_ACCESS_KEY_ID=$(AWS_ACCESS_KEY_ID) AWS_SECRET_ACCESS_KEY=$(AWS_SECRET_ACCESS_KEY) AWS_REGION=$(AWS_REGION)

.PHONY: compose-reset compose-up compose-down compose-logs

compose-reset:
	$(COMPOSE_ENV) docker compose -f $(COMPOSE_FILE) down -v --remove-orphans

compose-up:
	$(COMPOSE_ENV) docker compose -f $(COMPOSE_FILE) up -d --wait myceld-a myceld-b myceld-c

compose-down:
	$(COMPOSE_ENV) docker compose -f $(COMPOSE_FILE) down --remove-orphans

compose-logs:
	docker compose -f $(COMPOSE_FILE) logs --tail=100 minio minio-bucket myceld-a myceld-b myceld-c
