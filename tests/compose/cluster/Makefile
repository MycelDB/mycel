SHELL := /bin/sh

COMPOSE_FILE ?= compose.yml
MYCEL_IMAGE ?= local/mycel:dev
MYCEL_PULL_POLICY ?= never
MYCELD_CLUSTER_BACKEND_AUTH_TOKEN ?= mycel-compose-cluster-token
MINIO_IMAGE ?= quay.io/minio/minio:latest
MINIO_MC_IMAGE ?= quay.io/minio/mc:latest
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
