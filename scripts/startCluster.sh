#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NODE_NAME="${MYCELD_NODE_NAME:-${1:-node-a}}"
DATA_DIR="${MYCELD_DATA_DIR:-/tmp/mycel-${NODE_NAME}}"
GRPC_ADDR="${MYCELD_GRPC_ADDR:-0.0.0.0:9091}"
CLUSTER_NAME="${MYCELD_CLUSTER_NAME:-dev-cluster}"

if [[ -n "${MYCELD_CLUSTER_BACKEND_ADVERTISE_ADDR:-}" ]]; then
  BACKEND_ADDR="$MYCELD_CLUSTER_BACKEND_ADVERTISE_ADDR"
else
  # For local multi-daemon development, advertise localhost on the same port
  # as the daemon gRPC listener. Example: 0.0.0.0:9092 -> 127.0.0.1:9092.
  PORT="${GRPC_ADDR##*:}"
  BACKEND_ADDR="127.0.0.1:${PORT}"
fi

export MYCELD_DATA_DIR="$DATA_DIR"
export MYCELD_GRPC_ADDR="$GRPC_ADDR"
export MYCELD_NODE_NAME="$NODE_NAME"
export MYCELD_CLUSTER_NAME="$CLUSTER_NAME"
export MYCELD_CLUSTER_BACKEND_ADVERTISE_ADDR="$BACKEND_ADDR"
export MYCELD_CLUSTER_SEED_PEERS="${MYCELD_CLUSTER_SEED_PEERS:-}"

mkdir -p "$DATA_DIR"

echo "Starting myceld cluster-of-1"
echo "  MYCELD_DATA_DIR=$MYCELD_DATA_DIR"
echo "  MYCELD_GRPC_ADDR=$MYCELD_GRPC_ADDR"
echo "  MYCELD_NODE_NAME=$MYCELD_NODE_NAME"
echo "  MYCELD_CLUSTER_NAME=$MYCELD_CLUSTER_NAME"
echo "  MYCELD_CLUSTER_BACKEND_ADVERTISE_ADDR=$MYCELD_CLUSTER_BACKEND_ADVERTISE_ADDR"
echo "  MYCELD_CLUSTER_SEED_PEERS=$MYCELD_CLUSTER_SEED_PEERS"
echo "  clustering identity: $MYCELD_DATA_DIR/meta/clustering/node.json"
echo

cd "$ROOT_DIR"
exec go run ./cmd/myceld
