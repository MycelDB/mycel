#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NODE_NAME="${MYCELD_NODE_NAME:-${1:-node-a}}"
DATA_DIR="${MYCELD_DATA_DIR:-/tmp/mycel-${NODE_NAME}}"

if [[ -n "${MYCELD_GRPC_ADDR:-}" ]]; then
  GRPC_ADDR="$MYCELD_GRPC_ADDR"
else
  case "$NODE_NAME" in
    node-a) GRPC_ADDR="0.0.0.0:9093" ;;
    node-b) GRPC_ADDR="0.0.0.0:9094" ;;
    node-c) GRPC_ADDR="0.0.0.0:9095" ;;
    *) GRPC_ADDR="0.0.0.0:9091" ;;
  esac
fi

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

if [[ -n "${MYCELD_CLUSTER_BOOTSTRAP:-}" ]]; then
  BOOTSTRAP="$MYCELD_CLUSTER_BOOTSTRAP"
else
  case "$NODE_NAME" in
    node-a) BOOTSTRAP="true" ;;
    *) BOOTSTRAP="false" ;;
  esac
fi
export MYCELD_CLUSTER_BOOTSTRAP="$BOOTSTRAP"

if [[ -n "${MYCELD_CLUSTER_SEED_PEERS:-}" ]]; then
  SEED_PEERS="$MYCELD_CLUSTER_SEED_PEERS"
elif [[ "$BOOTSTRAP" == "true" ]]; then
  SEED_PEERS=""
else
  case "$NODE_NAME" in
    node-b) SEED_PEERS="127.0.0.1:9093" ;;
    node-c) SEED_PEERS="127.0.0.1:9093,127.0.0.1:9094" ;;
    *) SEED_PEERS="127.0.0.1:9093" ;;
  esac
fi
export MYCELD_CLUSTER_SEED_PEERS="$SEED_PEERS"
export MYCELD_CLUSTER_DISCOVERY_INTERVAL="${MYCELD_CLUSTER_DISCOVERY_INTERVAL:-5s}"
export MYCELD_CLUSTER_BACKEND_AUTH_TOKEN="${MYCELD_CLUSTER_BACKEND_AUTH_TOKEN:-}"
export MYCELD_CLUSTER_JOIN_TOKEN_FILE="${MYCELD_CLUSTER_JOIN_TOKEN_FILE:-}"
export MYCELD_CLUSTER_JOIN_TOKEN="${MYCELD_CLUSTER_JOIN_TOKEN:-}"

WIPE_DATA="${MYCELD_WIPE_DATA:-true}"
if [[ "$WIPE_DATA" == "true" ]]; then
  case "$MYCELD_DATA_DIR" in
    /tmp/mycel-*)
      echo "Wiping node data directory: $MYCELD_DATA_DIR"
      rm -rf "$MYCELD_DATA_DIR"
      ;;
    *)
      echo "Refusing to wipe non-dev MYCELD_DATA_DIR: $MYCELD_DATA_DIR" >&2
      echo "Use a /tmp/mycel-* data directory with this development script or set MYCELD_WIPE_DATA=false." >&2
      exit 1
      ;;
  esac
else
  echo "Preserving node data directory: $MYCELD_DATA_DIR"
fi
mkdir -p "$DATA_DIR"

echo "Starting myceld cluster-of-1"
echo "  MYCELD_DATA_DIR=$MYCELD_DATA_DIR"
echo "  MYCELD_GRPC_ADDR=$MYCELD_GRPC_ADDR"
echo "  MYCELD_NODE_NAME=$MYCELD_NODE_NAME"
echo "  MYCELD_CLUSTER_NAME=$MYCELD_CLUSTER_NAME"
echo "  MYCELD_CLUSTER_BACKEND_ADVERTISE_ADDR=$MYCELD_CLUSTER_BACKEND_ADVERTISE_ADDR"
echo "  MYCELD_CLUSTER_BOOTSTRAP=$MYCELD_CLUSTER_BOOTSTRAP"
echo "  MYCELD_CLUSTER_SEED_PEERS=$MYCELD_CLUSTER_SEED_PEERS"
echo "  MYCELD_CLUSTER_DISCOVERY_INTERVAL=$MYCELD_CLUSTER_DISCOVERY_INTERVAL"
echo "  MYCELD_CLUSTER_BACKEND_AUTH_TOKEN=${MYCELD_CLUSTER_BACKEND_AUTH_TOKEN:+<set>}"
echo "  MYCELD_CLUSTER_JOIN_TOKEN_FILE=$MYCELD_CLUSTER_JOIN_TOKEN_FILE"
echo "  MYCELD_WIPE_DATA=$WIPE_DATA"
echo "  clustering identity: $MYCELD_DATA_DIR/meta/clustering/node.json"
echo "  clustering state:    $MYCELD_DATA_DIR/meta/clustering/local_state.json"
echo "  clustering peers:    $MYCELD_DATA_DIR/meta/clustering/peers.json"
echo

cd "$ROOT_DIR"
exec go run ./cmd/myceld
