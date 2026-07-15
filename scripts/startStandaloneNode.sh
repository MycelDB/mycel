#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="${MYCELD_DATA_DIR:-/tmp/mycel-standalone}"
GRPC_ADDR="${MYCELD_GRPC_ADDR:-127.0.0.1:9092}"

export MYCELD_DATA_DIR="$DATA_DIR"
export MYCELD_GRPC_ADDR="$GRPC_ADDR"

# Clear cluster-facing settings so this starts in standalone state.
unset MYCELD_CLUSTER_NAME
unset MYCELD_CLUSTER_BACKEND_ADVERTISE_ADDR

case "$MYCELD_DATA_DIR" in
  /tmp/mycel-*)
    echo "Wiping node data directory: $MYCELD_DATA_DIR"
    rm -rf "$MYCELD_DATA_DIR"
    ;;
  *)
    echo "Refusing to wipe non-dev MYCELD_DATA_DIR: $MYCELD_DATA_DIR" >&2
    echo "Use a /tmp/mycel-* data directory with this development script." >&2
    exit 1
    ;;
esac
mkdir -p "$DATA_DIR"

echo "Starting myceld standalone"
echo "  MYCELD_DATA_DIR=$MYCELD_DATA_DIR"
echo "  MYCELD_GRPC_ADDR=$MYCELD_GRPC_ADDR"
echo "  clustering identity: $MYCELD_DATA_DIR/meta/clustering/node.json"
echo "  clustering state:    $MYCELD_DATA_DIR/meta/clustering/local_state.json"
echo "  clustering peers:    $MYCELD_DATA_DIR/meta/clustering/peers.json"
echo

cd "$ROOT_DIR"
exec go run ./cmd/myceld
