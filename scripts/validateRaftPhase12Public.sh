#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

BASE_DIR="${MYCEL_RAFT_VALIDATION_DIR:-$(mktemp -d /tmp/mycel-raft-phase12-public.XXXXXX)}"
ADMIN_USER="${MYCEL_RAFT_VALIDATION_ADMIN_USER:-admin}"
ADMIN_PASS="${MYCEL_RAFT_VALIDATION_ADMIN_PASS:-admin-password}"
USER_NAME="raft-user-$(date +%s)"
USER_PASS="user-password-123"
ADDRS=(127.0.0.1:19093 127.0.0.1:19094 127.0.0.1:19095)
RAFT_NODE_ADDRS="$(IFS=,; echo "${ADDRS[*]}")"
PIDS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do kill "$pid" >/dev/null 2>&1 || true; done
  wait >/dev/null 2>&1 || true
  if [[ -z "${MYCEL_RAFT_VALIDATION_KEEP_DIR:-}" ]]; then rm -rf "$BASE_DIR"; fi
}
trap cleanup EXIT

start_node() {
  local idx="$1" name="$2" addr="$3"
  local data_dir="$BASE_DIR/$name"
  env \
    MYCELD_NODE_NAME="$name" \
    MYCELD_DATA_DIR="$data_dir" \
    MYCELD_GRPC_ADDR="$addr" \
    MYCELD_MODE=standalone \
    MYCELD_BOOTSTRAP_ADMIN_USERNAME="$ADMIN_USER" \
    MYCELD_BOOTSTRAP_ADMIN_PASSWORD="$ADMIN_PASS" \
    MYCELD_CLUSTER_ENGINE=raft \
    MYCELD_CLUSTER_RAFT_NODE_COUNT=3 \
    MYCELD_CLUSTER_RAFT_PARTITION_COUNT=64 \
    MYCELD_CLUSTER_RAFT_REPLICA_FACTOR=3 \
    MYCELD_CLUSTER_RAFT_LOCAL_NODE_ID="$idx" \
    MYCELD_CLUSTER_RAFT_NODE_ADDRS="$RAFT_NODE_ADDRS" \
    MYCELD_CLUSTER_BACKEND_AUTH_TOKEN=phase12-public-token \
    MYCELD_WIPE_DATA=true \
    bash scripts/startClusterNode.sh "$name" >"$BASE_DIR/$name.log" 2>&1 &
  PIDS+=("$!")
}

run_cli() { go run ./cmd/mycel --daemon-addr "$1" --username "$2" --password "$3" --output json "${@:4}"; }
wait_for_cli() {
  local addr="$1"
  for _ in $(seq 1 90); do
    if run_cli "$addr" "$ADMIN_USER" "$ADMIN_PASS" user list >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  echo "daemon at $addr did not become ready; logs in $BASE_DIR" >&2
  return 1
}

start_node 1 node-a "${ADDRS[0]}"
start_node 2 node-b "${ADDRS[1]}"
start_node 3 node-c "${ADDRS[2]}"
for addr in "${ADDRS[@]}"; do wait_for_cli "$addr"; done

run_cli "${ADDRS[0]}" "$ADMIN_USER" "$ADMIN_PASS" user add --user-username "$USER_NAME" --new-password "$USER_PASS" >/dev/null
# Login on node B and perform an authenticated call on node C. The CLI logs in per
# command, so this validates that user records and refresh sessions are visible on non-creator nodes.
run_cli "${ADDRS[1]}" "$USER_NAME" "$USER_PASS" auth login >/dev/null
run_cli "${ADDRS[2]}" "$USER_NAME" "$USER_PASS" auth whoami | grep -q "$USER_NAME"
run_cli "${ADDRS[2]}" "$USER_NAME" "$USER_PASS" auth session list >/dev/null

echo "phase12 public raft validation passed user=$USER_NAME"
