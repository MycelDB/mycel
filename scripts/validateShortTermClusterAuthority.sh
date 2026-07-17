#!/usr/bin/env bash
set -euo pipefail

# Manual/dev validation for short-term clustering authority behavior.
# It starts a bootstrap primary and a joining follower, then verifies reads work
# on the follower while guarded writes are rejected there and accepted on primary.
#
# Usage:
#   cd mycel
#   MYCELD_BOOTSTRAP_ADMIN_USERNAME=admin \
#   MYCELD_BOOTSTRAP_ADMIN_PASSWORD=admin-password \
#   ./scripts/validateShortTermClusterAuthority.sh
#
# Optional:
#   MYCEL_BIN=/path/to/mycel ./scripts/validateShortTermClusterAuthority.sh
#   KEEP_CLUSTER_VALIDATION=1 ./scripts/validateShortTermClusterAuthority.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MYCEL_BIN="${MYCEL_BIN:-}"
ADMIN_USER="${MYCELD_BOOTSTRAP_ADMIN_USERNAME:-admin}"
ADMIN_PASS="${MYCELD_BOOTSTRAP_ADMIN_PASSWORD:-admin-password}"
RUN_ID="${RUN_ID:-$$}"
find_free_port(){ python3 - <<'PY'
import socket
s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()
PY
}
TOKEN_FILE="${TOKEN_FILE:-/tmp/mycel-node-b-${RUN_ID}.join}"
LOG_DIR="${LOG_DIR:-/tmp/mycel-cluster-authority-validation-${RUN_ID}}"
NODE_A_ADDR="${NODE_A_ADDR:-127.0.0.1:$(find_free_port)}"
NODE_B_ADDR="${NODE_B_ADDR:-127.0.0.1:$(find_free_port)}"

mkdir -p "$LOG_DIR"
rm -f "$TOKEN_FILE"

if [[ -n "$MYCEL_BIN" ]]; then
  MYCEL_CMD=("$MYCEL_BIN")
elif [[ -x "$ROOT_DIR/mycel" ]]; then
  MYCEL_CMD=("$ROOT_DIR/mycel")
else
  MYCEL_CMD=(go run ./cmd/mycel)
fi

pids=()
cleanup() {
  if [[ "${KEEP_CLUSTER_VALIDATION:-}" == "1" ]]; then
    echo "Leaving validation daemons running: ${pids[*]:-}"
    return
  fi
  for pid in "${pids[@]:-}"; do
    kill "$pid" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

wait_for_status() {
  local addr="$1"
  local label="$2"
  for _ in $(seq 1 60); do
    if "${MYCEL_CMD[@]}" --daemon-addr "$addr" -u "$ADMIN_USER" -p "$ADMIN_PASS" cluster status >/tmp/mycel-status.out 2>/tmp/mycel-status.err; then
      echo "$label is ready"
      return 0
    fi
    sleep 1
  done
  echo "Timed out waiting for $label at $addr" >&2
  cat /tmp/mycel-status.err >&2 || true
  return 1
}

cd "$ROOT_DIR"

export MYCELD_BOOTSTRAP_ADMIN_USERNAME="$ADMIN_USER"
export MYCELD_BOOTSTRAP_ADMIN_PASSWORD="$ADMIN_PASS"

MYCELD_GRPC_ADDR="0.0.0.0:${NODE_A_ADDR##*:}" MYCELD_CLUSTER_BACKEND_ADVERTISE_ADDR="$NODE_A_ADDR" MYCELD_DATA_DIR="/tmp/mycel-authority-${RUN_ID}-node-a" MYCELD_WIPE_DATA=true ./scripts/startClusterNode.sh node-a >"$LOG_DIR/node-a.log" 2>&1 &
pids+=("$!")
wait_for_status "$NODE_A_ADDR" "node-a"

"${MYCEL_CMD[@]}" --daemon-addr "$NODE_A_ADDR" -u "$ADMIN_USER" -p "$ADMIN_PASS" cluster status | tee "$LOG_DIR/node-a-status.txt"
if ! grep -qi "role=primary" "$LOG_DIR/node-a-status.txt"; then
  echo "Expected node-a status to show role=primary" >&2
  exit 1
fi

"${MYCEL_CMD[@]}" --daemon-addr "$NODE_A_ADDR" -u "$ADMIN_USER" -p "$ADMIN_PASS" cluster node add node-b --token-file "$TOKEN_FILE"

MYCELD_GRPC_ADDR="0.0.0.0:${NODE_B_ADDR##*:}" MYCELD_CLUSTER_BACKEND_ADVERTISE_ADDR="$NODE_B_ADDR" MYCELD_CLUSTER_SEED_PEERS="$NODE_A_ADDR" MYCELD_DATA_DIR="/tmp/mycel-authority-${RUN_ID}-node-b" MYCELD_CLUSTER_JOIN_TOKEN_FILE="$TOKEN_FILE" MYCELD_WIPE_DATA=true ./scripts/startClusterNode.sh node-b >"$LOG_DIR/node-b.log" 2>&1 &
pids+=("$!")
wait_for_status "$NODE_B_ADDR" "node-b"

"${MYCEL_CMD[@]}" --daemon-addr "$NODE_B_ADDR" -u "$ADMIN_USER" -p "$ADMIN_PASS" cluster status | tee "$LOG_DIR/node-b-status.txt"
if ! grep -qi "role=follower" "$LOG_DIR/node-b-status.txt"; then
  echo "Expected node-b status to show role=follower" >&2
  exit 1
fi

set +e
"${MYCEL_CMD[@]}" --daemon-addr "$NODE_B_ADDR" -u "$ADMIN_USER" -p "$ADMIN_PASS" cluster node add node-c >"$LOG_DIR/follower-write.out" 2>"$LOG_DIR/follower-write.err"
follower_rc=$?
set -e
if [[ "$follower_rc" == "0" ]]; then
  echo "Expected follower write to fail" >&2
  exit 1
fi
if ! grep -qi "not.*cluster primary\|not.*primary" "$LOG_DIR/follower-write.err" "$LOG_DIR/follower-write.out"; then
  echo "Expected follower write failure to mention primary" >&2
  cat "$LOG_DIR/follower-write.err" >&2
  cat "$LOG_DIR/follower-write.out" >&2
  exit 1
fi

"${MYCEL_CMD[@]}" --daemon-addr "$NODE_A_ADDR" -u "$ADMIN_USER" -p "$ADMIN_PASS" cluster node add node-c --token-file /tmp/mycel-node-c.join

echo "Short-term cluster authority validation passed. Logs: $LOG_DIR"
