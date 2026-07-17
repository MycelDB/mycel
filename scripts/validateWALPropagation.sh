#!/usr/bin/env bash
set -euo pipefail

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
TOKEN_FILE="${TOKEN_FILE:-/tmp/mycel-wal-node-b-${RUN_ID}.join}"
LOG_DIR="${LOG_DIR:-/tmp/mycel-wal-propagation-validation-${RUN_ID}}"
NODE_A_ADDR="${NODE_A_ADDR:-127.0.0.1:$(find_free_port)}"
NODE_B_ADDR="${NODE_B_ADDR:-127.0.0.1:$(find_free_port)}"
SPACE_NAME="${SPACE_NAME:-wal-propagation-space}"
OWNER_USERNAME="${OWNER_USERNAME:-wal-owner}"

mkdir -p "$LOG_DIR"
rm -f "$TOKEN_FILE"
if [[ -n "$MYCEL_BIN" ]]; then MYCEL_CMD=("$MYCEL_BIN"); elif [[ -x "$ROOT_DIR/mycel" ]]; then MYCEL_CMD=("$ROOT_DIR/mycel"); else MYCEL_CMD=(go run ./cmd/mycel); fi
pids=()
cleanup(){ if [[ "${KEEP_CLUSTER_VALIDATION:-}" == "1" ]]; then echo "Leaving daemons running: ${pids[*]:-}. Logs: $LOG_DIR"; return; fi; for pid in "${pids[@]:-}"; do kill "$pid" >/dev/null 2>&1 || true; done; }
dump_logs(){ echo "--- node-a.log ---" >&2; tail -80 "$LOG_DIR/node-a.log" >&2 2>/dev/null || true; echo "--- node-b.log ---" >&2; tail -80 "$LOG_DIR/node-b.log" >&2 2>/dev/null || true; echo "--- follower progress ---" >&2; cat "/tmp/mycel-wal-${RUN_ID}-node-b/meta/clustering/replication/progress.json" >&2 2>/dev/null || true; }
trap cleanup EXIT
wait_for_status(){ local addr="$1" label="$2"; for _ in $(seq 1 90); do if "${MYCEL_CMD[@]}" --daemon-addr "$addr" -u "$ADMIN_USER" -p "$ADMIN_PASS" cluster status >/tmp/mycel-wal-status.out 2>/tmp/mycel-wal-status.err; then echo "$label is ready"; return 0; fi; sleep 1; done; echo "Timed out waiting for $label" >&2; cat /tmp/mycel-wal-status.err >&2 || true; return 1; }

cd "$ROOT_DIR"
export MYCELD_BOOTSTRAP_ADMIN_USERNAME="$ADMIN_USER" MYCELD_BOOTSTRAP_ADMIN_PASSWORD="$ADMIN_PASS"
MYCELD_GRPC_ADDR="0.0.0.0:${NODE_A_ADDR##*:}" MYCELD_CLUSTER_BACKEND_ADVERTISE_ADDR="$NODE_A_ADDR" MYCELD_DATA_DIR="/tmp/mycel-wal-${RUN_ID}-node-a" MYCELD_WIPE_DATA=true ./scripts/startClusterNode.sh node-a >"$LOG_DIR/node-a.log" 2>&1 & pids+=("$!")
wait_for_status "$NODE_A_ADDR" node-a
"${MYCEL_CMD[@]}" --daemon-addr "$NODE_A_ADDR" -u "$ADMIN_USER" -p "$ADMIN_PASS" cluster node add node-b --token-file "$TOKEN_FILE"
MYCELD_GRPC_ADDR="0.0.0.0:${NODE_B_ADDR##*:}" MYCELD_CLUSTER_BACKEND_ADVERTISE_ADDR="$NODE_B_ADDR" MYCELD_CLUSTER_SEED_PEERS="$NODE_A_ADDR" MYCELD_DATA_DIR="/tmp/mycel-wal-${RUN_ID}-node-b" MYCELD_CLUSTER_JOIN_TOKEN_FILE="$TOKEN_FILE" MYCELD_WIPE_DATA=true ./scripts/startClusterNode.sh node-b >"$LOG_DIR/node-b.log" 2>&1 & pids+=("$!")
wait_for_status "$NODE_B_ADDR" node-b
"${MYCEL_CMD[@]}" --daemon-addr "$NODE_A_ADDR" -u "$ADMIN_USER" -p "$ADMIN_PASS" user add --user-username "$OWNER_USERNAME" --new-password wal-owner-password >"$LOG_DIR/create-user.out"
"${MYCEL_CMD[@]}" --daemon-addr "$NODE_A_ADDR" -u "$ADMIN_USER" -p "$ADMIN_PASS" space add "$SPACE_NAME" --owner-username "$OWNER_USERNAME" >"$LOG_DIR/create-space.out"
for _ in $(seq 1 60); do
  "${MYCEL_CMD[@]}" --daemon-addr "$NODE_B_ADDR" -u "$OWNER_USERNAME" -p wal-owner-password space list >"$LOG_DIR/follower-spaces.out" 2>"$LOG_DIR/follower-spaces.err" || true
  if grep -q "$SPACE_NAME" "$LOG_DIR/follower-spaces.out"; then echo "WAL propagation validation passed. Logs: $LOG_DIR"; exit 0; fi
  sleep 1
done
echo "Timed out waiting for follower to see replicated space" >&2
cat "$LOG_DIR/follower-spaces.out" >&2 || true
cat "$LOG_DIR/follower-spaces.err" >&2 || true
dump_logs
exit 1
