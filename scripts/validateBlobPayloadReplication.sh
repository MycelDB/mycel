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
TOKEN_FILE="${TOKEN_FILE:-/tmp/mycel-blob-node-b-${RUN_ID}.join}"
LOG_DIR="${LOG_DIR:-/tmp/mycel-blob-repl-validation-${RUN_ID}}"
NODE_A_ADDR="${NODE_A_ADDR:-127.0.0.1:$(find_free_port)}"
NODE_B_ADDR="${NODE_B_ADDR:-127.0.0.1:$(find_free_port)}"
SPACE_NAME="${SPACE_NAME:-blob-repl-space-${RUN_ID}}"
OWNER_USERNAME="${OWNER_USERNAME:-blob-owner-${RUN_ID}}"
OWNER_PASS="blob-owner-password"
mkdir -p "$LOG_DIR"; rm -f "$TOKEN_FILE"
if [[ -n "$MYCEL_BIN" ]]; then MYCEL_CMD=("$MYCEL_BIN"); elif [[ -x "$ROOT_DIR/mycel" ]]; then MYCEL_CMD=("$ROOT_DIR/mycel"); else MYCEL_CMD=(go run ./cmd/mycel); fi
pids=(); cleanup(){ if [[ "${KEEP_CLUSTER_VALIDATION:-}" == "1" ]]; then echo "Leaving daemons running: ${pids[*]:-}. Logs: $LOG_DIR"; return; fi; for pid in "${pids[@]:-}"; do kill "$pid" >/dev/null 2>&1 || true; done; }
trap cleanup EXIT
wait_for_status(){ local addr="$1" label="$2"; for _ in $(seq 1 90); do if "${MYCEL_CMD[@]}" --daemon-addr "$addr" -u "$ADMIN_USER" -p "$ADMIN_PASS" cluster status >/dev/null 2>"$LOG_DIR/$label-status.err"; then echo "$label ready"; return 0; fi; sleep 1; done; cat "$LOG_DIR/$label-status.err" >&2 || true; return 1; }
cd "$ROOT_DIR"
export MYCELD_BOOTSTRAP_ADMIN_USERNAME="$ADMIN_USER" MYCELD_BOOTSTRAP_ADMIN_PASSWORD="$ADMIN_PASS"
MYCELD_GRPC_ADDR="0.0.0.0:${NODE_A_ADDR##*:}" MYCELD_CLUSTER_BACKEND_ADVERTISE_ADDR="$NODE_A_ADDR" MYCELD_DATA_DIR="/tmp/mycel-blob-${RUN_ID}-node-a" MYCELD_WIPE_DATA=true ./scripts/startClusterNode.sh node-a >"$LOG_DIR/node-a.log" 2>&1 & pids+=("$!")
wait_for_status "$NODE_A_ADDR" node-a
"${MYCEL_CMD[@]}" --daemon-addr "$NODE_A_ADDR" -u "$ADMIN_USER" -p "$ADMIN_PASS" cluster node add node-b --token-file "$TOKEN_FILE" >"$LOG_DIR/add-node.out"
MYCELD_GRPC_ADDR="0.0.0.0:${NODE_B_ADDR##*:}" MYCELD_CLUSTER_BACKEND_ADVERTISE_ADDR="$NODE_B_ADDR" MYCELD_CLUSTER_SEED_PEERS="$NODE_A_ADDR" MYCELD_DATA_DIR="/tmp/mycel-blob-${RUN_ID}-node-b" MYCELD_CLUSTER_JOIN_TOKEN_FILE="$TOKEN_FILE" MYCELD_WIPE_DATA=true ./scripts/startClusterNode.sh node-b >"$LOG_DIR/node-b.log" 2>&1 & pids+=("$!")
wait_for_status "$NODE_B_ADDR" node-b
"${MYCEL_CMD[@]}" --daemon-addr "$NODE_A_ADDR" -u "$ADMIN_USER" -p "$ADMIN_PASS" user add --user-username "$OWNER_USERNAME" --new-password "$OWNER_PASS" >"$LOG_DIR/create-user.out"
"${MYCEL_CMD[@]}" --daemon-addr "$NODE_A_ADDR" -u "$ADMIN_USER" -p "$ADMIN_PASS" --output json space add "$SPACE_NAME" --owner-username "$OWNER_USERNAME" >"$LOG_DIR/create-space.json"
SPACE_ID="$(python3 - <<'PY' "$LOG_DIR/create-space.json"
import json,sys
j=json.load(open(sys.argv[1])); s=j.get('space') or j; print(s.get('space_id') or s.get('spaceId') or s.get('id') or '')
PY
)"
if [[ -z "$SPACE_ID" ]]; then echo "failed to parse space id" >&2; cat "$LOG_DIR/create-space.json" >&2; exit 1; fi
printf 'hello replicated blob %s\n' "$RUN_ID" >"$LOG_DIR/source.txt"
"${MYCEL_CMD[@]}" --daemon-addr "$NODE_A_ADDR" -u "$OWNER_USERNAME" -p "$OWNER_PASS" --output json blob upload --space-id "$SPACE_ID" --mime-type text/plain "$LOG_DIR/source.txt" >"$LOG_DIR/upload.json"
BLOB_ID="$(python3 - <<'PY' "$LOG_DIR/upload.json"
import json,sys
j=json.load(open(sys.argv[1])); print(j.get('blob_id') or j.get('blobId') or '')
PY
)"
if [[ -z "$BLOB_ID" ]]; then echo "failed to parse blob id" >&2; cat "$LOG_DIR/upload.json" >&2; exit 1; fi
for _ in $(seq 1 90); do
  if "${MYCEL_CMD[@]}" --daemon-addr "$NODE_B_ADDR" -u "$OWNER_USERNAME" -p "$OWNER_PASS" blob download --space-id "$SPACE_ID" --output-file "$LOG_DIR/follower.txt" "$BLOB_ID" >"$LOG_DIR/download.out" 2>"$LOG_DIR/download.err"; then
    if cmp -s "$LOG_DIR/source.txt" "$LOG_DIR/follower.txt"; then echo "Blob payload replication validation passed. Logs: $LOG_DIR"; exit 0; fi
  fi
  sleep 1
done
echo "timed out waiting for follower blob payload" >&2; cat "$LOG_DIR/download.err" >&2 || true; exit 1
