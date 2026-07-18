#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ADMIN_USER="${MYCELD_BOOTSTRAP_ADMIN_USERNAME:-admin}"
ADMIN_PASS="${MYCELD_BOOTSTRAP_ADMIN_PASSWORD:-admin-password}"
RUN_ID="${RUN_ID:-$$}"
find_free_port(){ python3 - <<'PY'
import socket
s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()
PY
}
NODE_A_ADDR="${NODE_A_ADDR:-127.0.0.1:$(find_free_port)}"; NODE_B_ADDR="${NODE_B_ADDR:-127.0.0.1:$(find_free_port)}"
NODE_A_DIR="/tmp/mycel-switch-${RUN_ID}-node-a"; NODE_B_DIR="/tmp/mycel-switch-${RUN_ID}-node-b"; TOKEN_FILE="/tmp/mycel-switch-${RUN_ID}.join"; LOG_DIR="/tmp/mycel-switch-validation-${RUN_ID}"
mkdir -p "$LOG_DIR"; rm -f "$TOKEN_FILE"
MYCEL_CMD=(go run ./cmd/mycel)
pids=(); kill_tree(){ local pid="$1"; for c in $(pgrep -P "$pid" 2>/dev/null || true); do kill_tree "$c"; done; kill "$pid" >/dev/null 2>&1 || true; }
cleanup(){ for pid in "${pids[@]:-}"; do kill_tree "$pid"; done; }
trap cleanup EXIT
wait_for_status(){ local addr="$1" label="$2"; for _ in $(seq 1 90); do if "${MYCEL_CMD[@]}" --daemon-addr "$addr" -u "$ADMIN_USER" -p "$ADMIN_PASS" cluster status >/dev/null 2>&1; then echo "$label ready"; return 0; fi; sleep 1; done; echo "timeout $label" >&2; return 1; }
json_field(){ python3 - "$1" "$2" <<'PY'
import json, sys
with open(sys.argv[1]) as f: d=json.load(f)
for p in sys.argv[2].split('.'):
 d=d.get(p,{}) if isinstance(d,dict) else {}
print(d if d is not None else '')
PY
}
cd "$ROOT_DIR"; export MYCELD_BOOTSTRAP_ADMIN_USERNAME="$ADMIN_USER" MYCELD_BOOTSTRAP_ADMIN_PASSWORD="$ADMIN_PASS" MYCELD_WAL_SEGMENT_BYTES=4096
MYCELD_GRPC_ADDR="0.0.0.0:${NODE_A_ADDR##*:}" MYCELD_CLUSTER_BACKEND_ADVERTISE_ADDR="$NODE_A_ADDR" MYCELD_DATA_DIR="$NODE_A_DIR" MYCELD_WIPE_DATA=true ./scripts/startClusterNode.sh node-a >"$LOG_DIR/node-a.log" 2>&1 & pids+=("$!")
wait_for_status "$NODE_A_ADDR" node-a
"${MYCEL_CMD[@]}" --daemon-addr "$NODE_A_ADDR" -u "$ADMIN_USER" -p "$ADMIN_PASS" cluster node add node-b --token-file "$TOKEN_FILE" >"$LOG_DIR/add.out"
MYCELD_GRPC_ADDR="0.0.0.0:${NODE_B_ADDR##*:}" MYCELD_CLUSTER_BACKEND_ADVERTISE_ADDR="$NODE_B_ADDR" MYCELD_CLUSTER_SEED_PEERS="$NODE_A_ADDR" MYCELD_DATA_DIR="$NODE_B_DIR" MYCELD_CLUSTER_JOIN_TOKEN_FILE="$TOKEN_FILE" MYCELD_WIPE_DATA=true ./scripts/startClusterNode.sh node-b >"$LOG_DIR/node-b.log" 2>&1 & pids+=("$!")
wait_for_status "$NODE_B_ADDR" node-b
"${MYCEL_CMD[@]}" --daemon-addr "$NODE_A_ADDR" -u "$ADMIN_USER" -p "$ADMIN_PASS" user add --user-username "switch-owner-${RUN_ID}" --new-password pass >"$LOG_DIR/user.out"
for _ in $(seq 1 60); do "${MYCEL_CMD[@]}" --output json --daemon-addr "$NODE_B_ADDR" -u "$ADMIN_USER" -p "$ADMIN_PASS" cluster status >"$LOG_DIR/b-status.json" 2>/dev/null || true; [[ "$(json_field "$LOG_DIR/b-status.json" replication.applied_lsn)" != "0" ]] && break; sleep 1; done
"${MYCEL_CMD[@]}" --daemon-addr "$NODE_A_ADDR" -u "$ADMIN_USER" -p "$ADMIN_PASS" cluster primary switch node-b >"$LOG_DIR/switch.out" 2>"$LOG_DIR/switch.err"
cat "$LOG_DIR/switch.out"
for _ in $(seq 1 30); do "${MYCEL_CMD[@]}" --output json --daemon-addr "$NODE_B_ADDR" -u "$ADMIN_USER" -p "$ADMIN_PASS" cluster status >"$LOG_DIR/b-after.json" 2>/dev/null || true; [[ "$(json_field "$LOG_DIR/b-after.json" node.role)" == "primary" ]] && break; sleep 1; done
"${MYCEL_CMD[@]}" --output json --daemon-addr "$NODE_A_ADDR" -u "$ADMIN_USER" -p "$ADMIN_PASS" cluster status >"$LOG_DIR/a-after.json"
[[ "$(json_field "$LOG_DIR/b-after.json" node.role)" == "primary" ]] || { echo "node-b not primary" >&2; exit 1; }
[[ "$(json_field "$LOG_DIR/a-after.json" node.role)" == "follower" ]] || { echo "node-a not follower" >&2; exit 1; }
if "${MYCEL_CMD[@]}" --daemon-addr "$NODE_A_ADDR" -u "$ADMIN_USER" -p "$ADMIN_PASS" user add --user-username "should-fail-${RUN_ID}" --new-password pass >"$LOG_DIR/old-write.out" 2>"$LOG_DIR/old-write.err"; then echo "old primary accepted write" >&2; exit 1; fi
if ! grep -q "node is not cluster primary" "$LOG_DIR/old-write.err"; then echo "old primary write did not return not-primary" >&2; cat "$LOG_DIR/old-write.err" >&2; exit 1; fi
if "${MYCEL_CMD[@]}" --daemon-addr "$NODE_A_ADDR" -u "$ADMIN_USER" -p "$ADMIN_PASS" cluster node add should-hint >"$LOG_DIR/old-admin-write.out" 2>"$LOG_DIR/old-admin-write.err"; then echo "old primary accepted admin cluster write" >&2; exit 1; fi
if ! grep -q "primary=node-b" "$LOG_DIR/old-admin-write.err" || ! grep -q "addr=$NODE_B_ADDR" "$LOG_DIR/old-admin-write.err"; then echo "not-primary admin error did not include new primary hint" >&2; cat "$LOG_DIR/old-admin-write.err" >&2; exit 1; fi
"${MYCEL_CMD[@]}" --daemon-addr "$NODE_B_ADDR" -u "$ADMIN_USER" -p "$ADMIN_PASS" user add --user-username "switch-new-${RUN_ID}" --new-password pass >"$LOG_DIR/new-write.out"
echo "Cluster primary switchover validation passed. Logs: $LOG_DIR"
