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
TOKEN_FILE="${TOKEN_FILE:-/tmp/mycel-resync-node-b-${RUN_ID}.join}"
LOG_DIR="${LOG_DIR:-/tmp/mycel-snapshot-resync-validation-${RUN_ID}}"
NODE_A_ADDR="${NODE_A_ADDR:-127.0.0.1:$(find_free_port)}"
NODE_B_ADDR="${NODE_B_ADDR:-127.0.0.1:$(find_free_port)}"
NODE_A_DIR="${NODE_A_DIR:-/tmp/mycel-resync-${RUN_ID}-node-a}"
NODE_B_DIR="${NODE_B_DIR:-/tmp/mycel-resync-${RUN_ID}-node-b}"
OWNER_USERNAME="${OWNER_USERNAME:-resync-owner-${RUN_ID}}"
OWNER_PASSWORD="${OWNER_PASSWORD:-resync-owner-password}"
SPACE_NAME="${SPACE_NAME:-resync-space-${RUN_ID}}"
FILLER_WRITES="${FILLER_WRITES:-30}"
WAL_SEGMENT_BYTES="${MYCELD_WAL_SEGMENT_BYTES:-1024}"

mkdir -p "$LOG_DIR"
rm -f "$TOKEN_FILE"
if [[ -n "$MYCEL_BIN" ]]; then MYCEL_CMD=("$MYCEL_BIN"); elif [[ -x "$ROOT_DIR/mycel" ]]; then MYCEL_CMD=("$ROOT_DIR/mycel"); else MYCEL_CMD=(go run ./cmd/mycel); fi
pids=()
kill_tree(){
  local pid="$1"
  local child
  for child in $(pgrep -P "$pid" 2>/dev/null || true); do kill_tree "$child"; done
  kill "$pid" >/dev/null 2>&1 || true
}
cleanup(){
  if [[ "${KEEP_CLUSTER_VALIDATION:-}" == "1" ]]; then echo "Leaving daemons running: ${pids[*]:-}. Logs: $LOG_DIR"; return; fi
  for pid in "${pids[@]:-}"; do kill_tree "$pid"; done
}
dump_logs(){
  echo "--- node-a.log ---" >&2; tail -120 "$LOG_DIR/node-a.log" >&2 2>/dev/null || true
  echo "--- node-b.log ---" >&2; tail -120 "$LOG_DIR/node-b.log" >&2 2>/dev/null || true
  echo "--- node-b-restart.log ---" >&2; tail -120 "$LOG_DIR/node-b-restart.log" >&2 2>/dev/null || true
  echo "--- node-b-after-resync.log ---" >&2; tail -120 "$LOG_DIR/node-b-after-resync.log" >&2 2>/dev/null || true
  echo "--- primary wal files ---" >&2; ls -la "$NODE_A_DIR/wal" >&2 2>/dev/null || true
  echo "--- follower progress ---" >&2; cat "$NODE_B_DIR/meta/clustering/replication/progress.json" >&2 2>/dev/null || true
}
trap cleanup EXIT
trap 'dump_logs' ERR

wait_for_status(){ local addr="$1" label="$2"; for _ in $(seq 1 90); do if "${MYCEL_CMD[@]}" --daemon-addr "$addr" -u "$ADMIN_USER" -p "$ADMIN_PASS" cluster status >/tmp/mycel-resync-status.out 2>/tmp/mycel-resync-status.err; then echo "$label is ready"; return 0; fi; sleep 1; done; echo "Timed out waiting for $label" >&2; cat /tmp/mycel-resync-status.err >&2 || true; return 1; }
json_field(){ python3 - "$1" "$2" <<'PY'
import json, sys
path, expr = sys.argv[1], sys.argv[2].split('.')
with open(path) as f: data=json.load(f)
cur=data
for p in expr:
    cur=cur.get(p, {}) if isinstance(cur, dict) else {}
print(cur if cur is not None else "")
PY
}
wait_for_catchup_state(){ local addr="$1" want="$2" out="$3"; for _ in $(seq 1 90); do "${MYCEL_CMD[@]}" --output json --daemon-addr "$addr" -u "$ADMIN_USER" -p "$ADMIN_PASS" cluster status >"$out" 2>"$out.err" || true; state="$(json_field "$out" replication.catchup_state 2>/dev/null || true)"; snap_next="$(json_field "$out" replication.snapshot_required.next_requested_lsn 2>/dev/null || true)"; if [[ "$state" == "$want" ]] || [[ "$want" == "snapshot_required" && -n "$snap_next" && "$snap_next" != "{}" ]]; then return 0; fi; sleep 1; done; echo "Timed out waiting for catchup_state=$want at $addr; last state=${state:-unknown}" >&2; cat "$out" >&2 || true; cat "$out.err" >&2 || true; return 1; }

cd "$ROOT_DIR"
export MYCELD_BOOTSTRAP_ADMIN_USERNAME="$ADMIN_USER" MYCELD_BOOTSTRAP_ADMIN_PASSWORD="$ADMIN_PASS"
export MYCELD_WAL_SEGMENT_BYTES="$WAL_SEGMENT_BYTES"

MYCELD_GRPC_ADDR="0.0.0.0:${NODE_A_ADDR##*:}" MYCELD_CLUSTER_BACKEND_ADVERTISE_ADDR="$NODE_A_ADDR" MYCELD_DATA_DIR="$NODE_A_DIR" MYCELD_WIPE_DATA=true ./scripts/startClusterNode.sh node-a >"$LOG_DIR/node-a.log" 2>&1 & pids+=("$!")
wait_for_status "$NODE_A_ADDR" node-a
"${MYCEL_CMD[@]}" --daemon-addr "$NODE_A_ADDR" -u "$ADMIN_USER" -p "$ADMIN_PASS" cluster node add node-b --token-file "$TOKEN_FILE" >"$LOG_DIR/add-node.out"
MYCELD_GRPC_ADDR="0.0.0.0:${NODE_B_ADDR##*:}" MYCELD_CLUSTER_BACKEND_ADVERTISE_ADDR="$NODE_B_ADDR" MYCELD_CLUSTER_SEED_PEERS="$NODE_A_ADDR" MYCELD_DATA_DIR="$NODE_B_DIR" MYCELD_CLUSTER_JOIN_TOKEN_FILE="$TOKEN_FILE" MYCELD_WIPE_DATA=true ./scripts/startClusterNode.sh node-b >"$LOG_DIR/node-b.log" 2>&1 & node_b_pid="$!"; pids+=("$node_b_pid")
wait_for_status "$NODE_B_ADDR" node-b

# Stop follower so its replication progress falls behind while the primary advances.
kill_tree "$node_b_pid"
wait "$node_b_pid" 2>/dev/null || true
for _ in $(seq 1 30); do
  if ! "${MYCEL_CMD[@]}" --daemon-addr "$NODE_B_ADDR" -u "$ADMIN_USER" -p "$ADMIN_PASS" cluster status >/dev/null 2>&1; then break; fi
  sleep 1
done

"${MYCEL_CMD[@]}" --daemon-addr "$NODE_A_ADDR" -u "$ADMIN_USER" -p "$ADMIN_PASS" user add --user-username "$OWNER_USERNAME" --new-password "$OWNER_PASSWORD" >"$LOG_DIR/create-owner.out"
"${MYCEL_CMD[@]}" --daemon-addr "$NODE_A_ADDR" -u "$ADMIN_USER" -p "$ADMIN_PASS" space add "$SPACE_NAME" --owner-username "$OWNER_USERNAME" >"$LOG_DIR/create-space.out"
for i in $(seq 1 "$FILLER_WRITES"); do
  "${MYCEL_CMD[@]}" --daemon-addr "$NODE_A_ADDR" -u "$ADMIN_USER" -p "$ADMIN_PASS" user add --user-username "resync-filler-${RUN_ID}-${i}" --new-password "$OWNER_PASSWORD" >"$LOG_DIR/filler-${i}.out"
done

# Force a retained-WAL gap for the stopped follower by pruning all closed older
# segments and keeping the current segment. This is intentionally test-only and
# relies on the tiny WAL segment size configured above.
wal_files=()
while IFS= read -r f; do wal_files+=("$f"); done <<EOF_WAL_FILES
$(ls "$NODE_A_DIR"/wal/*.wal 2>/dev/null | sort)
EOF_WAL_FILES
if (( ${#wal_files[@]} < 2 )); then
  echo "Expected multiple WAL segments with MYCELD_WAL_SEGMENT_BYTES=$WAL_SEGMENT_BYTES, found ${#wal_files[@]}" >&2
  exit 1
fi
for ((i=0; i<${#wal_files[@]}-1; i++)); do rm -f "${wal_files[$i]}"; done
ls -la "$NODE_A_DIR/wal" >"$LOG_DIR/primary-wal-after-prune.out"

# Restart follower with preserved identity/admission. It should request an LSN
# below the primary retained range and enter snapshot_required.
MYCELD_GRPC_ADDR="0.0.0.0:${NODE_B_ADDR##*:}" MYCELD_CLUSTER_BACKEND_ADVERTISE_ADDR="$NODE_B_ADDR" MYCELD_CLUSTER_SEED_PEERS="$NODE_A_ADDR" MYCELD_DATA_DIR="$NODE_B_DIR" MYCELD_WIPE_DATA=false ./scripts/startClusterNode.sh node-b >"$LOG_DIR/node-b-restart.log" 2>&1 & node_b_restart_pid="$!"; pids+=("$node_b_restart_pid")
wait_for_status "$NODE_B_ADDR" node-b-restarted
wait_for_catchup_state "$NODE_B_ADDR" snapshot_required "$LOG_DIR/follower-snapshot-required.json"

"${MYCEL_CMD[@]}" --daemon-addr "$NODE_A_ADDR" -u "$ADMIN_USER" -p "$ADMIN_PASS" cluster node resync node-b >"$LOG_DIR/resync.out" 2>"$LOG_DIR/resync.err"
cat "$LOG_DIR/resync.out"
operation_id="$(awk '/^Operation:/ {print $2}' "$LOG_DIR/resync.out")"
"${MYCEL_CMD[@]}" --output json --daemon-addr "$NODE_A_ADDR" -u "$ADMIN_USER" -p "$ADMIN_PASS" cluster node resync-history >"$LOG_DIR/resync-history.json" 2>"$LOG_DIR/resync-history.err"
python3 - "$LOG_DIR/resync-history.json" "$operation_id" <<'PY'
import json, sys
path, operation_id = sys.argv[1], sys.argv[2]
with open(path) as f: data = json.load(f)
ops = data.get("operations", [])
match = next((op for op in ops if op.get("operation_id") == operation_id), None)
if not match:
    raise SystemExit(f"missing resync history operation {operation_id}")
if match.get("status") != "succeeded":
    raise SystemExit(f"resync history status is {match.get('status')}, want succeeded")
if match.get("target_node_name") != "node-b":
    raise SystemExit(f"resync history target is {match.get('target_node_name')}, want node-b")
PY

# Verify the materialized snapshot data is available on the running follower
# immediately after resync; no follower restart should be required.
for _ in $(seq 1 60); do
  "${MYCEL_CMD[@]}" --daemon-addr "$NODE_B_ADDR" -u "$OWNER_USERNAME" -p "$OWNER_PASSWORD" space list >"$LOG_DIR/follower-spaces.out" 2>"$LOG_DIR/follower-spaces.err" || true
  if grep -q "$SPACE_NAME" "$LOG_DIR/follower-spaces.out"; then
    "${MYCEL_CMD[@]}" --output json --daemon-addr "$NODE_B_ADDR" -u "$ADMIN_USER" -p "$ADMIN_PASS" cluster status >"$LOG_DIR/follower-after-resync.json" 2>/dev/null || true
    "${MYCEL_CMD[@]}" --daemon-addr "$NODE_B_ADDR" -u "$ADMIN_USER" -p "$ADMIN_PASS" user list >"$LOG_DIR/follower-users.out" 2>"$LOG_DIR/follower-users.err"
    if ! grep -q "$OWNER_USERNAME" "$LOG_DIR/follower-users.out"; then
      echo "Follower admin/user reload validation failed: missing $OWNER_USERNAME" >&2
      cat "$LOG_DIR/follower-users.out" >&2 || true
      cat "$LOG_DIR/follower-users.err" >&2 || true
      exit 1
    fi
    echo "WAL snapshot resync validation passed. Logs: $LOG_DIR"
    exit 0
  fi
  sleep 1
done

echo "Timed out waiting for follower to serve resynced materialized data" >&2
cat "$LOG_DIR/follower-spaces.out" >&2 || true
cat "$LOG_DIR/follower-spaces.err" >&2 || true
exit 1
