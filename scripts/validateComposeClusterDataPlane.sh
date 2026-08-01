#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${MYCEL_COMPOSE_FILE:-${ROOT_DIR}/../../knot_pkm/knot_pkm_server/compose.dev.yml}"
SERVICES_CSV="${MYCEL_COMPOSE_SERVICES:-myceld-a,myceld-b,myceld-c}"
ADMIN_USERNAME="${MYCEL_OPERATOR_USERNAME:-${MYCELD_BOOTSTRAP_ADMIN_USERNAME:-admin}}"
ADMIN_PASSWORD="${MYCEL_OPERATOR_PASSWORD:-${MYCELD_BOOTSTRAP_ADMIN_PASSWORD:-admin-password}}"
DAEMON_ADDR="${MYCELD_DAEMON_ADDR:-127.0.0.1:9091}"
STATE_FILE="${MYCEL_COMPOSE_DATA_PLANE_STATE:-${MYCEL_DATA_PLANE_STATE:-}}"
CREATE_IF_MISSING="${MYCEL_DATA_PLANE_CREATE_IF_MISSING:-true}"
TIMEOUT_SECONDS="${MYCEL_COMPOSE_DATA_PLANE_TIMEOUT:-180}"
SLEEP_SECONDS="${MYCEL_COMPOSE_DATA_PLANE_INTERVAL:-3}"
CLI_TIMEOUT_SECONDS="${MYCEL_COMPOSE_CLI_TIMEOUT:-25}"

if [[ ! -f "$COMPOSE_FILE" ]]; then
  echo "compose file not found: $COMPOSE_FILE" >&2
  exit 1
fi

IFS=',' read -r -a SERVICES <<< "$SERVICES_CSV"
if [[ ${#SERVICES[@]} -eq 0 ]]; then
  echo "no services configured" >&2
  exit 1
fi

if [[ -z "$STATE_FILE" ]]; then
  STATE_FILE="$(mktemp)"
  trap 'rm -f "$STATE_FILE"' EXIT
fi

compose() { docker compose -f "$COMPOSE_FILE" "$@"; }

with_timeout() {
  if command -v timeout >/dev/null 2>&1; then
    timeout "$CLI_TIMEOUT_SECONDS" "$@"
  elif command -v gtimeout >/dev/null 2>&1; then
    gtimeout "$CLI_TIMEOUT_SECONDS" "$@"
  else
    "$@"
  fi
}

trim() { echo "$1" | xargs; }

service_at() {
  local idx="$1"
  trim "${SERVICES[$idx]}"
}

cli() {
  local service="$1"; shift
  with_timeout docker compose -f "$COMPOSE_FILE" exec -T "$service" mycel --daemon-addr "$DAEMON_ADDR" "$@"
}

admin_cli() {
  local service="$1"; shift
  cli "$service" --username "$ADMIN_USERNAME" --password "$ADMIN_PASSWORD" --output json "$@"
}

user_cli() {
  local service="$1"; shift
  cli "$service" --username "$TEST_USERNAME" --password "$TEST_PASSWORD" --output json "$@"
}

json_get() {
  local expr="$1"
  python3 -c 'import json,sys
expr=sys.argv[1]
data=json.load(sys.stdin)
cur=data
for part in expr.split("."):
    if not part:
        continue
    cur=cur[part]
print(cur)' "$expr"
}

uuid() { python3 -c 'import uuid; print(uuid.uuid4())'; }

shell_quote() {
  python3 -c 'import shlex,sys; print(shlex.quote(sys.argv[1]))' "$1"
}

load_state() {
  # shellcheck disable=SC1090
  source "$STATE_FILE"
}

save_state() {
  cat > "$STATE_FILE" <<EOF_STATE
TEST_USERNAME=$(shell_quote "$TEST_USERNAME")
TEST_PASSWORD=$(shell_quote "$TEST_PASSWORD")
SPACE_ID=$(shell_quote "$SPACE_ID")
DOMAIN_ID=$(shell_quote "$DOMAIN_ID")
PARENT_NODE_ID=$(shell_quote "$PARENT_NODE_ID")
CHILD_NODE_ID=$(shell_quote "$CHILD_NODE_ID")
EDGE_ID=$(shell_quote "$EDGE_ID")
MARKER=$(shell_quote "$MARKER")
COMMITTED_REVISION=$(shell_quote "${COMMITTED_REVISION:-0}")
EOF_STATE
}

create_data() {
  local writer="$1" raw session_id tx_id
  local suffix
  suffix="$(date +%s)-$RANDOM"
  TEST_USERNAME="${MYCEL_DATA_PLANE_USER:-phase-g5-$suffix}"
  TEST_PASSWORD="${MYCEL_DATA_PLANE_PASSWORD:-phase-g5-pass-$suffix}"
  MARKER="phase-g5-$suffix"
  PARENT_NODE_ID="$(uuid)"
  CHILD_NODE_ID="$(uuid)"
  EDGE_ID="$(uuid)"

  echo "Creating Phase G5 data-plane user/space through $writer" >&2
  admin_cli "$writer" user add --user-username "$TEST_USERNAME" --new-password "$TEST_PASSWORD" >/dev/null
  raw="$(admin_cli "$writer" space add "Phase G5 $suffix" --owner-username "$TEST_USERNAME" --default-domain-key g5 --default-domain-name "Phase G5")"
  SPACE_ID="$(printf '%s\n' "$raw" | json_get 'space.space_id')"
  DOMAIN_ID="$(printf '%s\n' "$raw" | json_get 'default_domain_id')"

  local candidate wrote=false
  for candidate in "${SERVICES[@]}"; do
    candidate="$(trim "$candidate")"
    [[ -n "$candidate" ]] || continue
    if try_create_graph_data "$candidate"; then
      writer="$candidate"
      wrote=true
      break
    fi
    echo "Graph write through $candidate did not reach the current partition leader; trying next service" >&2
  done
  if [[ "$wrote" != "true" ]]; then
    echo "failed to write Phase G5 graph data through any configured service" >&2
    return 1
  fi
  echo "Phase G5 graph data committed through $writer at revision ${COMMITTED_REVISION:-0}" >&2
  save_state
}

try_create_graph_data() {
  local writer="$1" raw session_id tx_id
  if ! raw="$(user_cli "$writer" session open --space-id "$SPACE_ID" --domain-id "$DOMAIN_ID")"; then
    return 1
  fi
  session_id="$(printf '%s\n' "$raw" | json_get 'session_id')"
  if ! raw="$(user_cli "$writer" transaction begin "$session_id" --mode read-write)"; then
    user_cli "$writer" session close "$session_id" >/dev/null || true
    return 1
  fi
  tx_id="$(printf '%s\n' "$raw" | json_get 'transaction_id')"
  if ! user_cli "$writer" graph node create --transaction-id "$tx_id" --node-id "$PARENT_NODE_ID" --label G5Parent --properties-json "{\"marker\":\"$MARKER\",\"role\":\"parent\"}" --payload-json "{\"text\":\"phase-g5-parent\"}" >/dev/null; then
    user_cli "$writer" transaction rollback "$tx_id" >/dev/null || true
    user_cli "$writer" session close "$session_id" >/dev/null || true
    return 1
  fi
  if ! user_cli "$writer" graph node create --transaction-id "$tx_id" --node-id "$CHILD_NODE_ID" --label G5Child --properties-json "{\"marker\":\"$MARKER\",\"role\":\"child\"}" --payload-json "{\"text\":\"phase-g5-child\"}" >/dev/null; then
    user_cli "$writer" transaction rollback "$tx_id" >/dev/null || true
    user_cli "$writer" session close "$session_id" >/dev/null || true
    return 1
  fi
  if ! user_cli "$writer" graph edge create --transaction-id "$tx_id" --edge-id "$EDGE_ID" --from "$PARENT_NODE_ID" --to "$CHILD_NODE_ID" --kind g5_contains --props-json "{\"marker\":\"$MARKER\"}" >/dev/null; then
    user_cli "$writer" transaction rollback "$tx_id" >/dev/null || true
    user_cli "$writer" session close "$session_id" >/dev/null || true
    return 1
  fi
  if ! raw="$(user_cli "$writer" transaction commit "$tx_id")"; then
    user_cli "$writer" transaction rollback "$tx_id" >/dev/null || true
    user_cli "$writer" session close "$session_id" >/dev/null || true
    return 1
  fi
  COMMITTED_REVISION="$(printf '%s\n' "$raw" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("committed_revision", 0))')"
  user_cli "$writer" session close "$session_id" >/dev/null || true
  return 0
}

validate_query_response() {
  local service="$1" raw="$2"
  RAW_JSON="$raw" python3 - "$service" "$PARENT_NODE_ID" "$MARKER" <<'PY'
import json, os, sys
service, node_id, marker = sys.argv[1:4]
data = json.loads(os.environ["RAW_JSON"])
rows = data.get("rows") or []
if len(rows) != 1:
    raise SystemExit(f"{service}: expected one query row, got {len(rows)}")
meta = data.get("read_metadata") or {}
if meta.get("consistency") != "strong" or bool(meta.get("stale")):
    raise SystemExit(f"{service}: expected strong non-stale read metadata, got {meta}")
field = (rows[0].get("fields") or {}).get("node") or {}
node = (((field.get("Value") or {}).get("Node")) or field.get("node") or {})
if node.get("node_id") != node_id:
    raise SystemExit(f"{service}: query returned node {node.get('node_id')} expected {node_id}")
props = node.get("properties") or {}
if props.get("marker") != marker:
    raise SystemExit(f"{service}: query marker mismatch: {props}")
PY
}

validate_node_response() {
  local service="$1" raw="$2" want="$3"
  RAW_JSON="$raw" python3 - "$service" "$want" <<'PY'
import json, os, sys
service, want = sys.argv[1:3]
data = json.loads(os.environ["RAW_JSON"])
if data.get("node_id") != want:
    raise SystemExit(f"{service}: node_id {data.get('node_id')} expected {want}")
PY
}

validate_edge_response() {
  local service="$1" raw="$2"
  RAW_JSON="$raw" python3 - "$service" "$EDGE_ID" "$PARENT_NODE_ID" "$CHILD_NODE_ID" <<'PY'
import json, os, sys
service, edge_id, parent, child = sys.argv[1:5]
data = json.loads(os.environ["RAW_JSON"])
if data.get("edge_id") != edge_id or data.get("from_node_id") != parent or data.get("to_node_id") != child:
    raise SystemExit(f"{service}: unexpected edge payload {data}")
PY
}

validate_service_once() {
  local service="$1" raw session_id tx_id
  raw="$(user_cli "$service" session open --space-id "$SPACE_ID" --domain-id "$DOMAIN_ID")"
  session_id="$(printf '%s\n' "$raw" | json_get 'session_id')"
  raw="$(user_cli "$service" transaction begin "$session_id" --mode read-only)"
  tx_id="$(printf '%s\n' "$raw" | json_get 'transaction_id')"
  raw="$(user_cli "$service" graph node get "$PARENT_NODE_ID" --transaction-id "$tx_id")"
  validate_node_response "$service" "$raw" "$PARENT_NODE_ID"
  raw="$(user_cli "$service" graph node get "$CHILD_NODE_ID" --transaction-id "$tx_id")"
  validate_node_response "$service" "$raw" "$CHILD_NODE_ID"
  raw="$(user_cli "$service" graph edge get "$EDGE_ID" --transaction-id "$tx_id")"
  validate_edge_response "$service" "$raw"
  raw="$(user_cli "$service" query nodes --transaction-id "$tx_id" --label G5Parent --property-equals "marker=$MARKER" --limit 5)"
  validate_query_response "$service" "$raw"
  user_cli "$service" transaction close "$tx_id" >/dev/null
  user_cli "$service" session close "$session_id" >/dev/null
}

validate_consistency_report_once() {
  local service="$1" raw
  raw="$(admin_cli "$service" cluster consistency-report --space-id "$SPACE_ID" --domain-id "$DOMAIN_ID")"
  RAW_JSON="$raw" python3 - "$service" <<'PY'
import json, os, sys
service = sys.argv[1]
data = json.loads(os.environ["RAW_JSON"])
status = data.get("status")
if status != "consistent":
    raise SystemExit(f"{service}: expected consistency report status=consistent, got {status}: {data}")
if len(data.get("replicas") or []) < 1:
    raise SystemExit(f"{service}: consistency report has no replicas")
PY
}

validate_once() {
  local service
  for service in "${SERVICES[@]}"; do
    service="$(trim "$service")"
    [[ -n "$service" ]] || continue
    validate_service_once "$service"
  done
  validate_consistency_report_once "$(service_at 0)"
}

if [[ -s "$STATE_FILE" ]]; then
  load_state
elif [[ "$CREATE_IF_MISSING" == "true" ]]; then
  create_data "$(service_at 0)"
else
  echo "data-plane state file is missing or empty: $STATE_FILE" >&2
  exit 1
fi

deadline=$((SECONDS + TIMEOUT_SECONDS))
last_error=""
while (( SECONDS <= deadline )); do
  if output="$(validate_once 2>&1)"; then
    echo "Compose cluster data-plane validation passed"
    echo "state_file=$STATE_FILE"
    echo "space_id=$SPACE_ID domain_id=$DOMAIN_ID marker=$MARKER committed_revision=${COMMITTED_REVISION:-0}"
    exit 0
  fi
  last_error="$output"
  echo "Waiting for compose cluster data-plane validation: $last_error" >&2
  sleep "$SLEEP_SECONDS"
done

echo "Compose cluster data-plane validation failed after ${TIMEOUT_SECONDS}s" >&2
echo "$last_error" >&2
for service in "${SERVICES[@]}"; do
  service="$(trim "$service")"
  [[ -n "$service" ]] || continue
  echo "--- logs: $service ---" >&2
  compose logs --tail=80 "$service" >&2 || true
done
exit 1
