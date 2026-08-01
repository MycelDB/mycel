#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${MYCEL_COMPOSE_FILE:-${ROOT_DIR}/../../knot_pkm/knot_pkm_server/compose.dev.yml}"
SERVICES_CSV="${MYCEL_COMPOSE_SERVICES:-myceld-a,myceld-b,myceld-c}"
USERNAME="${MYCEL_OPERATOR_USERNAME:-${MYCELD_BOOTSTRAP_ADMIN_USERNAME:-admin}}"
PASSWORD="${MYCEL_OPERATOR_PASSWORD:-${MYCELD_BOOTSTRAP_ADMIN_PASSWORD:-admin-password}}"
DAEMON_ADDR="${MYCELD_DAEMON_ADDR:-127.0.0.1:9091}"
EXPECTED_NODES="${MYCELD_CLUSTER_RAFT_NODE_COUNT:-3}"
TIMEOUT_SECONDS="${MYCEL_COMPOSE_VALIDATE_TIMEOUT:-180}"
SLEEP_SECONDS="${MYCEL_COMPOSE_VALIDATE_INTERVAL:-3}"
CLI_TIMEOUT_SECONDS="${MYCEL_COMPOSE_CLI_TIMEOUT:-20}"
VALIDATE_SOURCE="${MYCEL_COMPOSE_VALIDATE_SOURCE:-cli}"

if [[ ! -f "$COMPOSE_FILE" ]]; then
  echo "compose file not found: $COMPOSE_FILE" >&2
  exit 1
fi

IFS=',' read -r -a SERVICES <<< "$SERVICES_CSV"
if [[ ${#SERVICES[@]} -eq 0 ]]; then
  echo "no services configured" >&2
  exit 1
fi

compose() {
  docker compose -f "$COMPOSE_FILE" "$@"
}

with_timeout() {
  if command -v timeout >/dev/null 2>&1; then
    timeout "$CLI_TIMEOUT_SECONDS" "$@"
  elif command -v gtimeout >/dev/null 2>&1; then
    gtimeout "$CLI_TIMEOUT_SECONDS" "$@"
  else
    "$@"
  fi
}

status_json() {
  local service="$1"
  with_timeout docker compose -f "$COMPOSE_FILE" exec -T "$service" mycel --daemon-addr "$DAEMON_ADDR" --username "$USERNAME" --password "$PASSWORD" --output json cluster status
}

health_json() {
  local service="$1"
  with_timeout docker compose -f "$COMPOSE_FILE" exec -T "$service" mycel --daemon-addr "$DAEMON_ADDR" --username "$USERNAME" --password "$PASSWORD" --output json cluster health
}

cluster_files() {
  local service="$1"
  compose exec -T "$service" sh -c 'cat /data/mycel/meta/clustering/node.json; printf "\n---MYCEL---\n"; cat /data/mycel/meta/clustering/local_state.json; printf "\n---MYCEL---\n"; cat /data/mycel/meta/clustering/membership.json'
}

parse_status() {
  python3 -c 'import json,sys
service=sys.argv[1]
data=json.load(sys.stdin)
node=data.get("node") or {}
cluster=data.get("cluster") or {}
peers=data.get("peers") or []
print("\t".join([
  service,
  cluster.get("cluster_id", ""),
  cluster.get("cluster_name", ""),
  cluster.get("mode", ""),
  node.get("node_id", ""),
  node.get("state", ""),
  str(bool(node.get("admitted"))).lower(),
  str(len(peers)),
]))' "$1"
}

parse_health() {
  python3 -c 'import json,sys
service=sys.argv[1]
data=json.load(sys.stdin)
print("\t".join([
  service,
  data.get("status", ""),
  str(data.get("active_members", 0)),
  str(data.get("pending_members", 0)),
  str(data.get("unreachable_peers", 0)),
  "; ".join(data.get("warnings", []) or []),
]))' "$1"
}

parse_files() {
  python3 -c 'import json,sys
service=sys.argv[1]
parts=sys.stdin.read().split("\n---MYCEL---\n")
if len(parts) != 3:
    raise SystemExit(f"{service}: expected node/local_state/membership files")
node=json.loads(parts[0]); local=json.loads(parts[1]); membership=json.loads(parts[2])
members=membership.get("members") or []
active=sum(1 for m in members if m.get("state") == "active")
pending=sum(1 for m in members if m.get("state") == "pending")
unreachable=0
status="healthy" if node.get("cluster_admitted") and local.get("state") == "clustered" and active > 0 and pending == 0 else "unhealthy"
print("STATUS\t" + "\t".join([service, node.get("cluster_id", ""), node.get("cluster_name", ""), local.get("mode", ""), node.get("node_id", ""), local.get("state", ""), str(bool(node.get("cluster_admitted"))).lower(), str(len(members))]))
print("HEALTH\t" + "\t".join([service, status, str(active), str(pending), str(unreachable), ""]))' "$1"
}

validate_once() {
  local tmpdir status_file health_file service raw
  tmpdir="$(mktemp -d)"
  status_file="$tmpdir/status.tsv"
  health_file="$tmpdir/health.tsv"
  trap 'rm -rf "$tmpdir"' RETURN

  for service in "${SERVICES[@]}"; do
    service="$(echo "$service" | xargs)"
    [[ -n "$service" ]] || continue
    if [[ "$VALIDATE_SOURCE" == "files" ]]; then
      raw="$(cluster_files "$service" 2>/dev/null)" || return 1
      parsed="$(printf '%s\n' "$raw" | parse_files "$service")" || return 1
      printf '%s\n' "$parsed" | sed -n $'s/^STATUS\t//p' >> "$status_file"
      printf '%s\n' "$parsed" | sed -n $'s/^HEALTH\t//p' >> "$health_file"
    else
      raw="$(status_json "$service" 2>/dev/null)" || return 1
      printf '%s\n' "$raw" | parse_status "$service" >> "$status_file" || return 1
      raw="$(health_json "$service" 2>/dev/null)" || return 1
      printf '%s\n' "$raw" | parse_health "$service" >> "$health_file" || return 1
    fi
  done

  python3 - "$status_file" "$health_file" "$EXPECTED_NODES" <<'PY'
import sys
from pathlib import Path
status_path, health_path, expected_text = sys.argv[1:4]
expected = int(expected_text)
statuses = [line.rstrip("\n").split("\t") for line in Path(status_path).read_text().splitlines() if line.strip()]
health = [line.rstrip("\n").split("\t") for line in Path(health_path).read_text().splitlines() if line.strip()]
if len(statuses) != expected:
    raise SystemExit(f"got {len(statuses)} status rows, expected {expected}")
cluster_ids = {row[1] for row in statuses}
if len(cluster_ids) != 1 or not next(iter(cluster_ids)):
    raise SystemExit(f"cluster IDs are not one shared non-empty value: {sorted(cluster_ids)}")
for row in statuses:
    service, cluster_id, cluster_name, mode, node_id, state, admitted, peer_count = row
    if state != "clustered" or admitted != "true" or mode != "clustered":
        raise SystemExit(f"{service} is not clustered/admitted: state={state} admitted={admitted} mode={mode}")
    if int(peer_count) < expected:
        raise SystemExit(f"{service} peer count {peer_count} is below expected {expected}")
if len(health) != expected:
    raise SystemExit(f"got {len(health)} health rows, expected {expected}")
for row in health:
    service, health_status, active, pending, unreachable, warnings = row
    if health_status != "healthy":
        raise SystemExit(f"{service} health={health_status} active={active} pending={pending} unreachable={unreachable} warnings={warnings}")
    if int(active) < expected:
        raise SystemExit(f"{service} active member count {active} below expected {expected}")
    if int(pending) != 0 or int(unreachable) != 0:
        raise SystemExit(f"{service} has pending/unreachable members: pending={pending} unreachable={unreachable}")
print("cluster_id=" + next(iter(cluster_ids)))
for row in statuses:
    print("status\t" + "\t".join(row))
for row in health:
    print("health\t" + "\t".join(row))
PY
}

deadline=$((SECONDS + TIMEOUT_SECONDS))
last_error=""
while (( SECONDS <= deadline )); do
  if output="$(validate_once 2>&1)"; then
    echo "Compose cluster identity validation passed"
    echo "$output"
    exit 0
  fi
  last_error="$output"
  echo "Waiting for compose cluster identity/readiness: $last_error" >&2
  sleep "$SLEEP_SECONDS"
done

echo "Compose cluster identity validation failed after ${TIMEOUT_SECONDS}s" >&2
echo "$last_error" >&2
for service in "${SERVICES[@]}"; do
  echo "--- logs: $service ---" >&2
  compose logs --tail=80 "$service" >&2 || true
done
exit 1
