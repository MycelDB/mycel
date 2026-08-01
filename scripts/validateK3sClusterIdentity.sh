#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${MYCEL_K3S_NAMESPACE:-knotbase-dev}"
PODS_CSV="${MYCEL_K3S_PODS:-myceld-0,myceld-1,myceld-2}"
USERNAME="${MYCEL_OPERATOR_USERNAME:-${MYCELD_BOOTSTRAP_ADMIN_USERNAME:-admin}}"
PASSWORD="${MYCEL_OPERATOR_PASSWORD:-${MYCELD_BOOTSTRAP_ADMIN_PASSWORD:-admin-password}}"
DAEMON_ADDR="${MYCELD_DAEMON_ADDR:-127.0.0.1:9091}"
EXPECTED_NODES="${MYCELD_CLUSTER_RAFT_NODE_COUNT:-3}"
WAIT_TIMEOUT="${MYCEL_K3S_VALIDATE_WAIT_TIMEOUT:-5m}"

IFS=',' read -r -a PODS <<< "$PODS_CSV"
if [[ ${#PODS[@]} -eq 0 ]]; then
  echo "no pods configured" >&2
  exit 1
fi

kubectl -n "$NAMESPACE" wait --for=condition=Ready pod -l app.kubernetes.io/name=myceld --timeout="$WAIT_TIMEOUT"

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

status_file="$tmpdir/status.tsv"
health_file="$tmpdir/health.tsv"

parse_status() {
  python3 -c 'import json,sys
pod=sys.argv[1]
data=json.load(sys.stdin)
node=data.get("node") or {}
cluster=data.get("cluster") or {}
peers=data.get("peers") or []
print("\t".join([
  pod,
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
pod=sys.argv[1]
data=json.load(sys.stdin)
print("\t".join([
  pod,
  data.get("status", ""),
  str(data.get("active_members", 0)),
  str(data.get("pending_members", 0)),
  str(data.get("unreachable_peers", 0)),
  "; ".join(data.get("warnings", []) or []),
]))' "$1"
}

for pod in "${PODS[@]}"; do
  pod="$(echo "$pod" | xargs)"
  [[ -n "$pod" ]] || continue
  raw="$(kubectl -n "$NAMESPACE" exec "$pod" -- mycel --daemon-addr "$DAEMON_ADDR" --username "$USERNAME" --password "$PASSWORD" --output json cluster status)"
  printf '%s\n' "$raw" | parse_status "$pod" >> "$status_file"
  raw="$(kubectl -n "$NAMESPACE" exec "$pod" -- mycel --daemon-addr "$DAEMON_ADDR" --username "$USERNAME" --password "$PASSWORD" --output json cluster health)"
  printf '%s\n' "$raw" | parse_health "$pod" >> "$health_file"
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
    pod, cluster_id, cluster_name, mode, node_id, state, admitted, peer_count = row
    if state != "clustered" or admitted != "true" or mode != "clustered":
        raise SystemExit(f"{pod} is not clustered/admitted: state={state} admitted={admitted} mode={mode}")
    if int(peer_count) < expected:
        raise SystemExit(f"{pod} peer count {peer_count} is below expected {expected}")
if len(health) != expected:
    raise SystemExit(f"got {len(health)} health rows, expected {expected}")
for row in health:
    pod, health_status, active, pending, unreachable, warnings = row
    if health_status != "healthy":
        raise SystemExit(f"{pod} health={health_status} active={active} pending={pending} unreachable={unreachable} warnings={warnings}")
    if int(active) < expected:
        raise SystemExit(f"{pod} active member count {active} below expected {expected}")
    if int(pending) != 0 or int(unreachable) != 0:
        raise SystemExit(f"{pod} has pending/unreachable members: pending={pending} unreachable={unreachable}")
print("cluster_id=" + next(iter(cluster_ids)))
for row in statuses:
    print("status\t" + "\t".join(row))
for row in health:
    print("health\t" + "\t".join(row))
PY
