#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ORCH_DIR="${MYCEL_K3S_ORCHESTRATION_DIR:-${ROOT_DIR}/../../orchestration/knot_pkm_k3s}"
CLUSTER="${MYCEL_K3S_CLUSTER:-knotbase-dev}"
NAMESPACE="${MYCEL_K3S_NAMESPACE:-knotbase-dev}"
EXPECTED_NODES="${MYCELD_CLUSTER_RAFT_NODE_COUNT:-3}"
CLUSTER_VALIDATE_TIMEOUT_SECONDS="${MYCEL_K3S_CLUSTER_VALIDATE_TIMEOUT:-600}"
IMAGE="${MYCEL_K3S_IMAGE:-myceldb/mycel:k3s-local-$(git -C "$ROOT_DIR" rev-parse --short HEAD)}"
IMAGE_PULL_POLICY="${MYCEL_K3S_IMAGE_PULL_POLICY:-IfNotPresent}"
RESET="${MYCEL_K3S_RESET:-true}"
BUILD_IMAGE="${MYCEL_K3S_BUILD_IMAGE:-true}"
IMPORT_IMAGE="${MYCEL_K3S_IMPORT_IMAGE:-auto}"
ADMIN_USERNAME="${MYCELD_BOOTSTRAP_ADMIN_USERNAME:-admin}"
ADMIN_PASSWORD="${MYCELD_BOOTSTRAP_ADMIN_PASSWORD:-admin-password}"
DATA_PLANE_STATE="$(mktemp)"
trap 'rm -f "$DATA_PLANE_STATE"' EXIT

if [[ ! -d "$ORCH_DIR" ]]; then
  echo "orchestration directory not found: $ORCH_DIR" >&2
  exit 1
fi
if [[ ! -f "$ORCH_DIR/base/apps/myceld/statefulset.yaml" ]]; then
  echo "myceld StatefulSet manifest not found under: $ORCH_DIR" >&2
  exit 1
fi

create_k3d_cluster_if_needed() {
  if ! command -v k3d >/dev/null 2>&1; then
    return 0
  fi
  if ! k3d cluster list | awk 'NR > 1 {print $1}' | grep -qx "$CLUSTER"; then
    k3d cluster create "$CLUSTER" --agents 3 \
      -p '30080:30080@loadbalancer' \
      -p '30081:30081@loadbalancer' \
      -p '9091:9091@loadbalancer'
  fi
  if kubectl config get-contexts "k3d-$CLUSTER" >/dev/null 2>&1; then
    kubectl config use-context "k3d-$CLUSTER" >/dev/null
  fi
}

build_image() {
  if [[ "$BUILD_IMAGE" != "true" ]]; then
    return 0
  fi
  docker build -f "$ROOT_DIR/Dockerfile" -t "$IMAGE" "$ROOT_DIR/.."
}

import_image() {
  case "$IMPORT_IMAGE" in
    false) return 0 ;;
    true) ;;
    auto)
      command -v k3d >/dev/null 2>&1 || return 0
      k3d cluster list | awk 'NR > 1 {print $1}' | grep -qx "$CLUSTER" || return 0
      ;;
    *) echo "MYCEL_K3S_IMPORT_IMAGE must be auto, true, or false" >&2; exit 1 ;;
  esac
  k3d image import "$IMAGE" -c "$CLUSTER"
}

apply_myceld_manifests() {
  if [[ "$RESET" == "true" ]]; then
    kubectl delete namespace "$NAMESPACE" --wait=true --timeout=3m >/dev/null 2>&1 || true
  fi
  kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
  kubectl -n "$NAMESPACE" create secret docker-registry dockerhub-myceldb \
    --docker-server=docker.io \
    --docker-username=dummy \
    --docker-password=dummy \
    --dry-run=client -o yaml | kubectl apply -f -
  kubectl -n "$NAMESPACE" create secret generic myceld-secret \
    --from-literal=bootstrap-admin-username="$ADMIN_USERNAME" \
    --from-literal=bootstrap-admin-password="$ADMIN_PASSWORD" \
    --from-literal=cluster-backend-auth-token="$(openssl rand -base64 32)" \
    --dry-run=client -o yaml | kubectl apply -f -
  kubectl -n "$NAMESPACE" apply \
    -f "$ORCH_DIR/base/apps/myceld/configmap.yaml" \
    -f "$ORCH_DIR/base/apps/myceld/service-headless.yaml" \
    -f "$ORCH_DIR/base/apps/myceld/service.yaml" \
    -f "$ORCH_DIR/base/apps/myceld/service-admin.yaml"
  kubectl -n "$NAMESPACE" patch configmap myceld-config --type merge \
    -p '{"data":{"MYCELD_CLUSTER_RAFT_EMPTY_STORAGE_REJOIN_RECOVERY":"true"}}'
  IMAGE="$IMAGE" IMAGE_PULL_POLICY="$IMAGE_PULL_POLICY" STATEFULSET_PATH="$ORCH_DIR/base/apps/myceld/statefulset.yaml" python3 <<'PY' | kubectl -n "$NAMESPACE" apply -f -
import os
from pathlib import Path
image = os.environ["IMAGE"]
image_pull_policy = os.environ["IMAGE_PULL_POLICY"]
path = Path(os.environ["STATEFULSET_PATH"])
text = path.read_text()
source_lines = text.splitlines()
lines = []
replaced_image = False
replaced_pull_policy = False
idx = 0
while idx < len(source_lines):
    line = source_lines[idx]
    stripped = line.strip()
    if stripped == "- name: MYCELD_USER_STORE_ENCRYPTION_KEY_B64":
        skip_indent = len(line) - len(line.lstrip())
        idx += 1
        while idx < len(source_lines):
            next_line = source_lines[idx]
            next_stripped = next_line.strip()
            next_indent = len(next_line) - len(next_line.lstrip())
            if next_stripped and next_indent <= skip_indent:
                break
            idx += 1
        continue
    if stripped.startswith("image: ") and "mycel" in line:
        indent = line[: len(line) - len(line.lstrip())]
        lines.append(f"{indent}image: {image}")
        replaced_image = True
    elif stripped.startswith("imagePullPolicy: "):
        indent = line[: len(line) - len(line.lstrip())]
        lines.append(f"{indent}imagePullPolicy: {image_pull_policy}")
        replaced_pull_policy = True
    else:
        lines.append(line)
    idx += 1
if not replaced_image:
    raise SystemExit("did not find myceld image line to replace")
if not replaced_pull_policy:
    raise SystemExit("did not find myceld imagePullPolicy line to replace")
print("\n".join(lines))
PY
  kubectl -n "$NAMESPACE" rollout status statefulset/myceld --timeout=10m
}

validate_cluster() {
  local deadline output
  deadline=$((SECONDS + CLUSTER_VALIDATE_TIMEOUT_SECONDS))
  while (( SECONDS <= deadline )); do
    if output="$(MYCEL_K3S_NAMESPACE="$NAMESPACE" \
      MYCELD_CLUSTER_RAFT_NODE_COUNT="$EXPECTED_NODES" \
      MYCELD_BOOTSTRAP_ADMIN_USERNAME="$ADMIN_USERNAME" \
      MYCELD_BOOTSTRAP_ADMIN_PASSWORD="$ADMIN_PASSWORD" \
        "$ROOT_DIR/scripts/validateK3sClusterIdentity.sh" 2>&1)"; then
      printf '%s\n' "$output"
      return 0
    fi
    printf 'Waiting for K3s cluster identity/health validation: %s\n' "$output" >&2
    sleep 5
  done
  printf '%s\n' "$output" >&2
  return 1
}

validate_data_plane() {
  local create_if_missing="${1:-true}"
  MYCEL_K3S_NAMESPACE="$NAMESPACE" \
  MYCEL_K3S_DATA_PLANE_STATE="$DATA_PLANE_STATE" \
  MYCEL_DATA_PLANE_CREATE_IF_MISSING="$create_if_missing" \
  MYCELD_BOOTSTRAP_ADMIN_USERNAME="$ADMIN_USERNAME" \
  MYCELD_BOOTSTRAP_ADMIN_PASSWORD="$ADMIN_PASSWORD" \
    "$ROOT_DIR/scripts/validateK3sClusterDataPlane.sh"
}

force_raft_snapshots_on_active_quorum() {
  local reduced=$((EXPECTED_NODES - 1))
  local ordinal pod tmp err
  for ((ordinal = 0; ordinal < reduced; ordinal++)); do
    pod="myceld-${ordinal}"
    tmp="$(mktemp)"
    err="$(mktemp)"
    echo "Forcing raft snapshots on ${pod}"
    if ! kubectl --request-timeout=2m -n "$NAMESPACE" exec "$pod" -- /bin/sh -c 'timeout 120 "$@"' -- \
      mycel --daemon-addr 127.0.0.1:9091 \
      --username "$ADMIN_USERNAME" --password "$ADMIN_PASSWORD" --output json \
      cluster raft-snapshot create >"$tmp" 2>"$err"; then
      cat "$err" >&2 || true
      cat "$tmp" >&2 || true
      rm -f "$tmp" "$err"
      return 1
    fi
    python3 - "$pod" "$tmp" <<'PY'
import json, sys
pod = sys.argv[1]
path = sys.argv[2]
try:
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
except Exception as exc:
    raw = ""
    try:
        with open(path, "r", encoding="utf-8") as fh:
            raw = fh.read()
    except Exception:
        pass
    raise SystemExit(f"{pod}: raft snapshot command did not return JSON: {exc}; output={raw!r}")
results = data.get("results") or []
if not results:
    raise SystemExit(f"{pod}: raft snapshot command returned no results")
errors = [f"{r.get('group_id')}: {r.get('error')}" for r in results if r.get("error")]
if errors:
    raise SystemExit(f"{pod}: raft snapshot errors: {'; '.join(errors)}")
missing = [r.get("group_id") for r in results if int(r.get("snapshot_index") or 0) <= 0]
if missing:
    raise SystemExit(f"{pod}: raft snapshot returned zero index for groups: {missing}")
print(f"{pod}: forced raft snapshots for {len(results)} groups")
PY
    rm -f "$tmp" "$err"
  done
}

wait_for_rejoined_snapshots() {
  local pod="$1" deadline tmp output
  deadline=$((SECONDS + 300))
  while (( SECONDS <= deadline )); do
    tmp="$(mktemp)"
    if kubectl --request-timeout=2m -n "$NAMESPACE" exec "$pod" -- /bin/sh -c 'timeout 120 "$@"' -- \
      mycel --daemon-addr 127.0.0.1:9091 \
      --username "$ADMIN_USERNAME" --password "$ADMIN_PASSWORD" --output json \
      cluster raft-groups >"$tmp"; then
      if output="$(python3 - "$pod" "$tmp" <<'PY'
import json, sys
pod = sys.argv[1]
path = sys.argv[2]
with open(path, "r", encoding="utf-8") as fh:
    data = json.load(fh)
groups = data.get("groups") or []
if not groups:
    raise SystemExit(f"{pod}: no raft groups reported")
missing = [g.get("group_id") for g in groups if int(g.get("snapshot_index") or 0) <= 0]
if missing:
    raise SystemExit(f"{pod}: waiting for nonzero snapshot_index on groups: {missing}")
print(f"{pod}: nonzero snapshot_index on {len(groups)} raft groups")
PY
)"; then
        rm -f "$tmp"
        printf '%s\n' "$output"
        return 0
      fi
    else
      output="raft group query failed"
    fi
    rm -f "$tmp"
    printf 'Waiting for rejoined raft snapshots: %s\n' "$output" >&2
    sleep 5
  done
  printf '%s\n' "$output" >&2
  return 1
}

rolling_restart() {
  kubectl -n "$NAMESPACE" rollout restart statefulset/myceld
  kubectl -n "$NAMESPACE" rollout status statefulset/myceld --timeout=10m
}

replace_last_pvc() {
  local last_ordinal=$((EXPECTED_NODES - 1))
  local reduced=$((EXPECTED_NODES - 1))
  local pod="myceld-${last_ordinal}"
  local pvc="myceld-data-${pod}"
  force_raft_snapshots_on_active_quorum
  kubectl -n "$NAMESPACE" scale statefulset/myceld --replicas="$reduced"
  kubectl -n "$NAMESPACE" wait --for=delete "pod/${pod}" --timeout=3m
  kubectl -n "$NAMESPACE" delete pvc "$pvc" --wait=true --timeout=3m
  kubectl -n "$NAMESPACE" scale statefulset/myceld --replicas="$EXPECTED_NODES"
  kubectl -n "$NAMESPACE" rollout status statefulset/myceld --timeout=10m
}

create_k3d_cluster_if_needed
build_image
import_image
apply_myceld_manifests

echo "== fresh bootstrap validation =="
validate_cluster
validate_data_plane true

echo "== rolling restart validation =="
rolling_restart
validate_cluster
validate_data_plane false

echo "== single PVC replacement/rejoin validation =="
replace_last_pvc
validate_cluster
validate_data_plane false
wait_for_rejoined_snapshots "myceld-$((EXPECTED_NODES - 1))"

echo "K3s cluster validation passed"
