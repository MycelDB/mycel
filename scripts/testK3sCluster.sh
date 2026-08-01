#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ORCH_DIR="${MYCEL_K3S_ORCHESTRATION_DIR:-${ROOT_DIR}/../../orchestration/knot_pkm_k3s}"
CLUSTER="${MYCEL_K3S_CLUSTER:-knotbase-dev}"
NAMESPACE="${MYCEL_K3S_NAMESPACE:-knotbase-dev}"
EXPECTED_NODES="${MYCELD_CLUSTER_RAFT_NODE_COUNT:-3}"
IMAGE="${MYCEL_K3S_IMAGE:-myceldb/mycel:k3s-local-$(git -C "$ROOT_DIR" rev-parse --short HEAD)}"
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
    --from-literal=user-store-encryption-key-b64="$(openssl rand -base64 32)" \
    --from-literal=cluster-backend-auth-token="$(openssl rand -base64 32)" \
    --dry-run=client -o yaml | kubectl apply -f -
  kubectl -n "$NAMESPACE" apply \
    -f "$ORCH_DIR/base/apps/myceld/configmap.yaml" \
    -f "$ORCH_DIR/base/apps/myceld/service-headless.yaml" \
    -f "$ORCH_DIR/base/apps/myceld/service.yaml" \
    -f "$ORCH_DIR/base/apps/myceld/service-admin.yaml"
  IMAGE="$IMAGE" STATEFULSET_PATH="$ORCH_DIR/base/apps/myceld/statefulset.yaml" python3 <<'PY' | kubectl -n "$NAMESPACE" apply -f -
import os
from pathlib import Path
image = os.environ["IMAGE"]
path = Path(os.environ["STATEFULSET_PATH"])
text = path.read_text()
lines = []
replaced = False
for line in text.splitlines():
    if line.strip().startswith("image: ") and "mycel" in line:
        indent = line[: len(line) - len(line.lstrip())]
        lines.append(f"{indent}image: {image}")
        replaced = True
    else:
        lines.append(line)
if not replaced:
    raise SystemExit("did not find myceld image line to replace")
print("\n".join(lines))
PY
  kubectl -n "$NAMESPACE" rollout status statefulset/myceld --timeout=10m
}

validate_cluster() {
  MYCEL_K3S_NAMESPACE="$NAMESPACE" \
  MYCELD_CLUSTER_RAFT_NODE_COUNT="$EXPECTED_NODES" \
  MYCELD_BOOTSTRAP_ADMIN_USERNAME="$ADMIN_USERNAME" \
  MYCELD_BOOTSTRAP_ADMIN_PASSWORD="$ADMIN_PASSWORD" \
    "$ROOT_DIR/scripts/validateK3sClusterIdentity.sh"
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

rolling_restart() {
  kubectl -n "$NAMESPACE" rollout restart statefulset/myceld
  kubectl -n "$NAMESPACE" rollout status statefulset/myceld --timeout=10m
}

replace_last_pvc() {
  local last_ordinal=$((EXPECTED_NODES - 1))
  local reduced=$((EXPECTED_NODES - 1))
  local pod="myceld-${last_ordinal}"
  local pvc="myceld-data-${pod}"
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

echo "K3s cluster validation passed"
