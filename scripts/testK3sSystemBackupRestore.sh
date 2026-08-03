#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ORCH_DIR="${MYCEL_K3S_ORCHESTRATION_DIR:-${ROOT_DIR}/../../orchestration/knot_pkm_k3s}"
CLUSTER="${MYCEL_K3S_CLUSTER:-knotbase-dev}"
NAMESPACE="${MYCEL_K3S_NAMESPACE:-knotbase-dev}"
EXPECTED_NODES="${MYCELD_CLUSTER_RAFT_NODE_COUNT:-3}"
IMAGE="${MYCEL_K3S_IMAGE:-myceldb/mycel:k3s-system-backup-restore-$(git -C "$ROOT_DIR" rev-parse --short HEAD)}"
BUILD_IMAGE="${MYCEL_K3S_BUILD_IMAGE:-true}"
IMPORT_IMAGE="${MYCEL_K3S_IMPORT_IMAGE:-auto}"
ADMIN_USERNAME="${MYCELD_BOOTSTRAP_ADMIN_USERNAME:-admin}"
ADMIN_PASSWORD="${MYCELD_BOOTSTRAP_ADMIN_PASSWORD:-admin-password}"
DAEMON_ADDR="${MYCELD_DAEMON_ADDR:-127.0.0.1:9091}"
PODS_CSV="${MYCEL_K3S_PODS:-myceld-0,myceld-1,myceld-2}"
BACKUP_DIR="${MYCEL_K3S_SYSTEM_BACKUP_DIR:-/tmp/mycel-system-backups}"
WAIT_TIMEOUT="${MYCEL_K3S_VALIDATE_WAIT_TIMEOUT:-5m}"
CLI_TIMEOUT_SECONDS="${MYCEL_K3S_CLI_TIMEOUT:-30}"
VALIDATION_TIMEOUT_SECONDS="${MYCEL_K3S_SYSTEM_BACKUP_RESTORE_TIMEOUT:-240}"
VALIDATION_INTERVAL_SECONDS="${MYCEL_K3S_SYSTEM_BACKUP_RESTORE_INTERVAL:-4}"
USER_STORE_KEY_B64="${MYCELD_USER_STORE_ENCRYPTION_KEY_B64:-$(openssl rand -base64 32)}"
BACKEND_AUTH_TOKEN="${MYCELD_CLUSTER_BACKEND_AUTH_TOKEN:-$(openssl rand -base64 32)}"
TMP_DIR="$(mktemp -d)"
EXPECTED_FILE="$TMP_DIR/expected.env"

IFS=',' read -r -a PODS <<< "$PODS_CSV"

cleanup() {
  local status=$?
  if [[ $status -ne 0 ]]; then
    echo "Preserving K3s system backup/restore temp dir after failure: $TMP_DIR" >&2
    for pod in "${PODS[@]}"; do
      pod="$(trim "$pod")"
      [[ -n "$pod" ]] || continue
      echo "--- logs: $pod ---" >&2
      kubectl -n "$NAMESPACE" logs --tail=120 "$pod" >&2 || true
    done
  else
    rm -rf "$TMP_DIR"
  fi
}
trap cleanup EXIT

if [[ ! -d "$ORCH_DIR" ]]; then
  echo "orchestration directory not found: $ORCH_DIR" >&2
  exit 1
fi
if [[ ! -f "$ORCH_DIR/base/apps/myceld/statefulset.yaml" ]]; then
  echo "myceld StatefulSet manifest not found under: $ORCH_DIR" >&2
  exit 1
fi
if [[ ${#PODS[@]} -ne "$EXPECTED_NODES" ]]; then
  echo "expected $EXPECTED_NODES pods but MYCEL_K3S_PODS has ${#PODS[@]} entries" >&2
  exit 1
fi

trim() { echo "$1" | xargs; }

with_timeout() {
  if command -v timeout >/dev/null 2>&1; then
    timeout "$CLI_TIMEOUT_SECONDS" "$@"
  elif command -v gtimeout >/dev/null 2>&1; then
    gtimeout "$CLI_TIMEOUT_SECONDS" "$@"
  else
    "$@"
  fi
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

verify_sha256() {
  local path="$1" want="$2"
  python3 - "$path" "$want" <<'PY'
import hashlib, sys
path, want = sys.argv[1:3]
h = hashlib.sha256()
with open(path, 'rb') as f:
    for chunk in iter(lambda: f.read(1024 * 1024), b''):
        h.update(chunk)
got = h.hexdigest()
if got != want:
    raise SystemExit(f"sha256 mismatch for {path}: got {got} want {want}")
PY
}

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

reset_namespace() {
  kubectl delete namespace "$NAMESPACE" --wait=true --timeout=5m >/dev/null 2>&1 || true
  kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
}

apply_shared_resources() {
  kubectl -n "$NAMESPACE" create secret docker-registry dockerhub-myceldb \
    --docker-server=docker.io \
    --docker-username=dummy \
    --docker-password=dummy \
    --dry-run=client -o yaml | kubectl apply -f -
  kubectl -n "$NAMESPACE" create secret generic myceld-secret \
    --from-literal=bootstrap-admin-username="$ADMIN_USERNAME" \
    --from-literal=bootstrap-admin-password="$ADMIN_PASSWORD" \
    --from-literal=user-store-encryption-key-b64="$USER_STORE_KEY_B64" \
    --from-literal=cluster-backend-auth-token="$BACKEND_AUTH_TOKEN" \
    --dry-run=client -o yaml | kubectl apply -f -
  kubectl -n "$NAMESPACE" apply \
    -f "$ORCH_DIR/base/apps/myceld/configmap.yaml" \
    -f "$ORCH_DIR/base/apps/myceld/service-headless.yaml" \
    -f "$ORCH_DIR/base/apps/myceld/service.yaml" \
    -f "$ORCH_DIR/base/apps/myceld/service-admin.yaml"
}

apply_statefulset() {
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

apply_fresh_cluster() {
  reset_namespace
  apply_shared_resources
  apply_statefulset
}

wait_ready() {
  kubectl -n "$NAMESPACE" wait --for=condition=Ready pod -l app.kubernetes.io/name=myceld --timeout="$WAIT_TIMEOUT"
}

validate_cluster_identity() {
  MYCEL_K3S_NAMESPACE="$NAMESPACE" \
  MYCELD_CLUSTER_RAFT_NODE_COUNT="$EXPECTED_NODES" \
  MYCELD_BOOTSTRAP_ADMIN_USERNAME="$ADMIN_USERNAME" \
  MYCELD_BOOTSTRAP_ADMIN_PASSWORD="$ADMIN_PASSWORD" \
    "$ROOT_DIR/scripts/validateK3sClusterIdentity.sh"
}

cli() {
  local pod="$1"; shift
  with_timeout kubectl -n "$NAMESPACE" exec "$pod" -- mycel --daemon-addr "$DAEMON_ADDR" "$@"
}

admin_cli() {
  local pod="$1"; shift
  cli "$pod" --username "$ADMIN_USERNAME" --password "$ADMIN_PASSWORD" --output json "$@"
}

user_cli() {
  local pod="$1"; shift
  cli "$pod" --username "$TEST_USERNAME" --password "$TEST_PASSWORD" --output json "$@"
}

save_expected() {
  cat > "$EXPECTED_FILE" <<EOF_STATE
TEST_USERNAME=$(shell_quote "$TEST_USERNAME")
TEST_PASSWORD=$(shell_quote "$TEST_PASSWORD")
SPACE_ID=$(shell_quote "$SPACE_ID")
DOMAIN_ID=$(shell_quote "$DOMAIN_ID")
NORMAL_NODE_ID=$(shell_quote "$NORMAL_NODE_ID")
BLOB_NODE_ID=$(shell_quote "$BLOB_NODE_ID")
EDGE_ID=$(shell_quote "$EDGE_ID")
BLOB_ID=$(shell_quote "$BLOB_ID")
MARKER=$(shell_quote "$MARKER")
BLOB_PAYLOAD=$(shell_quote "$BLOB_PAYLOAD")
EOF_STATE
}

load_expected() {
  # shellcheck disable=SC1090
  source "$EXPECTED_FILE"
}

create_system_fixture() {
  local writer="$(trim "${PODS[0]}")" suffix raw
  suffix="$(date +%s)-$RANDOM"
  TEST_USERNAME="${MYCEL_K3S_SYSTEM_BACKUP_USER:-system-backup-$suffix}"
  TEST_PASSWORD="${MYCEL_K3S_SYSTEM_BACKUP_PASSWORD:-system-backup-pass-$suffix}"
  MARKER="system-backup-$suffix"
  NORMAL_NODE_ID="$(uuid)"
  BLOB_NODE_ID="$(uuid)"
  EDGE_ID="$(uuid)"
  BLOB_PAYLOAD="blob payload for $MARKER"

  echo "Creating system backup fixture user/space through $writer" >&2
  admin_cli "$writer" user add --user-username "$TEST_USERNAME" --new-password "$TEST_PASSWORD" >/dev/null
  raw="$(admin_cli "$writer" space add "System Backup $suffix" --owner-username "$TEST_USERNAME" --default-domain-key system-backup --default-domain-name "System Backup")"
  SPACE_ID="$(printf '%s\n' "$raw" | json_get 'space.space_id')"
  DOMAIN_ID="$(printf '%s\n' "$raw" | json_get 'default_domain_id')"

  local candidate wrote=false
  for candidate in "${PODS[@]}"; do
    candidate="$(trim "$candidate")"
    [[ -n "$candidate" ]] || continue
    if try_create_graph_blob_data "$candidate"; then
      wrote=true
      echo "System backup graph/blob fixture committed through $candidate" >&2
      break
    fi
    echo "Graph/blob write through $candidate failed; trying next pod" >&2
  done
  if [[ "$wrote" != "true" ]]; then
    echo "failed to write system backup graph/blob fixture through any configured pod" >&2
    return 1
  fi
  save_expected
}

try_create_graph_blob_data() {
  local writer="$1" raw session_id tx_id remote_blob_file
  remote_blob_file="/tmp/${MARKER}.blob"
  printf '%s' "$BLOB_PAYLOAD" | kubectl -n "$NAMESPACE" exec -i "$writer" -- sh -c "cat > '$remote_blob_file'"
  if ! raw="$(user_cli "$writer" session open --space-id "$SPACE_ID" --domain-id "$DOMAIN_ID")"; then
    return 1
  fi
  session_id="$(printf '%s\n' "$raw" | json_get 'session_id')"
  if ! raw="$(user_cli "$writer" transaction begin "$session_id" --mode read-write)"; then
    user_cli "$writer" session close "$session_id" >/dev/null || true
    return 1
  fi
  tx_id="$(printf '%s\n' "$raw" | json_get 'transaction_id')"
  if ! user_cli "$writer" graph node create --transaction-id "$tx_id" --node-id "$NORMAL_NODE_ID" --label SystemBackupNode --properties-json "{\"marker\":\"$MARKER\",\"kind\":\"normal\"}" --payload-json "{\"text\":\"normal node for $MARKER\"}" >/dev/null; then
    user_cli "$writer" transaction rollback "$tx_id" >/dev/null || true
    user_cli "$writer" session close "$session_id" >/dev/null || true
    return 1
  fi
  if ! raw="$(user_cli "$writer" graph blob-node create "$remote_blob_file" --transaction-id "$tx_id" --node-id "$BLOB_NODE_ID" --label SystemBackupBlob --mime-type text/plain --filename "${MARKER}.txt" --properties-json "{\"marker\":\"$MARKER\",\"kind\":\"blob\"}" --payload-json "{\"text\":\"blob node for $MARKER\"}")"; then
    user_cli "$writer" transaction rollback "$tx_id" >/dev/null || true
    user_cli "$writer" session close "$session_id" >/dev/null || true
    return 1
  fi
  BLOB_ID="$(printf '%s\n' "$raw" | json_get 'blob.blob_id')"
  if ! user_cli "$writer" graph edge create --transaction-id "$tx_id" --edge-id "$EDGE_ID" --from "$NORMAL_NODE_ID" --to "$BLOB_NODE_ID" --kind system_backup_blob --props-json "{\"marker\":\"$MARKER\"}" >/dev/null; then
    user_cli "$writer" transaction rollback "$tx_id" >/dev/null || true
    user_cli "$writer" session close "$session_id" >/dev/null || true
    return 1
  fi
  if ! user_cli "$writer" transaction commit "$tx_id" >/dev/null; then
    user_cli "$writer" transaction rollback "$tx_id" >/dev/null || true
    user_cli "$writer" session close "$session_id" >/dev/null || true
    return 1
  fi
  user_cli "$writer" session close "$session_id" >/dev/null || true
  return 0
}

validate_fixture_once() {
  local pod="$1" raw session_id tx_id remote_download downloaded
  raw="$(user_cli "$pod" session open --space-id "$SPACE_ID" --domain-id "$DOMAIN_ID")"
  session_id="$(printf '%s\n' "$raw" | json_get 'session_id')"
  raw="$(user_cli "$pod" transaction begin "$session_id" --mode read-only)"
  tx_id="$(printf '%s\n' "$raw" | json_get 'transaction_id')"
  raw="$(user_cli "$pod" graph node get "$NORMAL_NODE_ID" --transaction-id "$tx_id")"
  RAW_JSON="$raw" python3 - "$pod" "$NORMAL_NODE_ID" "$MARKER" <<'PY'
import json, os, sys
pod, want_id, marker = sys.argv[1:4]
data = json.loads(os.environ["RAW_JSON"])
if data.get("node_id") != want_id:
    raise SystemExit(f"{pod}: normal node mismatch: {data}")
if (data.get("properties") or {}).get("marker") != marker:
    raise SystemExit(f"{pod}: normal node marker mismatch: {data}")
PY
  raw="$(user_cli "$pod" graph node get "$BLOB_NODE_ID" --transaction-id "$tx_id")"
  RAW_JSON="$raw" python3 - "$pod" "$BLOB_NODE_ID" "$MARKER" <<'PY'
import json, os, sys
pod, want_id, marker = sys.argv[1:4]
data = json.loads(os.environ["RAW_JSON"])
if data.get("node_id") != want_id:
    raise SystemExit(f"{pod}: blob node mismatch: {data}")
if (data.get("properties") or {}).get("marker") != marker:
    raise SystemExit(f"{pod}: blob node marker mismatch: {data}")
PY
  raw="$(user_cli "$pod" graph edge get "$EDGE_ID" --transaction-id "$tx_id")"
  RAW_JSON="$raw" python3 - "$pod" "$EDGE_ID" "$NORMAL_NODE_ID" "$BLOB_NODE_ID" <<'PY'
import json, os, sys
pod, edge_id, from_id, to_id = sys.argv[1:5]
data = json.loads(os.environ["RAW_JSON"])
if data.get("edge_id") != edge_id or data.get("from_node_id") != from_id or data.get("to_node_id") != to_id:
    raise SystemExit(f"{pod}: edge mismatch: {data}")
PY
  user_cli "$pod" transaction close "$tx_id" >/dev/null
  user_cli "$pod" session close "$session_id" >/dev/null

  remote_download="/tmp/${MARKER}-${BLOB_ID}.download"
  user_cli "$pod" blob download "$BLOB_ID" --space-id "$SPACE_ID" --output-file "$remote_download" >/dev/null
  downloaded="$(kubectl -n "$NAMESPACE" exec "$pod" -- sh -c "cat '$remote_download'")"
  if [[ "$downloaded" != "$BLOB_PAYLOAD" ]]; then
    echo "$pod: blob payload mismatch for $BLOB_ID" >&2
    return 1
  fi
}

validate_fixture() {
  load_expected
  local deadline=$((SECONDS + VALIDATION_TIMEOUT_SECONDS)) last_error="" pod
  while (( SECONDS <= deadline )); do
    if output="$({ for pod in "${PODS[@]}"; do pod="$(trim "$pod")"; [[ -n "$pod" ]] || continue; validate_fixture_once "$pod"; done; } 2>&1)"; then
      echo "K3s system backup/restore fixture validation passed"
      return 0
    fi
    last_error="$output"
    echo "Waiting for K3s system backup/restore fixture validation: $last_error" >&2
    sleep "$VALIDATION_INTERVAL_SECONDS"
  done
  echo "K3s system backup/restore fixture validation failed after ${VALIDATION_TIMEOUT_SECONDS}s" >&2
  echo "$last_error" >&2
  return 1
}

capture_backups() {
  local pod raw archive checksum dest manifest
  for pod in "${PODS[@]}"; do
    pod="$(trim "$pod")"
    [[ -n "$pod" ]] || continue
    echo "Capturing system backup from $pod" >&2
    kubectl -n "$NAMESPACE" exec "$pod" -- sh -c "rm -rf '$BACKUP_DIR' && mkdir -p '$BACKUP_DIR'"
    admin_cli "$pod" admin backup policy set --disabled --dir "$BACKUP_DIR" --archive-format tar --keep 5 --interval-hours 24 --schedule interval --quiesce-timeout 30s --backup-timeout 5m --retry-after 1m --history-limit 10 >/dev/null
    raw="$(admin_cli "$pod" admin backup trigger --reason "k3s system backup restore test")"
    archive="$(printf '%s\n' "$raw" | json_get 'backup.archive_name')"
    checksum="$(printf '%s\n' "$raw" | json_get 'backup.checksum_sha256')"
    mkdir -p "$TMP_DIR/backups/$pod"
    dest="$TMP_DIR/backups/$pod/$archive"
    manifest="$TMP_DIR/backups/$pod/${archive%.tar}.manifest.json"
    kubectl -n "$NAMESPACE" cp "$pod:$BACKUP_DIR/$archive" "$dest"
    kubectl -n "$NAMESPACE" cp "$pod:$BACKUP_DIR/${archive%.tar}.manifest.json" "$manifest"
    verify_sha256 "$dest" "$checksum"
    printf '%s\t%s\n' "$pod" "$dest" >> "$TMP_DIR/backups.tsv"
  done
}

create_restore_pvcs() {
  local ordinal pod pvc
  for ((ordinal=0; ordinal<EXPECTED_NODES; ordinal++)); do
    pod="myceld-${ordinal}"
    pvc="myceld-data-${pod}"
    cat <<EOF_PVC | kubectl -n "$NAMESPACE" apply -f -
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ${pvc}
  labels:
    app.kubernetes.io/name: myceld
    app.kubernetes.io/component: storage
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 20Gi
EOF_PVC
  done
}

restore_archives_to_pvcs() {
  local pod archive restore_pod
  while IFS=$'\t' read -r pod archive; do
    [[ -n "$pod" && -n "$archive" ]] || continue
    restore_pod="restore-${pod}"
    echo "Restoring $archive into PVC for $pod" >&2
    cat <<EOF_POD | kubectl -n "$NAMESPACE" apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: ${restore_pod}
spec:
  restartPolicy: Never
  containers:
    - name: restore
      image: alpine:3.21
      command: ["/bin/sh", "-ec", "sleep 3600"]
      volumeMounts:
        - name: data
          mountPath: /data/mycel
        - name: restore
          mountPath: /restore
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: myceld-data-${pod}
    - name: restore
      emptyDir: {}
EOF_POD
    kubectl -n "$NAMESPACE" wait --for=condition=Ready "pod/${restore_pod}" --timeout=5m
    kubectl -n "$NAMESPACE" cp "$archive" "${restore_pod}:/restore/backup.tar"
    kubectl -n "$NAMESPACE" exec "$restore_pod" -- sh -ec "find /data/mycel -mindepth 1 -maxdepth 1 -exec rm -rf {} + && tar -xf /restore/backup.tar -C /data/mycel && find /data/mycel -maxdepth 2 -type f | sort | head"
    kubectl -n "$NAMESPACE" delete pod "$restore_pod" --wait=true --timeout=3m
  done < "$TMP_DIR/backups.tsv"
}

create_k3d_cluster_if_needed
build_image
import_image

apply_fresh_cluster
wait_ready

echo "== fresh cluster identity validation =="
validate_cluster_identity

echo "== create graph/blob fixture =="
create_system_fixture
validate_fixture

echo "== capture per-pod system backups =="
capture_backups

echo "== wipe namespace including PVCs =="
reset_namespace
apply_shared_resources
create_restore_pvcs

echo "== restore per-pod backups into fresh PVCs =="
restore_archives_to_pvcs

echo "== start restored cluster =="
apply_statefulset
wait_ready

echo "== restored cluster validation =="
validate_cluster_identity
validate_fixture

echo "K3s system backup/restore validation passed"
