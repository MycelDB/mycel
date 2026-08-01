#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_ROOT="${MYCEL_CLUSTER_SOAK_COMPOSE_ROOT:-${ROOT_DIR}/../../knot_pkm/knot_pkm_server}"
COMPOSE_FILE="${MYCEL_COMPOSE_FILE:-${COMPOSE_ROOT}/compose.dev.yml}"
ITERATIONS="${MYCEL_CLUSTER_SOAK_ITERATIONS:-3}"
WRITES="${MYCEL_CLUSTER_SOAK_WRITES:-1}"
RESTART_EVERY="${MYCEL_CLUSTER_SOAK_RESTART_EVERY:-2}"
RESET="${MYCEL_CLUSTER_SOAK_RESET:-true}"
FORCE_SNAPSHOTS="${MYCEL_CLUSTER_SOAK_FORCE_SNAPSHOTS:-false}"
REPLACE_PVC="${MYCEL_CLUSTER_SOAK_REPLACE_PVC:-false}"
BACKEND_TOKEN="${MYCELD_CLUSTER_BACKEND_AUTH_TOKEN:-mycel-compose-cluster-token}"

if [[ ! -f "$COMPOSE_FILE" ]]; then
  echo "compose file not found: $COMPOSE_FILE" >&2
  exit 1
fi
if ! [[ "$ITERATIONS" =~ ^[0-9]+$ ]] || [[ "$ITERATIONS" -le 0 ]]; then
  echo "MYCEL_CLUSTER_SOAK_ITERATIONS must be a positive integer" >&2
  exit 2
fi
if ! [[ "$WRITES" =~ ^[0-9]+$ ]] || [[ "$WRITES" -le 0 ]]; then
  echo "MYCEL_CLUSTER_SOAK_WRITES must be a positive integer" >&2
  exit 2
fi
if ! [[ "$RESTART_EVERY" =~ ^[0-9]+$ ]]; then
  echo "MYCEL_CLUSTER_SOAK_RESTART_EVERY must be a non-negative integer" >&2
  exit 2
fi
if [[ "$FORCE_SNAPSHOTS" == "true" || "$FORCE_SNAPSHOTS" == "1" ]]; then
  echo "MYCEL_CLUSTER_SOAK_FORCE_SNAPSHOTS requires a future safe admin snapshot API; refusing to silently skip forced snapshots" >&2
  exit 2
fi
if [[ "$REPLACE_PVC" == "true" || "$REPLACE_PVC" == "1" ]]; then
  echo "MYCEL_CLUSTER_SOAK_REPLACE_PVC requires a future controlled PVC replacement harness; refusing to mutate volumes from soak script" >&2
  exit 2
fi

echo "Cluster soak settings: iterations=$ITERATIONS writes_per_iteration=$WRITES restart_every=$RESTART_EVERY force_snapshots=$FORCE_SNAPSHOTS replace_pvc=$REPLACE_PVC"

state="$(mktemp)"
trap 'rm -f "$state"' EXIT

if [[ "$RESET" == "true" || "$RESET" == "1" ]]; then
  (cd "$COMPOSE_ROOT" && MYCELD_CLUSTER_BACKEND_AUTH_TOKEN="$BACKEND_TOKEN" make compose-reset compose-up)
else
  (cd "$COMPOSE_ROOT" && MYCELD_CLUSTER_BACKEND_AUTH_TOKEN="$BACKEND_TOKEN" docker compose -f "$COMPOSE_FILE" up -d --wait myceld-a myceld-b myceld-c knot-pkm-server)
fi

"$ROOT_DIR/scripts/validateComposeClusterIdentity.sh"
MYCEL_COMPOSE_DATA_PLANE_STATE="$state" "$ROOT_DIR/scripts/validateComposeClusterDataPlane.sh"

for i in $(seq 1 "$ITERATIONS"); do
  echo "== cluster soak iteration $i/$ITERATIONS =="
  "$ROOT_DIR/scripts/validateComposeClusterIdentity.sh"
  for write in $(seq 1 "$WRITES"); do
    MYCEL_DATA_PLANE_CREATE_IF_MISSING=false MYCEL_COMPOSE_DATA_PLANE_STATE="$state" "$ROOT_DIR/scripts/validateComposeClusterDataPlane.sh"
  done
  if [[ "$RESTART_EVERY" -gt 0 && $((i % RESTART_EVERY)) -eq 0 && "$i" -lt "$ITERATIONS" ]]; then
    echo "== cluster soak rolling compose restart after iteration $i =="
    (cd "$COMPOSE_ROOT" && MYCELD_CLUSTER_BACKEND_AUTH_TOKEN="$BACKEND_TOKEN" docker compose -f "$COMPOSE_FILE" restart myceld-a myceld-b myceld-c)
    (cd "$COMPOSE_ROOT" && MYCELD_CLUSTER_BACKEND_AUTH_TOKEN="$BACKEND_TOKEN" docker compose -f "$COMPOSE_FILE" up -d --wait myceld-a myceld-b myceld-c knot-pkm-server)
  fi
done

echo "Cluster soak validation passed"
