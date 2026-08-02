#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${MYCEL_COMPOSE_FILE:-${ROOT_DIR}/../../knot_pkm/knot_pkm_server/compose.dev.yml}"
COMPOSE_PROJECT_DIR="$(cd "$(dirname "$COMPOSE_FILE")" && pwd)"
SERVICES_CSV="${MYCEL_COMPOSE_SERVICES:-myceld-a,myceld-b,myceld-c}"
ADMIN_USERNAME="${MYCEL_OPERATOR_USERNAME:-${MYCELD_BOOTSTRAP_ADMIN_USERNAME:-admin}}"
ADMIN_PASSWORD="${MYCEL_OPERATOR_PASSWORD:-${MYCELD_BOOTSTRAP_ADMIN_PASSWORD:-admin-password}}"
DAEMON_ADDR="${MYCELD_DAEMON_ADDR:-127.0.0.1:9091}"
BACKEND_AUTH_TOKEN="${MYCELD_CLUSTER_BACKEND_AUTH_TOKEN:-mycel-compose-cluster-token}"
MYCEL_IMAGE="${MYCEL_IMAGE:-local/mycel:user-backup-restore}"
BUILD_MYCEL_IMAGE="${MYCEL_USER_BACKUP_BUILD_IMAGE:-true}"
CLI_TIMEOUT_SECONDS="${MYCEL_COMPOSE_CLI_TIMEOUT:-45}"
VALIDATION_TIMEOUT_SECONDS="${MYCEL_USER_BACKUP_RESTORE_TIMEOUT:-240}"
SLEEP_SECONDS="${MYCEL_USER_BACKUP_RESTORE_INTERVAL:-3}"
ARCHIVE_COMPRESSION="${MYCEL_USER_BACKUP_ARCHIVE_COMPRESSION:-zstd}"

if [[ ! -f "$COMPOSE_FILE" ]]; then
  echo "compose file not found: $COMPOSE_FILE" >&2
  exit 1
fi

IFS=',' read -r -a SERVICES <<< "$SERVICES_CSV"
if [[ ${#SERVICES[@]} -eq 0 ]]; then
  echo "no services configured" >&2
  exit 1
fi

TMP_DIR="$(mktemp -d)"
EXPECTED_FILE="$TMP_DIR/expected-domains.tsv"
MAP_FILE="$TMP_DIR/restored-maps.tsv"
: > "$EXPECTED_FILE"
: > "$MAP_FILE"
cleanup() {
  local rc=$?
  if [[ $rc -eq 0 || "${MYCEL_USER_BACKUP_KEEP_TMP:-false}" != "true" ]]; then
    rm -rf "$TMP_DIR"
  else
    echo "Preserving user backup restore temp dir after failure: $TMP_DIR" >&2
  fi
}
trap cleanup EXIT

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
service_at() { local idx="$1"; trim "${SERVICES[$idx]}"; }

cli() {
  local service="$1"; shift
  with_timeout docker compose -f "$COMPOSE_FILE" exec -T "$service" mycel --daemon-addr "$DAEMON_ADDR" "$@"
}

admin_cli() {
  local service="$1"; shift
  cli "$service" --username "$ADMIN_USERNAME" --password "$ADMIN_PASSWORD" --output json "$@"
}

user_cli_with_creds() {
  local service="$1" username="$2" password="$3"; shift 3
  cli "$service" --username "$username" --password "$password" --output json "$@"
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

build_mycel_image() {
  if [[ "$BUILD_MYCEL_IMAGE" != "true" ]]; then
    echo "Skipping mycel image build; using MYCEL_IMAGE=$MYCEL_IMAGE" >&2
    return 0
  fi
  echo "Building mycel image $MYCEL_IMAGE from current worktree" >&2
  docker build -f "$ROOT_DIR/Dockerfile" -t "$MYCEL_IMAGE" "$(dirname "$ROOT_DIR")"
}

reset_compose_cluster() {
  echo "Resetting compose cluster with MYCEL_IMAGE=$MYCEL_IMAGE" >&2
  (cd "$COMPOSE_PROJECT_DIR" && MYCEL_IMAGE="$MYCEL_IMAGE" MYCELD_CLUSTER_BACKEND_AUTH_TOKEN="$BACKEND_AUTH_TOKEN" make compose-reset compose-up)
  MYCEL_IMAGE="$MYCEL_IMAGE" "$ROOT_DIR/scripts/validateComposeClusterIdentity.sh"
}

copy_from_service() {
  local service="$1" remote="$2" local_path="$3"
  docker compose -f "$COMPOSE_FILE" exec -T "$service" cat "$remote" > "$local_path"
}

copy_to_service() {
  local service="$1" local_path="$2" remote="$3"
  docker compose -f "$COMPOSE_FILE" exec -T "$service" sh -c "cat > '$remote'" < "$local_path"
}

create_user() {
  local service="$1" username="$2" password="$3"
  echo "Creating user $username" >&2
  admin_cli "$service" user add --user-username "$username" --new-password "$password" >/dev/null
}

create_space() {
  local service="$1" username="$2" password="$3" name="$4" key="$5" display="$6"
  local raw
  raw="$(admin_cli "$service" space add "$name" --owner-username "$username" --default-domain-key "$key" --default-domain-name "$display")"
  printf '%s\t%s\n' "$(printf '%s\n' "$raw" | json_get 'space.space_id')" "$(printf '%s\n' "$raw" | json_get 'default_domain_id')"
}

create_domain() {
  local service="$1" username="$2" password="$3" space_id="$4" key="$5" display="$6"
  local raw
  raw="$(user_cli_with_creds "$service" "$username" "$password" domain add "$key" --space-id "$space_id" --name "$display")"
  printf '%s\n' "$raw" | json_get 'domain_id'
}

open_tx() {
  local service="$1" username="$2" password="$3" space_id="$4" domain_id="$5" mode="$6"
  local raw session_id tx_id
  raw="$(user_cli_with_creds "$service" "$username" "$password" session open --space-id "$space_id" --domain-id "$domain_id")"
  session_id="$(printf '%s\n' "$raw" | json_get 'session_id')"
  raw="$(user_cli_with_creds "$service" "$username" "$password" transaction begin "$session_id" --mode "$mode")"
  tx_id="$(printf '%s\n' "$raw" | json_get 'transaction_id')"
  printf '%s\t%s\n' "$session_id" "$tx_id"
}

create_graph_fixture() {
  local service="$1" username="$2" password="$3" space_id="$4" domain_id="$5" marker="$6" components="$7"
  local tx_pair session_id tx_id comp i prev node_id blob_node_id edge_id blob_id raw node_ids=() edge_ids=() blob_specs=()
  tx_pair="$(open_tx "$service" "$username" "$password" "$space_id" "$domain_id" read-write)"
  session_id="${tx_pair%%$'\t'*}"
  tx_id="${tx_pair##*$'\t'}"
  for ((comp=1; comp<=components; comp++)); do
    prev=""
    for ((i=1; i<=4; i++)); do
      node_id="$(uuid)"
      node_ids+=("$node_id")
      user_cli_with_creds "$service" "$username" "$password" graph node create \
        --transaction-id "$tx_id" \
        --node-id "$node_id" \
        --label UserBackupNode \
        --properties-json "{\"domain_marker\":\"$marker\",\"component\":$comp,\"ordinal\":$i,\"kind\":\"plain\"}" \
        --payload-json "{\"text\":\"$marker component $comp node $i\"}" >/dev/null
      if [[ -n "$prev" ]]; then
        edge_id="$(uuid)"
        edge_ids+=("$edge_id")
        user_cli_with_creds "$service" "$username" "$password" graph edge create \
          --transaction-id "$tx_id" \
          --edge-id "$edge_id" \
          --from "$prev" \
          --to "$node_id" \
          --kind backup_fixture_link \
          --props-json "{\"domain_marker\":\"$marker\",\"component\":$comp}" >/dev/null
      fi
      prev="$node_id"
    done
    blob_node_id="$(uuid)"
    node_ids+=("$blob_node_id")
    local blob_file="/tmp/${marker}-component-${comp}.txt"
    local blob_payload="blob payload for $marker component $comp"
    printf '%s' "$blob_payload" | docker compose -f "$COMPOSE_FILE" exec -T "$service" sh -c "cat > '$blob_file'"
    raw="$(user_cli_with_creds "$service" "$username" "$password" graph blob-node create "$blob_file" \
      --transaction-id "$tx_id" \
      --node-id "$blob_node_id" \
      --label UserBackupNode \
      --label UserBackupBlob \
      --mime-type text/plain \
      --properties-json "{\"domain_marker\":\"$marker\",\"component\":$comp,\"ordinal\":5,\"kind\":\"blob\"}" \
      --payload-json "{\"text\":\"$marker component $comp blob\"}")"
    blob_id="$(printf '%s\n' "$raw" | json_get 'blob.blob_id')"
    blob_specs+=("${blob_id}::${blob_payload}")
    edge_id="$(uuid)"
    edge_ids+=("$edge_id")
    user_cli_with_creds "$service" "$username" "$password" graph edge create \
      --transaction-id "$tx_id" \
      --edge-id "$edge_id" \
      --from "$prev" \
      --to "$blob_node_id" \
      --kind backup_fixture_link \
      --props-json "{\"domain_marker\":\"$marker\",\"component\":$comp}" >/dev/null
  done
  user_cli_with_creds "$service" "$username" "$password" transaction commit "$tx_id" >/dev/null
  user_cli_with_creds "$service" "$username" "$password" session close "$session_id" >/dev/null || true
  printf '%s\t%s\t%s\t%s\t%d\t%d\t%d\t%s\t%s\t%s\n' \
    "$username" "$space_id" "$domain_id" "$marker" "$((components*5))" "$((components*4))" "$components" \
    "$(IFS=,; echo "${node_ids[*]}")" "$(IFS=,; echo "${edge_ids[*]}")" "$(IFS=,; echo "${blob_specs[*]}")" >> "$EXPECTED_FILE"
}

create_source_fixture() {
  local writer="$1" suffix="$2"
  EMPTY_USER="ubr-empty-$suffix"
  ONE_USER="ubr-one-$suffix"
  COMPLEX_USER="ubr-complex-$suffix"
  SRC_EMPTY_PASS="src-empty-pass-$suffix"
  SRC_ONE_PASS="src-one-pass-$suffix"
  SRC_COMPLEX_PASS="src-complex-pass-$suffix"
  RESTORE_EMPTY_PASS="restore-empty-pass-$suffix"
  RESTORE_ONE_PASS="restore-one-pass-$suffix"
  RESTORE_COMPLEX_PASS="restore-complex-pass-$suffix"

  create_user "$writer" "$EMPTY_USER" "$SRC_EMPTY_PASS"
  create_user "$writer" "$ONE_USER" "$SRC_ONE_PASS"
  create_user "$writer" "$COMPLEX_USER" "$SRC_COMPLEX_PASS"

  local pair space_id domain_id extra_domain_id
  pair="$(create_space "$writer" "$ONE_USER" "$SRC_ONE_PASS" "UBR One $suffix" one "One")"
  space_id="${pair%%$'\t'*}"
  domain_id="${pair##*$'\t'}"
  create_graph_fixture "$writer" "$ONE_USER" "$SRC_ONE_PASS" "$space_id" "$domain_id" "ubr-one-default-$suffix" 1

  pair="$(create_space "$writer" "$COMPLEX_USER" "$SRC_COMPLEX_PASS" "UBR Complex A $suffix" complex-a "Complex A")"
  space_id="${pair%%$'\t'*}"
  domain_id="${pair##*$'\t'}"
  create_graph_fixture "$writer" "$COMPLEX_USER" "$SRC_COMPLEX_PASS" "$space_id" "$domain_id" "ubr-complex-a-default-$suffix" 2

  pair="$(create_space "$writer" "$COMPLEX_USER" "$SRC_COMPLEX_PASS" "UBR Complex B $suffix" complex-b "Complex B")"
  space_id="${pair%%$'\t'*}"
  domain_id="${pair##*$'\t'}"
  create_graph_fixture "$writer" "$COMPLEX_USER" "$SRC_COMPLEX_PASS" "$space_id" "$domain_id" "ubr-complex-b-default-$suffix" 2
  extra_domain_id="$(create_domain "$writer" "$COMPLEX_USER" "$SRC_COMPLEX_PASS" "$space_id" complex-b-extra "Complex B Extra")"
  create_graph_fixture "$writer" "$COMPLEX_USER" "$SRC_COMPLEX_PASS" "$space_id" "$extra_domain_id" "ubr-complex-b-extra-$suffix" 2
}

export_user_backup() {
  local service="$1" username="$2" archive="$3"
  local remote="/tmp/${username}.user-backup.tar.zst" raw user_id
  echo "Exporting backup for $username" >&2
  raw="$(admin_cli "$service" user find --user-username "$username")"
  user_id="$(printf '%s\n' "$raw" | json_get 'user_id')"
  admin_cli "$service" admin user-backup export --user-id "$user_id" --file "$remote" --compression "$ARCHIVE_COMPRESSION" --include-blobs --source-label "$service" >/dev/null
  admin_cli "$service" admin user-backup validate --file "$remote" --compression "$ARCHIVE_COMPRESSION" >/dev/null
  copy_from_service "$service" "$remote" "$archive"
}

restore_user_backup() {
  local service="$1" username="$2" password="$3" archive="$4"
  local remote="/tmp/${username}.restore.user-backup.tar.zst" raw_file="$TMP_DIR/import-${username}.json" dry_file="$TMP_DIR/dry-${username}.json"
  echo "Restoring backup for $username" >&2
  copy_to_service "$service" "$archive" "$remote"
  admin_cli "$service" admin user-backup validate --file "$remote" --compression "$ARCHIVE_COMPRESSION" >/dev/null
  admin_cli "$service" admin user-backup import --file "$remote" --compression "$ARCHIVE_COMPRESSION" --target-username "$username" --create-user --new-password "$password" > "$dry_file"
  python3 - "$username" "$dry_file" <<'PY'
import json, sys
username, path = sys.argv[1:3]
data = json.load(open(path))
if data.get("dry_run") is not True:
    raise SystemExit(f"{username}: dry-run plan did not report dry_run=true: {data}")
if data.get("target_username") != username:
    raise SystemExit(f"{username}: dry-run target mismatch: {data}")
PY
  admin_cli "$service" admin user-backup import --file "$remote" --compression "$ARCHIVE_COMPRESSION" --target-username "$username" --create-user --new-password "$password" --execute > "$raw_file"
  python3 - "$username" "$raw_file" >> "$MAP_FILE" <<'PY'
import json, sys
username, path = sys.argv[1:3]
data = json.load(open(path))
for src, dst in (data.get("space_id_map") or {}).items():
    print("space\t{}\t{}\t{}".format(username, src, dst))
for src, dst in (data.get("domain_id_map") or {}).items():
    print("domain\t{}\t{}\t{}".format(username, src, dst))
PY
}

map_lookup() {
  local kind="$1" username="$2" source_id="$3"
  awk -F '\t' -v kind="$kind" -v user="$username" -v src="$source_id" '$1==kind && $2==user && $3==src { print $4; found=1; exit } END { if (!found) exit 1 }' "$MAP_FILE"
}

verify_node_query_count() {
  local service="$1" raw="$2" want_count="$3" marker="$4"
  RAW_JSON="$raw" python3 - "$service" "$want_count" "$marker" <<'PY'
import json, os, sys
service, want, marker = sys.argv[1], int(sys.argv[2]), sys.argv[3]
data = json.loads(os.environ["RAW_JSON"])
rows = data.get("rows") or []
if len(rows) != want:
    raise SystemExit(f"{service}: marker {marker} expected {want} query rows, got {len(rows)}")
meta = data.get("read_metadata") or {}
if meta.get("stale"):
    raise SystemExit(f"{service}: marker {marker} query returned stale metadata: {meta}")
PY
}

verify_domain_once() {
  local service="$1" username="$2" password="$3" target_space_id="$4" target_domain_id="$5" marker="$6" want_nodes="$7" want_edges="$8" want_blobs="$9" node_ids_csv="${10}" edge_ids_csv="${11}" blob_spec="${12}"
  local tx_pair session_id tx_id raw node_id edge_id blob_part blob_id payload remote_download downloaded
  tx_pair="$(open_tx "$service" "$username" "$password" "$target_space_id" "$target_domain_id" read-only)"
  session_id="${tx_pair%%$'\t'*}"
  tx_id="${tx_pair##*$'\t'}"

  raw="$(user_cli_with_creds "$service" "$username" "$password" query nodes --transaction-id "$tx_id" --label UserBackupNode --property-equals "domain_marker=$marker" --limit 100)"
  verify_node_query_count "$service" "$raw" "$want_nodes" "$marker"
  raw="$(user_cli_with_creds "$service" "$username" "$password" query nodes --transaction-id "$tx_id" --label UserBackupBlob --property-equals "domain_marker=$marker" --limit 100)"
  verify_node_query_count "$service" "$raw" "$want_blobs" "$marker"

  IFS=',' read -r -a node_ids <<< "$node_ids_csv"
  for node_id in "${node_ids[@]}"; do
    [[ -n "$node_id" ]] || continue
    raw="$(user_cli_with_creds "$service" "$username" "$password" graph node get "$node_id" --transaction-id "$tx_id")"
    RAW_JSON="$raw" python3 - "$service" "$node_id" <<'PY'
import json, os, sys
service, want = sys.argv[1:3]
data = json.loads(os.environ["RAW_JSON"])
if data.get("node_id") != want:
    raise SystemExit(f"{service}: expected node {want}, got {data}")
PY
  done

  IFS=',' read -r -a edge_ids <<< "$edge_ids_csv"
  local edge_count=0
  for edge_id in "${edge_ids[@]}"; do
    [[ -n "$edge_id" ]] || continue
    edge_count=$((edge_count + 1))
    raw="$(user_cli_with_creds "$service" "$username" "$password" graph edge get "$edge_id" --transaction-id "$tx_id")"
    RAW_JSON="$raw" python3 - "$service" "$edge_id" <<'PY'
import json, os, sys
service, want = sys.argv[1:3]
data = json.loads(os.environ["RAW_JSON"])
if data.get("edge_id") != want:
    raise SystemExit(f"{service}: expected edge {want}, got {data}")
PY
  done
  if [[ "$edge_count" -ne "$want_edges" ]]; then
    echo "$service: marker $marker expected $want_edges edge ids, got $edge_count" >&2
    return 1
  fi

  IFS=',' read -r -a blob_parts <<< "$blob_spec"
  for blob_part in "${blob_parts[@]}"; do
    [[ -n "$blob_part" ]] || continue
    blob_id="${blob_part%%::*}"
    payload="${blob_part#*::}"
    remote_download="/tmp/${marker}-${blob_id}.download"
    user_cli_with_creds "$service" "$username" "$password" blob download "$blob_id" --space-id "$target_space_id" --output-file "$remote_download" >/dev/null
    downloaded="$(docker compose -f "$COMPOSE_FILE" exec -T "$service" cat "$remote_download")"
    if [[ "$downloaded" != "$payload" ]]; then
      echo "$service: blob payload mismatch for $blob_id marker $marker" >&2
      return 1
    fi
  done

  user_cli_with_creds "$service" "$username" "$password" transaction close "$tx_id" >/dev/null
  user_cli_with_creds "$service" "$username" "$password" session close "$session_id" >/dev/null || true
}

verify_restored_user_has_no_spaces() {
  local service="$1" username="$2" password="$3" raw
  raw="$(user_cli_with_creds "$service" "$username" "$password" space list)"
  RAW_JSON="$raw" python3 - "$service" "$username" <<'PY'
import json, os, sys
service, username = sys.argv[1:3]
data = json.loads(os.environ["RAW_JSON"])
if data:
    raise SystemExit(f"{service}: expected no spaces for {username}, got {data}")
PY
}

validate_source_data_once() {
  local service="$1" username source_space_id source_domain_id marker want_nodes want_edges want_blobs node_ids edge_ids blob_spec password
  verify_restored_user_has_no_spaces "$service" "$EMPTY_USER" "$SRC_EMPTY_PASS"
  while IFS=$'\t' read -r username source_space_id source_domain_id marker want_nodes want_edges want_blobs node_ids edge_ids blob_spec; do
    [[ -n "$username" ]] || continue
    case "$username" in
      "$ONE_USER") password="$SRC_ONE_PASS" ;;
      "$COMPLEX_USER") password="$SRC_COMPLEX_PASS" ;;
      *) echo "unknown expected username $username" >&2; return 1 ;;
    esac
    verify_domain_once "$service" "$username" "$password" "$source_space_id" "$source_domain_id" "$marker" "$want_nodes" "$want_edges" "$want_blobs" "$node_ids" "$edge_ids" "$blob_spec"
  done < "$EXPECTED_FILE"
}

validate_source_data() {
  local service output deadline last_error=""
  deadline=$((SECONDS + VALIDATION_TIMEOUT_SECONDS))
  while (( SECONDS <= deadline )); do
    if output="$({ for service in "${SERVICES[@]}"; do service="$(trim "$service")"; [[ -n "$service" ]] || continue; validate_source_data_once "$service"; done; } 2>&1)"; then
      echo "Compose source user backup fixture validation passed"
      return 0
    fi
    last_error="$output"
    echo "Waiting for source user backup fixture validation: $last_error" >&2
    sleep "$SLEEP_SECONDS"
  done
  echo "Compose source user backup fixture validation failed after ${VALIDATION_TIMEOUT_SECONDS}s" >&2
  echo "$last_error" >&2
  return 1
}

validate_restored_data_once() {
  local service="$1" line username source_space_id source_domain_id marker want_nodes want_edges want_blobs node_ids edge_ids blob_spec password target_space_id target_domain_id
  verify_restored_user_has_no_spaces "$service" "$EMPTY_USER" "$RESTORE_EMPTY_PASS"
  while IFS=$'\t' read -r username source_space_id source_domain_id marker want_nodes want_edges want_blobs node_ids edge_ids blob_spec; do
    [[ -n "$username" ]] || continue
    case "$username" in
      "$ONE_USER") password="$RESTORE_ONE_PASS" ;;
      "$COMPLEX_USER") password="$RESTORE_COMPLEX_PASS" ;;
      *) echo "unknown expected username $username" >&2; return 1 ;;
    esac
    target_space_id="$(map_lookup space "$username" "$source_space_id")"
    target_domain_id="$(map_lookup domain "$username" "$source_domain_id")"
    verify_domain_once "$service" "$username" "$password" "$target_space_id" "$target_domain_id" "$marker" "$want_nodes" "$want_edges" "$want_blobs" "$node_ids" "$edge_ids" "$blob_spec"
  done < "$EXPECTED_FILE"
}

assert_source_password_rejected() {
  local service="$1" username="$2" old_password="$3"
  if user_cli_with_creds "$service" "$username" "$old_password" auth whoami >/dev/null 2>&1; then
    echo "$service: source password still authenticates for restored user $username" >&2
    return 1
  fi
}

validate_restored_data() {
  local service output deadline last_error=""
  deadline=$((SECONDS + VALIDATION_TIMEOUT_SECONDS))
  while (( SECONDS <= deadline )); do
    if output="$({ for service in "${SERVICES[@]}"; do service="$(trim "$service")"; [[ -n "$service" ]] || continue; validate_restored_data_once "$service"; done; } 2>&1)"; then
      echo "Compose user backup/restore validation passed"
      return 0
    fi
    last_error="$output"
    echo "Waiting for restored user backup data validation: $last_error" >&2
    sleep "$SLEEP_SECONDS"
  done
  echo "Compose user backup/restore validation failed after ${VALIDATION_TIMEOUT_SECONDS}s" >&2
  echo "$last_error" >&2
  for service in "${SERVICES[@]}"; do
    service="$(trim "$service")"
    [[ -n "$service" ]] || continue
    echo "--- logs: $service ---" >&2
    compose logs --tail=100 "$service" >&2 || true
  done
  return 1
}

suffix="$(date +%s)-$RANDOM"
writer="$(service_at 0)"

build_mycel_image
reset_compose_cluster
create_source_fixture "$writer" "$suffix"
validate_source_data

export_user_backup "$writer" "$EMPTY_USER" "$TMP_DIR/${EMPTY_USER}.tar.zst"
export_user_backup "$writer" "$ONE_USER" "$TMP_DIR/${ONE_USER}.tar.zst"
export_user_backup "$writer" "$COMPLEX_USER" "$TMP_DIR/${COMPLEX_USER}.tar.zst"

reset_compose_cluster
restore_user_backup "$writer" "$EMPTY_USER" "$RESTORE_EMPTY_PASS" "$TMP_DIR/${EMPTY_USER}.tar.zst"
restore_user_backup "$writer" "$ONE_USER" "$RESTORE_ONE_PASS" "$TMP_DIR/${ONE_USER}.tar.zst"
restore_user_backup "$writer" "$COMPLEX_USER" "$RESTORE_COMPLEX_PASS" "$TMP_DIR/${COMPLEX_USER}.tar.zst"

assert_source_password_rejected "$writer" "$EMPTY_USER" "$SRC_EMPTY_PASS"
assert_source_password_rejected "$writer" "$ONE_USER" "$SRC_ONE_PASS"
assert_source_password_rejected "$writer" "$COMPLEX_USER" "$SRC_COMPLEX_PASS"

validate_restored_data
