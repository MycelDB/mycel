#!/usr/bin/env bash
set -euo pipefail

DAEMON_ADDR="${DAEMON_ADDR:-127.0.0.1:9091}"
MYCEL_BIN="${MYCEL_BIN:-./bin/mycel}"
MYCEL_USER="${MYCEL_USER:-${USER_NAME:-}}"
MYCEL_PASS="${MYCEL_PASS:-${PASSWORD:-}}"
SPACE_ID="${SPACE_ID:-}"
DOMAIN_ID="${DOMAIN_ID:-}"
DOMAIN_KEY="${DOMAIN_KEY:-default}"

if [[ -z "$MYCEL_USER" || -z "$MYCEL_PASS" || -z "$SPACE_ID" ]]; then
  cat >&2 <<'USAGE'
Usage:
  MYCEL_USER=alice MYCEL_PASS=pass SPACE_ID=<space-id> [DOMAIN_ID=<domain-id>|DOMAIN_KEY=default] \
    scripts/create-acd-graph.sh

Optional env:
  DAEMON_ADDR=127.0.0.1:9091
  MYCEL_BIN=./bin/mycel
USAGE
  exit 2
fi

need_jq() {
  if ! command -v jq >/dev/null 2>&1; then
    echo "jq is required" >&2
    exit 2
  fi
}
need_jq

mycel_json() {
  "$MYCEL_BIN" --daemon-addr "$DAEMON_ADDR" -u "$MYCEL_USER" -p "$MYCEL_PASS" --output json "$@"
}

open_args=(session open --space-id "$SPACE_ID")
if [[ -n "$DOMAIN_ID" ]]; then
  open_args+=(--domain-id "$DOMAIN_ID")
else
  open_args+=(--domain "$DOMAIN_KEY")
fi

SESSION_ID="$(mycel_json "${open_args[@]}" | jq -r '.session_id')"
TX_ID="$(mycel_json transaction begin "$SESSION_ID" --mode read-write | jq -r '.transaction_id')"

cleanup() {
  if [[ -n "${TX_ID:-}" ]]; then
    "$MYCEL_BIN" --daemon-addr "$DAEMON_ADDR" -u "$MYCEL_USER" -p "$MYCEL_PASS" transaction rollback "$TX_ID" >/dev/null 2>&1 || true
  fi
  if [[ -n "${SESSION_ID:-}" ]]; then
    "$MYCEL_BIN" --daemon-addr "$DAEMON_ADDR" -u "$MYCEL_USER" -p "$MYCEL_PASS" session close "$SESSION_ID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

A_ID="$(mycel_json graph node create --transaction-id "$TX_ID" --content A | jq -r '.node_id')"
C_ID="$(mycel_json graph node create --transaction-id "$TX_ID" --content C --props-json '{"tags":["test1"]}' | jq -r '.node_id')"
D_ID="$(mycel_json graph node create --transaction-id "$TX_ID" --content D | jq -r '.node_id')"

mycel_json graph edge create --transaction-id "$TX_ID" --from "$A_ID" --to "$C_ID" --kind contains --props-json '{"order":0}' >/dev/null
mycel_json graph edge create --transaction-id "$TX_ID" --from "$A_ID" --to "$D_ID" --kind contains --props-json '{"order":1}' >/dev/null

COMMIT_JSON="$(mycel_json transaction commit "$TX_ID")"
TX_ID=""
mycel_json session close "$SESSION_ID" >/dev/null
SESSION_ID=""
trap - EXIT

cat <<EOF
Created graph:
  A=$A_ID
  C=$C_ID tags=[test1]
  D=$D_ID
Commit:
$COMMIT_JSON
EOF
