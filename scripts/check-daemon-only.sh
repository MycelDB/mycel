#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if ! command -v rg >/dev/null 2>&1; then
  echo "error: ripgrep (rg) is required for daemon-only enforcement checks" >&2
  exit 1
fi

fail=0

report_match() {
  local description="$1"
  shift
  local tmp
  tmp="$(mktemp)"
  if "$@" >"$tmp" 2>/dev/null; then
    echo "daemon-only check failed: $description" >&2
    sed 's/^/  /' "$tmp" >&2
    fail=1
  fi
  rm -f "$tmp"
}

if [ -e engine ]; then
  echo "daemon-only check failed: legacy engine tree must not exist" >&2
  find engine -maxdepth 3 -print 2>/dev/null | sed 's/^/  /' >&2
  fail=1
fi

if [ -e session ]; then
  echo "daemon-only check failed: public session package directory must not exist" >&2
  find session -maxdepth 2 -print 2>/dev/null | sed 's/^/  /' >&2
  fail=1
fi

if [ -e api/proto ] || [ -e gen/go ] || [ -e buf.yaml ]; then
  echo "daemon-only check failed: protobuf sources belong in github.com/myceldb/mycel-api and generated public stubs must not be committed in mycel" >&2
  for path in api/proto gen/go buf.yaml; do
    [ -e "$path" ] && echo "  $path" >&2
  done
  fail=1
fi

report_match "Go code must not import public engine/session packages" \
  rg -n 'github\.com/myceldb/mycel/(engine|session)("|/)' --glob '*.go' --glob '!gen/**' .

report_match "Go code must not reference legacy MYCELDB_* embedded environment variables" \
  rg -n 'MYCELDB_' --glob '*.go' --glob '!gen/**' .

report_match "Go code must not expose removed embedded CLI flags" \
  rg -n '"(data-dir|auth-token-ttl|auth-refresh-[^"]*|blob-(stale|max)[^"]*|semantic-advanced-enabled|user-store-encryption-key-b64)"' --glob '*.go' --glob '!gen/**' .

public_packages="$(go list ./... | grep -E '^github\.com/myceldb/mycel/(engine|session)($|/)' || true)"
if [ -n "$public_packages" ]; then
  echo "daemon-only check failed: public engine/session packages are present in go list" >&2
  printf '%s\n' "$public_packages" | sed 's/^/  /' >&2
  fail=1
fi

cli_deps="$(go list -deps ./cmd/mycel | grep -E '^github\.com/myceldb/mycel/(engine|session)($|/)' || true)"
if [ -n "$cli_deps" ]; then
  echo "daemon-only check failed: mycel CLI depends on embedded engine/session packages" >&2
  printf '%s\n' "$cli_deps" | sed 's/^/  /' >&2
  fail=1
fi

if [ "$fail" -ne 0 ]; then
  exit 1
fi

echo "daemon-only checks passed"
