#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
workspace_root=""
strict=0
workspace_explicit=0

usage() {
  cat <<'USAGE'
Usage: scripts/check-public-surface.sh [--workspace ROOT] [--strict]

Checks the daemon-only Go package boundary.

Default mode:
  - fails if removed public engine/session packages are present
  - fails if new top-level Go packages appear outside cmd/internal
  - fails if legacy public implementation packages domain/store/query are present
  - fails if external workspace imports of mycel implementation packages are present

Use --workspace to scan sibling repos for forbidden imports. When omitted, the
script auto-detects the Knotbase workspace if this repo is checked out at
<workspace>/myceldb/mycel. --strict is accepted for compatibility and uses the
same failing behavior.
USAGE
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --workspace)
      if [ "$#" -lt 2 ]; then
        echo "error: --workspace requires a path" >&2
        exit 2
      fi
      workspace_root="$2"
      workspace_explicit=1
      shift 2
      ;;
    --strict)
      strict=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

cd "$repo_root"

if [ -z "$workspace_root" ]; then
  candidate_workspace="$(cd "$repo_root/../.." && pwd)"
  candidate_repo="$candidate_workspace/myceldb/mycel"
  if [ -d "$candidate_repo" ] && [ "$(cd "$candidate_repo" && pwd)" = "$repo_root" ]; then
    workspace_root="$candidate_workspace"
  fi
fi

search_external_go() {
  local pattern="$1"
  if command -v rg >/dev/null 2>&1; then
    rg -n "$pattern" . --glob '*.go' --glob '!myceldb/mycel/**' --glob '!**/vendor/**'
  else
    find . -type f -name '*.go' \
      -not -path './myceldb/mycel/*' \
      -not -path '*/vendor/*' \
      -print0 | xargs -0 grep -n -E "$pattern"
  fi
}

fail=0

print_block() {
  local title="$1"
  local body="$2"
  echo "$title" >&2
  printf '%s\n' "$body" | sed 's/^/  /' >&2
}

packages="$(go list ./...)"

removed_public="$(printf '%s\n' "$packages" | grep -E '^github\.com/myceldb/mycel/(engine|session)($|/)' || true)"
if [ -n "$removed_public" ]; then
  print_block "public-surface check failed: removed public engine/session packages are present" "$removed_public"
  fail=1
fi

unexpected_public="$(printf '%s\n' "$packages" | grep -E '^github\.com/myceldb/mycel/[^/]+' | grep -Ev '^github\.com/myceldb/mycel($|/(cmd|internal)($|/))' || true)"
if [ -n "$unexpected_public" ]; then
  print_block "public-surface check failed: unexpected top-level Go packages outside cmd/internal" "$unexpected_public"
  fail=1
fi

legacy_public="$(printf '%s\n' "$packages" | grep -E '^github\.com/myceldb/mycel/(domain|store|query)($|/)' || true)"
if [ -n "$legacy_public" ]; then
  print_block "public-surface check failed: implementation packages remain public" "$legacy_public"
  fail=1
fi

if [ -n "$workspace_root" ]; then
  if [ ! -d "$workspace_root" ]; then
    echo "public-surface check failed: workspace does not exist: $workspace_root" >&2
    fail=1
  else
    external_hits="$(cd "$workspace_root" && search_external_go 'github\.com/myceldb/mycel(/(domain|store|query|engine|session|$)|")' || true)"
    if [ -n "$external_hits" ]; then
      print_block "public-surface check failed: external consumers import Mycel implementation packages" "$external_hits"
      fail=1
    fi
  fi
elif [ "$workspace_explicit" -eq 1 ]; then
  echo "public-surface check failed: workspace does not exist: $workspace_root" >&2
  fail=1
fi

if [ "$fail" -ne 0 ]; then
  exit 1
fi

if [ "$strict" -eq 1 ]; then
  echo "public-surface checks passed in strict mode"
else
  echo "public-surface checks passed"
fi
