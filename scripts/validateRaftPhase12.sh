#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "Validating Raft Phase 12: system identity/session replication/failover"
go test ./internal/daemon/modules/user ./internal/daemon/modules/admin ./internal/daemon/app -run 'Test.*Raft|Test.*SessionSystemRaft|Test.*SystemRaft' -count=1

echo "phase12 raft validation passed"
