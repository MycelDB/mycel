#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "Validating Raft Phase 9: graph data replication/read-forwarding/failover"
go test ./internal/daemon/modules/graph -run 'Test.*Raft|TestGraph.*Forward' -count=1

echo "phase9 raft validation passed"
