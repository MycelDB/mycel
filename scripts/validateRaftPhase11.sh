#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "Validating Raft Phase 11: semantic metadata/maintenance replication/read-forwarding/failover"
go test ./internal/daemon/modules/semantic -run 'Test.*Raft|TestSemantic.*Forward|TestMaintenance.*Raft' -count=1

echo "phase11 raft validation passed"
