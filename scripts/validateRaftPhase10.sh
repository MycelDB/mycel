#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "Validating Raft Phase 10: blob metadata/payload replication/failover"
go test ./internal/daemon/modules/blob -run 'Test.*Raft|TestBlob.*Payload|Test.*Upload|Test.*Metadata' -count=1

echo "phase10 raft validation passed"
