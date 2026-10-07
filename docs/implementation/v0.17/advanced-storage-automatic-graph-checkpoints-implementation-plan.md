# Advanced storage phase 4: automatic graph checkpointing implementation plan

## Status

Planned/implemented on the `advanced_storage` branch.

This phase builds on:

- phase 1 domain-scoped graph store ownership
- phase 2 domain graph checkpoint storage and fast-open recovery
- phase 3 manual admin checkpoint create/status operations

## Goal

Create local domain graph checkpoints automatically so operators do not need to
manually checkpoint active domains. Expose enough status to tell whether a local
domain store is checkpointing, how stale the latest checkpoint is, and whether
recent checkpoint attempts are failing.

## Scope

Implement a conservative local policy:

- disabled by default
- daemon-local only
- applies to currently opened domain graph stores
- triggers after a configurable number of revisions since the latest checkpoint
- runs from a single background worker per daemon
- uses existing `LocalStore.WriteCheckpoint(ctx)` so checkpoint files remain the
  same format introduced in phase 2

## Configuration

Add daemon configuration:

```text
MYCELD_GRAPH_CHECKPOINT_AUTO_ENABLED=false
MYCELD_GRAPH_CHECKPOINT_AUTO_INTERVAL=1m
MYCELD_GRAPH_CHECKPOINT_AUTO_REVISIONS=10000
MYCELD_GRAPH_CHECKPOINT_AUTO_TIMEOUT=30s
```

Semantics:

- `AUTO_ENABLED=false` means no background checkpoint worker is started.
- `AUTO_INTERVAL` is the polling interval for opened stores.
- `AUTO_REVISIONS` is the minimum tail revisions since checkpoint before an
  automatic checkpoint is attempted. A store with no checkpoint becomes eligible
  once current revision is at least this threshold.
- `AUTO_TIMEOUT` bounds an individual checkpoint write attempt.

## Service behavior

The graph service owns the worker because it owns local domain store lifecycle.
The worker should:

1. take a snapshot of opened domain stores
2. read each store's checkpoint status
3. skip revision 0 stores
4. checkpoint when tail revisions are above threshold
5. record per-domain attempt/success/error/duration metadata
6. log failures without crashing the daemon

Manual checkpoint creation should also update the same status metadata.

## Admin/API status

Extend existing phase 3 checkpoint status with automatic policy fields:

- `auto_checkpoint_enabled`
- `auto_checkpoint_revision_threshold`
- `auto_checkpoint_interval`
- `last_checkpoint_attempt_at`
- `last_checkpoint_success_at`
- `last_checkpoint_duration_ms`
- `last_checkpoint_error`
- `checkpoint_age_seconds`

The status remains local-only and does not collect peer status.

## Out of scope

Do not implement in this phase:

- cluster-wide fanout checkpoint orchestration
- segment compaction
- persistent indexes
- checkpoint retention controls beyond the phase 2 latest-checkpoint cleanup
- Prometheus/OpenTelemetry integration

## Tests

Add tests for:

- config env parsing and validation
- service-level automatic checkpoint eligibility by revision threshold
- manual checkpoint status metadata update
- admin/CLI status mapping for new fields

## Validation

Run:

```bash
./scripts/generate-proto.sh
go test ./internal/daemon/config ./internal/graph/storage ./internal/graph/service ./internal/daemon/api/admin ./internal/cli/cmd -count=1
make docs-check
git diff --check
make test
```
