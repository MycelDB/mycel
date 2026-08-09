# add_callbacks Parking-Lot Implementation Plan

Status: parked on `add_callbacks`; downstream compatibility, example docs,
integration notes, and current-doc cleanup have a follow-up pass recorded in
[`add-callbacks-integration-notes.md`](add-callbacks-integration-notes.md).

This plan captures the remaining non-blocking work after the graph-change
notification, operation correlation, public graph-change watch API, SDK helper,
and legacy internal changestream removal work landed on the coordinated
`add_callbacks` branches.

## Current branch heads

At the time this work was parked:

- `mycel-api/add_callbacks`: `3be68b4 Clarify graph change watch checkpoint and field filters`
- `mycel/add_callbacks`: `f9f70ed Remove legacy internal changestream`
- `mycel-go-sdk/add_callbacks`: `7d5aa39 Regenerate graph change comments`
- `mycel-rust-sdk/add_callbacks`: `96c20f8 Update API submodule after graph change comments`

## Completed baseline

The parked branch line includes:

- public `operation_id` fields on transaction begin/transaction/commit responses;
- daemon UUID generation and UUID validation for client-provided operation IDs;
- Go and Rust SDK operation ID helpers;
- replacement of the old public change-stream API with
  `GraphChangeService.WatchGraphChanges`;
- public graph-change watch helpers in the Go and Rust SDKs;
- internal committed graph-change notification subsystem;
- daemon graph-change watch backed by the notification subsystem;
- raft leader fail-closed behavior for notification/watch delivery;
- checkpoint/resume/gap semantics for public watches;
- filtered public envelope affected-node/edge IDs;
- best-effort `changed_fields` documentation;
- automation migrated from the legacy internal changestream observer to direct
  graph-change notification consumption;
- deletion of the legacy internal `internal/changestream/service` module.

## Validation baseline

The following validation passed before parking:

```sh
# mycel-api
make test

# mycel
MYCEL_API_ROOT=../mycel-api make test
make docs-check
git diff --check

# mycel-go-sdk
MYCEL_API_ROOT=../mycel-api make generate
go test ./...

# mycel-rust-sdk
MYCEL_API_ROOT=/Users/martinbeauvais/Projects/knotbase/Knotbase/myceldb/mycel-api make ci
```

A final focused review after removing the legacy internal changestream path
reported no blockers.

## Remaining work

### AC-F1 — Downstream compatibility check

Goal: confirm downstream applications do not depend on removed public
change-stream symbols, or migrate them to graph-change watch.

Tasks:

1. Search downstream repos for removed public symbols:
   - `ChangeStreamService`
   - `WatchDomainChanges`
   - `WatchDomainChangesRequest`
   - `WatchDomainChangesResponse`
   - `ChangeEventType`
2. If unused, record that no downstream migration is required.
3. If used, migrate callers to:
   - `GraphChangeService.WatchGraphChanges`
   - `WatchGraphChangesRequest`
   - `GraphChangeType`
   - `GraphChangeEvent`
4. Update callers to handle `gap` by resyncing/rebuilding local state and
   reconnecting with a fresh checkpoint.
5. Update callers to persist the last processed `event.revision` and resume
   with `after_revision`.

Expected validation:

```sh
rg 'ChangeStreamService|WatchDomainChanges|WatchDomainChangesRequest|WatchDomainChangesResponse|ChangeEventType' ../knot_pkm ../orchestration || true
```

Exit criteria:

- Downstream usage is migrated or explicitly recorded as absent.

### AC-F2 — Client-facing examples and migration docs

Goal: provide concise examples for the new operation correlation and graph-change
watch lifecycle.

Tasks:

1. Add examples showing how to generate an operation ID.
2. Add examples showing how to begin a transaction with an operation ID.
3. Add examples showing how to watch graph changes.
4. Add examples showing how to ignore own writes by comparing
   `event.origin.operation_id`.
5. Add examples showing how to persist `event.revision` and resume with
   `after_revision`.
6. Document that SDK watch helpers refresh/retry only during setup; callers own
   reconnect/resume after terminal stream responses, gaps, or transport errors.

Expected validation:

```sh
make docs-check
```

Exit criteria:

- Public docs include operation correlation and graph-change watch lifecycle
  examples.

### AC-F3 — Cross-repo integration notes

Goal: prepare PR/release notes for the four coordinated repos.

Tasks:

1. Document the breaking API change:
   - old `ChangeStreamService.WatchDomainChanges` removed;
   - new `GraphChangeService.WatchGraphChanges` added.
2. Document new operation ID fields and SDK helpers.
3. Document graph-change watch checkpoint/resume/gap semantics.
4. Document validation commands and results for each repo.
5. Record branch heads at handoff time.

Exit criteria:

- PR descriptions or integration notes are ready for `mycel-api`, `mycel`,
  `mycel-go-sdk`, and `mycel-rust-sdk`.

### AC-F4 — Optional current-doc cleanup

Goal: remove confusing non-historical references to the deleted legacy internal
changestream package.

Tasks:

1. Review current docs that still mention `internal/changestream/service`.
2. Preserve historical release-plan references when they are intentionally
   historical.
3. Update current design/operations docs to refer to graph-change notification
   instead of the deleted internal module.
4. Keep public CLI compatibility language clear: `change-stream` and `changes`
   may remain CLI aliases, but the public API is graph-change watch.

Candidate searches:

```sh
rg 'internal/changestream|changestream/service|ChangeStreamService|WatchDomainChanges' docs
```

Expected validation:

```sh
make docs-check
git diff --check
```

Exit criteria:

- Current docs no longer instruct operators or contributors to use deleted
  package paths or removed public RPCs.

### AC-F5 — Final cross-repo validation before merge

Goal: prove all four coordinated branches remain compatible before PR merge or
release.

Commands:

```sh
# mycel-api
make test

# mycel
MYCEL_API_ROOT=../mycel-api make test
make docs-check
git diff --check

# mycel-go-sdk
MYCEL_API_ROOT=../mycel-api make generate
go test ./...

# mycel-rust-sdk
MYCEL_API_ROOT=/Users/martinbeauvais/Projects/knotbase/Knotbase/myceldb/mycel-api make ci
```

Exit criteria:

- All validation commands pass from clean working trees.
- Generated public SDK/API code changes are intentional and reviewed.
- Generated ANTLR/internal generated code is not committed unless explicitly
  approved.

## Non-goals

- No automatic restore, repair, merge, rebalance, PVC repair, or
  authoritative-node selection.
- No graph callbacks attached to mutable nodes.
- No use of `operation_id` for authorization, idempotency, replay protection,
  or ordering.
- No automatic SDK reconnect/resume loop after watch streams terminate.
- No new public `commit_id` field on graph-change events.
