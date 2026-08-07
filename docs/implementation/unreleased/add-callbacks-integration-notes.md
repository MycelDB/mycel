# add_callbacks Integration Notes

Status: draft integration notes for the coordinated `add_callbacks` branches.

These notes summarize the remaining integration work completed after the branch
was parked: downstream compatibility, migration examples, current-doc cleanup,
and final validation.

## Branches

Current coordinated branches before this follow-up documentation pass:

- `mycel-api/add_callbacks`: `3be68b4 Clarify graph change watch checkpoint and field filters`
- `mycel/add_callbacks`: `71891a0 Document add_callbacks parking lot`
- `mycel-go-sdk/add_callbacks`: `7d5aa39 Regenerate graph change comments`
- `mycel-rust-sdk/add_callbacks`: `96c20f8 Update API submodule after graph change comments`

## Breaking API migration

The old public Client change-stream service is removed on `add_callbacks`:

- removed: `ChangeStreamService.WatchDomainChanges`
- removed: `WatchDomainChangesRequest`
- removed: `WatchDomainChangesResponse`
- removed: `ChangeEvent` / `ChangeEventType` from the old public stream API

Use the new committed graph-change watch API instead:

- added: `GraphChangeService.WatchGraphChanges`
- added: `WatchGraphChangesRequest`
- added: `WatchGraphChangesResponse`
- added: `GraphChangeEvent`
- added: `GraphObjectChange`
- added: `GraphChangeType`
- added: `GraphChangeOrigin`
- added: `GraphChangeGap`
- added: `GraphChangeCheckpoint`

The service streams committed graph changes for one `space_id` + `domain_id`.
It is not a webhook/callback registration surface and does not stream
uncommitted transaction operations.

## Watch lifecycle semantics

- `include_current` sends a checkpoint with the current observed revision before
  live events.
- `after_revision` means replay events after the caller's last processed event
  revision.
- If `include_current` and explicit `after_revision` are both set, replay after
  `after_revision` may follow the checkpoint even when replayed event revisions
  are less than or equal to the checkpoint revision.
- `GraphChangeGap` is sent when requested history is unavailable. Clients should
  invalidate or rebuild derived state from authoritative graph reads and then
  reconnect from a fresh checkpoint.
- SDK helpers refresh/retry only while opening the stream. They do not run an
  automatic reconnect/resume loop after stream termination.
- Callers should persist the last successfully processed `event.revision` and
  use it as `after_revision` when reconnecting.

## Operation ID semantics

`BeginTransactionRequest.operation_id` is optional transaction correlation
metadata:

- when omitted, the daemon generates a UUID;
- when present, the daemon validates it as a UUID string;
- `GraphTransaction.operation_id` and `TransactionCommit.operation_id` echo the
  operation ID;
- `GraphChangeOrigin.operation_id` exposes it on committed graph-change events.

`operation_id` is not authorization, idempotency, replay protection, conflict
resolution, or ordering metadata. Clients may use it to correlate their own
write workflow with later graph-change watch events and optionally skip their own
writes in local cache invalidation code.

## Downstream compatibility check

Search command:

```sh
rg 'ChangeStreamService|WatchDomainChanges|WatchDomainChangesRequest|WatchDomainChangesResponse|ChangeEventType' ../knot_pkm ../orchestration ../myceldb/mycel-bench ../myceldb/mycel-admin || true
```

Result:

- No downstream application code usages were found in Knot PKM or orchestration.
- `mycel-bench/docs/perf-test-plan.md` contained benchmark-plan references to
  `WatchDomainChanges`; those were updated to
  `GraphChangeService.WatchGraphChanges`.

## Client examples

Examples were added or expanded in:

- `mycel/docs/design/api/change-stream.md`
- `mycel/docs/design/api/session-transaction.md`
- `mycel-go-sdk/README.md`
- `mycel-rust-sdk/README.md`
- `mycel-api/README.md`

The examples cover:

- generating an operation ID;
- beginning a read-write transaction with that operation ID;
- watching graph changes;
- ignoring a write by matching `event.origin.operation_id`;
- persisting `event.revision` and resuming with `after_revision`;
- handling `gap` by rebuilding/resyncing derived state.

## Current-doc cleanup

Current design/operations docs were updated to avoid presenting the deleted
legacy `internal/changestream/service` package as current architecture. Historical
release plans may still mention that package as past implementation history.

## Validation

Final follow-up validation passed with:

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

Before merging or releasing, re-run the same commands from clean working trees
and record the exact branch heads in the PR descriptions.
