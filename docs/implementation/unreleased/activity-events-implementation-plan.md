# Activity events implementation plan

## Status

First Activity Events tranche implemented across API, daemon storage/service,
Admin API authorization, CLI, SDK client surfaces, Console Activity page, and
operator docs. Phase 6 destructive retention pruning/export hardening remains
documented but intentionally disabled until retention semantics are explicitly
finalized.

## Context

Mycel needs a first-class, durable operator-facing event stream for important
lifecycle, identity/access, space/domain, backup/restore, cluster, semantic,
automation, and external pod/service activity. The design is described in
[Activity Events Design](../../design/activity/README.md).

The event stream should power the Console **Overview / Activity** page and give
operators a reliable history of important facts without scraping process logs or
misusing graph-change notifications, inference usage events, or backup
manifests.

## Goals

- Add an Activity subsystem with durable append-only event storage.
- Add Admin APIs to append, list, and fetch activity events.
- Emit initial daemon-owned events for daemon lifecycle, identity/access,
  spaces/domains, backups/restores, and cluster degraded/readiness changes.
- Allow trusted external pods/services to append sanitized activity events with
  idempotency keys.
- Replace the Console `/activity` placeholder with a filterable activity stream.
- Keep reads bounded by indexed storage; do not rely on unbounded historical
  scans.
- Preserve safety-critical raft behavior: activity events are evidence, not
  authority, and must not trigger automatic repair/rebalance/merge behavior.

## Non-goals

- Do not mirror ordinary process logs into Activity.
- Do not replace subsystem-specific authoritative records such as backup
  manifests, automation runs, semantic maintenance work items, or graph-change
  streams.
- Do not add automatic repair or reconciliation behavior based on events.
- Do not store secrets, tokens, plaintext credentials, private keys, or raw
  provider responses in event metadata.
- Do not require a fully distributed/raft-owned global event log in the first
  tranche if a safe standalone/file-backed implementation can ship first.

## Phase 0 — API and storage contract design

### Work

- Add protobuf source in `../mycel-api` for Admin Activity APIs, likely:
  - `api/proto/mycel/admin/v1/activity.proto`
- Define messages:
  - `ActivityEvent`
  - `ActivityEventSource`
  - `ActivityEventActor`
  - `ActivityEventResource`
  - `AppendActivityEventRequest/Response`
  - `ListActivityEventsRequest/Response`
  - `GetActivityEventRequest/Response`
- Define stable enum/string values for:
  - severity: `info`, `warning`, `error`
  - category: `lifecycle`, `identity`, `access`, `space`, `domain`, `backup`,
    `cluster`, `semantic`, `automation`, `external`
- Decide whether severity/category are proto enums or validated strings.
  - Recommendation: validated strings for easier extension before release, with
    generated constants/helpers in daemon code.
- Define pagination and filter fields:
  - `since`, `until`
  - severity/category/type
  - source node/pod/component/service
  - actor principal id
  - resource kind/id
  - correlation id
  - page size/page token
- Define append idempotency behavior:
  - `(source.service || source.component || source.node_id, idempotency_key)`
    deduplicates retries.

### Acceptance

- API source compiles and generated stubs expose all required request/response
  fields.
- No generated public SDK/API code is committed unless explicitly approved.
- API docs clearly distinguish Activity from process logs and graph-change
  notifications.

## Phase 1 — Daemon domain model, validation, and storage

### Work

- Add internal Activity subsystem packages, for example:
  - `internal/activity/model`
  - `internal/activity/storage`
  - `internal/activity/service`
- Define internal model types mirroring the API shape.
- Implement validation:
  - required `severity`, `category`, `type`, `message`, `source`
  - bounded message length
  - bounded metadata size/depth
  - valid timestamps; default missing `occurred_at` to `ingested_at`
  - reject or sanitize obviously secret-bearing metadata keys where possible
- Implement file-backed append-only store with indexes sufficient for bounded
  Console reads:
  - time index
  - severity/category/type indexes
  - actor principal id index
  - resource kind/id index
  - source node/pod/component/service index
  - correlation id index
  - idempotency key index
- Add retention configuration placeholders but keep destructive pruning disabled
  until retention semantics are explicitly implemented.

### Acceptance

- Unit tests cover validation, secret-key rejection/sanitization, append, get,
  list pagination, filters, and idempotency.
- Common list filters are bounded by indexes or capped recent windows.
- Store remains append-only for normal writes.

## Phase 2 — Admin API implementation and authorization

### Work

- Add Admin Activity service adapter under `internal/daemon/api/admin`.
- Wire the Activity service into daemon composition root without moving existing
  adapters out of `internal/daemon/api`.
- Add capabilities:
  - `audit.read` for listing/fetching activity events
  - `audit.write` for external service event append
- Ensure existing `audit.reader` role includes `audit.read`.
- Decide and implement service-principal grant path for `audit.write`.
- API behavior:
  - `AppendActivityEvent` requires `audit.write` for external callers.
  - daemon-internal emitters may call subsystem service directly.
  - `ListActivityEvents` and `GetActivityEvent` require `audit.read`.
  - invalid input returns `InvalidArgument`.
  - denied access returns `PermissionDenied`.
  - missing event returns `NotFound`.

### Acceptance

- Admin Activity API tests cover append/read authorization, validation errors,
  not found, and idempotent append.
- Capability/role normalization includes `audit.write` if added as a new
  capability.
- Existing identity/access tests continue to pass.

## Phase 3 — Internal event emitters

### Work

Add helper functions so subsystem code emits consistent events without repeated
string literals.

Initial event emitters:

- daemon lifecycle:
  - `daemon.started`
  - `daemon.stopping`
  - `daemon.stopped`
  - `daemon.config.loaded` if useful and sanitized
- identity/access:
  - `principal.created`
  - `principal.disabled`
  - `principal.password.changed`
  - `principal.session.revoked`
  - `role.granted`, `role.revoked`
  - `capability.granted`, `capability.revoked`
- spaces/domains:
  - `space.created`, `space.deleted`, `space.degraded`, `space.recovered`
  - `domain.created`, `domain.deleted`, `domain.schema.updated`
- backup/restore:
  - `backup.started`, `backup.completed`, `backup.failed`
  - `restore.started`, `restore.completed`, `restore.failed`
- cluster/raft:
  - `cluster.node.joined`, `cluster.node.left`
  - `raft.group.started`
  - `raft.readiness.degraded`

Implementation rules:

- Emit success lifecycle/mutation events only after authoritative mutations are
  accepted/committed.
- Failure events must not hide the original API error.
- Security-sensitive events should include identifiers and fingerprints, not
  secret values.
- Event emission failure must be logged and surfaced in tests, but should not
  roll back unrelated non-security mutations unless that event is explicitly
  required for forensic integrity.

### Acceptance

- Unit/integration tests verify representative events are emitted for daemon
  lifecycle, principal creation/disablement, space/domain creation, and backup
  lifecycle.
- Tests verify event metadata omits credential material and active tokens.
- No automatic repair/rebalance/merge behavior is introduced.

## Phase 4 — CLI and SDK support

### Work

- Add CLI commands for operator use:
  - `mycel admin activity list`
  - `mycel admin activity get <event-id>`
  - `mycel admin activity append --file event.json` for service/operator
    testing and external integrations
- Add examples for external event payloads.
- Update Go and Rust SDK surfaces if generated clients are not sufficient for a
  convenient append/list/get workflow.
- Ensure SDK examples use idempotency keys for retrying external emitters.

### Acceptance

- CLI tests cover list/get/append validation and output formats.
- Docs mention safe external event reporting with service principals.
- SDK helpers/examples do not store or transmit secrets in event metadata.

## Phase 5 — Console Activity page

### Work

Repository: `../mycel-console`

- Replace `/activity` placeholder with a real Activity page under the Overview
  group.
- Add Tauri Admin commands:
  - `admin_list_activity_events`
  - `admin_get_activity_event`
  - optional `admin_append_activity_event` only if Console should provide manual
    operator event append/debugging.
- Add frontend service/types.
- Activity page default view:
  - newest first
  - severity icon/badge
  - category/type
  - message
  - source pod/node/component/service
  - actor
  - resource
  - occurred/ingested timestamps
  - expandable metadata/details drawer
- Filters:
  - severity
  - category
  - type
  - source
  - actor principal
  - resource kind/id
  - time range
- Deep-link rows to subsystem pages where IDs are present:
  - space/domain detail
  - backup detail/status if available
  - semantic rule/activity
  - automation run/binding
  - principal detail

### Acceptance

- `/activity` no longer shows the placeholder.
- Activity page works for auditors with `audit.read`.
- Missing `audit.read` shows capability-gated read-only/unavailable messaging.
- Frontend tests cover rendering, filtering, empty state, errors, and details.
- Console build and Tauri cargo check pass.

## Phase 6 — Retention, export, and cluster behavior hardening

### Work

- Implement conservative retention controls:
  - max age
  - max event count
  - max bytes
  - category-specific overrides if required
- Emit retention summary events when pruning occurs.
- Add backup/export integration for operator-forensic bundles.
- Harden raft/cluster semantics:
  - identify which event categories must be raft-owned system metadata
  - define behavior when the authoritative Activity store is unavailable
  - ensure partitions preserve source node/pod identity
  - avoid claims that events prove cluster authority

### Acceptance

- Retention tests prove bounded growth and safe pruning.
- Export tests prove secrets/tokens are not included.
- Cluster-sensitive tests confirm activity failure does not bypass fail-closed
  raft metadata behavior.

## Phase 7 — Documentation and operations

### Work

- Add Admin API design doc for Activity if separate API details are warranted:
  - `docs/design/admin/activity.md`
- Add operations docs:
  - reading events from CLI
  - granting `audit.write` to external service principals
  - example Kubernetes pod/service event append
  - retention knobs
- Update Console docs/screenshots if present.

### Acceptance

- `make docs-check` passes.
- Operator docs clearly explain Activity versus logs and graph-change streams.
- Examples use explicit service principal credentials and idempotency keys.

## Validation checklist

Run after relevant phases:

```sh
cd ../mycel-api && make test
cd mycel && make test
cd mycel && make docs-check
cd ../mycel-console && MYCEL_API_ROOT="$(cd ../mycel-api && pwd)" cargo check --manifest-path src-tauri/Cargo.toml --no-default-features
cd ../mycel-console && npm test -- --runInBand
cd ../mycel-console && npm run build
git diff --check
```

For raft/clustering-sensitive changes, also consider:

```sh
make test-phase-d
make test-phase-e
make test-phase-f
make test-phase-g
```

Do not run destructive Compose/K3s validation unless explicitly requested.

## Rollout order

Recommended delivery order:

1. API/model/storage behind no emitters.
2. Admin API authorization and CLI list/get/append.
3. Daemon lifecycle and basic identity/space/domain emitters.
4. Console Activity page.
5. Backup/cluster/semantic/automation emitters.
6. Retention/export and raft hardening.

This keeps each tranche functional and reviewable while making the Console page
useful early.

## Risks and mitigations

| Risk | Mitigation |
| --- | --- |
| Event log becomes noisy like process logs | Keep curated event type registry and avoid high-volume debug events. |
| Sensitive data leaks through metadata | Validate size/depth, reject obvious secret keys, add tests, and require sanitized subsystem helpers. |
| Activity is mistaken for authority | Docs/API comments must state events are evidence only; raft metadata remains authoritative. |
| Unbounded list queries | Require indexed filters/page tokens and cap recent-window fallbacks. |
| Event emission failure impacts critical paths inconsistently | Define per-event criticality; log non-critical failures; consider same-transaction emission for security-sensitive events only. |
| Multiple pods duplicate events on retry | Require idempotency keys for external emitters and use source+idempotency dedupe. |

## Open decisions

- Should external write capability be named `audit.write`, `activity.write`, or
  both with normalization aliases?
- Which events are forensic-critical enough that mutation should fail if the
  event cannot be persisted?
- Which categories need longer retention by default?
- Should the first store be standalone file-backed only, or should system
  raft-owned storage be part of the first implementation?
- Should Console support manual operator event append, or should append remain
  CLI/API-only for now?
