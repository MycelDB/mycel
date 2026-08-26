# Activity Events Design

Activity events are Mycel's operator-facing event stream for important daemon,
cluster, identity, access, space/domain, backup, semantic, and automation
activity. They are durable, queryable, authorization-protected records intended
for Console activity views, operator diagnostics, and external service
reporting.

Activity events are not a replacement for process logs. Logs remain the right
place for high-volume debugging detail. Activity events are curated records for
facts an operator may need to answer questions such as:

- when did this daemon or pod start or stop?
- who created, disabled, or revoked this principal?
- when was a space or domain created, deleted, restored, or made degraded?
- which node/pod reported a cluster warning?
- which backup or restore operation started, completed, or failed?
- which external service reported an application-level operational event?

The Console presents these records through the **Activity** page.

## Goals

- Provide a first-class, daemon-owned event log subsystem for important
  operational and audit-adjacent events.
- Let every daemon pod record local lifecycle and operational events without
  depending on stdout scraping.
- Let trusted external pods/services append events through an Admin API using a
  service principal.
- Make events queryable by time, severity, category, source, actor, and
  resource.
- Keep event writes safe and non-destructive. Activity events must not trigger
  automatic repair, merge, rebalance, or other operational mutation.
- Preserve enough structured context for Console filtering and future export.

## Non-goals

- Do not mirror every log line into Activity.
- Do not use Activity events as the source of truth for system raft metadata,
  graph data, identity state, backup manifests, or semantic maintenance state.
- Do not use Activity events as a distributed command bus.
- Do not include secrets, plaintext passwords, tokens, credential material, or
  unsanitized provider responses in event metadata.
- Do not automatically infer an authoritative node or repair split-brain state
  from activity records.

## Terminology

- **Activity event**: a single durable record describing an important fact.
- **Emitter**: daemon subsystem or external service that appends an event.
- **Source**: process, node, pod, component, or service that emitted the event.
- **Actor**: principal on whose behalf the event occurred, when applicable.
- **Resource**: primary object the event describes, such as a space, domain,
  principal, backup, automation run, or cluster node.
- **Idempotency key**: caller-provided stable key used to deduplicate retries.

## Event model

A normalized event record has this shape conceptually:

```json
{
  "eventId": "evt_...",
  "occurredAt": "2026-08-26T15:00:00Z",
  "ingestedAt": "2026-08-26T15:00:01Z",
  "severity": "info",
  "category": "space",
  "type": "space.created",
  "message": "Space created",
  "source": {
    "nodeId": "node_...",
    "podName": "mycel-0",
    "component": "space"
  },
  "actor": {
    "principalId": "prn_...",
    "username": "admin"
  },
  "resource": {
    "kind": "space",
    "id": "sp_...",
    "name": "Team Knowledge Base"
  },
  "correlationId": "op_...",
  "idempotencyKey": "space.created:sp_...",
  "metadata": {
    "domainCount": 2
  }
}
```

### Required fields

| Field | Description |
| --- | --- |
| `event_id` | Daemon-assigned event identifier. |
| `occurred_at` | Time the event happened according to the emitter. |
| `ingested_at` | Time the daemon accepted the event. |
| `severity` | `info`, `warning`, or `error`. |
| `category` | Broad grouping used by Console filters. |
| `type` | Stable dotted event type. |
| `message` | Short operator-facing summary. |
| `source` | Component/process/service that emitted the event. |

### Optional fields

| Field | Description |
| --- | --- |
| `actor` | Principal context for user/admin/service actions. |
| `resource` | Primary resource affected by the event. |
| `correlation_id` | Operation, request, run, or transaction correlation id. |
| `idempotency_key` | Stable deduplication key for retrying emitters. |
| `metadata` | Sanitized structured context. |

## Severities

| Severity | Use |
| --- | --- |
| `info` | Normal lifecycle or administrative activity. |
| `warning` | Degraded but non-fatal state, denied action, retryable failure, or configuration concern. |
| `error` | Failed operation, unavailable dependency, invariant violation, or operator action required. |

Severity is not authorization. Sensitive events remain access-controlled even
when severity is `info`.

## Categories and example types

Initial categories should be broad and stable. New event types can be added
without changing category names.

| Category | Example types |
| --- | --- |
| `lifecycle` | `daemon.started`, `daemon.stopping`, `daemon.stopped`, `daemon.config.loaded` |
| `identity` | `principal.created`, `principal.disabled`, `principal.password.changed`, `principal.session.revoked` |
| `access` | `capability.granted`, `capability.revoked`, `role.granted`, `role.revoked`, `credential.revoked` |
| `space` | `space.created`, `space.deleted`, `space.degraded`, `space.recovered` |
| `domain` | `domain.created`, `domain.deleted`, `domain.schema.updated` |
| `backup` | `backup.started`, `backup.completed`, `backup.failed`, `restore.started`, `restore.completed`, `restore.failed` |
| `cluster` | `cluster.node.joined`, `cluster.node.left`, `raft.group.started`, `raft.readiness.degraded` |
| `semantic` | `semantic.rule.created`, `semantic.backfill.started`, `semantic.backfill.failed` |
| `automation` | `automation.binding.created`, `automation.run.started`, `automation.run.failed` |
| `external` | `external.service.started`, `external.service.stopped`, `external.service.warning` |

Event type names should be stable and machine-filterable. The `message` field
can evolve for clarity.

## Emitters

### Daemon internal emitters

Daemon subsystems append events at important lifecycle and mutation points:

- daemon startup and shutdown
- identity principal creation, disablement, password reset, role/capability
  grant, and session revocation
- space and domain creation/deletion
- schema update
- backup/restore start, completion, failure, and cancellation
- cluster readiness, raft group startup, and degraded state transitions
- semantic generation rule lifecycle and backfill lifecycle
- graph automation binding lifecycle and run failure summaries

For mutation APIs, event emission should happen after the authoritative mutation
is accepted/committed. Failed mutations can emit failure events when useful, but
must not obscure the original API error.

### External service emitters

Other pods and services can report events through an Admin API using service
principal credentials. External event writes require an explicit activity write
capability. External emitters should provide:

- event type
- severity
- source service/pod/component
- message
- resource, actor, correlation id, and metadata when available
- idempotency key for retry safety

The daemon assigns `event_id` and `ingested_at` and validates/sanitizes the
record before storage.

## API design

Activity APIs belong under the Admin API surface because they are operator and
service-principal surfaces, not ordinary graph-data APIs.

Initial Admin RPCs:

- `AppendActivityEvent`
- `ListActivityEvents`
- `GetActivityEvent`

### AppendActivityEvent

Used by trusted external emitters and by daemon subsystem adapters when they go
through the API layer.

Important behavior:

- requires activity write capability
- validates severity, category, type, source, message, and metadata size
- assigns `event_id` and `ingested_at`
- preserves caller `occurred_at` when reasonable, otherwise defaults it to
  `ingested_at`
- deduplicates by `(source, idempotency_key)` when an idempotency key is present
- rejects metadata that exceeds configured size limits
- rejects known secret-bearing field names when possible and relies on emitter
  discipline for semantic sanitization

### ListActivityEvents

Filters:

- time range: `since`, `until`
- severity
- category
- type
- source node id / pod / component / service
- actor principal id
- resource kind/id
- correlation id
- page size and page token

Default ordering is newest first. Page tokens must be stable across normal
pagination and should not require unbounded scans.

### GetActivityEvent

Fetches one event by id for detail drawers, deep links, and diagnostics.

## Authorization

Recommended capabilities:

| Capability | Meaning |
| --- | --- |
| `audit.read` | Read activity/audit/diagnostic events. Existing auditor role should include this. |
| `audit.write` | Append external activity events. Intended for service principals. |

Daemon internals may append directly through subsystem services without going
through external capability checks, but all user/service API calls must be
authorized.

Activity reads may later support scope filtering by resource space/domain. The
initial reader capability is system/operator-oriented and should be conservative.

## Storage and indexing

The activity subsystem owns durable append-only storage. The initial storage can
be a per-node file-backed store integrated with the daemon WAL and snapshot
patterns used by other subsystems. Clustered operation should converge toward a
raft-owned system metadata event log for events that must be cluster-wide.

Required indexes for bounded reads:

- `occurred_at` / `ingested_at`
- severity
- category and type
- source node/pod/component/service
- actor principal id
- resource kind/id
- correlation id
- idempotency key

Activity listing must not require unbounded historical scans for common Console
queries.

## Cluster and pod behavior

Multiple pods may emit events concurrently. The design supports this by making
source identity explicit and by requiring idempotency keys for retrying external
emitters.

Cluster behavior principles:

- System raft metadata remains authoritative; activity events are evidence, not
  authority.
- Activity writes should fail closed when the authoritative activity store is not
  available, but the failure to emit a non-security informational event must not
  roll back unrelated committed graph/identity/space state.
- Security-sensitive audit events should be emitted in the same committed path as
  the mutation when feasible, or the mutation should record enough forensic
  context for recovery if event emission fails.
- During partitions, events should retain source node/pod identity so operators
  can reason about which side observed which fact.

## Retention and export

Activity events need retention controls to avoid unbounded growth.

Recommended retention knobs:

- max age
- max event count
- max bytes
- category-specific overrides for security/audit events

Retention deletion is local housekeeping, not repair. It should emit its own
summary event when records are pruned.

Future export should support operator-forensic bundles and backup integration,
while continuing to omit secrets and active sessions/tokens.

## Console design

The Console **Overview / Activity** route should replace the current placeholder
with a real activity stream.

Default view:

- newest events first
- severity badge/icon
- category/type
- message
- source pod/node/component
- actor
- resource
- occurred/ingested timestamps
- expandable metadata/detail drawer

Filters:

- severity
- category
- type
- source node/pod/component/service
- actor principal
- resource kind/id
- time range
- text search over message/type/source/resource labels if bounded by indexes or
  a capped recent window

Recommended Console grouping:

- lifecycle and cluster warnings near the top when active
- failed backup/restore and automation events highlighted
- semantic maintenance/automation/inference events can deep-link to their
  subsystem pages when IDs are present

## Operational semantics

Activity event emission should be simple for subsystem code:

```go
activity.Append(ctx, activity.Event{
    Severity: activity.SeverityInfo,
    Category: activity.CategorySpace,
    Type:     "space.created",
    Message:  "Space created",
    Actor:    activity.ActorFromContext(ctx),
    Resource: activity.Resource{Kind: "space", ID: spaceID, Name: spaceName},
})
```

Subsystems should use small helper functions for common events to keep event
names and metadata consistent.

## Security and privacy

Activity metadata must be sanitized. The following must never be stored:

- plaintext passwords
- access tokens or refresh tokens
- API keys or credential ciphertext/plaintext
- private key material
- raw provider responses that may contain sensitive prompt/user content
- active session tokens

For sensitive events, store identifiers, fingerprints, last-four displays, or
sanitized summaries instead.

## Relationship to existing event-like systems

| Existing system | Relationship |
| --- | --- |
| Process logs | High-volume local diagnostics; Activity is curated durable operator history. |
| Graph-change notifications | Domain graph mutation stream for graph consumers; Activity is cross-subsystem operator history. |
| Inference usage events | Workload usage ledger; Activity may reference failures/summaries but does not replace usage accounting. |
| Automation run records | Detailed automation execution state; Activity surfaces notable run lifecycle and failure summaries. |
| Backup manifests | Authoritative backup content metadata; Activity records backup lifecycle and operator-facing failures. |

## Initial implementation slices

A practical implementation can be delivered in slices:

1. Add protobuf/Admin API, model, validation, file-backed store, and CLI listing.
2. Emit daemon lifecycle, principal, space, domain, and backup events.
3. Add external `AppendActivityEvent` for service principals with idempotency.
4. Replace Console `/activity` placeholder with filterable event list.
5. Add cluster/raft metadata integration and retention/export controls.
6. Add semantic, automation, inference, and external application event helpers.

Each slice should remain safe and functional on its own.

## Open questions

- Should `audit.write` be a separate capability from future narrower
  `activity.write`, or should `audit.write` remain the canonical capability for
  all externally reported operator events?
- Which event categories require retention longer than ordinary operational
  events?
- Should some security-sensitive events fail the originating mutation when event
  persistence fails, or is forensic fallback sufficient?
- What minimum indexes are required for the first Console Activity page without
  overbuilding storage?
