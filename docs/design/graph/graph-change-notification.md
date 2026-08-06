# Graph-change notification subsystem

## Status

Implemented internal design for the `add_callbacks` branch. This document defines
the internal notification subsystem and canonical graph-change model. It
intentionally does not define the public streaming API or future GraphQL
subscription syntax; those surfaces should be layered on top of this subsystem
after the internal model is stable.

## Purpose

mycel needs one internal mechanism for observing committed graph changes so that:

- user interfaces can detect that graph data they are editing changed elsewhere;
- internal caches can invalidate or update derived entries after commits;
- existing and future change-stream surfaces can share one event vocabulary;
- future GraphQL subscriptions can be implemented without coupling core graph
  writes to GraphQL runtime code.

The graph-change notification subsystem is the internal broker for committed
graph change events. The graph service publishes committed events to this
subsystem after successful graph commits. The subsystem matches events against
registered consumers and invokes consumer handlers asynchronously.

## Non-goals

This design does not add:

- a public GraphQL subscription API;
- a public webhook/callback registration API;
- automatic graph repair, merge, conflict resolution, rebalance, or cache rebuild;
- uncommitted transaction operation streams;
- automatic authoritative-node selection;
- follower-served stale notifications in raft mode.

## Terminology

| Term | Meaning |
| --- | --- |
| Graph service | The graph subsystem that applies graph writes and commits graph transactions. |
| Committed graph change event | A transaction-level fact that graph data changed after a successful commit. |
| Graph-change notification subsystem | The internal subsystem that records/enqueues committed graph events, matches registrations, and delivers events to consumers. |
| Consumer | Internal subsystem code interested in graph changes, such as a cache invalidator or semantic dirty marker. |
| Consumer handler | The method invoked for a consumer when a matching event is delivered. |
| Registration | A consumer's declared interest in graph changes, including scope, filters, projection, and resume point. |
| Scope | The graph area covered by a registration, such as a space/domain or specific nodes. |
| Filter | Event predicates inside a scope, such as event types, labels, node IDs, edge IDs, or changed fields. |
| Projection | The event fields and graph object fields delivered to a consumer. |
| Checkpoint | The last successfully processed graph revision for a consumer. |
| Gap | A condition where the requested resume revision is older than retained event history. |
| Origin metadata | Optional write-origin information echoed in events so clients can distinguish their own writes from external writes. |

Use `consumer handler` in code and docs. `Callback` is acceptable in product
conversation, but the implementation should use consumer/handler terminology.

## Current related code

The repository already contains overlapping graph-change structures:

- `internal/graph/change.CommittedEvent` and `graphchange.Sink`;
- `internal/graph/service.GraphChange` in graph commit results;
- `internal/changestream/service.GraphChange` for the current change-stream
  subsystem.

The graph-change notification design should make `internal/graph/change` the
canonical internal graph-change model and migrate duplicate structures toward it.

## Canonical graph-change model

`internal/graph/change` should define the internal event vocabulary. It must stay
lightweight and subsystem-neutral.

It may depend on:

- standard library packages;
- `internal/graph/model`;
- `internal/space/model`.

It must not depend on:

- daemon API packages;
- change-stream service packages;
- automation service packages;
- semantic service packages;
- graph service internals.

Suggested canonical model:

```go
package graphchange

type CommittedEvent struct {
    ID            uuid.UUID
    CommitID      uuid.UUID
    TransactionID uuid.UUID

    SpaceID       domainspace.SpaceID
    DomainID      graph.DomainID
    Revision      uint64
    CommittedAt   time.Time

    Origin        OriginMetadata
    Changes       []Change

    // Aggregate fields may remain for efficient consumers that only need broad
    // invalidation or semantic maintenance hints.
    CreatedNodeIDs []graph.NodeID
    UpdatedNodeIDs []graph.NodeID
    DeletedNodeIDs []graph.NodeID
    ChangedEdgeIDs []graph.EdgeID
    AffectedNodeIDs []graph.NodeID
}

type Change struct {
    Type ChangeType

    NodeID graph.NodeID
    EdgeID graph.EdgeID

    Node    *graph.Node
    OldNode *graph.Node
    Edge    *graph.Edge
    OldEdge *graph.Edge

    ChangedFields []string
    AffectedNodeIDs []graph.NodeID
}

type ChangeType string

const (
    ChangeTypeNodeCreated ChangeType = "node_created"
    ChangeTypeNodeUpdated ChangeType = "node_updated"
    ChangeTypeNodeDeleted ChangeType = "node_deleted"
    ChangeTypeEdgeCreated ChangeType = "edge_created"
    ChangeTypeEdgeUpdated ChangeType = "edge_updated"
    ChangeTypeEdgeDeleted ChangeType = "edge_deleted"
    ChangeTypeRevisionAdvanced ChangeType = "revision_advanced"
)

type OriginMetadata struct {
    ClientID         string
    ClientInstanceID string
    OperationID      string
    SessionID        string
    TransactionID    string
    UserID           string
}
```

The exact Go field names can be refined during implementation. The important
contract is that `CommittedEvent` is the transaction-level envelope and `Change`
is the per-object change detail.

## Producer model

The graph service is the producer. After a graph transaction successfully
commits, and only after it is durable/visible, the graph service publishes one
committed graph change event:

```text
graph transaction commit succeeds
  -> graph service builds CommittedEvent
  -> graph service publishes event to graph-change notification subsystem
  -> graph-change notification subsystem handles delivery
```

The graph commit path should not invoke arbitrary consumer code directly. It
should only publish/enqueue the committed event into the notification subsystem.
Consumer handler invocation happens outside the critical commit path.

## Graph-change notification subsystem responsibilities

The subsystem owns:

1. receiving committed graph change events;
2. recording or buffering recent events for resumability;
3. maintaining process-local V1 registrations;
4. matching events against registration scope/filter;
5. applying projections before delivery;
6. invoking internal consumer handlers asynchronously;
7. recording handler failures and delivery diagnostics;
8. detecting resume gaps;
9. supporting leader-routed operation in raft mode.

The subsystem must not decide how a cache repairs itself. On a gap, it reports
the gap. Each consumer decides whether to invalidate broadly, refresh a domain,
or rebuild its own derived state.

## Internal registration API

A cache or other subsystem should register as a consumer. The API should be
programmatic and internal, not GraphQL-specific.

V1 registrations are process-local. They exist only for the lifetime of the
current daemon process and are recreated by subsystem initialization after
restart. Durable consumer state belongs in consumer-owned checkpoints, not in the
registration table. If a daemon restarts, consumers register again and resume from
their persisted checkpoint.

Suggested shape:

```go
type Registrar interface {
    RegisterConsumer(
        ctx context.Context,
        spec ConsumerSpec,
        consumer Consumer,
    ) (Registration, error)
}

type Registration interface {
    Close() error
}

type Consumer interface {
    HandleGraphChange(ctx context.Context, event ProjectedEvent) error
    HandleGraphChangeGap(ctx context.Context, gap Gap) error
}
```

Suggested registration spec:

```go
type ConsumerSpec struct {
    ConsumerName string
    Scope        Scope
    Filter       Filter
    Projection   Projection
    Start        StartPosition
}

type Scope struct {
    SpaceID  string
    DomainID string
    NodeIDs  []string
    EdgeIDs  []string
}

type Filter struct {
    EventTypes []ChangeType
    Labels     []string
    Fields     []string
}

type Projection struct {
    IncludeRevision        bool
    IncludeOrigin          bool
    IncludeAffectedNodeIDs bool
    IncludeAffectedEdgeIDs bool
    IncludeChangedFields   bool
    IncludeNodeSnapshot    bool
    IncludeEdgeSnapshot    bool

    // Optional selected fields for later refinement. V1 may accept this shape
    // while initially supporting only coarse include flags.
    NodeFields []string
    EdgeFields []string
}

type StartPosition struct {
    AfterRevision *uint64
}
```

Example cache registration:

```go
registration, err := graphChanges.RegisterConsumer(ctx, ConsumerSpec{
    ConsumerName: "node-cache",
    Scope: Scope{
        SpaceID:  spaceID,
        DomainID: domainID,
    },
    Filter: Filter{
        EventTypes: []ChangeType{
            ChangeTypeNodeCreated,
            ChangeTypeNodeUpdated,
            ChangeTypeNodeDeleted,
            ChangeTypeEdgeCreated,
            ChangeTypeEdgeUpdated,
            ChangeTypeEdgeDeleted,
        },
    },
    Projection: Projection{
        IncludeRevision:        true,
        IncludeOrigin:          true,
        IncludeAffectedNodeIDs: true,
        IncludeAffectedEdgeIDs: true,
        IncludeChangedFields:   true,
    },
    Start: StartPosition{AfterRevision: cache.LastSeenRevision()},
}, cacheInvalidator)
if err != nil {
    return err
}
defer registration.Close()
```

A cache handler can then invalidate broadly:

```go
func (c *NodeCacheInvalidator) HandleGraphChange(ctx context.Context, event ProjectedEvent) error {
    for _, nodeID := range event.AffectedNodeIDs {
        c.cache.InvalidateNode(event.SpaceID, event.DomainID, nodeID)
    }
    return c.checkpoints.Save(ctx, event.SpaceID, event.DomainID, event.Revision)
}

func (c *NodeCacheInvalidator) HandleGraphChangeGap(ctx context.Context, gap Gap) error {
    c.cache.InvalidateDomain(gap.SpaceID, gap.DomainID)
    return c.checkpoints.Save(ctx, gap.SpaceID, gap.DomainID, gap.CurrentRevision)
}
```

## Projection behavior

Projection controls what a consumer receives. This keeps consumers from depending
on full graph objects when they only need invalidation keys.

Initial projection should support:

- revision and commit metadata;
- origin metadata;
- affected node IDs;
- affected edge IDs;
- changed field names;
- optional old and new node snapshots;
- optional old and new edge snapshots.

Old and new snapshots are both available in V1 but must be explicitly requested
through projection. Default cache-oriented projections should prefer revision,
origin, affected IDs, and changed fields rather than full snapshots.

Future GraphQL subscriptions can map GraphQL selection sets to this projection
model, but this subsystem does not need to parse GraphQL.

## Persistence, retention, resume, and gap behavior

Graph-change history is a local persistent bounded replay buffer. It is retained
for resumability, not auditing. It is derived from committed graph changes and is
not a separate authoritative history source.

V1 persistence model:

- graph data remains authoritative and durable through graph commit/WAL/raft
  mechanisms;
- graph-change event history is persisted locally as a bounded derived log;
- consumer checkpoints are persisted by the consumer that owns the derived state;
- loss or compaction of graph-change history is handled through explicit gaps.

Default V1 retention:

```text
max_events: 10,000 committed events per space/domain/partition
max_age:    24 hours
```

Whichever threshold is reached first may compact older events. These defaults are
intended to cover daemon restarts, UI reconnects, cache worker restarts, short
outages, and app sleep/wake. They are not an audit-log guarantee.

Consumers should be able to resume from the last processed graph revision:

```text
AfterRevision = last_successfully_processed_revision
```

If the subsystem still retains history after that revision, it replays matching
events and then continues with live events.

If the requested revision is older than retained history, the subsystem reports a
gap:

```go
type Gap struct {
    SpaceID string
    DomainID string
    RequestedAfterRevision uint64
    OldestAvailableRevision uint64
    CurrentRevision uint64
}
```

A gap does not imply automatic repair. Consumers must handle it explicitly, often
by invalidating a whole domain or rebuilding a derived cache.

This retained history is not intended for compliance or audit. If mycel needs an
audit log later, it should be a separate explicit audit subsystem with separate
retention, integrity, and authorization requirements.

## Origin metadata

V1 exposes a minimal public operation reference and keeps the broader origin
model internal. Clients may provide `BeginTransactionRequest.operation_id` as a
UUID string for the logical write operation. If omitted, the daemon generates a
UUID and returns it on `GraphTransaction.operation_id` and
`TransactionCommit.operation_id`.

The notification subsystem stores the operation ID in resolved origin metadata
and echoes it in committed events. The server also enriches internal origin
metadata with trusted server-side fields such as user ID, session ID, and
transaction ID.

Internal origin fields:

```text
operation_id        // public, client-provided or server-generated UUID
client_id           // internal/future
client_instance_id  // internal/future
label               // internal/future
user_id             // server-populated
session_id          // server-populated
transaction_id      // server-populated
```

This is important for applications like Knot PKM, where the same node may be open
in a main editor and a side panel. Origin metadata lets a client distinguish:

- its own write acknowledgement;
- a different tab or client instance from the same app;
- a different user or third-party client.

Origin metadata is advisory. Authorization and conflict handling remain separate
concerns.

## Raft-mode behavior

In raft mode, notifications must be leader-routed and derived from committed raft
application, not local file observation.

Required behavior:

- graph writes are accepted only by the owning partition leader or routed there;
- committed events are emitted only after the raft command is committed and
  applied;
- subscription/registration delivery for a partition is owned by the current
  partition leader;
- if leadership changes, active delivery may stop and consumers must reconnect or
  resume from checkpoint;
- follower-local stale notifications are not allowed by default;
- if the committed event source is unavailable, consumers fail closed or receive a
  gap/unavailable signal rather than reading local unproven state.

Graph-change event history should not be separately raft-owned in V1. In raft
mode it is a local persistent derived log built from committed/applied raft graph
commands. Because each replica applies the same committed graph changes, each
replica can maintain its own local derived replay buffer. After leadership
changes, the new leader serves from its local derived buffer when available; if it
cannot resume from a requested revision, it reports a gap.

This preserves system raft authority, read consistency, and fail-closed behavior
without adding raft write amplification for notification history.

## Delivery and failure semantics

The commit path should not be blocked by slow consumer handlers. Recommended
behavior:

- the producer publishes/enqueues the committed event synchronously enough for the
  subsystem's durability/resume contract;
- consumer handler invocation is asynchronous;
- one slow consumer does not block unrelated consumers;
- handler errors are recorded in diagnostics;
- consumers checkpoint only after successful handling;
- panics in consumer handlers are recovered and recorded as delivery failures.

For consumers that require strict processing, use ordered delivery per
space/domain/partition and checkpoint after each handled revision.

## Relationship to existing change streams

The existing Client Change Stream API is a public streaming surface. It should
become a consumer or adapter of the graph-change notification subsystem rather
than owning a separate graph-change vocabulary.

Migration target:

```text
graph service
  -> graph-change notification subsystem
      -> internal consumers
      -> Client Change Stream API adapter
      -> future GraphQL subscription adapter
```

## Consolidation plan

A safe implementation plan should avoid a large rewrite:

1. Extend `internal/graph/change` with canonical `Change`, `ChangeType`,
   projection, filter, and origin structures.
2. Make graph service produce canonical committed events and canonical per-object
   changes.
3. Adapt `internal/graph/service.CommitResult` to use the canonical change type
   or an alias.
4. Adapt `internal/changestream/service` to consume/adapt canonical graph-change
   events instead of defining a duplicate `GraphChange` vocabulary.
5. Add the graph-change notification subsystem registration API and internal
   delivery loop.
6. Move cache/derived-state consumers onto the registration API.
7. Later, design public streaming and GraphQL subscription surfaces on top of the
   same subsystem.

## Affected node IDs

`AffectedNodeIDs` must include edge endpoints for all edge changes in V1. For an
edge create, update, or delete, both `from_node_id` and `to_node_id` are affected
nodes. This supports broad cache invalidation without requiring each cache to
reconstruct endpoint context from edge payloads.

## Open questions

No open questions remain for the V1 design draft. Implementation may still refine
field names and package boundaries.
