# Client Change Stream API

## Status

Draft design for the daemon-oriented Client Change Stream API on the `refactor_daemon` branch.

The protobuf source of truth is:

```text
api/proto/mycel/client/v1/change_stream.proto
```

This document depends on:

```text
docs/design/access-control.md
docs/design/api/session-transaction.md
docs/design/api/graph.md
```

## Purpose

`ChangeStreamService` is a server-streaming Client API that lets clients and connectors watch committed changes for a space/domain.

It streams durable committed changes. It does not stream uncommitted transaction operations.

## Scope

The initial `ChangeStreamService` focuses on domain graph changes:

- committed transaction events
- node created/updated/deleted
- edge created/updated/deleted
- domain revision advancement
- checkpoints
- stream heartbeats

The initial service does not include:

- uncommitted transaction operation streams
- session state streams
- template/blob/domain metadata changes
- space-level watches
- admin/mesh event streams

Those can be added later as separate methods or services.

## Service definition

```protobuf
service ChangeStreamService {
  rpc WatchDomainChanges(WatchDomainChangesRequest) returns (stream WatchDomainChangesResponse);
}
```

## WatchDomainChanges

Watches committed graph changes for one space/domain.

Request:

```protobuf
message WatchDomainChangesRequest {
  string space_id = 1;
  string domain_id = 2;
  optional int64 after_revision = 3;
  bool include_current = 4;
  repeated ChangeEventType event_types = 5;
}
```

Response stream:

```protobuf
message WatchDomainChangesResponse {
  oneof message {
    ChangeCheckpoint checkpoint = 1;
    ChangeEvent event = 2;
    ChangeStreamHeartbeat heartbeat = 3;
  }
}
```

## Checkpoints

A checkpoint reports the daemon's current observed domain revision.

```protobuf
message ChangeCheckpoint {
  string space_id = 1;
  string domain_id = 2;
  int64 current_revision = 3;
  google.protobuf.Timestamp checkpoint_time = 4;
}
```

Checkpoints are useful for connector cache startup and synchronization.

When `include_current` is true, the daemon should send a checkpoint before streaming subsequent changes.

## Resume behavior

`after_revision` lets a connector resume from the last seen domain revision:

```text
WatchDomainChanges(after_revision = last_seen_revision)
```

The daemon should stream events after that revision.

If the daemon no longer has sufficient history to resume from the requested revision, it should return:

```text
OUT_OF_RANGE
```

This tells the connector to perform a full resync.

## Events

A change event describes one committed transaction/revision event.

```protobuf
message ChangeEvent {
  string event_id = 1;
  string space_id = 2;
  string domain_id = 3;
  int64 revision = 4;
  string commit_id = 5;
  google.protobuf.Timestamp event_time = 6;
  repeated GraphChange changes = 7;
}
```

`commit_id` and `revision` connect stream events to committed transaction metadata.

A single event may contain multiple graph changes from the same commit.

## Graph changes

Graph changes are typed as:

- node created
- node updated
- node deleted
- edge created
- edge updated
- edge deleted
- revision advanced

Create/update events should include full node/edge payloads where practical.

Delete events use ids because the object no longer exists in the current graph state.

```protobuf
message GraphChange {
  ChangeEventType type = 1;
  oneof subject {
    Node node = 2;
    Edge edge = 3;
    string node_id = 4;
    string edge_id = 5;
  }
}
```

## Heartbeats

Long-lived streams should send periodic heartbeats.

```protobuf
message ChangeStreamHeartbeat {
  google.protobuf.Timestamp heartbeat_time = 1;
}
```

Heartbeats help clients detect stalled streams and keep transport connections alive.

## Authorization

Watching graph changes requires:

```text
graph.read
```

The stream may include graph payloads, so the daemon must enforce graph visibility for the caller.

If future fine-grained node/subtree access is added, the stream must filter events accordingly.

## Error model

The protobuf does not define custom error messages for this draft. Implementations should use standard gRPC status codes.

Suggested mappings:

| Condition | gRPC status |
| --- | --- |
| missing/invalid access token | `UNAUTHENTICATED` |
| missing graph read capability | `PERMISSION_DENIED` |
| malformed request | `INVALID_ARGUMENT` |
| space/domain not found | `NOT_FOUND` |
| resume revision no longer available | `OUT_OF_RANGE` |
| stream rate/resource pressure | `RESOURCE_EXHAUSTED` |
| daemon shutting down/unavailable | `UNAVAILABLE` |

For normal Client API callers, returning `NOT_FOUND` for inaccessible spaces/domains can avoid leaking existence.

## Connector behavior

Connectors should:

- track the last seen domain revision
- resume with `after_revision`
- resync on `OUT_OF_RANGE`
- process events in revision order
- update local caches from create/update/delete payloads
- treat stream heartbeats as liveness signals

## Mesh implications

Streams represent the daemon's local view of committed domain revisions.

In mesh mode:

- stream events should follow domain revision order
- not all daemons may be equally up-to-date
- replication-applied commits should appear as committed changes
- connectors may need to reconnect/resume across daemon endpoints

The detailed mesh consistency model is future design work.

## Future work

Potential future additions:

- `WatchSpaceChanges`
- template/blob/domain metadata events
- stream filtering by node id, edge kind, or template
- compacted event history and snapshot handoff
- client acknowledgements for managed subscriptions
- richer conflict/replication metadata
