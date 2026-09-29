# Advanced storage phase 3: graph checkpoint admin operations implementation plan

## Status

Planned/implemented on the `advanced_storage` branch.

This phase adds operator-facing manual checkpoint operations for domain graph
stores. It builds on:

- phase 1 domain-scoped graph store ownership
- phase 2 domain graph checkpoint storage and fast-open recovery

## Goal

Expose a safe, authenticated admin surface and CLI commands to:

1. create a local graph checkpoint for one space/domain
2. inspect local checkpoint status for one space/domain

These operations are intentionally local to the daemon receiving the request.
They do not create a cluster-wide consensus checkpoint and do not require the
local pod to be the Raft leader.

## API shape

Add methods to `AdminClusterService`:

```proto
rpc CreateGraphCheckpoint(CreateGraphCheckpointRequest) returns (CreateGraphCheckpointResponse);
rpc GetGraphCheckpointStatus(GetGraphCheckpointStatusRequest) returns (GetGraphCheckpointStatusResponse);
```

Suggested messages:

```proto
message CreateGraphCheckpointRequest {
  string space_id = 1;
  string domain_id = 2;
}

message CreateGraphCheckpointResponse {
  GraphCheckpointStatus status = 1;
}

message GetGraphCheckpointStatusRequest {
  string space_id = 1;
  string domain_id = 2;
}

message GetGraphCheckpointStatusResponse {
  GraphCheckpointStatus status = 1;
}

message GraphCheckpointStatus {
  string space_id = 1;
  string domain_id = 2;
  uint64 current_revision = 3;
  bool checkpoint_present = 4;
  uint64 checkpoint_revision = 5;
  string checkpoint_created_at = 6;
  uint64 node_count = 7;
  uint64 edge_count = 8;
  string graph_checksum = 9;
  string checksum_algorithm = 10;
  uint64 tail_revisions = 11;
  string source = 12;
}
```

## Service/provider shape

Add a graph service interface for the admin API:

```go
type GraphCheckpointProvider interface {
    CreateGraphCheckpoint(ctx context.Context, spaceID string, domainID string) (graphservice.GraphCheckpointStatus, error)
    GraphCheckpointStatus(ctx context.Context, spaceID string, domainID string) (graphservice.GraphCheckpointStatus, error)
}
```

The graph service implementation should:

- open/resolve the local domain store
- call `LocalStore.WriteCheckpoint(ctx)` for create
- read current revision and latest checkpoint metadata for status
- return a local-only status object

## CLI shape

Add under the existing `mycel cluster` command namespace:

```bash
mycel cluster graph-checkpoint create --space-id <space> --domain-id <domain>
mycel cluster graph-checkpoint status --space-id <space> --domain-id <domain>
```

The output should be JSON, consistent with existing cluster diagnostics commands.

## Auth and safety

Both admin API methods require authenticated admin/operator context using the same
protection as the other `AdminClusterService` diagnostics.

Manual checkpoint creation is local and derived from committed graph state. It
should not bypass Raft, mutate logical graph data, or emit graph-change events.

## Out of scope

Do not implement in this phase:

- automatic checkpoint scheduling
- cluster-wide fanout checkpoint orchestration
- compaction/deleting old segments
- persistent indexes
- public SDK helpers

## Tests

Add tests for:

- admin service requires auth
- admin service maps checkpoint provider output to proto
- CLI command shape/output where practical
- graph service checkpoint status/create methods

## Validation

Run:

```bash
./scripts/generate-proto.sh
go test ./internal/graph/storage ./internal/graph/service ./internal/daemon/api/admin ./internal/cli/cmd -count=1
make docs-check
git diff --check
make test
```
