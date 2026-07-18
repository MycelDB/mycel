# Cluster Safe Planned Switchover

## Status

Design proposal.

## Objective

Add a manual, operator-initiated planned primary switchover for a healthy cluster:

```bash
mycel cluster primary switch NODE
```

`NODE` may be a node name or node ID. The command is run against the current primary and promotes an active, caught-up follower to primary without losing acknowledged writes.

This is not emergency failover. The current primary must be alive and able to coordinate the transition.

## Non-goals

- automatic failover
- election/Raft/consensus
- promoting a stale follower
- force-promoting when the old primary is unavailable
- resolving split brain after operator error
- changing membership/admission rules

## Terms

- **old primary**: current authority primary before switchover
- **new primary**: target follower selected by the operator
- **authority epoch**: monotonic epoch persisted with authority; incremented on switchover
- **final LSN**: old primary WAL last committed LSN after writes are quiesced
- **caught up**: target follower has applied at least final LSN

## Operator command

Preferred CLI:

```bash
mycel cluster primary switch node-b
```

Potential flags:

```bash
--timeout 60s
--dry-run
```

Initial implementation can omit flags and use a fixed timeout.

## Safety invariants

1. Only the current primary can initiate planned switchover.
2. Target must be an active admitted follower.
3. Target must have a backend advertise address.
4. Old primary must quiesce durable writes before final LSN is selected.
5. New primary must have applied `final_lsn` before authority changes.
6. Authority epoch must increase monotonically.
7. Old primary must persist the new authority before returning success.
8. New primary must persist the new authority before accepting writes.
9. Followers must reject stale authority updates with an older or equal epoch.
10. The old primary must reject writes after it observes authority no longer points to itself.

## High-level flow

```text
operator -> old primary: SwitchPrimary(target=node-b)
old primary:
  require local role primary
  resolve target active follower
  quiesce durable writes
  final_lsn = wal.LastCommittedLSN()
  wait target applied_lsn >= final_lsn
  new_authority = { primary: target, epoch: old_epoch + 1 }
  install new_authority on target
  install new_authority locally
  broadcast/propagate authority to known followers
  release quiesce
  return success
```

After success:

```text
old primary role => follower
new primary role => primary
followers continue reads and follow new primary
client durable writes accepted only by new primary
old primary returns not-primary hints pointing to new primary
```

## Detailed phase sequence

### Phase 1: Admission and role validation

The current primary validates:

- clustering manager exists
- local node admitted
- local role is `primary`
- target resolves by node name or node ID
- target is not the local node
- target is not the current primary
- target membership state is `active`
- target has non-empty backend advertise address

Errors:

- non-primary local node: existing `node is not cluster primary` + primary hint
- missing target: `FailedPrecondition` or `NotFound`, message `switchover target not found`
- target not active follower: `FailedPrecondition`, message `switchover target is not an active follower`

### Phase 2: Quiesce writes on old primary

The old primary enters a quiesce mode that blocks durable write entry points.

Use existing quiesce coordinator/gate if possible.

Important: the switchover RPC itself must be quiesce-exempt, similar to backup/resync operations.

### Phase 3: Determine final LSN

After quiesce drains writes:

```go
finalLSN := wal.LastCommittedLSN()
```

This is the final acknowledged write that the new primary must have applied before promotion.

### Phase 4: Verify target catch-up

The old primary queries the target over backend RPC for replication status, or uses existing backend view if sufficiently fresh.

Preferred new backend RPC:

```proto
rpc GetReplicationStatus(GetReplicationStatusRequest) returns (GetReplicationStatusResponse);
```

Response should include:

```proto
uint64 received_lsn = 1;
uint64 applied_lsn = 2;
string primary_node_id = 3;
int64 authority_epoch = 4;
string catchup_state = 5;
```

The old primary waits until:

```text
target.applied_lsn >= final_lsn
```

If the timeout expires, abort and release quiesce. Authority remains unchanged.

### Phase 5: Create new authority

Old authority:

```json
{
  "primary": "node-a",
  "authority_epoch": 1
}
```

New authority:

```json
{
  "primary": "node-b",
  "authority_epoch": 2,
  "source": "planned_switchover"
}
```

Authority record should include target node ID, name, backend advertise address, and updated timestamp.

### Phase 6: Install authority on new primary first

Old primary sends internal backend RPC to target:

```proto
rpc InstallAuthority(InstallAuthorityRequest) returns (InstallAuthorityResponse);
```

Target validates:

- request cluster ID matches local cluster ID
- target node ID matches local node ID
- new authority primary is local node
- new epoch is greater than current epoch
- local node is admitted active member
- optional: target applied LSN >= final LSN included in request

Target persists `authority.json` with new primary and epoch.

After this point, the target can derive local role as primary. It should accept writes only after the install RPC completes successfully.

### Phase 7: Install authority on old primary

After target acknowledges authority install, old primary persists the same new authority locally.

Once persisted, old primary derives local role as follower and rejects durable writes with not-primary hints pointing to the new primary.

### Phase 8: Propagate to other followers

Old primary best-effort sends the new authority to other active followers through the same `InstallAuthority` backend RPC.

If some followers are unreachable:

- switchover can still succeed if target and old primary persisted the new authority
- unreachable followers will learn authority via registration/view refresh when they reconnect
- stale followers must reject writes anyway because they are not primary

### Phase 9: Release quiesce and return

Release old primary quiesce.

Return response:

```proto
message SwitchClusterPrimaryResponse {
  string old_primary_node_id = 1;
  string old_primary_node_name = 2;
  string new_primary_node_id = 3;
  string new_primary_node_name = 4;
  int64 authority_epoch = 5;
  uint64 final_lsn = 6;
}
```

## Failure behavior

### Target not caught up

Abort before authority changes.

```text
old primary remains primary
quiesce released
operator sees timeout/catch-up error
```

### InstallAuthority fails on target

Abort before old primary authority changes.

```text
old primary remains primary
target remains follower
quiesce released
```

### Target installs authority but old primary fails before local persist

This is the main partial-failure window.

Possible state:

```text
target thinks it is primary epoch N+1
old primary still thinks it is primary epoch N
```

Mitigation options:

1. Make old primary persist a local switchover intent before target install.
2. On restart, old primary sees pending intent and completes or steps down.
3. Backend/client write paths reject stale epoch if they learn higher authority.

Initial implementation should include a switchover intent file:

```text
<data_dir>/meta/clustering/switchover-intent.json
```

Intent fields:

```json
{
  "operation_id": "switch_...",
  "cluster_id": "cluster_...",
  "old_primary_node_id": "node-a",
  "new_primary_node_id": "node-b",
  "new_authority_epoch": 2,
  "final_lsn": 123,
  "phase": "target_installing",
  "created_at": "..."
}
```

On startup, if local node has a pending intent with a higher epoch target install possibly completed, the safest behavior is to refuse primary writes and surface an operator error until authority is reconciled. A later phase can automate recovery.

### Old primary cannot propagate to other followers

Switchover still succeeds if target and old primary have persisted new authority.

Other followers refresh later.

### Old primary dies before command starts

Not handled by planned switchover. Use future emergency failover workflow.

## Public admin API

Add to `AdminClusterService`:

```proto
rpc SwitchClusterPrimary(SwitchClusterPrimaryRequest) returns (SwitchClusterPrimaryResponse);

message SwitchClusterPrimaryRequest {
  string target = 1;
  int64 timeout_seconds = 2;
  bool dry_run = 3;
}

message SwitchClusterPrimaryResponse {
  string old_primary_node_id = 1;
  string old_primary_node_name = 2;
  string new_primary_node_id = 3;
  string new_primary_node_name = 4;
  int64 authority_epoch = 5;
  uint64 final_lsn = 6;
}
```

## Internal backend API

Add to `ClusterBackendService`:

```proto
rpc GetReplicationStatus(GetReplicationStatusRequest) returns (GetReplicationStatusResponse);
rpc InstallAuthority(InstallAuthorityRequest) returns (InstallAuthorityResponse);
```

`InstallAuthorityRequest` should include:

- cluster ID
- target node ID
- new authority primary node ID/name/backend address
- new authority epoch
- final LSN
- operation ID

## CLI

Add:

```bash
mycel cluster primary switch NODE
```

Output:

```text
Primary switched
Old primary: node-a (node_...)
New primary: node-b (node_...)
Authority epoch: 2
Final LSN: 123
```

## mycel-admin

Initial UI can show a planned switchover action on active follower rows when connected to primary:

- confirmation modal
- warning that this is planned switchover, not dead-primary failover
- success/error result

## Client and SDK behavior

Planned switchover is expected to be disruptive for write-capable client connections that are still connected to the old primary. After authority changes, the old primary becomes a follower and durable write attempts against it fail with the existing structured not-primary error:

```text
node is not cluster primary
MYCEL_CLUSTER_NOT_PRIMARY
```

The daemon already includes primary hints in this error:

- `mycel-primary-node-id`
- `mycel-primary-node-name`
- `mycel-primary-backend-advertise-addr`
- `mycel-authority-epoch`

However, application code should not be required to manually parse this metadata, discover the new primary, rebuild clients, and reconnect. SDKs should provide a primary-following connection layer.

### SDK primary-following behavior

Go and Rust SDKs should support an opt-in, and eventually default-on, primary-follow policy:

```text
on MYCEL_CLUSTER_NOT_PRIMARY:
  extract primary hint
  update cached primary endpoint
  reconnect to hinted primary
  refresh/login if credentials/session provider is available
  retry safe operation once, or return typed error with client now pointed at primary
```

This means application code can keep using the same SDK client. If an operation cannot be safely retried, the SDK should still move its internal connection to the new primary and return a clear typed error indicating that the caller may retry/reopen the operation.

### Safe automatic retries

The SDK may automatically retry operations that are read-only or otherwise idempotent, for example:

- metadata reads
- list/get operations
- query/read operations
- login/session creation where credentials are available

### Unsafe automatic retries

The SDK must not blindly retry operations that may have partially committed before the not-primary or connection error was observed, for example:

- graph transaction commit
- blob upload
- create/update/delete mutations without idempotency keys
- any write where the request may have reached the old primary and committed but the response was lost

For these operations, the SDK should:

1. follow/reconnect to the hinted primary,
2. invalidate any old-primary transaction/session handle if needed,
3. return a typed error such as `PrimaryChangedRetryRequired`, including the new primary hint.

### Node-local sessions and transactions

Current sessions are node-local in the short-term architecture. Planned switchover does not transparently migrate open sessions or graph transactions from the old primary to the new primary.

Rules for initial implementation:

- read-only operations may continue on followers where supported
- write transactions open on the old primary must be committed before switchover final LSN or reopened on the new primary
- SDK transaction handles should become invalid when a primary change is detected
- clients should reopen sessions/transactions on the new primary

### Future idempotency support

To safely retry more write operations automatically, public mutation APIs should eventually accept idempotency keys. Once a mutation has a stable idempotency key, the SDK can safely retry it after following the new primary.

### Addressing caveat

The current primary hint metadata uses `mycel-primary-backend-advertise-addr`. In local/dev deployments the backend and public client gRPC address are the same listener, so SDK primary-follow works with this address.

For production deployments, internode/backend and client-facing addresses may diverge. Before separating those listeners, authority/primary hints should be extended with a public client address, for example:

- `mycel-primary-client-advertise-addr`
- `client_advertise_addr` in authority/topology status

SDKs should prefer the client advertise address when present and fall back to backend advertise address for current deployments.

## Tests

### Unit tests

- target resolution rejects missing/pending/primary/self target
- switchover requires local primary
- target catch-up timeout aborts authority change
- target install failure aborts local authority change
- success increments epoch and changes local roles
- stale authority install rejected
- target validates final LSN catch-up

### E2E script

Add:

```text
mycel/scripts/validateClusterPrimarySwitchover.sh
```

Flow:

1. Start node-a primary and node-b follower.
2. Create data on node-a.
3. Wait for node-b caught up.
4. Run:

   ```bash
   mycel cluster primary switch node-b
   ```

5. Verify node-a reports role follower and not-primary hints target node-b.
6. Verify node-b reports role primary.
7. Verify durable write to node-a fails.
8. Verify durable write to node-b succeeds.
9. Verify follower/read behavior remains usable.

## Operational limitations

- Requires old primary alive.
- Requires target caught up.
- Does not handle emergency failover.
- Does not guarantee automatic reconciliation if primary dies mid-switchover until intent recovery is implemented.

## Recommended implementation phases

1. Add backend proto for replication status and authority install.
2. Implement target replication status backend service.
3. Implement authority install backend service with epoch validation.
4. Implement switchover coordinator on old primary.
5. Add public admin API.
6. Add CLI command.
7. Add switchover intent persistence for partial-failure safety.
8. Add tests.
9. Add e2e validation script.
10. Add mycel-admin UI action.
