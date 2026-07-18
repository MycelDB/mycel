# WAL Propagation MVP Implementation Plan

## Status

Implementation plan for `wal-propagation-mvp.md`.

## Objective

Implement a minimal primary-to-follower WAL propagation path for static-primary clusters.

The MVP must prove this loop:

```text
primary client write
  -> primary WAL append/sync/apply
  -> primary streams committed WAL record
  -> follower stores record in receive log
  -> follower applies record through existing WAL applier registry
  -> follower read sees replicated state
```

Followers must continue to reject client/operator writes while accepting primary-originated WAL apply.

## Scope

In scope:

- internal backend `StreamWal` RPC
- WAL transport conversion helpers
- file-backed follower receive log
- file-backed replication progress store
- follower receive-log replay on startup
- follower replication worker
- replication status in daemon/admin API/CLI/UI
- e2e validation script

Out of scope:

- automatic election
- quorum commit
- manual promotion/fencing
- transparent write forwarding
- snapshot transfer
- read-after-LSN API
- retention coordination based on follower lag
- production internode mTLS

## Existing foundations to reuse

- WAL manager, reader/following APIs under `mycel/internal/wal`
- WAL registry/applier model
- cluster manager authority and role derivation
- internal backend service package under `mycel/internal/clustering/backend`
- internal backend proto under `mycel/internal/clustering/proto/mycel/cluster/v1/backend.proto`
- public admin cluster proto under `mycel-api/api/proto/mycel/admin/v1/cluster.proto`
- daemon runtime assembly under `mycel/internal/daemon/app` and `mycel/internal/daemon/runtime`
- CLI cluster status under `mycel/internal/cli/cmd/cluster.go`
- `mycel-admin` Cluster page

## Phase 0: Pre-implementation audit

Before writing code, inspect and document:

1. How `wal.Manager` reads committed records after an LSN.
2. Whether `wal.Follow` can tail committed records with context cancellation.
3. The exact `wal.Record` fields needed for reconstruction.
4. Whether `wal.Registry` exposes enough API to apply one record outside recovery.
5. Where daemon runtime has access to:
   - cluster manager
   - WAL manager
   - WAL registry
   - logger
   - lifecycle start/stop hooks

Deliverable:

- Update this plan with any API-specific notes if the implementation deviates from assumptions.

## Phase 1: Backend proto and generated code

### 1.1 Extend internal backend proto

File:

```text
mycel/internal/clustering/proto/mycel/cluster/v1/backend.proto
```

Add:

```proto
rpc StreamWal(StreamWalRequest) returns (stream WalRecord);

message StreamWalRequest {
  ClusterProtocolVersion protocol_version = 1;
  string cluster_id = 2;
  string follower_node_id = 3;
  int64 after_lsn = 4;
  int64 authority_epoch = 5;
}

message WalRecord {
  int64 lsn = 1;
  string type = 2;
  uint32 schema_version = 3;
  string timestamp = 4;
  string encoding = 5;
  bytes payload = 6;
}
```

If `timestamp` or `encoding` already has proto enum conventions in existing generated code, follow the existing style.

### 1.2 Regenerate proto code

Run:

```bash
cd mycel
./scripts/generate-proto.sh
```

Confirm generated files update under:

```text
mycel/internal/gen/mycel/cluster/v1/
mycel-api/api/proto/mycel/cluster/v1/backend.proto
```

if the script mirrors internal cluster proto to `mycel-api`.

### 1.3 Add conversion tests

File candidates:

```text
mycel/internal/clustering/backend/wal_convert.go
mycel/internal/clustering/backend/wal_convert_test.go
```

Implement helpers:

```go
func walRecordToProto(rec wal.Record) (*clusterpb.WalRecord, error)
func walRecordFromProto(pb *clusterpb.WalRecord) (wal.Record, error)
```

Test:

- round-trip type
- schema version
- timestamp
- encoding
- payload bytes
- LSN
- invalid timestamp
- invalid/missing type
- unsupported encoding, if applicable

Acceptance:

- internal proto generation succeeds
- conversion tests pass

## Phase 2: Receive log

### 2.1 Create replication package

Directory:

```text
mycel/internal/clustering/replication
```

Files:

```text
record.go
receive_log.go
receive_log_test.go
```

Suggested record type:

```go
type Record struct {
    LSN           wal.LSN              `json:"lsn"`
    Type          wal.RecordType       `json:"type"`
    SchemaVersion uint32               `json:"schema_version"`
    Timestamp     time.Time            `json:"timestamp"`
    Encoding      wal.PayloadEncoding  `json:"encoding"`
    Payload       json.RawMessage      `json:"payload"`
}
```

If WAL payloads are raw `[]byte`, store as base64 automatically via JSON or use a custom field.

### 2.2 File layout

Use:

```text
<data_dir>/meta/clustering/replication/receive-log/
  00000000000000000001.json
  00000000000000000002.json
```

Filename format:

```go
fmt.Sprintf("%020d.json", lsn)
```

### 2.3 ReceiveLog API

Implement:

```go
type ReceiveLog struct { dir string }

func NewReceiveLog(dir string) *ReceiveLog
func (l *ReceiveLog) Put(ctx context.Context, rec Record) error
func (l *ReceiveLog) Get(ctx context.Context, lsn wal.LSN) (Record, error)
func (l *ReceiveLog) ScanAfter(ctx context.Context, after wal.LSN) ([]Record, error)
func (l *ReceiveLog) TruncateBefore(ctx context.Context, before wal.LSN) error
```

### 2.4 Idempotency behavior

`Put` rules:

- if file does not exist: write temp file, fsync if project pattern exists, rename
- if file exists and decoded record matches byte-for-byte/logically: return nil
- if file exists but differs: return conflict error

### 2.5 Tests

Test cases:

- put/get one record
- scan after returns ascending records
- duplicate identical put succeeds
- duplicate conflicting put fails
- gap is preserved in scan; gap detection belongs to applier/worker
- truncate before removes older records and keeps newer records
- invalid/corrupt file returns useful error

Acceptance:

```bash
cd mycel
go test ./internal/clustering/replication
```

## Phase 3: Progress store

### 3.1 Progress model

File:

```text
mycel/internal/clustering/replication/progress.go
```

Model:

```go
type Progress struct {
    Version        int       `json:"version"`
    ClusterID      string    `json:"cluster_id"`
    PrimaryNodeID  string    `json:"primary_node_id"`
    AuthorityEpoch int64     `json:"authority_epoch"`
    ReceivedLSN    wal.LSN   `json:"received_lsn"`
    AppliedLSN     wal.LSN   `json:"applied_lsn"`
    LastRecordAt   time.Time `json:"last_record_at,omitempty"`
    LastError      string    `json:"last_error,omitempty"`
    UpdatedAt      time.Time `json:"updated_at"`
}
```

### 3.2 Store API

```go
type ProgressStore struct { path string }

func NewProgressStore(path string) *ProgressStore
func (s *ProgressStore) Load(ctx context.Context) (Progress, error)
func (s *ProgressStore) Save(ctx context.Context, progress Progress) error
func (s *ProgressStore) UpdateError(ctx context.Context, err error) error
```

Missing file returns zero progress, not an error.

### 3.3 Validation

On save:

- version defaults to 1
- `applied_lsn <= received_lsn`
- cluster ID, primary node ID, and epoch are internally consistent when set

### 3.4 Tests

Test:

- missing file
- save/load round trip
- invalid `applied_lsn > received_lsn`
- update error preserves LSNs
- atomic write behavior if existing project helpers are available

Acceptance:

```bash
go test ./internal/clustering/replication
```

## Phase 4: Primary StreamWal service

### 4.1 Service dependencies

`internal/clustering/backend.Service` currently has identity, state, topology, membership, authority. Add WAL reader dependency without making clustering own daemon runtime.

Suggested field:

```go
WALReader WALReader
```

Interface local to backend package:

```go
type WALReader interface {
    RecordsAfter(ctx context.Context, after wal.LSN) (<-chan wal.Record, <-chan error)
}
```

Or adapt to existing WAL APIs discovered in Phase 0.

Add builder:

```go
func (s *Service) WithWAL(reader WALReader) *Service
```

### 4.2 Implement StreamWal

Validation:

- protocol version valid
- request cluster ID equals `s.Identity.ClusterID`
- local identity admitted
- local role is primary according to authority
- request authority epoch equals current authority epoch, if non-zero
- follower node ID non-empty
- follower is active/admitted in membership if membership store is present
- `after_lsn >= 0`
- WAL reader configured; otherwise `Unavailable`

Streaming:

```go
records, errs := s.WALReader.RecordsAfter(ctx, wal.LSN(req.AfterLsn))
for rec := range records {
    pb, err := walRecordToProto(rec)
    if err != nil { return status.Error(codes.Internal, ...) }
    if err := stream.Send(pb); err != nil { return err }
}
return <-errs
```

If existing WAL APIs are pull-based, implement finite catch-up first:

```go
records, err := reader.ReadAfter(ctx, after)
for _, rec := range records { stream.Send(...) }
return nil
```

Long-lived tailing can be added after MVP catch-up works.

### 4.3 Tests

File:

```text
mycel/internal/clustering/backend/service_wal_test.go
```

Test:

- rejects unsupported protocol
- rejects wrong cluster ID
- rejects non-primary with primary hint
- rejects authority epoch mismatch
- rejects unknown/unadmitted follower when membership exists
- streams records after requested LSN
- does not stream record at `after_lsn`
- sends records in order
- propagates stream send cancellation/error

Acceptance:

```bash
go test ./internal/clustering/backend
```

## Phase 5: Follower apply engine

### 5.1 Apply engine type

Package:

```text
mycel/internal/clustering/replication
```

Suggested type:

```go
type Applier struct {
    Log      *ReceiveLog
    Progress *ProgressStore
    Registry *wal.Registry
    Logger   *slog.Logger
}
```

Methods:

```go
func (a *Applier) ApplyReceived(ctx context.Context, clusterID, primaryNodeID string, epoch int64, rec Record) error
func (a *Applier) Replay(ctx context.Context) error
```

### 5.2 ApplyReceived behavior

1. Load progress.
2. Validate authority identity/epoch against progress.
3. If `rec.LSN <= progress.AppliedLSN`, skip.
4. If `rec.LSN != progress.AppliedLSN + 1`, return gap error.
5. Put record into receive log.
6. Set `received_lsn` after durable put.
7. Convert to `wal.Record`.
8. Apply through registry.
9. Set `applied_lsn`.
10. Save progress.

Important: if apply fails, do not advance `applied_lsn`.

### 5.3 WAL registry apply API

If `wal.Registry` does not expose `Apply(ctx, rec)`, add a small method there rather than duplicating dispatch logic.

Example:

```go
func (r *Registry) Apply(ctx context.Context, rec Record) error
```

Ensure existing recovery uses the same dispatch path if practical.

### 5.4 Replay behavior

On follower startup:

- load progress
- scan receive log after applied LSN
- apply contiguous records
- stop at first gap or apply error

### 5.5 Tests

Use fake appliers and fake records.

Test:

- applies next LSN and advances progress
- skips already-applied LSN
- detects gap
- does not advance applied LSN on applier error
- replay applies stored unapplied records
- replay stops on gap

Acceptance:

```bash
go test ./internal/clustering/replication ./internal/wal
```

## Phase 6: Backend client support

### 6.1 Add client streaming method

File:

```text
mycel/internal/clustering/backend/client.go
```

Add method:

```go
func (c *Client) StreamWal(ctx context.Context, req *clusterpb.StreamWalRequest) (WalStream, error)
```

Define an interface to simplify tests:

```go
type WalStream interface {
    Recv() (*clusterpb.WalRecord, error)
}
```

### 6.2 Tests

If backend client tests use bufconn/fakes, add a stream test. Otherwise, keep this thin and rely on worker tests.

Acceptance:

```bash
go test ./internal/clustering/backend
```

## Phase 7: Follower replication worker

### 7.1 Worker package/type

Package:

```text
mycel/internal/clustering/replication
```

Suggested type:

```go
type Follower struct {
    Manager     ClusterManagerView
    Dialer      BackendDialer
    Applier     *Applier
    Progress    *ProgressStore
    Interval    time.Duration
    Logger      *slog.Logger
}
```

Interfaces:

```go
type ClusterManagerView interface {
    Identity() model.NodeIdentity
    State() model.NodeState
    LocalRole() clustering.NodeRole
    IsAdmitted() bool
    Authority() (clustering.Authority, bool)
    Topology() *topology.Registry
}

type BackendDialer interface {
    StreamWal(ctx context.Context, addr string, req *clusterpb.StreamWalRequest) (WalStream, error)
}
```

Adapt exact interfaces to avoid import cycles.

### 7.2 Primary endpoint resolution

Resolution order:

1. authority primary backend advertise address
2. topology peer matching primary node ID
3. membership member matching primary node ID

If no endpoint is known, update progress `last_error` and retry.

### 7.3 Loop behavior

```text
Start(ctx): spawn goroutine
Stop(ctx): cancel and wait
```

Each iteration:

1. If local node is not admitted follower, sleep.
2. Resolve authority and primary endpoint.
3. Load progress.
4. Build `StreamWalRequest` with `after_lsn=progress.applied_lsn`.
5. Receive stream records until EOF/error/context cancellation.
6. For each record, convert to replication record and apply.
7. On error, persist last error and back off.

### 7.4 Tests

Use fake manager, fake stream, fake applier.

Test:

- no-op when standalone
- no-op when primary
- no-op when unadmitted
- errors when primary endpoint missing
- streams after applied LSN
- applies received records
- persists last error on stream failure
- stops on context cancellation

Acceptance:

```bash
go test ./internal/clustering/replication
```

## Phase 8: Daemon integration

### 8.1 Runtime fields

Add runtime fields if needed:

```go
ReplicationFollower daemonruntime.Service
ReplicationProgress *replication.ProgressStore
```

or register follower as a normal daemon service.

Preferred: implement follower as a `daemonruntime.Starter`/`Stopper` service if that fits existing service registry conventions.

### 8.2 Assembly

In daemon app assembly:

- create receive log path under `<data_dir>/meta/clustering/replication/receive-log`
- create progress store path under `<data_dir>/meta/clustering/replication/progress.json`
- create applier with runtime WAL registry
- create backend dialer/client
- create follower worker
- register/start it after WAL recovery and module appliers are registered

Important ordering:

```text
modules register WAL appliers
WAL recovery runs
replication replay applies received unapplied records
follower stream starts
```

If current startup order makes this difficult, start worker only after all services have initialized.

### 8.3 Primary backend service WAL dependency

When constructing internal backend service on daemon, pass a WAL reader/follower capable object to backend service via `WithWAL(...)`.

Acceptance:

```bash
go test ./internal/daemon/app ./internal/daemon/server ./internal/...
```

## Phase 9: Replication status API

### 9.1 Public admin proto

File:

```text
mycel-api/api/proto/mycel/admin/v1/cluster.proto
```

Add optional replication status to `GetClusterStatusResponse`:

```proto
ClusterReplicationStatus replication = N;

message ClusterReplicationStatus {
  string primary_node_id = 1;
  string primary_node_name = 2;
  string primary_backend_advertise_addr = 3;
  int64 authority_epoch = 4;
  int64 received_lsn = 5;
  int64 applied_lsn = 6;
  int64 primary_last_lsn = 7;
  int64 lag_records = 8;
  bool connected = 9;
  string last_error = 10;
  string updated_at = 11;
}
```

Regenerate:

```bash
cd mycel
./scripts/generate-proto.sh
cd ../mycel-api
go run github.com/bufbuild/buf/cmd/buf@v1.50.1 lint
```

Then regenerate/update SDKs as current repo workflow requires.

### 9.2 Daemon admin service

Update:

```text
mycel/internal/daemon/api/admin/cluster_service.go
```

Populate replication status from progress store/worker status.

For primary nodes:

- connected can be false/omitted
- primary last LSN can be local WAL last committed LSN
- applied LSN may be local WAL applied LSN if useful

For followers:

- show progress store fields
- show `connected` from worker runtime state if available
- show `last_error`

### 9.3 Tests

- status includes replication for follower when progress exists
- primary status does not report misleading follower lag
- JSON/CLI mapping handles nil replication

Acceptance:

```bash
go test ./internal/daemon/api/admin ./internal/cli/cmd
```

## Phase 10: CLI and mycel-admin UI

### 10.1 CLI

Update:

```text
mycel/internal/cli/cmd/cluster.go
```

Text output example:

```text
node=node_b role=follower primary=node-a epoch=1 replication=connected applied_lsn=42 lag=0
```

JSON output should include:

```json
"replication": {
  "primary_node_id": "node_a",
  "authority_epoch": 1,
  "received_lsn": 42,
  "applied_lsn": 42,
  "connected": true,
  "last_error": ""
}
```

### 10.2 Rust SDK / Tauri

Update generated proto and mapping in:

```text
mycel-rust-sdk
mycel-admin/src-tauri/src/commands/cluster.rs
```

### 10.3 React UI

Update:

```text
mycel-admin/src/types/cluster.ts
mycel-admin/src/features/cluster/pages/ClusterPage.tsx
```

Cluster General tab should show:

- replication state
- applied LSN
- received LSN
- primary endpoint
- last error if present

Follower node detail can show the same later; General tab is enough for MVP.

### 10.4 Tests

- CLI status JSON/text with replication
- Tauri mapping test if existing pattern exists
- React ClusterPage renders replication card

Acceptance:

```bash
cd mycel-admin
npm test -- --runInBand
npm run build
cd src-tauri && cargo check
```

## Phase 11: E2E validation script

Create:

```text
mycel/scripts/validateWALPropagation.sh
```

Flow:

1. start node-a bootstrap primary
2. add node-b and start follower
3. wait until node-b role=follower
4. create a simple primary write, preferably create space
5. poll node-b cluster status until `replication.applied_lsn >= returned/write LSN` or applied LSN increases
6. query node-b for replicated state
7. verify follower still rejects a guarded write
8. print logs on failure

If create-space CLI does not expose LSN, use status applied LSN increase as proxy initially.

Acceptance:

- script passes on local dev machine
- script is documented in WAL propagation design/status docs

## Phase 12: Documentation updates

Update:

```text
mycel/docs/design/wal-propagation-mvp.md
mycel/docs/design/write-ahead-log-replication-readiness.md
mycel/docs/design/clustering-architecture-evolution.md
```

Add:

- actual file paths
- actual proto fields
- known limitations
- validation command/script
- operational notes for follower lag/errors

## Cross-phase invariants

Throughout implementation, preserve these invariants:

1. Client/operator writes on followers remain rejected.
2. WAL appliers can mutate follower state during replication/recovery.
3. Follower progress advances only after durable receive-log write and successful apply.
4. Primary streams only committed WAL records.
5. Primary LSN ordering is preserved on followers.
6. Followers do not skip gaps silently.
7. Secrets are not newly exposed in transport/status/logging.

## Validation matrix

Run frequently:

```bash
cd mycel
go test ./internal/clustering/replication ./internal/clustering/backend ./internal/wal
```

Run after daemon integration:

```bash
cd mycel
go test ./internal/...
```

Run after proto/API changes:

```bash
cd mycel
./scripts/generate-proto.sh

cd ../mycel-api
go run github.com/bufbuild/buf/cmd/buf@v1.50.1 lint
```

Run after SDK/UI updates:

```bash
cd mycel-go-sdk
go test ./...

cd ../mycel-rust-sdk
cargo check -p mycel-proto
cargo check -p mycel-sdk

cd ../mycel-admin/src-tauri
cargo check

cd ..
npm test -- --runInBand
npm run build
```

Run final e2e:

```bash
cd mycel
./scripts/validateWALPropagation.sh
```

## Acceptance criteria

The WAL propagation MVP is complete when:

- internal backend exposes and tests `StreamWal`
- primary streams committed WAL records after requested LSN
- follower has durable receive log and progress store
- follower replays unapplied receive-log records on restart
- follower worker receives and applies primary records
- follower read can observe a primary write after replication catches up
- follower still rejects client writes
- replication status is visible in admin API, CLI, and `mycel-admin`
- full unit/integration validation passes
- local e2e validation script passes

## Suggested PR/chunking strategy

To keep review manageable:

1. Proto + conversion helpers.
2. Receive log + progress store.
3. Backend StreamWal service.
4. Follower applier/replay.
5. Follower worker + daemon integration.
6. Status/API/CLI/UI.
7. E2E script + docs finalization.
