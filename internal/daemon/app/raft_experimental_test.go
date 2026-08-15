package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	backupservice "github.com/myceldb/mycel/internal/backup/service"
	blobservice "github.com/myceldb/mycel/internal/blob/service"
	"github.com/myceldb/mycel/internal/clustering"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/daemon/config"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	graphservice "github.com/myceldb/mycel/internal/graph/service"
	identityservice "github.com/myceldb/mycel/internal/identity/service"
	schemaservice "github.com/myceldb/mycel/internal/schema/service"
	semanticservice "github.com/myceldb/mycel/internal/semantic/service"
	spaceservice "github.com/myceldb/mycel/internal/space/service"
	"github.com/myceldb/mycel/internal/wal"
)

func TestReconcileSystemMetadataBootstrapsSingleNodeRaft(t *testing.T) {
	ctx := context.Background()
	cfg := config.Config{DataDir: t.TempDir(), NodeName: "node-a", Cluster: config.ClusterConfig{Name: "dev", BackendAdvertiseAddr: "127.0.0.1:19093", RaftNodeCount: 1, RaftPartitionCount: 4, RaftReplicaFactor: 1, RaftLocalNodeID: 1}}
	rt := daemonruntime.New(cfg, nil, "", nil)
	mgr, err := clustering.NewManager(ctx, clustering.Options{DataDir: cfg.DataDir, NodeName: cfg.NodeName, ClusterName: cfg.Cluster.Name, BackendAdvertiseAddr: cfg.Cluster.BackendAdvertiseAddr, RaftMode: true, RaftLocalNodeID: 1, RaftNodeCount: 1}, nil)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	rt.ClusterManager = mgr
	sm := consensus.NewSystemStateMachine()
	if err := initializeExperimentalRaft(ctx, rt, func() consensus.StateMachine { return sm }, nil); err != nil {
		t.Fatalf("initializeExperimentalRaft() error = %v", err)
	}
	defer rt.RaftGroups.Stop()
	if err := reconcileSystemMetadata(ctx, rt, sm); err != nil {
		t.Fatalf("reconcileSystemMetadata() error = %v", err)
	}
	meta := sm.Metadata()
	if meta.ClusterID == "" || meta.ClusterName != "dev" || len(meta.Nodes) != 1 {
		t.Fatalf("unexpected metadata: %#v", meta)
	}
	if mgr.Identity().ClusterID != meta.ClusterID || !mgr.Identity().ClusterAdmitted || !mgr.Identity().ClusterBootstrap {
		t.Fatalf("manager identity not admitted from metadata: %#v meta=%#v", mgr.Identity(), meta)
	}
	if !mgr.Readiness().ClientReady || !mgr.Readiness().PartitionGroupsStarted {
		t.Fatalf("manager not marked ready after partition startup: %#v", mgr.Readiness())
	}
}

func TestExpectedLocalPartitionGroupsUsesPlacement(t *testing.T) {
	meta := consensus.SystemMetadata{
		ClusterID:      "cluster_test",
		NodeCount:      3,
		PartitionCount: 3,
		ReplicaFactor:  2,
		Nodes: map[string]consensus.SystemNode{
			"node_1": {NodeID: "node_1", RaftNodeID: 1, NodeName: "node-a"},
			"node_2": {NodeID: "node_2", RaftNodeID: 2, NodeName: "node-b"},
			"node_3": {NodeID: "node_3", RaftNodeID: 3, NodeName: "node-c"},
		},
		Placement: map[uint32]consensus.PartitionPlacement{
			0: {PartitionID: 0, ReplicaNodeIDs: []string{"node_1", "node_2"}, PreferredLeader: "node_1"},
			1: {PartitionID: 1, ReplicaNodeIDs: []string{"node_2", "node_3"}, PreferredLeader: "node_2"},
			2: {PartitionID: 2, ReplicaNodeIDs: []string{"node_3", "node_1"}, PreferredLeader: "node_3"},
		},
	}
	if got := expectedLocalPartitionGroups(meta, 3); got != 2 {
		t.Fatalf("expectedLocalPartitionGroups(node3)=%d want 2", got)
	}
}

type testRaftHandler struct {
	name       string
	scope      consensus.CommandScope
	recordType wal.RecordType
	fn         func(context.Context, consensus.ApplyContext, consensus.RaftCommand) error
}

func (h testRaftHandler) RaftStateMachineName() string { return h.name }

func (h testRaftHandler) SupportsRaftCommandRecord(scope consensus.CommandScope, recordType wal.RecordType) bool {
	return scope == h.scope && recordType == h.recordType
}

func (h testRaftHandler) ApplyCommand(ctx context.Context, apply consensus.ApplyContext, cmd consensus.RaftCommand) error {
	if h.fn != nil {
		return h.fn(ctx, apply, cmd)
	}
	return nil
}

type testSnapshotRaftHandler struct {
	name     string
	payload  []byte
	restored []byte
}

func (h *testSnapshotRaftHandler) RaftStateMachineName() string { return h.name }

func (h *testSnapshotRaftHandler) ApplyCommand(ctx context.Context, apply consensus.ApplyContext, cmd consensus.RaftCommand) error {
	return nil
}

func (h *testSnapshotRaftHandler) Snapshot() ([]byte, error) {
	return append([]byte(nil), h.payload...), nil
}

func (h *testSnapshotRaftHandler) RestoreSnapshot(data []byte) error {
	h.restored = append([]byte(nil), data...)
	return nil
}

type testNeutralRaftHandler struct{ name string }

func (h testNeutralRaftHandler) RaftStateMachineName() string { return h.name }

func (h testNeutralRaftHandler) ApplyCommand(ctx context.Context, apply consensus.ApplyContext, cmd consensus.RaftCommand) error {
	return nil
}

func (h testNeutralRaftHandler) RaftSnapshotNeutral() bool { return true }

func TestCompositePartitionStateMachineContinuesOnlyUnsupportedRecordTypes(t *testing.T) {
	cmd := consensus.NewCommand(consensus.CommandScopeSpacePartition, wal.RecordType("graph.commit.v1"), []byte(`{}`), "cmd-1")
	realErr := errors.New("graph apply failed")
	sm := compositePartitionStateMachine{
		consensus.StateMachineFunc(func(context.Context, consensus.ApplyContext, consensus.RaftCommand) error { return realErr }),
		consensus.StateMachineFunc(func(context.Context, consensus.ApplyContext, consensus.RaftCommand) error {
			return fmt.Errorf("unsupported semantic raft record type %s", cmd.RecordType)
		}),
	}

	if err := sm.ApplyCommand(context.Background(), consensus.ApplyContext{}, cmd); !errors.Is(err, realErr) {
		t.Fatalf("ApplyCommand() error = %v, want %v", err, realErr)
	}
}

func TestCompositePartitionStateMachineSkipsUnsupportedRecordTypes(t *testing.T) {
	cmd := consensus.NewCommand(consensus.CommandScopeSpacePartition, wal.RecordType("graph.commit.v1"), []byte(`{}`), "cmd-1")
	applied := false
	sm := compositePartitionStateMachine{
		consensus.StateMachineFunc(func(context.Context, consensus.ApplyContext, consensus.RaftCommand) error {
			return fmt.Errorf("unsupported space raft record type %s", cmd.RecordType)
		}),
		consensus.StateMachineFunc(func(context.Context, consensus.ApplyContext, consensus.RaftCommand) error {
			applied = true
			return nil
		}),
	}

	if err := sm.ApplyCommand(context.Background(), consensus.ApplyContext{}, cmd); err != nil {
		t.Fatalf("ApplyCommand() error = %v", err)
	}
	if !applied {
		t.Fatal("expected matching state machine to be applied")
	}
}

func TestCompositeStateMachineSnapshotRoundTrip(t *testing.T) {
	left := &testSnapshotRaftHandler{name: "left", payload: []byte(`{"left":true}`)}
	right := &testSnapshotRaftHandler{name: "right", payload: []byte(`{"right":true}`)}
	data, err := (compositeSystemStateMachine{left, testNeutralRaftHandler{name: "neutral"}, right}).Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	var envelope compositeRaftSnapshotEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		t.Fatalf("snapshot envelope is not JSON: %v", err)
	}
	if envelope.Version != compositeRaftSnapshotVersion || envelope.GroupKind != "system" || len(envelope.Children) != 2 {
		t.Fatalf("unexpected envelope: %#v", envelope)
	}
	restoreLeft := &testSnapshotRaftHandler{name: "left"}
	restoreRight := &testSnapshotRaftHandler{name: "right"}
	if err := (compositeSystemStateMachine{restoreLeft, testNeutralRaftHandler{name: "neutral"}, restoreRight}).RestoreSnapshot(data); err != nil {
		t.Fatalf("RestoreSnapshot() error = %v", err)
	}
	if string(restoreLeft.restored) != string(left.payload) || string(restoreRight.restored) != string(right.payload) {
		t.Fatalf("restored payloads left=%s right=%s", restoreLeft.restored, restoreRight.restored)
	}
}

func TestCompositeStateMachineSnapshotFailsClosedForUnsupportedChild(t *testing.T) {
	_, err := (compositePartitionStateMachine{&testSnapshotRaftHandler{name: "graph", payload: []byte(`{}`)}, testRaftHandler{name: "blob"}}).Snapshot()
	if err == nil || !strings.Contains(err.Error(), "cannot create snapshots") || !strings.Contains(err.Error(), "blob") {
		t.Fatalf("Snapshot() error = %v, want unsupported child", err)
	}
}

func TestCompositeStateMachineRestoreRejectsMismatchedEnvelope(t *testing.T) {
	data, err := (compositeSystemStateMachine{&testSnapshotRaftHandler{name: "system", payload: []byte(`{}`)}}).Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if err := (compositePartitionStateMachine{&testSnapshotRaftHandler{name: "system"}}).RestoreSnapshot(data); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("RestoreSnapshot() error = %v, want group kind mismatch", err)
	}
}

func TestCompositeStateMachineRestoreRejectsChecksumMismatch(t *testing.T) {
	data, err := (compositeSystemStateMachine{&testSnapshotRaftHandler{name: "system", payload: []byte(`{}`)}}).Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	var envelope compositeRaftSnapshotEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	envelope.Children[0].Payload = []byte(`{"tampered":true}`)
	data, err = json.Marshal(envelope)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	if err := (compositeSystemStateMachine{&testSnapshotRaftHandler{name: "system"}}).RestoreSnapshot(data); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("RestoreSnapshot() error = %v, want checksum mismatch", err)
	}
}

func TestCompositeStateMachineRestoreRejectsMissingUnsupportedChildWithoutPartialRestore(t *testing.T) {
	data, err := (compositeSystemStateMachine{&testSnapshotRaftHandler{name: "system", payload: []byte(`{}`)}}).Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	system := &testSnapshotRaftHandler{name: "system"}
	if err := (compositeSystemStateMachine{system, testRaftHandler{name: "identity.admin"}}).RestoreSnapshot(data); err == nil || !strings.Contains(err.Error(), "identity.admin") {
		t.Fatalf("RestoreSnapshot() error = %v, want missing unsupported child", err)
	}
	if len(system.restored) != 0 {
		t.Fatalf("system child was partially restored before composite restore failed: %s", system.restored)
	}
}

func TestCompositeSystemStateMachineSkipsUnsupportedSystemRecordTypes(t *testing.T) {
	cmd := consensus.NewCommand(consensus.CommandScopeSystem, wal.RecordType("identity.admin.session.put.v1"), []byte(`{}`), "cmd-1")
	applied := false
	sm := compositeSystemStateMachine{
		consensus.NewSystemStateMachine(),
		testRaftHandler{name: "admin", scope: consensus.CommandScopeSystem, recordType: cmd.RecordType, fn: func(context.Context, consensus.ApplyContext, consensus.RaftCommand) error {
			applied = true
			return nil
		}},
	}

	if err := sm.ApplyCommand(context.Background(), consensus.ApplyContext{}, cmd); err != nil {
		t.Fatalf("ApplyCommand() error = %v", err)
	}
	if !applied {
		t.Fatal("expected matching system state machine to be applied")
	}
}

func TestCompositeStateMachineRecordOwnershipIsUnique(t *testing.T) {
	systemSMs := []consensus.StateMachine{
		consensus.NewSystemStateMachine(),
		identityservice.PrincipalRaftStateMachine{},
		backupservice.RaftStateMachine{},
		semanticservice.RaftStateMachine{},
	}
	partitionSMs := []consensus.StateMachine{
		spaceservice.RaftStateMachine{},
		schemaservice.RaftStateMachine{},
		graphservice.RaftStateMachine{},
		blobservice.RaftStateMachine{},
		semanticservice.RaftStateMachine{},
	}
	systemRecords := map[wal.RecordType]string{
		consensus.SystemRecordBootstrapMetadata: "system.metadata",
		consensus.SystemRecordRegisterNode:      "system.metadata",
		"identity.principal.put.v1":             "identity.principal",
		"identity.role_binding.put.v1":          "identity.principal",
		"identity.capability_grant.put.v1":      "identity.principal",
		"identity.principal.session.put.v1":     "identity.principal",
		"semantic.global.mutation.v1":           "semantic",
		"daemon.backup.policy.update.v1":        "backup",
		"daemon.backup.delete.v1":               "backup",
	}
	partitionRecords := map[wal.RecordType]string{
		"space.create_with_default_domain.v1": "space",
		"space.domain.create.v1":              "space",
		"space.domain.update.v1":              "space",
		"space.domain.delete.v1":              "space",
		"space.access.grant.v1":               "space",
		"space.delete.v1":                     "space",
		"schema.put.v1":                       "schema",
		"schema.delete.v1":                    "schema",
		"graph.commit.v1":                     "graph",
		"blob.meta.put.v1":                    "blob",
		"blob.meta.delete.v1":                 "blob",
		"semantic.space.mutation.v1":          "semantic",
		"semantic.maintenance.mutation.v1":    "semantic",
	}
	for recordType, want := range systemRecords {
		cmd := consensus.RaftCommand{Scope: consensus.CommandScopeSystem, RecordType: recordType, CommandID: "test"}
		matches, hasMetadata := matchingRaftStateMachines(cmd, systemSMs)
		if !hasMetadata || len(matches) != 1 || matches[0].Name != want {
			t.Fatalf("system record %s handlers=%v hasMetadata=%v, want exactly %s", recordType, raftHandlerNames(matches), hasMetadata, want)
		}
	}
	for recordType, want := range partitionRecords {
		cmd := consensus.RaftCommand{Scope: consensus.CommandScopeSpacePartition, RecordType: recordType, CommandID: "test"}
		matches, hasMetadata := matchingRaftStateMachines(cmd, partitionSMs)
		if !hasMetadata || len(matches) != 1 || matches[0].Name != want {
			t.Fatalf("partition record %s handlers=%v hasMetadata=%v, want exactly %s", recordType, raftHandlerNames(matches), hasMetadata, want)
		}
	}
}

func TestCompositeStateMachineUnknownAndDuplicateRecordsAreActionable(t *testing.T) {
	unknown := consensus.NewCommand(consensus.CommandScopeSystem, wal.RecordType("unknown.record.v1"), []byte(`{}`), "cmd-unknown")
	beforeUnsupported := unsupportedRaftRecordApplyErrors.Load()
	err := (compositeSystemStateMachine{consensus.NewSystemStateMachine()}).ApplyCommand(context.Background(), consensus.ApplyContext{}, unknown)
	if err == nil {
		t.Fatal("expected unknown record error")
	}
	for _, want := range []string{"unsupported system raft command", "scope=system", "record_type=unknown.record.v1", "command_id=\"cmd-unknown\"", "no state machine handler"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("unknown error %q missing %q", err.Error(), want)
		}
	}
	if got := unsupportedRaftRecordApplyErrors.Load(); got != beforeUnsupported+1 {
		t.Fatalf("unsupported counter=%d, want %d", got, beforeUnsupported+1)
	}

	dup := consensus.NewCommand(consensus.CommandScopeSystem, wal.RecordType("dup.record.v1"), []byte(`{}`), "cmd-dup")
	beforeAmbiguous := ambiguousRaftRecordApplyErrors.Load()
	err = (compositeSystemStateMachine{
		testRaftHandler{name: "first", scope: consensus.CommandScopeSystem, recordType: dup.RecordType},
		testRaftHandler{name: "second", scope: consensus.CommandScopeSystem, recordType: dup.RecordType},
	}).ApplyCommand(context.Background(), consensus.ApplyContext{}, dup)
	if err == nil {
		t.Fatal("expected duplicate handler error")
	}
	for _, want := range []string{"ambiguous system raft command", "record_type=dup.record.v1", "first", "second"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("duplicate error %q missing %q", err.Error(), want)
		}
	}
	if got := ambiguousRaftRecordApplyErrors.Load(); got != beforeAmbiguous+1 {
		t.Fatalf("ambiguous counter=%d, want %d", got, beforeAmbiguous+1)
	}
}
