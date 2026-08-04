package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	backupcore "github.com/myceldb/mycel/internal/backup"
	clusterbackup "github.com/myceldb/mycel/internal/backup/cluster"
	"github.com/myceldb/mycel/internal/clustering/backend"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	config "github.com/myceldb/mycel/internal/runtime/runtimetest"
	daemonruntime "github.com/myceldb/mycel/internal/runtime/runtimetest"
	"github.com/myceldb/mycel/internal/wal"
)

func TestBackupRaftStateMachineAppliesClusterBackupLifecycle(t *testing.T) {
	ctx := context.Background()
	m := newInitializedBackupModule(t)
	request := clusterBackupRequestRecord{BackupSetID: "backup-set-1", ClusterID: "cluster-1", Reason: "test", CreatedAt: time.Now().UTC(), Expected: []clusterBackupExpectedNode{{PodName: "myceld-0", NodeID: "node_1", Ordinal: 0, RaftNodeID: 1}}}
	applyClusterRecord(t, ctx, m, recordTypeClusterBackupRequest, request, "request")
	_, runs := m.clusterBackupSnapshot()
	if runs[request.BackupSetID].Phase != clusterBackupPhaseRequested {
		t.Fatalf("phase=%s want requested", runs[request.BackupSetID].Phase)
	}
	applyClusterRecord(t, ctx, m, recordTypeClusterBackupPhase, clusterBackupPhaseRecord{BackupSetID: request.BackupSetID, Phase: clusterBackupPhasePrechecking, UpdatedAt: time.Now().UTC()}, "phase")
	applyClusterRecord(t, ctx, m, recordTypeClusterBackupBarrier, clusterBackupBarrierRecord{BackupSetID: request.BackupSetID, Barriers: map[string]uint64{"system": 10}, UpdatedAt: time.Now().UTC()}, "barrier")
	node := clusterbackup.NodeArtifact{PodName: "myceld-0", NodeID: "node_1", Ordinal: 0, RaftNodeID: 1, ArchiveName: "mycel-system-20260803T183500Z-myceld-0-backup-set-1.tar.zst", ManifestName: "mycel-system-20260803T183500Z-myceld-0-backup-set-1.manifest.json", SizeBytes: 1, ChecksumSHA256: shaHex("a"), AppliedIndexes: map[string]uint64{"system": 10}}
	applyClusterRecord(t, ctx, m, recordTypeClusterBackupNodeResult, clusterBackupNodeResultRecord{BackupSetID: request.BackupSetID, Node: node, UpdatedAt: time.Now().UTC()}, "node-result")
	manifest := clusterbackup.Manifest{Version: clusterbackup.ManifestVersion, BackupSetID: request.BackupSetID, ClusterID: "cluster-1", Complete: true, State: clusterbackup.StateSucceeded, ExpectedNodes: 1, ArchiveFormat: backupcore.ArchiveFormatTarZst, Nodes: []clusterbackup.NodeArtifact{node}}
	applyClusterRecord(t, ctx, m, recordTypeClusterBackupComplete, clusterBackupCompleteRecord{BackupSetID: request.BackupSetID, Manifest: manifest, CompletedAt: time.Now().UTC()}, "complete")
	active, runs := m.clusterBackupSnapshot()
	if active != "" {
		t.Fatalf("active=%q want empty", active)
	}
	if runs[request.BackupSetID].Phase != clusterBackupPhaseSucceeded || runs[request.BackupSetID].Manifest == nil {
		t.Fatalf("run=%#v want succeeded with manifest", runs[request.BackupSetID])
	}
}

func TestBackupRaftStateMachineRejectsUnexpectedNodeResult(t *testing.T) {
	ctx := context.Background()
	m := newInitializedBackupModule(t)
	applyClusterRecord(t, ctx, m, recordTypeClusterBackupRequest, clusterBackupRequestRecord{BackupSetID: "backup-set-1", CreatedAt: time.Now().UTC(), Expected: []clusterBackupExpectedNode{{PodName: "myceld-0", NodeID: "node_1", Ordinal: 0, RaftNodeID: 1}}}, "request")
	raw, err := json.Marshal(clusterBackupNodeResultRecord{BackupSetID: "backup-set-1", Node: clusterbackup.NodeArtifact{PodName: "myceld-1", NodeID: "node_2", Ordinal: 1, RaftNodeID: 2}, UpdatedAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	cmd, err := m.buildBackupRaftCommand(recordTypeClusterBackupNodeResult, raw, "unexpected-node")
	if err != nil {
		t.Fatal(err)
	}
	if err := (RaftStateMachine{Module: m}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 2, RaftTerm: 1}, cmd); err == nil || !strings.Contains(err.Error(), "unexpected pod") {
		t.Fatalf("ApplyCommand(unexpected node) error=%v, want unexpected pod", err)
	}
}

func TestBackupRaftStateMachineRejectsInvalidCompletionManifest(t *testing.T) {
	ctx := context.Background()
	m := newInitializedBackupModule(t)
	applyClusterRecord(t, ctx, m, recordTypeClusterBackupRequest, clusterBackupRequestRecord{BackupSetID: "backup-set-1", ClusterID: "cluster-1", CreatedAt: time.Now().UTC(), Expected: []clusterBackupExpectedNode{{PodName: "myceld-0", NodeID: "node_1", Ordinal: 0, RaftNodeID: 1}}}, "request")
	node := clusterbackup.NodeArtifact{PodName: "myceld-0", NodeID: "node_1", Ordinal: 0, RaftNodeID: 1, ArchiveName: "mycel-system-20260803T183500Z-myceld-0-backup-set-1.tar.zst", ManifestName: "mycel-system-20260803T183500Z-myceld-0-backup-set-1.manifest.json", SizeBytes: 1, ChecksumSHA256: shaHex("a")}
	applyClusterRecord(t, ctx, m, recordTypeClusterBackupNodeResult, clusterBackupNodeResultRecord{BackupSetID: "backup-set-1", Node: node, UpdatedAt: time.Now().UTC()}, "node-result")
	manifest := clusterbackup.Manifest{Version: clusterbackup.ManifestVersion, BackupSetID: "backup-set-1", ClusterID: "cluster-1", Complete: true, State: clusterbackup.StateSucceeded, ExpectedNodes: 1, ArchiveFormat: backupcore.ArchiveFormatTarZst, Nodes: []clusterbackup.NodeArtifact{{PodName: "myceld-0", NodeID: "node_1", Ordinal: 0, RaftNodeID: 1, ArchiveName: node.ArchiveName, ManifestName: node.ManifestName, SizeBytes: 1, ChecksumSHA256: strings.Repeat("0", 64)}}}
	raw, err := json.Marshal(clusterBackupCompleteRecord{BackupSetID: "backup-set-1", Manifest: manifest, CompletedAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	cmd, err := m.buildBackupRaftCommand(recordTypeClusterBackupComplete, raw, "complete")
	if err != nil {
		t.Fatal(err)
	}
	if err := (RaftStateMachine{Module: m}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 3, RaftTerm: 1}, cmd); err == nil || !strings.Contains(err.Error(), "does not match recorded node result") {
		t.Fatalf("ApplyCommand(invalid complete) error=%v, want mismatch", err)
	}
}

func TestBackupRaftStateMachineRejectsDuplicateBackupSetIDAfterTerminal(t *testing.T) {
	ctx := context.Background()
	m := newInitializedBackupModule(t)
	applyClusterRecord(t, ctx, m, recordTypeClusterBackupRequest, clusterBackupRequestRecord{BackupSetID: "backup-set-1", CreatedAt: time.Now().UTC()}, "request")
	applyClusterRecord(t, ctx, m, recordTypeClusterBackupFail, clusterBackupFailureRecord{BackupSetID: "backup-set-1", Phase: clusterBackupPhasePrechecking, Message: "failed", UpdatedAt: time.Now().UTC()}, "fail")
	raw, err := json.Marshal(clusterBackupRequestRecord{BackupSetID: "backup-set-1", CreatedAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	cmd, err := m.buildBackupRaftCommand(recordTypeClusterBackupRequest, raw, "duplicate-request")
	if err != nil {
		t.Fatal(err)
	}
	if err := (RaftStateMachine{Module: m}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 3, RaftTerm: 1}, cmd); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("ApplyCommand(duplicate request) error=%v, want already exists", err)
	}
}

func TestBackupRaftStateMachineRejectsSecondActiveClusterBackup(t *testing.T) {
	ctx := context.Background()
	m := newInitializedBackupModule(t)
	applyClusterRecord(t, ctx, m, recordTypeClusterBackupRequest, clusterBackupRequestRecord{BackupSetID: "backup-set-1", CreatedAt: time.Now().UTC()}, "request-1")
	raw, err := json.Marshal(clusterBackupRequestRecord{BackupSetID: "backup-set-2", CreatedAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	cmd, err := m.buildBackupRaftCommand(recordTypeClusterBackupRequest, raw, "request-2")
	if err != nil {
		t.Fatal(err)
	}
	if err := (RaftStateMachine{Module: m}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 2, RaftTerm: 1}, cmd); err == nil || !strings.Contains(err.Error(), "already active") {
		t.Fatalf("ApplyCommand(second request) error=%v, want already active", err)
	}
}

func TestBackupRaftStateMachineFailReleasesActiveClusterBackup(t *testing.T) {
	ctx := context.Background()
	m := newInitializedBackupModule(t)
	applyClusterRecord(t, ctx, m, recordTypeClusterBackupRequest, clusterBackupRequestRecord{BackupSetID: "backup-set-1", CreatedAt: time.Now().UTC()}, "request")
	applyClusterRecord(t, ctx, m, recordTypeClusterBackupFail, clusterBackupFailureRecord{BackupSetID: "backup-set-1", Phase: clusterBackupPhasePrechecking, Message: "node missing", UpdatedAt: time.Now().UTC()}, "fail")
	active, runs := m.clusterBackupSnapshot()
	if active != "" {
		t.Fatalf("active=%q want empty", active)
	}
	if runs["backup-set-1"].Phase != clusterBackupPhaseFailed || runs["backup-set-1"].Failure == nil {
		t.Fatalf("run=%#v want failed", runs["backup-set-1"])
	}
	applyClusterRecord(t, ctx, m, recordTypeClusterBackupRequest, clusterBackupRequestRecord{BackupSetID: "backup-set-2", CreatedAt: time.Now().UTC()}, "request-2")
}

func TestClusterBackupPrecheckRejectsUnsetExpectedNodeCount(t *testing.T) {
	input := validPrecheckInput()
	input.ExpectedNodeCount = 0
	result := EvaluateClusterBackupPreconditions(input)
	if result.OK || !strings.Contains(strings.Join(result.Failures, "\n"), "expected node count") {
		t.Fatalf("result=%#v, want expected node count failure", result)
	}
}

func TestClusterBackupPrecheckAcceptsHealthyCluster(t *testing.T) {
	result := EvaluateClusterBackupPreconditions(validPrecheckInput())
	if !result.OK {
		t.Fatalf("precheck failed: %v", result.Failures)
	}
}

func TestClusterBackupPrecheckRejectsUnhealthyCluster(t *testing.T) {
	input := validPrecheckInput()
	input.Nodes[1].Ready = false
	input.Nodes[1].CaughtUp = false
	input.RaftGroups[0].AppliedByNode[2] = 9
	input.Destinations[1].Writable = false
	result := EvaluateClusterBackupPreconditions(input)
	if result.OK {
		t.Fatal("precheck succeeded, want failure")
	}
	joined := strings.Join(result.Failures, "\n")
	for _, want := range []string{"not ready", "not caught up", "behind commit index", "not writable"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("failures %q missing %q", joined, want)
		}
	}
}

func TestClusterBackupPrecheckRejectsActiveBackup(t *testing.T) {
	m := newInitializedBackupModule(t)
	m.restoreClusterBackupState("backup-set-active", map[string]clusterBackupRun{"backup-set-active": {BackupSetID: "backup-set-active", Phase: clusterBackupPhaseRequested}})
	input := validPrecheckInput()
	result := m.EvaluateClusterBackupPreconditions(input)
	if result.OK || !strings.Contains(strings.Join(result.Failures, "\n"), "already active") {
		t.Fatalf("result=%#v, want active backup failure", result)
	}
}

func TestClusterBackupLocalBarriersRequireRaftGroups(t *testing.T) {
	m := newInitializedBackupModule(t)
	if _, err := m.LocalRaftBackupBarriers(); err == nil || !strings.Contains(err.Error(), "raft groups") {
		t.Fatalf("LocalRaftBackupBarriers() error=%v, want raft groups", err)
	}
	if _, err := m.WaitLocalRaftBackupBarriers(context.Background(), map[string]uint64{"system": 1}); err == nil || !strings.Contains(err.Error(), "raft groups") {
		t.Fatalf("WaitLocalRaftBackupBarriers() error=%v, want raft groups", err)
	}
}

func TestClusterBackupDestinationMustBeOutsideDataDir(t *testing.T) {
	dataDir := t.TempDir()
	if err := validateLocalClusterBackupDestination(dataDir, filepath.Join(dataDir, "backups")); err == nil || !strings.Contains(err.Error(), "outside data dir") {
		t.Fatalf("validateLocalClusterBackupDestination() error=%v, want outside data dir", err)
	}
}

func TestClusterBackupLocalArchiveRequestRequiresRecordedMembershipAndBarriers(t *testing.T) {
	m := newInitializedBackupModule(t)
	m.restoreClusterBackupState("backup-set-1", map[string]clusterBackupRun{"backup-set-1": {BackupSetID: "backup-set-1", ClusterID: "cluster-1", Phase: clusterBackupPhaseBarrierWait, Expected: []clusterBackupExpectedNode{{PodName: "myceld-0", NodeID: "node_1", Ordinal: 0, RaftNodeID: 1}}, Barriers: map[string]uint64{"system": 10}}})
	valid := backendArchiveInputForTest()
	if err := m.validateRecordedLocalClusterBackupRequest(valid, map[string]uint64{"system": 10}); err != nil {
		t.Fatalf("validateRecordedLocalClusterBackupRequest(valid) error=%v", err)
	}
	wrongSet := valid
	wrongSet.BackupSetID = "backup-set-2"
	if err := m.validateRecordedLocalClusterBackupRequest(wrongSet, map[string]uint64{"system": 10}); err == nil || !strings.Contains(err.Error(), "not recorded") {
		t.Fatalf("wrong backup set error=%v, want not recorded", err)
	}
	wrongOrdinal := valid
	wrongOrdinal.Ordinal = 1
	if err := m.validateRecordedLocalClusterBackupRequest(wrongOrdinal, map[string]uint64{"system": 10}); err == nil || !strings.Contains(err.Error(), "ordinal") {
		t.Fatalf("wrong ordinal error=%v, want ordinal", err)
	}
	if err := m.validateRecordedLocalClusterBackupRequest(valid, map[string]uint64{"system": 9}); err == nil || !strings.Contains(err.Error(), "want recorded barrier") {
		t.Fatalf("stale barrier error=%v, want recorded barrier", err)
	}
}

func TestClusterBackupBarriersRejectDuplicates(t *testing.T) {
	_, err := clusterBackupBarriersFromInput([]backend.BackupRaftBarrier{{GroupID: "system", Index: 1}, {GroupID: "system", Index: 2}})
	if err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("clusterBackupBarriersFromInput() error=%v, want duplicate", err)
	}
}

func backendArchiveInputForTest() backend.CreateLocalBackupArchiveInput {
	return backend.CreateLocalBackupArchiveInput{ClusterID: "cluster-1", BackupSetID: "backup-set-1", PodName: "myceld-0", NodeID: "node_1", RaftNodeID: 1, Ordinal: 0}
}

func applyClusterRecord(t *testing.T, ctx context.Context, m *Module, typ wal.RecordType, payload any, id string) {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	cmd, err := m.buildBackupRaftCommand(typ, raw, id)
	if err != nil {
		t.Fatal(err)
	}
	if err := (RaftStateMachine{Module: m}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, cmd); err != nil {
		t.Fatalf("ApplyCommand(%s) error = %v", id, err)
	}
}

func newInitializedBackupModule(t *testing.T) *Module {
	t.Helper()
	m := NewModule()
	if result := m.Init(context.Background(), &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("Init() error=%v", result.Error)
	}
	m.PrepareExperimentalRaftMode()
	return m
}

func validPrecheckInput() ClusterBackupPrecheckInput {
	return ClusterBackupPrecheckInput{
		BackupSetID:       "backup-set-1",
		ClusterID:         "cluster-1",
		ExpectedNodeCount: 2,
		Nodes: []ClusterBackupPrecheckNode{
			{PodName: "myceld-0", NodeID: "node_1", Ordinal: 0, RaftNodeID: 1, BackendAdvertiseAddr: "myceld-0:9091", ClusterID: "cluster-1", Reachable: true, Ready: true, Admitted: true, CaughtUp: true},
			{PodName: "myceld-1", NodeID: "node_2", Ordinal: 1, RaftNodeID: 2, BackendAdvertiseAddr: "myceld-1:9091", ClusterID: "cluster-1", Reachable: true, Ready: true, Admitted: true, CaughtUp: true},
		},
		RaftGroups: []ClusterBackupPrecheckRaftGroup{{GroupID: consensus.SystemGroupID, Leader: 1, HasQuorum: true, CommitIndex: 10, AppliedByNode: map[uint64]uint64{1: 10, 2: 10}}},
		Destinations: []ClusterBackupDestinationCheck{
			{PodName: "myceld-0", Path: "/mnt/backups/myceld-0", Writable: true, OutsideDataDir: true},
			{PodName: "myceld-1", Path: "/mnt/backups/myceld-1", Writable: true, OutsideDataDir: true},
		},
	}
}

func shaHex(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
