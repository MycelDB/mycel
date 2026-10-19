package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	backupcore "github.com/myceldb/mycel/internal/backup"
	"github.com/myceldb/mycel/internal/graph/change"
	domaingraph "github.com/myceldb/mycel/internal/graph/model"
	graphquery "github.com/myceldb/mycel/internal/graph/query"
	"github.com/myceldb/mycel/internal/runtime/quiesce"
	config "github.com/myceldb/mycel/internal/runtime/runtimetest"
	daemonruntime "github.com/myceldb/mycel/internal/runtime/runtimetest"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
	"github.com/myceldb/mycel/internal/wal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type testTransactionValidator struct {
	mu     sync.Mutex
	active bool
}

func (v *testTransactionValidator) ValidateTransactionActive(context.Context, sessionmodel.GraphTransaction) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if !v.active {
		return ErrInvalidState
	}
	return nil
}

func (v *testTransactionValidator) ValidateTransactionFinalized(context.Context, sessionmodel.GraphTransaction) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.active {
		return ErrInvalidState
	}
	return nil
}

func (v *testTransactionValidator) setActive(active bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.active = active
}

func TestModuleWALGraphCommitAppendsAndApplies(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	walManager, err := wal.Open(ctx, wal.Options{Dir: filepath.Join(dataDir, "wal"), SegmentBytes: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer walManager.Close()
	progress := wal.NewFileProgressStore(filepath.Join(dataDir, "meta", "wal", "progress.json"))
	m := NewModule()
	rt := &daemonruntime.Runtime{Config: config.Config{DataDir: dataDir}, LoggerValue: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)), WALValue: walManager, RegistryValue: wal.NewRegistry(), ProgressValue: progress, WaiterValue: wal.NewApplyWaiter()}
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init graph module: %v", result.Error)
	}
	tx := graphTx(uuid.NewString(), uuid.NewString(), 0)
	parent, err := m.CreateNode(ctx, tx, domaingraph.NodeInput{Content: "wal parent", Props: map[string]any{}})
	if err != nil {
		t.Fatalf("CreateNode(parent) error = %v", err)
	}
	child, err := m.CreateNode(ctx, tx, domaingraph.NodeInput{Content: "wal child", Props: map[string]any{}})
	if err != nil {
		t.Fatalf("CreateNode(child) error = %v", err)
	}
	edge, err := m.CreateEdge(ctx, tx, domaingraph.EdgeInput{FromNodeID: parent.ID.String(), ToNodeID: child.ID.String(), Labels: []string{"REFERENCES"}, Properties: map[string]any{"confidence": 0.75}, Payload: map[string]any{"text": "wal edge payload"}, Meta: map[string]any{"source": "test"}})
	if err != nil {
		t.Fatalf("CreateEdge() error = %v", err)
	}
	commit, err := m.CommitTransactionGraph(ctx, tx)
	if err != nil {
		t.Fatalf("CommitTransactionGraph() error = %v", err)
	}
	if commit.CommittedRevision != 1 || commit.OperationCount != 3 {
		t.Fatalf("commit=%#v", commit)
	}
	if got := walManager.LastCommittedLSN(); got != 1 {
		t.Fatalf("LastCommittedLSN() = %v, want 1", got)
	}
	if applied, err := progress.AppliedLSN(ctx); err != nil || applied != 1 {
		t.Fatalf("AppliedLSN() = %v, %v; want 1", applied, err)
	}
	readTx := graphTx(tx.SpaceID, tx.DomainID, commit.CommittedRevision)
	got, err := m.GetNode(ctx, readTx, parent.ID.String())
	if err != nil || got.ID != parent.ID {
		t.Fatalf("GetNode() = %#v, %v", got, err)
	}
	gotEdge, err := m.GetEdge(ctx, readTx, edge.ID.String())
	if err != nil || gotEdge.ID != edge.ID {
		t.Fatalf("GetEdge() = %#v, %v", gotEdge, err)
	}
	if gotEdge.DomainID.String() != tx.DomainID || !reflect.DeepEqual(gotEdge.Labels, edge.Labels) || !reflect.DeepEqual(gotEdge.Properties, edge.Properties) || !reflect.DeepEqual(gotEdge.Payload, edge.Payload) || !reflect.DeepEqual(gotEdge.Meta, edge.Meta) || gotEdge.CreatedAt.IsZero() || gotEdge.UpdatedAt.IsZero() {
		t.Fatalf("edge fields did not round trip through WAL commit: got %+v want %+v", gotEdge, edge)
	}
}

func TestModuleWriteTraceLogsGraphApplyTiming(t *testing.T) {
	ctx := context.Background()
	t.Setenv("MYCELD_GRAPH_WRITE_TRACE", "1")
	var logs bytes.Buffer
	m := NewModule()
	rt := &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.New(slog.NewTextHandler(&logs, nil))}
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init graph module: %v", result.Error)
	}
	spaceID := uuid.NewString()
	domainID := uuid.NewString()
	nodeID := domaingraph.NodeID(uuid.New())
	if _, _, _, err := m.applyGraphCommitRecord(ctx, graphCommitRecord{SpaceID: spaceID, DomainID: domainID, PutNodes: []domaingraph.Node{{ID: nodeID, DomainID: domaingraph.DomainID(uuid.MustParse(domainID)), Labels: []string{"Trace"}, Properties: map[string]any{"title": "trace"}}}, OperationCount: 1}); err != nil {
		t.Fatalf("applyGraphCommitRecord() error = %v", err)
	}
	out := logs.String()
	for _, want := range []string{"graph_write_apply_timing", "storage_commit_ms", "put_nodes_ms", "storage_in_memory_apply_ms"} {
		if !strings.Contains(out, want) {
			t.Fatalf("trace log missing %q in:\n%s", want, out)
		}
	}
}

func TestModuleGraphCheckpointCreateAndStatus(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	rt := &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))}
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init graph module: %v", result.Error)
	}
	tx := graphTx(uuid.NewString(), uuid.NewString(), 0)
	if _, err := m.CreateNode(ctx, tx, domaingraph.NodeInput{Content: "checkpoint me", Props: map[string]any{}}); err != nil {
		t.Fatalf("CreateNode() error = %v", err)
	}
	commit, err := m.CommitTransactionGraph(ctx, tx)
	if err != nil {
		t.Fatalf("CommitTransactionGraph() error = %v", err)
	}
	pre, err := m.GraphCheckpointStatus(ctx, tx.SpaceID, tx.DomainID)
	if err != nil {
		t.Fatalf("GraphCheckpointStatus() before create error = %v", err)
	}
	if pre.CurrentRevision != uint64(commit.CommittedRevision) || pre.CheckpointPresent {
		t.Fatalf("unexpected pre-checkpoint status: %+v", pre)
	}
	created, err := m.CreateGraphCheckpoint(ctx, tx.SpaceID, tx.DomainID)
	if err != nil {
		t.Fatalf("CreateGraphCheckpoint() error = %v", err)
	}
	if !created.CheckpointPresent || created.CurrentRevision != uint64(commit.CommittedRevision) || created.CheckpointRevision != uint64(commit.CommittedRevision) || created.NodeCount != 1 || created.Source != "local_checkpoint" || created.LastCheckpointAttemptAt.IsZero() || created.LastCheckpointSuccessAt.IsZero() {
		t.Fatalf("unexpected created checkpoint status: %+v", created)
	}
	status, err := m.GraphCheckpointStatus(ctx, tx.SpaceID, tx.DomainID)
	if err != nil {
		t.Fatalf("GraphCheckpointStatus() error = %v", err)
	}
	if status.CheckpointRevision != created.CheckpointRevision || status.GraphChecksum == "" || status.ChecksumAlgorithm == "" {
		t.Fatalf("unexpected checkpoint status: %+v", status)
	}
}

func TestModuleGraphCheckpointCreateRejectedDuringQuiesce(t *testing.T) {
	ctx := context.Background()
	m := newTestGraphModule(t, ctx)
	tx := commitCheckpointTestNode(t, ctx, m, "checkpoint blocked")
	lease, err := m.gate.Quiesce(ctx, quiesce.Request{Reason: "test backup", Mode: quiesce.ModeBackup, Source: "test"})
	if err != nil {
		t.Fatalf("Quiesce() error = %v", err)
	}
	_, err = m.CreateGraphCheckpoint(ctx, tx.SpaceID, tx.DomainID)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("CreateGraphCheckpoint() code = %v, want %v (err=%v)", status.Code(err), codes.Unavailable, err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatalf("release graph gate: %v", err)
	}
	created, err := m.CreateGraphCheckpoint(ctx, tx.SpaceID, tx.DomainID)
	if err != nil {
		t.Fatalf("CreateGraphCheckpoint() after release error = %v", err)
	}
	if !created.CheckpointPresent {
		t.Fatalf("checkpoint not created after release: %+v", created)
	}
}

func TestModuleAutomaticGraphCheckpointPolicy(t *testing.T) {
	ctx := context.Background()
	m := NewModule().WithCheckpointPolicy(CheckpointPolicyConfig{Enabled: true, Interval: time.Hour, RevisionThreshold: 2, Timeout: time.Second})
	rt := &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))}
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init graph module: %v", result.Error)
	}
	spaceID := uuid.NewString()
	domainID := uuid.NewString()
	first := graphTx(spaceID, domainID, 0)
	if _, err := m.CreateNode(ctx, first, domaingraph.NodeInput{Content: "one", Props: map[string]any{}}); err != nil {
		t.Fatalf("CreateNode(first) error = %v", err)
	}
	if _, err := m.CommitTransactionGraph(ctx, first); err != nil {
		t.Fatalf("CommitTransactionGraph(first) error = %v", err)
	}
	m.runCheckpointPolicyOnce(ctx, normalizeCheckpointPolicyConfig(m.checkpointPolicy))
	pre, err := m.GraphCheckpointStatus(ctx, spaceID, domainID)
	if err != nil {
		t.Fatalf("GraphCheckpointStatus(pre) error = %v", err)
	}
	if pre.CheckpointPresent {
		t.Fatalf("checkpoint created before threshold: %+v", pre)
	}
	second := graphTx(spaceID, domainID, 1)
	if _, err := m.CreateNode(ctx, second, domaingraph.NodeInput{Content: "two", Props: map[string]any{}}); err != nil {
		t.Fatalf("CreateNode(second) error = %v", err)
	}
	if _, err := m.CommitTransactionGraph(ctx, second); err != nil {
		t.Fatalf("CommitTransactionGraph(second) error = %v", err)
	}
	m.runCheckpointPolicyOnce(ctx, normalizeCheckpointPolicyConfig(m.checkpointPolicy))
	status, err := m.GraphCheckpointStatus(ctx, spaceID, domainID)
	if err != nil {
		t.Fatalf("GraphCheckpointStatus() error = %v", err)
	}
	if !status.CheckpointPresent || status.CheckpointRevision != 2 || status.AutoCheckpointRevisionThreshold != 2 || !status.AutoCheckpointEnabled || status.LastCheckpointAttemptAt.IsZero() || status.LastCheckpointSuccessAt.IsZero() || status.LastCheckpointError != "" {
		t.Fatalf("unexpected automatic checkpoint status: %+v", status)
	}
}

func TestModuleAutomaticGraphCheckpointSkippedDuringQuiesce(t *testing.T) {
	ctx := context.Background()
	m := newTestGraphModule(t, ctx)
	m.checkpointPolicy = CheckpointPolicyConfig{Enabled: true, Interval: time.Hour, RevisionThreshold: 1, Timeout: time.Second}
	tx := commitCheckpointTestNode(t, ctx, m, "auto blocked")
	lease, err := m.gate.Quiesce(ctx, quiesce.Request{Reason: "test backup", Mode: quiesce.ModeBackup, Source: "test"})
	if err != nil {
		t.Fatalf("Quiesce() error = %v", err)
	}
	m.runCheckpointPolicyOnce(ctx, normalizeCheckpointPolicyConfig(m.checkpointPolicy))
	if err := lease.Release(ctx); err != nil {
		t.Fatalf("release graph gate: %v", err)
	}
	status, err := m.GraphCheckpointStatus(ctx, tx.SpaceID, tx.DomainID)
	if err != nil {
		t.Fatalf("GraphCheckpointStatus() error = %v", err)
	}
	if status.CheckpointPresent {
		t.Fatalf("automatic checkpoint was written while quiesced: %+v", status)
	}
	if status.LastCheckpointError == "" {
		t.Fatalf("expected checkpoint failure status while quiesced: %+v", status)
	}
}

func commitCheckpointTestNode(t *testing.T, ctx context.Context, m *Module, content string) sessionmodel.GraphTransaction {
	t.Helper()
	tx := graphTx(uuid.NewString(), uuid.NewString(), 0)
	if _, err := m.CreateNode(ctx, tx, domaingraph.NodeInput{Content: content, Props: map[string]any{}}); err != nil {
		t.Fatalf("CreateNode() error = %v", err)
	}
	if _, err := m.CommitTransactionGraph(ctx, tx); err != nil {
		t.Fatalf("CommitTransactionGraph() error = %v", err)
	}
	return tx
}

func TestModuleQuiesceRejectsGraphCommit(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	rt := &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))}
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init graph module: %v", result.Error)
	}
	tx := graphTx(uuid.NewString(), uuid.NewString(), 0)
	if _, err := m.CreateNode(ctx, tx, domaingraph.NodeInput{Content: "blocked", Props: map[string]any{}}); err != nil {
		t.Fatalf("stage node: %v", err)
	}
	lease, err := m.gate.Quiesce(ctx, quiesce.Request{Reason: "test backup", Source: "test"})
	if err != nil {
		t.Fatalf("Quiesce() error = %v", err)
	}
	defer lease.Release(ctx)
	_, err = m.CommitTransactionGraph(ctx, tx)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("CommitTransactionGraph() code = %v, want %v (err=%v)", status.Code(err), codes.Unavailable, err)
	}
}

func TestPhase8BackupWaitsForGraphWorkAndReleasesWrites(t *testing.T) {
	ctx := context.Background()
	coordinator := quiesce.NewCoordinator()
	m := NewModule()
	rt := daemonruntime.New(config.Config{DataDir: t.TempDir()}, slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)), "", nil)
	rt.Quiesce = coordinator
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init graph module: %v", result.Error)
	}
	dataDir := t.TempDir()
	mgr := backupcore.NewManager(backupcore.ManagerConfig{DataDir: dataDir, Policy: backupcore.Policy{BackupDir: t.TempDir()}, Quiesce: coordinator})
	releaseActive, err := m.gate.Enter(ctx)
	if err != nil {
		t.Fatalf("enter graph gate: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := mgr.Trigger(ctx, backupcore.TriggerInput{Source: "test"})
		done <- err
	}()
	waitForGraphGateQuiesced(t, m.gate)
	select {
	case err := <-done:
		t.Fatalf("backup completed before active graph work drained: %v", err)
	default:
	}
	releaseActive()
	if err := <-done; err != nil {
		t.Fatalf("backup trigger after graph drain: %v", err)
	}

	lease, err := m.gate.Quiesce(ctx, quiesce.Request{Reason: "test", Mode: quiesce.ModeBackup})
	if err != nil {
		t.Fatalf("quiesce graph gate: %v", err)
	}
	tx := graphTx(uuid.NewString(), uuid.NewString(), 0)
	if _, err := m.CreateNode(ctx, tx, domaingraph.NodeInput{Content: "blocked", Props: map[string]any{}}); err != nil {
		t.Fatalf("stage blocked node: %v", err)
	}
	if _, err := m.CommitTransactionGraph(ctx, tx); status.Code(err) != codes.Unavailable {
		t.Fatalf("quiesced commit code = %v, want unavailable (err=%v)", status.Code(err), err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatalf("release graph gate: %v", err)
	}
	if _, err := m.CommitTransactionGraph(ctx, tx); err != nil {
		t.Fatalf("commit after release: %v", err)
	}
}

func waitForGraphGateQuiesced(t *testing.T, gate *quiesce.Gate) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		if gate.Status().Quiesced {
			return
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for graph gate to quiesce")
		case <-time.After(time.Millisecond):
		}
	}
}

func TestModuleFineGrainedOCC(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	rt := &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))}
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init graph module: %v", result.Error)
	}
	spaceID := uuid.NewString()
	domainID := uuid.NewString()
	seedTx := graphTx(spaceID, domainID, 0)
	seed, err := m.CreateNode(ctx, seedTx, domaingraph.NodeInput{Content: "seed", Props: map[string]any{}})
	if err != nil {
		t.Fatalf("create seed: %v", err)
	}
	if commit, err := m.CommitTransactionGraph(ctx, seedTx); err != nil || commit.CommittedRevision != 1 {
		t.Fatalf("seed commit = %#v, %v", commit, err)
	}

	stale := graphTx(spaceID, domainID, 0)
	content := "stale update"
	if _, err := m.UpdateNode(ctx, stale, domaingraph.UpdateNodeInput{NodeID: seed.ID.String(), Content: &content, UpdateMask: []string{"content"}}); err != nil {
		t.Fatalf("stage stale update: %v", err)
	}
	if _, err := m.CommitTransactionGraph(ctx, stale); err != ErrConflict {
		t.Fatalf("stale overlapping commit = %v, want ErrConflict", err)
	}

	base, err := m.CurrentRevision(ctx, spaceID)
	if err != nil {
		t.Fatalf("current revision: %v", err)
	}
	disjointA := graphTx(spaceID, domainID, base)
	disjointB := graphTx(spaceID, domainID, base)
	if _, err := m.CreateNode(ctx, disjointA, domaingraph.NodeInput{Content: "a", Props: map[string]any{}}); err != nil {
		t.Fatalf("stage disjoint A: %v", err)
	}
	if _, err := m.CreateNode(ctx, disjointB, domaingraph.NodeInput{Content: "b", Props: map[string]any{}}); err != nil {
		t.Fatalf("stage disjoint B: %v", err)
	}
	if _, err := m.CommitTransactionGraph(ctx, disjointA); err != nil {
		t.Fatalf("commit disjoint A: %v", err)
	}
	if _, err := m.CommitTransactionGraph(ctx, disjointB); err != nil {
		t.Fatalf("commit disjoint B should not conflict: %v", err)
	}
}

func TestReadOnlyTransactionUsesBaseRevisionSnapshot(t *testing.T) {
	ctx := context.Background()
	m := newTestGraphModule(t, ctx)
	spaceID := uuid.NewString()
	domainID := uuid.NewString()

	seed := graphTx(spaceID, domainID, 0)
	parent, err := m.CreateNode(ctx, seed, domaingraph.NodeInput{Labels: []string{"Container"}, Content: "parent", Props: map[string]any{}})
	if err != nil {
		t.Fatalf("create parent: %v", err)
	}
	child, err := m.CreateNode(ctx, seed, domaingraph.NodeInput{Labels: []string{"Note"}, Content: "revision 1", Props: map[string]any{domaingraph.NodePropTags: []string{"stable"}}})
	if err != nil {
		t.Fatalf("create child: %v", err)
	}
	edge, err := m.CreateEdge(ctx, seed, domaingraph.EdgeInput{FromNodeID: parent.ID.String(), ToNodeID: child.ID.String(), Labels: []string{"contains"}, Properties: map[string]any{"order": 1}})
	if err != nil {
		t.Fatalf("create edge: %v", err)
	}
	commit1, err := m.CommitTransactionGraph(ctx, seed)
	if err != nil {
		t.Fatalf("commit seed: %v", err)
	}
	if commit1.CommittedRevision != 1 {
		t.Fatalf("seed revision=%d want 1", commit1.CommittedRevision)
	}

	readOnly := graphTx(spaceID, domainID, commit1.CommittedRevision)
	readOnly.Mode = sessionmodel.TransactionModeReadOnly

	mutate := graphTx(spaceID, domainID, commit1.CommittedRevision)
	newContent := "revision 2"
	if _, err := m.UpdateNode(ctx, mutate, domaingraph.UpdateNodeInput{NodeID: child.ID.String(), Labels: []string{"Archived"}, Content: &newContent, UpdateMask: []string{"labels", "content"}}); err != nil {
		t.Fatalf("update child: %v", err)
	}
	newNode, err := m.CreateNode(ctx, mutate, domaingraph.NodeInput{Labels: []string{"Note"}, Content: "new after snapshot", Props: map[string]any{domaingraph.NodePropTags: []string{"stable"}}})
	if err != nil {
		t.Fatalf("create new node: %v", err)
	}
	if _, err := m.DeleteEdge(ctx, mutate, edge.ID.String()); err != nil {
		t.Fatalf("delete edge: %v", err)
	}
	commit2, err := m.CommitTransactionGraph(ctx, mutate)
	if err != nil {
		t.Fatalf("commit mutate: %v", err)
	}
	if commit2.CommittedRevision != 2 {
		t.Fatalf("mutate revision=%d want 2", commit2.CommittedRevision)
	}

	got, err := m.GetNode(ctx, readOnly, child.ID.String())
	if err != nil {
		t.Fatalf("snapshot GetNode(child): %v", err)
	}
	if got.Content != "revision 1" || !sameStringSet(got.Labels, []string{"Note"}) {
		t.Fatalf("snapshot child = %+v, want revision 1 Note", got)
	}
	if _, err := m.GetNode(ctx, readOnly, newNode.ID.String()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("snapshot GetNode(new) error = %v, want ErrNotFound", err)
	}
	children, err := m.ListChildren(ctx, readOnly, parent.ID.String())
	if err != nil {
		t.Fatalf("snapshot ListChildren: %v", err)
	}
	if len(children) != 1 || children[0].ID != edge.ID {
		t.Fatalf("snapshot children = %+v, want original edge", children)
	}
	parentEdge, err := m.GetParent(ctx, readOnly, child.ID.String())
	if err != nil {
		t.Fatalf("snapshot GetParent: %v", err)
	}
	if parentEdge == nil || parentEdge.ID != edge.ID {
		t.Fatalf("snapshot parent edge = %+v, want original edge", parentEdge)
	}
	notes, _, _, err := m.ScanLabel(ctx, readOnly, graphquery.LabelScan{Label: "Note"})
	if err != nil {
		t.Fatalf("snapshot ScanLabel: %v", err)
	}
	if len(notes) != 1 || notes[0].ID != child.ID {
		t.Fatalf("snapshot Note scan = %+v, want original child only", notes)
	}
	stable, _, _, err := m.ScanTag(ctx, readOnly, graphquery.TagScan{Tag: "stable"})
	if err != nil {
		t.Fatalf("snapshot ScanTag: %v", err)
	}
	if len(stable) != 1 || stable[0].ID != child.ID {
		t.Fatalf("snapshot stable scan = %+v, want original child only", stable)
	}
	adjacent, _, _, err := m.ScanAdjacency(ctx, readOnly, graphquery.AdjacencyScan{NodeID: parent.ID.String(), Label: "contains", Direction: graphquery.AdjacencyDirectionOut})
	if err != nil {
		t.Fatalf("snapshot ScanAdjacency: %v", err)
	}
	if len(adjacent) != 1 || adjacent[0].ID != edge.ID {
		t.Fatalf("snapshot adjacency = %+v, want original edge", adjacent)
	}

	current := graphTx(spaceID, domainID, commit2.CommittedRevision)
	current.Mode = sessionmodel.TransactionModeReadOnly
	if _, err := m.GetNode(ctx, current, newNode.ID.String()); err != nil {
		t.Fatalf("current read-only GetNode(new): %v", err)
	}
}

func TestOrderedPropertyOverlayHonorsExamineBoundAndPaginates(t *testing.T) {
	ctx := context.Background()
	m := newTestGraphModule(t, ctx)
	tx := graphTx(uuid.NewString(), uuid.NewString(), 0)
	index := schemamodel.IndexDefinition{
		Name: "note_rank", TargetKind: schemamodel.IndexTargetNode, Labels: []string{"Note"},
		Field: schemamodel.FieldPath{Namespace: "properties", Name: "rank"},
		Kind:  schemamodel.IndexKindOrdered, Direction: schemamodel.IndexSortDirectionAsc,
	}
	if err := m.ConfigureIndexes(ctx, tx, "schema-1", []schemamodel.IndexDefinition{index}); err != nil {
		t.Fatal(err)
	}
	for rank := 1; rank <= 5; rank++ {
		if _, err := m.CreateNode(ctx, tx, domaingraph.NodeInput{Labels: []string{"Note"}, Properties: map[string]any{"rank": int64(rank)}}); err != nil {
			t.Fatal(err)
		}
	}
	_, _, _, err := m.ScanNodePropertyOrdered(ctx, tx, graphquery.OrderedNodePropertyScan{
		IndexName: index.Name, Direction: schemamodel.IndexSortDirectionAsc, Limit: 1, ExamineLimit: 4,
	})
	if !errors.Is(err, graphquery.ErrScanBudgetExceeded) {
		t.Fatalf("tight overlay scan error = %v, want ErrScanBudgetExceeded", err)
	}

	cursor := ""
	var got []int64
	for page := 0; page < 6; page++ {
		nodes, next, stats, err := m.ScanNodePropertyOrdered(ctx, tx, graphquery.OrderedNodePropertyScan{
			IndexName: index.Name, Direction: schemamodel.IndexSortDirectionAsc,
			Limit: 1, Cursor: cursor, ExamineLimit: 16,
		})
		if err != nil {
			t.Fatalf("overlay page %d: %v", page, err)
		}
		if stats.IndexEntriesScanned > 16 {
			t.Fatalf("overlay page %d examined %d", page, stats.IndexEntriesScanned)
		}
		for _, node := range nodes {
			value, _ := domaingraph.Property(node, "rank")
			got = append(got, value.(int64))
		}
		if next == "" {
			break
		}
		if next == cursor {
			t.Fatalf("overlay cursor did not advance: %q", next)
		}
		cursor = next
	}
	if !reflect.DeepEqual(got, []int64{1, 2, 3, 4, 5}) {
		t.Fatalf("overlay ranks=%v", got)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, _, err := m.ScanNodePropertyOrdered(canceled, tx, graphquery.OrderedNodePropertyScan{IndexName: index.Name, Limit: 1, ExamineLimit: 16}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled overlay scan error = %v", err)
	}
}

func TestOrderedPropertyOverlayReportsActualLoadsAndPaginatesDescending(t *testing.T) {
	ctx := context.Background()
	m := newTestGraphModule(t, ctx)
	spaceID, domainID := uuid.NewString(), uuid.NewString()
	seed := graphTx(spaceID, domainID, 0)
	index := schemamodel.IndexDefinition{
		Name: "note_rank", TargetKind: schemamodel.IndexTargetNode, Labels: []string{"Note"},
		Field: schemamodel.FieldPath{Namespace: "properties", Name: "rank"},
		Kind:  schemamodel.IndexKindOrdered, Direction: schemamodel.IndexSortDirectionAsc,
	}
	if err := m.ConfigureIndexes(ctx, seed, "schema-1", []schemamodel.IndexDefinition{index}); err != nil {
		t.Fatal(err)
	}
	for _, rank := range []int64{2, 3} {
		if _, err := m.CreateNode(ctx, seed, domaingraph.NodeInput{Labels: []string{"Note"}, Properties: map[string]any{"rank": rank}}); err != nil {
			t.Fatal(err)
		}
	}
	commit, err := m.CommitTransactionGraph(ctx, seed)
	if err != nil {
		t.Fatal(err)
	}
	tx := graphTx(spaceID, domainID, commit.CommittedRevision)
	for _, rank := range []int64{1, 4} {
		if _, err := m.CreateNode(ctx, tx, domaingraph.NodeInput{Labels: []string{"Note"}, Properties: map[string]any{"rank": rank}}); err != nil {
			t.Fatal(err)
		}
	}

	nodes, _, stats, err := m.ScanNodePropertyOrdered(ctx, tx, graphquery.OrderedNodePropertyScan{
		IndexName: index.Name, Direction: schemamodel.IndexSortDirectionAsc, Limit: 1, ExamineLimit: 16,
	})
	if err != nil || len(nodes) != 1 {
		t.Fatalf("ascending mixed overlay nodes=%+v stats=%+v err=%v", nodes, stats, err)
	}
	value, _ := domaingraph.Property(nodes[0], "rank")
	if value != int64(1) || stats.NodesLoaded != 4 {
		t.Fatalf("ascending rank=%v nodes loaded=%d, want rank 1 and 4 actual loads", value, stats.NodesLoaded)
	}

	cursor := ""
	var got []int64
	for page := 0; page < 5; page++ {
		nodes, next, pageStats, err := m.ScanNodePropertyOrdered(ctx, tx, graphquery.OrderedNodePropertyScan{
			IndexName: index.Name, Direction: schemamodel.IndexSortDirectionDesc,
			Limit: 1, Cursor: cursor, ExamineLimit: 16,
		})
		if err != nil || pageStats.NodesLoaded > 4 {
			t.Fatalf("descending page %d nodes=%+v stats=%+v err=%v", page, nodes, pageStats, err)
		}
		for _, node := range nodes {
			value, _ := domaingraph.Property(node, "rank")
			got = append(got, value.(int64))
		}
		if next == "" {
			break
		}
		if next == cursor {
			t.Fatalf("descending cursor did not advance: %q", next)
		}
		cursor = next
	}
	if !reflect.DeepEqual(got, []int64{4, 3, 2, 1}) {
		t.Fatalf("descending ranks=%v", got)
	}
}

func TestSnapshotLabelScanMakesBoundedProgress(t *testing.T) {
	ctx := context.Background()
	m := newTestGraphModule(t, ctx)
	spaceID, domainID := uuid.NewString(), uuid.NewString()
	seed := graphTx(spaceID, domainID, 0)
	var want domaingraph.NodeID
	for i := 1; i <= 5; i++ {
		label := "Other"
		if i == 5 {
			label = "Note"
		}
		id := fmt.Sprintf("00000000-0000-0000-0000-%012d", i)
		node, err := m.CreateNode(ctx, seed, domaingraph.NodeInput{NodeID: id, Labels: []string{label}})
		if err != nil {
			t.Fatal(err)
		}
		if label == "Note" {
			want = node.ID
		}
	}
	commit, err := m.CommitTransactionGraph(ctx, seed)
	if err != nil {
		t.Fatal(err)
	}
	readOnly := graphTx(spaceID, domainID, commit.CommittedRevision)
	readOnly.Mode = sessionmodel.TransactionModeReadOnly

	cursor := ""
	emptyProgress := 0
	var got []domaingraph.NodeID
	for page := 0; page < 6; page++ {
		nodes, next, stats, err := m.ScanLabel(ctx, readOnly, graphquery.LabelScan{Label: "Note", Limit: 1, Cursor: cursor})
		if err != nil {
			t.Fatalf("snapshot label page %d: %v", page, err)
		}
		if stats.IndexEntriesScanned > 1 {
			t.Fatalf("snapshot label page %d examined %d entries", page, stats.IndexEntriesScanned)
		}
		if len(nodes) == 0 && next != "" {
			emptyProgress++
		}
		for _, node := range nodes {
			got = append(got, node.ID)
		}
		if next == "" {
			break
		}
		if next == cursor {
			t.Fatalf("snapshot label cursor did not advance: %q", next)
		}
		cursor = next
	}
	if !reflect.DeepEqual(got, []domaingraph.NodeID{want}) || emptyProgress != 4 {
		t.Fatalf("snapshot label IDs=%v empty progress=%d", got, emptyProgress)
	}
}

func TestMutationStateRestoresCompleteTransactionOverlay(t *testing.T) {
	ctx := context.Background()
	m := newTestGraphModule(t, ctx)
	tx := graphTx(uuid.NewString(), uuid.NewString(), 0)
	original, err := m.CreateNode(ctx, tx, domaingraph.NodeInput{Content: "original"})
	if err != nil {
		t.Fatalf("CreateNode(original) error = %v", err)
	}
	mutationCtx, scope, err := m.BeginMutationStatement(ctx, tx)
	if err != nil {
		t.Fatalf("BeginMutationStatement() error = %v", err)
	}
	updatedContent := "updated"
	if _, err := m.UpdateNode(mutationCtx, tx, domaingraph.UpdateNodeInput{NodeID: original.ID.String(), Content: &updatedContent, UpdateMask: []string{"content"}}); err != nil {
		t.Fatalf("UpdateNode() error = %v", err)
	}
	created, err := m.CreateNode(mutationCtx, tx, domaingraph.NodeInput{Content: "created-after-savepoint"})
	if err != nil {
		t.Fatalf("CreateNode(after savepoint) error = %v", err)
	}
	if err := m.FinishMutationStatement(ctx, tx, scope, false); err != nil {
		t.Fatalf("FinishMutationStatement() error = %v", err)
	}
	got, err := m.GetNode(ctx, tx, original.ID.String())
	if err != nil {
		t.Fatalf("GetNode(original) error = %v", err)
	}
	if got.Content != "original" {
		t.Fatalf("restored original content = %q", got.Content)
	}
	if _, err := m.GetNode(ctx, tx, created.ID.String()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetNode(created after savepoint) error = %v, want ErrNotFound", err)
	}
	identity, err := m.ReadViewIdentity(ctx, tx)
	if err != nil || identity != "1" {
		t.Fatalf("restored identity = %q, %v; want 1", identity, err)
	}
}

func TestMutationStatementJournalTracksTouchedEntriesNotWholeOverlay(t *testing.T) {
	ctx := context.Background()
	m := newTestGraphModule(t, ctx)
	tx := graphTx(uuid.NewString(), uuid.NewString(), 0)
	m.mu.Lock()
	o := m.overlay(tx.ID)
	ids := make([]domaingraph.NodeID, 256)
	for index := range ids {
		ids[index] = uuid.New()
		o.putNodes[ids[index]] = domaingraph.Node{ID: ids[index], DomainID: mustDomainID(tx.DomainID), Content: fmt.Sprintf("node-%d", index)}
		o.opCount++
	}
	m.mu.Unlock()

	mutationCtx, captured, err := m.BeginMutationStatement(ctx, tx)
	if err != nil {
		t.Fatalf("BeginMutationStatement() error = %v", err)
	}
	scope := captured.(*mutationScope)
	if len(scope.nodes) != 0 || len(scope.edges) != 0 {
		t.Fatalf("journal eagerly copied overlay: nodes=%d edges=%d", len(scope.nodes), len(scope.edges))
	}
	updated := "updated"
	if _, err := m.UpdateNode(mutationCtx, tx, domaingraph.UpdateNodeInput{NodeID: ids[0].String(), Content: &updated, UpdateMask: []string{"content"}}); err != nil {
		t.Fatalf("UpdateNode() error = %v", err)
	}
	if len(scope.nodes) != 1 || len(scope.edges) != 0 {
		t.Fatalf("journal nodes=%d edges=%d, want one touched node", len(scope.nodes), len(scope.edges))
	}
	if err := m.FinishMutationStatement(ctx, tx, scope, false); err != nil {
		t.Fatalf("FinishMutationStatement() error = %v", err)
	}
}

func TestMutationStatementSerializesConcurrentTransactionWriteBeforeRollback(t *testing.T) {
	ctx := context.Background()
	m := newTestGraphModule(t, ctx)
	tx := graphTx(uuid.NewString(), uuid.NewString(), 0)
	mutationCtx, scope, err := m.BeginMutationStatement(ctx, tx)
	if err != nil {
		t.Fatalf("BeginMutationStatement() error = %v", err)
	}
	if _, err := m.CreateNode(mutationCtx, tx, domaingraph.NodeInput{Content: "failed-statement"}); err != nil {
		t.Fatalf("CreateNode(scoped) error = %v", err)
	}

	started := make(chan struct{})
	done := make(chan error, 1)
	var concurrent domaingraph.Node
	go func() {
		close(started)
		var createErr error
		concurrent, createErr = m.CreateNode(ctx, tx, domaingraph.NodeInput{Content: "concurrent-success"})
		done <- createErr
	}()
	<-started
	select {
	case err := <-done:
		t.Fatalf("concurrent write completed inside active mutation scope: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if err := m.FinishMutationStatement(ctx, tx, scope, false); err != nil {
		t.Fatalf("FinishMutationStatement(rollback) error = %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("concurrent CreateNode() error = %v", err)
	}
	if _, err := m.GetNode(ctx, tx, concurrent.ID.String()); err != nil {
		t.Fatalf("GetNode(concurrent success) error = %v", err)
	}
	nodes, _, err := m.ListNodes(ctx, tx, 10, "")
	if err != nil {
		t.Fatalf("ListNodes() error = %v", err)
	}
	if len(nodes) != 1 || nodes[0].ID != concurrent.ID {
		t.Fatalf("overlay nodes after interleaving = %+v, want concurrent success only", nodes)
	}
}

func TestTransactionFinalizationSerializesWithMutationScopes(t *testing.T) {
	ctx := context.Background()
	t.Run("mutation before commit", func(t *testing.T) {
		m := newTestGraphModule(t, ctx)
		tx := graphTx(uuid.NewString(), uuid.NewString(), 0)
		mutationCtx, scope, err := m.BeginMutationStatement(ctx, tx)
		if err != nil {
			t.Fatalf("BeginMutationStatement() error = %v", err)
		}
		if _, err := m.CreateNode(mutationCtx, tx, domaingraph.NodeInput{Content: "before-commit"}); err != nil {
			t.Fatalf("CreateNode() error = %v", err)
		}
		done := make(chan error, 1)
		go func() {
			_, commitErr := m.CommitTransactionGraph(ctx, tx)
			done <- commitErr
		}()
		select {
		case err := <-done:
			t.Fatalf("commit completed inside active mutation scope: %v", err)
		case <-time.After(50 * time.Millisecond):
		}
		if err := m.FinishMutationStatement(ctx, tx, scope, true); err != nil {
			t.Fatalf("FinishMutationStatement() error = %v", err)
		}
		if err := <-done; err != nil {
			t.Fatalf("CommitTransactionGraph() error = %v", err)
		}
		if _, err := m.CreateNode(ctx, tx, domaingraph.NodeInput{Content: "after-commit"}); !errors.Is(err, ErrInvalidState) {
			t.Fatalf("CreateNode(after commit) error = %v, want ErrInvalidState", err)
		}
	})

	t.Run("commit before mutation", func(t *testing.T) {
		m := newTestGraphModule(t, ctx)
		tx := graphTx(uuid.NewString(), uuid.NewString(), 0)
		if _, err := m.CommitTransactionGraph(ctx, tx); err != nil {
			t.Fatalf("CommitTransactionGraph() error = %v", err)
		}
		if _, _, err := m.BeginMutationStatement(ctx, tx); !errors.Is(err, ErrInvalidState) {
			t.Fatalf("BeginMutationStatement(after commit) error = %v, want ErrInvalidState", err)
		}
	})

	t.Run("mutation before discard", func(t *testing.T) {
		m := newTestGraphModule(t, ctx)
		tx := graphTx(uuid.NewString(), uuid.NewString(), 0)
		mutationCtx, scope, err := m.BeginMutationStatement(ctx, tx)
		if err != nil {
			t.Fatalf("BeginMutationStatement() error = %v", err)
		}
		if _, err := m.CreateNode(mutationCtx, tx, domaingraph.NodeInput{Content: "before-discard"}); err != nil {
			t.Fatalf("CreateNode() error = %v", err)
		}
		done := make(chan error, 1)
		go func() { done <- m.DiscardTransactionGraph(ctx, tx.ID) }()
		select {
		case err := <-done:
			t.Fatalf("discard completed inside active mutation scope: %v", err)
		case <-time.After(50 * time.Millisecond):
		}
		if err := m.FinishMutationStatement(ctx, tx, scope, true); err != nil {
			t.Fatalf("FinishMutationStatement() error = %v", err)
		}
		if err := <-done; err != nil {
			t.Fatalf("DiscardTransactionGraph() error = %v", err)
		}
		if _, err := m.CreateNode(ctx, tx, domaingraph.NodeInput{Content: "after-discard"}); !errors.Is(err, ErrInvalidState) {
			t.Fatalf("CreateNode(after discard) error = %v, want ErrInvalidState", err)
		}
	})

	t.Run("discard before mutation", func(t *testing.T) {
		m := newTestGraphModule(t, ctx)
		tx := graphTx(uuid.NewString(), uuid.NewString(), 0)
		if err := m.DiscardTransactionGraph(ctx, tx.ID); err != nil {
			t.Fatalf("DiscardTransactionGraph() error = %v", err)
		}
		if _, _, err := m.BeginMutationStatement(ctx, tx); !errors.Is(err, ErrInvalidState) {
			t.Fatalf("BeginMutationStatement(after discard) error = %v, want ErrInvalidState", err)
		}
	})
}

func TestCommitTransactionGraphRetriesReturnCachedFinalization(t *testing.T) {
	ctx := context.Background()
	m := newTestGraphModule(t, ctx)
	tx := graphTx(uuid.NewString(), uuid.NewString(), 0)
	if _, err := m.CreateNode(ctx, tx, domaingraph.NodeInput{Content: "cached-commit"}); err != nil {
		t.Fatalf("CreateNode() error = %v", err)
	}
	first, err := m.CommitTransactionGraph(ctx, tx)
	if err != nil {
		t.Fatalf("first CommitTransactionGraph() error = %v", err)
	}
	second, err := m.CommitTransactionGraph(ctx, tx)
	if err != nil {
		t.Fatalf("retry CommitTransactionGraph() error = %v", err)
	}
	if second.OperationCount != first.OperationCount || second.CommittedRevision != first.CommittedRevision {
		t.Fatalf("retry result = %+v, want operation count/revision from %+v", second, first)
	}
}

func TestAcknowledgedTransactionFinalizationsAreReclaimed(t *testing.T) {
	ctx := context.Background()
	m := newTestGraphModule(t, ctx)
	validator := &testTransactionValidator{active: true}
	m.SetTransactionValidator(validator)
	for index := 0; index < 256; index++ {
		tx := graphTx(uuid.NewString(), uuid.NewString(), 0)
		validator.setActive(true)
		if index%2 == 0 {
			if _, err := m.CommitTransactionGraph(ctx, tx); err != nil {
				t.Fatalf("CommitTransactionGraph(%d) error = %v", index, err)
			}
		} else if err := m.DiscardTransactionGraph(ctx, tx.ID); err != nil {
			t.Fatalf("DiscardTransactionGraph(%d) error = %v", index, err)
		}
		if index == 0 {
			if err := m.AcknowledgeTransactionFinalization(ctx, tx); !errors.Is(err, ErrInvalidState) {
				t.Fatalf("AcknowledgeTransactionFinalization(active) error = %v, want ErrInvalidState", err)
			}
			m.mu.Lock()
			finalized := len(m.finalizedTransactions)
			m.mu.Unlock()
			if finalized != 1 {
				t.Fatalf("terminal graph records after premature acknowledgement = %d, want 1", finalized)
			}
		}
		validator.setActive(false)
		if err := m.AcknowledgeTransactionFinalization(ctx, tx); err != nil {
			t.Fatalf("AcknowledgeTransactionFinalization(%d) error = %v", index, err)
		}
		m.mu.Lock()
		finalized := len(m.finalizedTransactions)
		m.mu.Unlock()
		if finalized != 0 {
			t.Fatalf("terminal graph records after acknowledgement = %d, want 0", finalized)
		}
		if _, err := m.CreateNode(ctx, tx, domaingraph.NodeInput{Content: "stale"}); !errors.Is(err, ErrInvalidState) {
			t.Fatalf("CreateNode(stale transaction %d) error = %v, want ErrInvalidState", index, err)
		}
	}
}

func TestMutationStatementRevalidatesTransactionAfterWaitingForLease(t *testing.T) {
	ctx := context.Background()
	m := newTestGraphModule(t, ctx)
	tx := graphTx(uuid.NewString(), uuid.NewString(), 0)
	validator := &testTransactionValidator{active: true}
	m.SetTransactionValidator(validator)
	_, release, err := m.lockTransactionMutation(ctx, tx.ID)
	if err != nil {
		t.Fatalf("lockTransactionMutation() error = %v", err)
	}
	done := make(chan error, 1)
	go func() {
		_, _, beginErr := m.BeginMutationStatement(ctx, tx)
		done <- beginErr
	}()
	validator.setActive(false)
	release()
	if err := <-done; !errors.Is(err, ErrInvalidState) {
		t.Fatalf("BeginMutationStatement() error = %v, want ErrInvalidState", err)
	}
}

func TestListNodesUsesCursorDrivenPagesBoundToTransactionOverlay(t *testing.T) {
	ctx := context.Background()
	m := newTestGraphModule(t, ctx)
	spaceID := uuid.NewString()
	domainID := uuid.NewString()
	seed := graphTx(spaceID, domainID, 0)
	for i := 0; i < 3; i++ {
		if _, err := m.CreateNode(ctx, seed, domaingraph.NodeInput{Content: fmt.Sprintf("node-%d", i)}); err != nil {
			t.Fatalf("CreateNode(seed %d) error = %v", i, err)
		}
	}
	commit, err := m.CommitTransactionGraph(ctx, seed)
	if err != nil {
		t.Fatalf("CommitTransactionGraph(seed) error = %v", err)
	}

	tx := graphTx(spaceID, domainID, commit.CommittedRevision)
	identity, err := m.ReadViewIdentity(ctx, tx)
	if err != nil || identity != "0" {
		t.Fatalf("ReadViewIdentity(initial) = %q, %v", identity, err)
	}
	first, next, err := m.ListNodes(ctx, tx, 1, "")
	if err != nil {
		t.Fatalf("ListNodes(first) error = %v", err)
	}
	if len(first) != 1 || !strings.HasPrefix(next, nodePageTokenPrefix) {
		t.Fatalf("ListNodes(first) nodes=%+v next=%q", first, next)
	}
	staleAfterMutation := next
	ids := []string{first[0].ID.String()}
	for next != "" {
		page, continuation, err := m.ListNodes(ctx, tx, 1, next)
		if err != nil {
			t.Fatalf("ListNodes(continuation) error = %v", err)
		}
		if len(page) != 1 {
			t.Fatalf("ListNodes(continuation) nodes=%+v next=%q", page, continuation)
		}
		ids = append(ids, page[0].ID.String())
		next = continuation
	}
	if len(ids) != 3 || !(ids[0] < ids[1] && ids[1] < ids[2]) {
		t.Fatalf("cursor-driven node IDs = %v", ids)
	}
	if legacy, _, err := m.ListNodes(ctx, tx, 1, "1"); err != nil || len(legacy) != 1 {
		t.Fatalf("ListNodes(legacy token) nodes=%+v error=%v", legacy, err)
	}
	if _, err := m.CreateNode(ctx, tx, domaingraph.NodeInput{Content: "overlay"}); err != nil {
		t.Fatalf("CreateNode(overlay) error = %v", err)
	}
	identity, err = m.ReadViewIdentity(ctx, tx)
	if err != nil || identity != "1" {
		t.Fatalf("ReadViewIdentity(mutated) = %q, %v", identity, err)
	}
	if _, _, err := m.ListNodes(ctx, tx, 1, staleAfterMutation); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("ListNodes(stale overlay cursor) error = %v, want ErrInvalidInput", err)
	}
	if _, _, err := m.ListNodes(ctx, tx, 1, "1"); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("ListNodes(legacy overlay cursor) error = %v, want ErrInvalidInput", err)
	}
}

func TestScanAdjacencyOverlayMakesBoundedProgressAndPreservesOrder(t *testing.T) {
	ctx := context.Background()
	m := newTestGraphModule(t, ctx)
	spaceID := uuid.NewString()
	domainID := uuid.NewString()
	seed := graphTx(spaceID, domainID, 0)
	parent, err := m.CreateNode(ctx, seed, domaingraph.NodeInput{Content: "parent"})
	if err != nil {
		t.Fatal(err)
	}
	children := make([]domaingraph.Node, 3)
	for i := range children {
		children[i], err = m.CreateNode(ctx, seed, domaingraph.NodeInput{Content: fmt.Sprintf("child-%d", i)})
		if err != nil {
			t.Fatal(err)
		}
	}
	deleted, err := m.CreateEdge(ctx, seed, domaingraph.EdgeInput{FromNodeID: parent.ID.String(), ToNodeID: children[0].ID.String(), Labels: []string{"contains"}, Properties: map[string]any{"order": int64(2)}})
	if err != nil {
		t.Fatal(err)
	}
	retained, err := m.CreateEdge(ctx, seed, domaingraph.EdgeInput{FromNodeID: parent.ID.String(), ToNodeID: children[2].ID.String(), Labels: []string{"contains"}, Properties: map[string]any{"order": int64(4)}})
	if err != nil {
		t.Fatal(err)
	}
	commit, err := m.CommitTransactionGraph(ctx, seed)
	if err != nil {
		t.Fatal(err)
	}

	tx := graphTx(spaceID, domainID, commit.CommittedRevision)
	if _, err := m.DeleteEdge(ctx, tx, deleted.ID.String()); err != nil {
		t.Fatal(err)
	}
	staged, err := m.CreateEdge(ctx, tx, domaingraph.EdgeInput{FromNodeID: parent.ID.String(), ToNodeID: children[1].ID.String(), Labels: []string{"contains"}, Properties: map[string]any{"order": int64(3)}})
	if err != nil {
		t.Fatal(err)
	}

	cursor := ""
	emptyProgress := 0
	var got []domaingraph.EdgeID
	for {
		page, next, stats, err := m.ScanAdjacency(ctx, tx, graphquery.AdjacencyScan{NodeID: parent.ID.String(), Label: "contains", Direction: graphquery.AdjacencyDirectionOut, Limit: 1, Cursor: cursor})
		if err != nil {
			t.Fatalf("ScanAdjacency() error = %v", err)
		}
		if stats.IndexEntriesScanned <= 0 {
			t.Fatalf("ScanAdjacency() stats = %+v", stats)
		}
		if len(page) == 0 && next != "" {
			emptyProgress++
		}
		for _, edge := range page {
			got = append(got, edge.ID)
		}
		if next == "" {
			break
		}
		if next == cursor {
			t.Fatalf("non-progressing cursor %q", next)
		}
		cursor = next
	}
	if !reflect.DeepEqual(got, []domaingraph.EdgeID{staged.ID, retained.ID}) || emptyProgress != 0 {
		t.Fatalf("adjacency IDs=%v empty progress=%d", got, emptyProgress)
	}

	if _, _, _, err := m.ScanAdjacency(ctx, tx, graphquery.AdjacencyScan{
		NodeID: parent.ID.String(), Direction: graphquery.AdjacencyDirectionOut, Limit: 1, ExamineLimit: 3,
	}); !errors.Is(err, graphquery.ErrScanBudgetExceeded) {
		t.Fatalf("tight wildcard overlay scan error = %v", err)
	}
	cursor = ""
	got = nil
	for {
		page, next, stats, err := m.ScanAdjacency(ctx, tx, graphquery.AdjacencyScan{
			NodeID: parent.ID.String(), Direction: graphquery.AdjacencyDirectionOut,
			Limit: 1, Cursor: cursor, ExamineLimit: 16,
		})
		if err != nil {
			t.Fatalf("ScanAdjacency(wildcard) error = %v", err)
		}
		if stats.IndexEntriesScanned > 16 {
			t.Fatalf("wildcard examined %d entries", stats.IndexEntriesScanned)
		}
		for _, edge := range page {
			got = append(got, edge.ID)
		}
		if next == "" {
			break
		}
		cursor = next
	}
	if !reflect.DeepEqual(got, []domaingraph.EdgeID{staged.ID, retained.ID}) {
		t.Fatalf("wildcard adjacency IDs=%v", got)
	}
}

func graphTx(spaceID string, domainID string, baseRevision int64) sessionmodel.GraphTransaction {
	now := time.Now().UTC()
	return sessionmodel.GraphTransaction{ID: uuid.NewString(), SessionID: uuid.NewString(), PrincipalID: uuid.NewString(), SpaceID: spaceID, DomainID: domainID, Mode: sessionmodel.TransactionModeReadWrite, State: sessionmodel.TransactionStateActive, BaseRevision: baseRevision, CreatedAt: now, LastSeen: now, ExpiresAt: now.Add(time.Hour)}
}

func sameStringSet(got []string, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	seen := map[string]int{}
	for _, value := range got {
		seen[value]++
	}
	for _, value := range want {
		seen[value]--
		if seen[value] < 0 {
			return false
		}
	}
	return true
}

func TestModuleGraphChangeSinkFailureDoesNotFailCommit(t *testing.T) {
	ctx := context.Background()
	m := newTestGraphModule(t, ctx)
	sinkErr := errors.New("sink failed")
	m.SetChangeSink(graphchange.SinkFunc(func(context.Context, graphchange.CommittedEvent) error {
		return sinkErr
	}))
	spaceID := uuid.NewString()
	domainID := uuid.NewString()
	tx := graphTx(spaceID, domainID, 0)
	if _, err := m.CreateNode(ctx, tx, domaingraph.NodeInput{Content: "node", Props: map[string]any{}}); err != nil {
		t.Fatalf("create node: %v", err)
	}
	commit, err := m.CommitTransactionGraph(ctx, tx)
	if err != nil {
		t.Fatalf("commit should succeed despite sink failure: %v", err)
	}
	if commit.CommittedRevision != 1 {
		t.Fatalf("unexpected committed revision: %+v", commit)
	}
	if !errors.Is(m.LastGraphChangeSinkError(), sinkErr) {
		t.Fatalf("expected recorded sink error, got %v", m.LastGraphChangeSinkError())
	}
}

func TestModuleGraphChangeSinkReceivesPostCommitEvent(t *testing.T) {
	ctx := context.Background()
	m := newTestGraphModule(t, ctx)
	seen := false
	m.SetChangeSink(graphchange.SinkFunc(func(_ context.Context, event graphchange.CommittedEvent) error {
		seen = true
		if event.TxnID == uuid.Nil || event.TransactionID == uuid.Nil {
			t.Fatalf("expected txn ids")
		}
		if event.GraphRevision == 0 || event.Revision == 0 {
			t.Fatalf("expected graph revision aliases")
		}
		if len(event.CreatedNodeIDs) != 1 {
			t.Fatalf("expected one created node, got %+v", event)
		}
		if len(event.Changes) != 1 || event.Changes[0].Type != graphchange.ChangeTypeNodeCreated || event.Changes[0].Node == nil {
			t.Fatalf("expected canonical created-node change, got %+v", event.Changes)
		}
		if len(event.AffectedNodeIDs) != 1 || event.AffectedNodeIDs[0] != event.CreatedNodeIDs[0] {
			t.Fatalf("affected nodes = %+v, created = %+v", event.AffectedNodeIDs, event.CreatedNodeIDs)
		}
		return nil
	}))
	spaceID := uuid.NewString()
	domainID := uuid.NewString()
	tx := graphTx(spaceID, domainID, 0)
	if _, err := m.CreateNode(ctx, tx, domaingraph.NodeInput{Content: "node", Props: map[string]any{}}); err != nil {
		t.Fatalf("create node: %v", err)
	}
	commit, err := m.CommitTransactionGraph(ctx, tx)
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if len(commit.Changes) != 1 || commit.Changes[0].Type != graphchange.ChangeTypeNodeCreated || len(commit.Changes[0].AffectedNodeIDs) != 1 {
		t.Fatalf("commit canonical changes = %+v", commit.Changes)
	}
	if !seen {
		t.Fatalf("expected sink invocation")
	}
	if err := m.LastGraphChangeSinkError(); err != nil {
		t.Fatalf("unexpected sink error: %v", err)
	}
}

func TestModuleGraphChangeSinkReceivesTransactionOrigin(t *testing.T) {
	ctx := context.Background()
	m := newTestGraphModule(t, ctx)
	spaceID := uuid.NewString()
	domainID := uuid.NewString()
	origin := graphchange.OriginMetadata{ClientID: "knotpkm", ClientInstanceID: "tab-1", OperationID: "save"}
	seen := false
	m.SetChangeSink(graphchange.SinkFunc(func(_ context.Context, event graphchange.CommittedEvent) error {
		seen = true
		if event.Origin != origin {
			t.Fatalf("event origin = %#v, want %#v", event.Origin, origin)
		}
		return nil
	}))
	tx := graphTx(spaceID, domainID, 0)
	tx.Origin = origin
	if _, err := m.CreateNode(ctx, tx, domaingraph.NodeInput{Content: "node"}); err != nil {
		t.Fatalf("CreateNode() error = %v", err)
	}
	if _, err := m.CommitTransactionGraph(ctx, tx); err != nil {
		t.Fatalf("CommitTransactionGraph() error = %v", err)
	}
	if !seen {
		t.Fatal("sink was not invoked")
	}
}

func TestModuleGraphChangeEdgeChangesAffectEndpoints(t *testing.T) {
	ctx := context.Background()
	m := newTestGraphModule(t, ctx)
	spaceID := uuid.NewString()
	domainID := uuid.NewString()

	seed := graphTx(spaceID, domainID, 0)
	from, err := m.CreateNode(ctx, seed, domaingraph.NodeInput{Content: "from", Props: map[string]any{}})
	if err != nil {
		t.Fatalf("create from: %v", err)
	}
	to, err := m.CreateNode(ctx, seed, domaingraph.NodeInput{Content: "to", Props: map[string]any{}})
	if err != nil {
		t.Fatalf("create to: %v", err)
	}
	edge, err := m.CreateEdge(ctx, seed, domaingraph.EdgeInput{FromNodeID: from.ID.String(), ToNodeID: to.ID.String(), Labels: []string{"related"}})
	if err != nil {
		t.Fatalf("create edge: %v", err)
	}
	commit, err := m.CommitTransactionGraph(ctx, seed)
	if err != nil {
		t.Fatalf("commit seed: %v", err)
	}
	var edgeChange graphchange.Change
	for _, change := range commit.Changes {
		if change.EdgeID == edge.ID.String() {
			edgeChange = change
			break
		}
	}
	if edgeChange.Type != graphchange.ChangeTypeEdgeCreated {
		t.Fatalf("edge change = %+v", edgeChange)
	}
	if !sameStringSet(edgeChange.AffectedNodeIDs, []string{from.ID.String(), to.ID.String()}) {
		t.Fatalf("edge affected nodes = %+v, want endpoints", edgeChange.AffectedNodeIDs)
	}
	if len(edgeChange.AffectedEdgeIDs) != 1 || edgeChange.AffectedEdgeIDs[0] != edge.ID.String() {
		t.Fatalf("edge affected edges = %+v", edgeChange.AffectedEdgeIDs)
	}
}

func newTestGraphModule(t *testing.T, ctx context.Context) *Module {
	t.Helper()
	m, _ := newTestGraphModuleWithDataDir(t, ctx)
	return m
}

func newTestGraphModuleWithDataDir(t *testing.T, ctx context.Context) (*Module, string) {
	t.Helper()
	dataDir := t.TempDir()
	m := NewModule()
	rt := &daemonruntime.Runtime{Config: config.Config{DataDir: dataDir}, LoggerValue: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))}
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init graph module: %v", result.Error)
	}
	return m, dataDir
}

func TestModuleUsesDomainScopedGraphStoreDirectories(t *testing.T) {
	ctx := context.Background()
	m, dataDir := newTestGraphModuleWithDataDir(t, ctx)
	spaceID := uuid.NewString()
	domainA := uuid.NewString()
	domainB := uuid.NewString()

	txA := graphTx(spaceID, domainA, 0)
	if _, err := m.CreateNode(ctx, txA, domaingraph.NodeInput{Content: "domain A"}); err != nil {
		t.Fatalf("CreateNode(domain A) error = %v", err)
	}
	if _, err := m.CommitTransactionGraph(ctx, txA); err != nil {
		t.Fatalf("CommitTransactionGraph(domain A) error = %v", err)
	}

	txB := graphTx(spaceID, domainB, 0)
	if _, err := m.CreateNode(ctx, txB, domaingraph.NodeInput{Content: "domain B"}); err != nil {
		t.Fatalf("CreateNode(domain B) error = %v", err)
	}
	if _, err := m.CommitTransactionGraph(ctx, txB); err != nil {
		t.Fatalf("CommitTransactionGraph(domain B) error = %v", err)
	}

	for _, domainID := range []string{domainA, domainB} {
		manifest := filepath.Join(dataDir, "graphs", spaceID, "domains", domainID, "manifest.mycel")
		if _, err := os.Stat(manifest); err != nil {
			t.Fatalf("domain manifest %s missing: %v", manifest, err)
		}
	}
	legacyManifest := filepath.Join(dataDir, "graphs", spaceID, "manifest.mycel")
	if _, err := os.Stat(legacyManifest); !os.IsNotExist(err) {
		t.Fatalf("legacy space-level graph manifest exists/error = %v", err)
	}

	readA := graphTx(spaceID, domainA, 1)
	readA.Mode = sessionmodel.TransactionModeReadOnly
	nodesA, _, err := m.ListNodes(ctx, readA, 0, "")
	if err != nil {
		t.Fatalf("ListNodes(domain A) error = %v", err)
	}
	if len(nodesA) != 1 || nodesA[0].Content != "domain A" {
		t.Fatalf("domain A nodes = %+v", nodesA)
	}
	readB := graphTx(spaceID, domainB, 1)
	readB.Mode = sessionmodel.TransactionModeReadOnly
	nodesB, _, err := m.ListNodes(ctx, readB, 0, "")
	if err != nil {
		t.Fatalf("ListNodes(domain B) error = %v", err)
	}
	if len(nodesB) != 1 || nodesB[0].Content != "domain B" {
		t.Fatalf("domain B nodes = %+v", nodesB)
	}
}

func TestModuleRejectsCrossDomainStagedWrites(t *testing.T) {
	ctx := context.Background()
	m := newTestGraphModule(t, ctx)
	spaceID := uuid.NewString()
	domainA := uuid.NewString()
	domainB := uuid.NewString()
	tx := graphTx(spaceID, domainA, 0)
	otherDomain := uuid.MustParse(domainB)
	if err := m.stageNode(ctx, tx, domaingraph.Node{ID: uuid.New(), DomainID: domaingraph.DomainID(otherDomain), Props: map[string]any{}}); err == nil {
		t.Fatal("stageNode accepted mismatched domain")
	}
	if err := m.stageEdge(ctx, tx, domaingraph.Edge{ID: uuid.New(), DomainID: domaingraph.DomainID(otherDomain), FromID: uuid.New(), ToID: uuid.New()}); err == nil {
		t.Fatal("stageEdge accepted mismatched domain")
	}
}

func TestModuleGraphChangeSinkNotInvokedForDiscardReadOnlyOrNoop(t *testing.T) {
	ctx := context.Background()
	m := newTestGraphModule(t, ctx)
	calls := 0
	m.SetChangeSink(graphchange.SinkFunc(func(context.Context, graphchange.CommittedEvent) error {
		calls++
		return nil
	}))
	spaceID := uuid.NewString()
	domainID := uuid.NewString()

	discarded := graphTx(spaceID, domainID, 0)
	if _, err := m.CreateNode(ctx, discarded, domaingraph.NodeInput{Content: "discarded", Props: map[string]any{}}); err != nil {
		t.Fatalf("create discarded node: %v", err)
	}
	m.DiscardTransactionGraph(ctx, discarded.ID)
	if calls != 0 {
		t.Fatalf("discard emitted %d events", calls)
	}

	readOnly := graphTx(spaceID, domainID, 0)
	readOnly.Mode = sessionmodel.TransactionModeReadOnly
	if _, err := m.CommitTransactionGraph(ctx, readOnly); err != nil {
		t.Fatalf("read-only commit: %v", err)
	}
	if calls != 0 {
		t.Fatalf("read-only commit emitted %d events", calls)
	}

	noop := graphTx(spaceID, domainID, 0)
	if _, err := m.CommitTransactionGraph(ctx, noop); err != nil {
		t.Fatalf("noop commit: %v", err)
	}
	if calls != 0 {
		t.Fatalf("noop commit emitted %d events", calls)
	}
}

func TestModuleGraphChangeSinkIncludesMoveReorderAndDeleteContext(t *testing.T) {
	ctx := context.Background()
	m := newTestGraphModule(t, ctx)
	events := []graphchange.CommittedEvent{}
	m.SetChangeSink(graphchange.SinkFunc(func(_ context.Context, event graphchange.CommittedEvent) error {
		events = append(events, event)
		return nil
	}))
	spaceID := uuid.NewString()
	domainID := uuid.NewString()

	base := int64(0)
	seed := graphTx(spaceID, domainID, base)
	root, err := m.CreateNode(ctx, seed, domaingraph.NodeInput{Content: "root", Props: map[string]any{}})
	if err != nil {
		t.Fatalf("create root: %v", err)
	}
	oldParent, err := m.CreateNode(ctx, seed, domaingraph.NodeInput{Content: "old", Props: map[string]any{}})
	if err != nil {
		t.Fatalf("create old parent: %v", err)
	}
	newParent, err := m.CreateNode(ctx, seed, domaingraph.NodeInput{Content: "new", Props: map[string]any{}})
	if err != nil {
		t.Fatalf("create new parent: %v", err)
	}
	child, err := m.CreateNode(ctx, seed, domaingraph.NodeInput{Content: "child", Props: map[string]any{}})
	if err != nil {
		t.Fatalf("create child: %v", err)
	}
	sibling, err := m.CreateNode(ctx, seed, domaingraph.NodeInput{Content: "sibling", Props: map[string]any{}})
	if err != nil {
		t.Fatalf("create sibling: %v", err)
	}
	if _, err := m.CreateEdge(ctx, seed, domaingraph.EdgeInput{FromNodeID: root.ID.String(), ToNodeID: oldParent.ID.String(), Labels: []string{"contains"}, Properties: map[string]any{"order": 0}}); err != nil {
		t.Fatalf("create root/old edge: %v", err)
	}
	if _, err := m.CreateEdge(ctx, seed, domaingraph.EdgeInput{FromNodeID: root.ID.String(), ToNodeID: newParent.ID.String(), Labels: []string{"contains"}, Properties: map[string]any{"order": 1}}); err != nil {
		t.Fatalf("create root/new edge: %v", err)
	}
	if _, err := m.CreateEdge(ctx, seed, domaingraph.EdgeInput{FromNodeID: oldParent.ID.String(), ToNodeID: child.ID.String(), Labels: []string{"contains"}, Properties: map[string]any{"order": 0}}); err != nil {
		t.Fatalf("create old/child edge: %v", err)
	}
	if _, err := m.CreateEdge(ctx, seed, domaingraph.EdgeInput{FromNodeID: newParent.ID.String(), ToNodeID: sibling.ID.String(), Labels: []string{"contains"}, Properties: map[string]any{"order": 0}}); err != nil {
		t.Fatalf("create new/sibling edge: %v", err)
	}
	commit, err := m.CommitTransactionGraph(ctx, seed)
	if err != nil {
		t.Fatalf("seed commit: %v", err)
	}
	base = commit.CommittedRevision

	events = nil
	move := graphTx(spaceID, domainID, base)
	if _, err := m.MoveSubtree(ctx, move, child.ID.String(), newParent.ID.String(), nil); err != nil {
		t.Fatalf("move subtree: %v", err)
	}
	commit, err = m.CommitTransactionGraph(ctx, move)
	if err != nil {
		t.Fatalf("move commit: %v", err)
	}
	base = commit.CommittedRevision
	if len(events) != 1 {
		t.Fatalf("move emitted %d events", len(events))
	}
	moveEvent := events[0]
	if moveEvent.OldParentByNodeID[child.ID] != oldParent.ID || moveEvent.NewParentByNodeID[child.ID] != newParent.ID {
		t.Fatalf("move parent context = old %s new %s, want old %s new %s", moveEvent.OldParentByNodeID[child.ID], moveEvent.NewParentByNodeID[child.ID], oldParent.ID, newParent.ID)
	}
	if len(moveEvent.ChangedEdges) == 0 {
		t.Fatalf("move should report changed edges: %+v", moveEvent)
	}

	events = nil
	reorder := graphTx(spaceID, domainID, base)
	if _, err := m.ReorderChildren(ctx, reorder, newParent.ID.String(), []string{child.ID.String(), sibling.ID.String()}); err != nil {
		t.Fatalf("reorder children: %v", err)
	}
	commit, err = m.CommitTransactionGraph(ctx, reorder)
	if err != nil {
		t.Fatalf("reorder commit: %v", err)
	}
	base = commit.CommittedRevision
	if len(events) != 1 || len(events[0].ChangedEdges) == 0 {
		t.Fatalf("reorder event missing changed edges: %+v", events)
	}

	events = nil
	deleteTx := graphTx(spaceID, domainID, base)
	if _, _, err := m.DeleteNode(ctx, deleteTx, child.ID.String(), true); err != nil {
		t.Fatalf("delete child: %v", err)
	}
	if _, err := m.CommitTransactionGraph(ctx, deleteTx); err != nil {
		t.Fatalf("delete commit: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("delete emitted %d events", len(events))
	}
	deleteEvent := events[0]
	if len(deleteEvent.DeletedNodeIDs) != 1 || deleteEvent.DeletedNodeIDs[0] != child.ID {
		t.Fatalf("unexpected delete event: %+v", deleteEvent)
	}
	if deleteEvent.OldParentByNodeID[child.ID] != newParent.ID {
		t.Fatalf("delete old parent = %s, want %s", deleteEvent.OldParentByNodeID[child.ID], newParent.ID)
	}
}

func TestModuleReplaceReferencesReconcilesOutgoingReferenceSet(t *testing.T) {
	ctx := context.Background()
	m := newTestGraphModule(t, ctx)
	tx := graphTx(uuid.NewString(), uuid.NewString(), 0)
	source, err := m.CreateNode(ctx, tx, domaingraph.NodeInput{Labels: []string{"Note"}, Properties: map[string]any{"title": "source"}})
	if err != nil {
		t.Fatalf("CreateNode(source) error = %v", err)
	}
	targetA, err := m.CreateNode(ctx, tx, domaingraph.NodeInput{Labels: []string{"Page"}, Properties: map[string]any{"title": "a"}})
	if err != nil {
		t.Fatalf("CreateNode(targetA) error = %v", err)
	}
	targetB, err := m.CreateNode(ctx, tx, domaingraph.NodeInput{Labels: []string{"Page"}, Properties: map[string]any{"title": "b"}})
	if err != nil {
		t.Fatalf("CreateNode(targetB) error = %v", err)
	}
	targetC, err := m.CreateNode(ctx, tx, domaingraph.NodeInput{Labels: []string{"Page"}, Properties: map[string]any{"title": "c"}})
	if err != nil {
		t.Fatalf("CreateNode(targetC) error = %v", err)
	}
	if _, err := m.CreateEdge(ctx, tx, domaingraph.EdgeInput{FromNodeID: source.ID.String(), ToNodeID: targetA.ID.String(), Labels: []string{"REFERENCES"}, Properties: map[string]any{"count": 1}}); err != nil {
		t.Fatalf("CreateEdge(source->a) error = %v", err)
	}
	edgeB, err := m.CreateEdge(ctx, tx, domaingraph.EdgeInput{FromNodeID: source.ID.String(), ToNodeID: targetB.ID.String(), Labels: []string{"REFERENCES"}, Properties: map[string]any{"count": 1}})
	if err != nil {
		t.Fatalf("CreateEdge(source->b) error = %v", err)
	}
	other, err := m.CreateEdge(ctx, tx, domaingraph.EdgeInput{FromNodeID: source.ID.String(), ToNodeID: targetC.ID.String(), Labels: []string{"MENTIONS"}, Properties: map[string]any{"kind": "other"}})
	if err != nil {
		t.Fatalf("CreateEdge(source->c other) error = %v", err)
	}

	replacement := domaingraph.ReplaceReferencesInput{
		SourceNodeID: source.ID.String(),
		Labels:       []string{"REFERENCES"},
		Mode:         domaingraph.ReferenceReplacementModeReplace,
		Targets: []domaingraph.ReferenceTargetInput{
			{TargetNodeID: targetA.ID.String(), Properties: map[string]any{"count": 2}, HasProps: true},
			{TargetNodeID: targetC.ID.String(), Properties: map[string]any{"count": 1}, HasProps: true},
		},
	}
	result, err := m.ReplaceReferences(ctx, tx, replacement)
	if err != nil {
		t.Fatalf("ReplaceReferences() error = %v", err)
	}
	if len(result.AddedEdges) != 1 || result.AddedEdges[0].ToID != targetC.ID {
		t.Fatalf("added edges = %+v, want one edge to targetC", result.AddedEdges)
	}
	if len(result.UpdatedEdges) != 1 || result.UpdatedEdges[0].ToID != targetA.ID || !reflect.DeepEqual(result.UpdatedEdges[0].Properties, map[string]any{"count": 2}) {
		t.Fatalf("updated edges = %+v, want targetA count=2", result.UpdatedEdges)
	}
	if !reflect.DeepEqual(result.DeletedEdgeIDs, []string{edgeB.ID.String()}) {
		t.Fatalf("deleted edge ids = %+v, want %s", result.DeletedEdgeIDs, edgeB.ID)
	}
	if _, err := m.GetEdge(ctx, tx, edgeB.ID.String()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted reference edge lookup err=%v, want ErrNotFound", err)
	}
	if _, err := m.GetEdge(ctx, tx, other.ID.String()); err != nil {
		t.Fatalf("non-matching edge should remain: %v", err)
	}

	noop, err := m.ReplaceReferences(ctx, tx, replacement)
	if err != nil {
		t.Fatalf("ReplaceReferences(noop) error = %v", err)
	}
	if len(noop.AddedEdges) != 0 || len(noop.UpdatedEdges) != 0 || len(noop.DeletedEdgeIDs) != 0 {
		t.Fatalf("noop result = %+v, want no changes", noop)
	}
}

func TestModuleReplaceReferencesModesAndAtomicNodeUpdate(t *testing.T) {
	ctx := context.Background()
	m := newTestGraphModule(t, ctx)
	tx := graphTx(uuid.NewString(), uuid.NewString(), 0)
	source, err := m.CreateNode(ctx, tx, domaingraph.NodeInput{Labels: []string{"Note"}, Properties: map[string]any{"title": "old"}})
	if err != nil {
		t.Fatalf("CreateNode(source) error = %v", err)
	}
	targetA, err := m.CreateNode(ctx, tx, domaingraph.NodeInput{Labels: []string{"Page"}})
	if err != nil {
		t.Fatalf("CreateNode(targetA) error = %v", err)
	}
	targetB, err := m.CreateNode(ctx, tx, domaingraph.NodeInput{Labels: []string{"Page"}})
	if err != nil {
		t.Fatalf("CreateNode(targetB) error = %v", err)
	}
	updated, err := m.UpdateNode(ctx, tx, domaingraph.UpdateNodeInput{NodeID: source.ID.String(), Labels: []string{"Note"}, Properties: map[string]any{"title": "new"}, UpdateMask: []string{"properties"}})
	if err != nil {
		t.Fatalf("UpdateNode() error = %v", err)
	}
	if got := updated.Properties["title"]; got != "new" {
		t.Fatalf("updated title = %v, want new", got)
	}

	add, err := m.ReplaceReferences(ctx, tx, domaingraph.ReplaceReferencesInput{SourceNodeID: source.ID.String(), Labels: []string{"REFERENCES"}, Mode: domaingraph.ReferenceReplacementModeAdd, Targets: []domaingraph.ReferenceTargetInput{{TargetNodeID: targetA.ID.String()}, {TargetNodeID: targetB.ID.String()}}})
	if err != nil {
		t.Fatalf("ReplaceReferences(add) error = %v", err)
	}
	if len(add.AddedEdges) != 2 {
		t.Fatalf("add result = %+v, want two added edges", add)
	}
	remove, err := m.ReplaceReferences(ctx, tx, domaingraph.ReplaceReferencesInput{SourceNodeID: source.ID.String(), Labels: []string{"REFERENCES"}, Mode: domaingraph.ReferenceReplacementModeRemove, Targets: []domaingraph.ReferenceTargetInput{{TargetNodeID: targetA.ID.String()}}})
	if err != nil {
		t.Fatalf("ReplaceReferences(remove) error = %v", err)
	}
	if len(remove.DeletedEdgeIDs) != 1 {
		t.Fatalf("remove result = %+v, want one deleted edge", remove)
	}
	edges, _, err := m.ListEdges(ctx, tx, 0, "")
	if err != nil {
		t.Fatalf("ListEdges() error = %v", err)
	}
	matching := 0
	for _, edge := range edges {
		if edge.FromID == source.ID && domaingraph.EdgeHasLabels(edge, []string{"REFERENCES"}) {
			matching++
			if edge.ToID != targetB.ID {
				t.Fatalf("remaining reference target = %s, want targetB", edge.ToID)
			}
		}
	}
	if matching != 1 {
		t.Fatalf("matching references = %d, want 1", matching)
	}
}
