package graph

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/daemon/config"
	daemonsession "github.com/myceldb/mycel/internal/daemon/modules/session"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	"github.com/myceldb/mycel/internal/graph/change"
	domaingraph "github.com/myceldb/mycel/internal/graph/model"
)

func TestModuleFineGrainedOCC(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	rt := &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, Logger: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))}
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init graph module: %v", result.Error)
	}
	spaceID := uuid.NewString()
	domainID := uuid.NewString()
	seedTx := graphTx(spaceID, domainID, 0)
	seed, err := m.CreateNode(ctx, seedTx, NodeInput{Content: "seed", Props: map[string]any{}})
	if err != nil {
		t.Fatalf("create seed: %v", err)
	}
	if commit, err := m.CommitTransactionGraph(ctx, seedTx); err != nil || commit.CommittedRevision != 1 {
		t.Fatalf("seed commit = %#v, %v", commit, err)
	}

	stale := graphTx(spaceID, domainID, 0)
	content := "stale update"
	if _, err := m.UpdateNode(ctx, stale, UpdateNodeInput{NodeID: seed.ID.String(), Content: &content, UpdateMask: []string{"content"}}); err != nil {
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
	if _, err := m.CreateNode(ctx, disjointA, NodeInput{Content: "a", Props: map[string]any{}}); err != nil {
		t.Fatalf("stage disjoint A: %v", err)
	}
	if _, err := m.CreateNode(ctx, disjointB, NodeInput{Content: "b", Props: map[string]any{}}); err != nil {
		t.Fatalf("stage disjoint B: %v", err)
	}
	if _, err := m.CommitTransactionGraph(ctx, disjointA); err != nil {
		t.Fatalf("commit disjoint A: %v", err)
	}
	if _, err := m.CommitTransactionGraph(ctx, disjointB); err != nil {
		t.Fatalf("commit disjoint B should not conflict: %v", err)
	}
}

func graphTx(spaceID string, domainID string, baseRevision int64) daemonsession.GraphTransaction {
	now := time.Now().UTC()
	return daemonsession.GraphTransaction{ID: uuid.NewString(), SessionID: uuid.NewString(), UserID: uuid.NewString(), SpaceID: spaceID, DomainID: domainID, Mode: daemonsession.TransactionModeReadWrite, State: daemonsession.TransactionStateActive, BaseRevision: baseRevision, CreatedAt: now, LastSeen: now, ExpiresAt: now.Add(time.Hour)}
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
	if _, err := m.CreateNode(ctx, tx, NodeInput{Content: "node", Props: map[string]any{}}); err != nil {
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
		if event.TxnID == uuid.Nil {
			t.Fatalf("expected txn id")
		}
		if event.GraphRevision == 0 {
			t.Fatalf("expected graph revision")
		}
		if len(event.CreatedNodeIDs) != 1 {
			t.Fatalf("expected one created node, got %+v", event)
		}
		return nil
	}))
	spaceID := uuid.NewString()
	domainID := uuid.NewString()
	tx := graphTx(spaceID, domainID, 0)
	if _, err := m.CreateNode(ctx, tx, NodeInput{Content: "node", Props: map[string]any{}}); err != nil {
		t.Fatalf("create node: %v", err)
	}
	if _, err := m.CommitTransactionGraph(ctx, tx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if !seen {
		t.Fatalf("expected sink invocation")
	}
	if err := m.LastGraphChangeSinkError(); err != nil {
		t.Fatalf("unexpected sink error: %v", err)
	}
}

func newTestGraphModule(t *testing.T, ctx context.Context) *Module {
	t.Helper()
	m := NewModule()
	rt := &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, Logger: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))}
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init graph module: %v", result.Error)
	}
	return m
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
	if _, err := m.CreateNode(ctx, discarded, NodeInput{Content: "discarded", Props: map[string]any{}}); err != nil {
		t.Fatalf("create discarded node: %v", err)
	}
	m.DiscardTransactionGraph(ctx, discarded.ID)
	if calls != 0 {
		t.Fatalf("discard emitted %d events", calls)
	}

	readOnly := graphTx(spaceID, domainID, 0)
	readOnly.Mode = daemonsession.TransactionModeReadOnly
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
	root, err := m.CreateNode(ctx, seed, NodeInput{Content: "root", Props: map[string]any{}})
	if err != nil {
		t.Fatalf("create root: %v", err)
	}
	oldParent, err := m.CreateNode(ctx, seed, NodeInput{Content: "old", Props: map[string]any{}})
	if err != nil {
		t.Fatalf("create old parent: %v", err)
	}
	newParent, err := m.CreateNode(ctx, seed, NodeInput{Content: "new", Props: map[string]any{}})
	if err != nil {
		t.Fatalf("create new parent: %v", err)
	}
	child, err := m.CreateNode(ctx, seed, NodeInput{Content: "child", Props: map[string]any{}})
	if err != nil {
		t.Fatalf("create child: %v", err)
	}
	sibling, err := m.CreateNode(ctx, seed, NodeInput{Content: "sibling", Props: map[string]any{}})
	if err != nil {
		t.Fatalf("create sibling: %v", err)
	}
	if _, err := m.CreateEdge(ctx, seed, EdgeInput{FromNodeID: root.ID.String(), ToNodeID: oldParent.ID.String(), Kind: string(domaingraph.EdgeKindContains), Props: map[string]any{"order": 0}}); err != nil {
		t.Fatalf("create root/old edge: %v", err)
	}
	if _, err := m.CreateEdge(ctx, seed, EdgeInput{FromNodeID: root.ID.String(), ToNodeID: newParent.ID.String(), Kind: string(domaingraph.EdgeKindContains), Props: map[string]any{"order": 1}}); err != nil {
		t.Fatalf("create root/new edge: %v", err)
	}
	if _, err := m.CreateEdge(ctx, seed, EdgeInput{FromNodeID: oldParent.ID.String(), ToNodeID: child.ID.String(), Kind: string(domaingraph.EdgeKindContains), Props: map[string]any{"order": 0}}); err != nil {
		t.Fatalf("create old/child edge: %v", err)
	}
	if _, err := m.CreateEdge(ctx, seed, EdgeInput{FromNodeID: newParent.ID.String(), ToNodeID: sibling.ID.String(), Kind: string(domaingraph.EdgeKindContains), Props: map[string]any{"order": 0}}); err != nil {
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
