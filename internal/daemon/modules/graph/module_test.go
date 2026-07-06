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
	"github.com/myceldb/mycel/internal/graphchange"
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
