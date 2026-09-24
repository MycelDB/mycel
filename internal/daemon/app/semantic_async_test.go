package app

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	daemonconfig "github.com/myceldb/mycel/internal/daemon/config"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	graph "github.com/myceldb/mycel/internal/graph/model"
	graphnotification "github.com/myceldb/mycel/internal/graph/notification"
	semanticservice "github.com/myceldb/mycel/internal/semantic/service"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestAsyncSemanticDirtyConsumerAppendsEventAndCheckpoint(t *testing.T) {
	ctx := context.Background()
	semantic := newTestSemanticModule(t)
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	nodeID := graph.NodeID(uuid.New())
	event := graphchange.CommittedEvent{ID: uuid.New(), SpaceID: spaceID, DomainID: domainID, DomainIDs: []graph.DomainID{domainID}, TxnID: uuid.New(), TransactionID: uuid.New(), GraphRevision: 7, Revision: 7, UpdatedNodeIDs: []graph.NodeID{nodeID}, AffectedNodeIDs: []graph.NodeID{nodeID}, CommittedAt: time.Now().UTC()}
	consumer := &asyncSemanticDirtyConsumer{semantic: semantic, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if err := consumer.HandleGraphChange(ctx, event); err != nil {
		t.Fatalf("HandleGraphChange() error = %v", err)
	}
	mgr, err := semantic.MaintenanceManager(ctx, spaceID)
	if err != nil {
		t.Fatalf("MaintenanceManager() error = %v", err)
	}
	dirty, err := mgr.ListGraphDirtyEvents(ctx)
	if err != nil {
		t.Fatalf("ListGraphDirtyEvents() error = %v", err)
	}
	if len(dirty) != 1 {
		t.Fatalf("dirty events = %d, want 1", len(dirty))
	}
	if dirty[0].GraphRevision != 7 || dirty[0].SpaceID != spaceID || len(dirty[0].UpdatedNodeIDs) != 1 || dirty[0].UpdatedNodeIDs[0] != nodeID {
		t.Fatalf("unexpected dirty event: %+v", dirty[0])
	}
	checkpoint, err := mgr.GetCheckpoint(ctx, asyncSemanticDirtyCheckpointConsumer(domainID.String()))
	if err != nil {
		t.Fatalf("GetCheckpoint() error = %v", err)
	}
	if checkpoint.LastGraphRevision != 7 || checkpoint.SpaceID != spaceID {
		t.Fatalf("unexpected checkpoint: %+v", checkpoint)
	}
}

func TestAsyncSemanticDirtyReplayUsesCheckpoint(t *testing.T) {
	ctx := context.Background()
	semantic := newTestSemanticModule(t)
	notifications := graphnotification.NewModule()
	notifications.SetDataDirForTest(t.TempDir())
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	firstNode := graph.NodeID(uuid.New())
	secondNode := graph.NodeID(uuid.New())
	first := graphchange.CommittedEvent{ID: uuid.New(), SpaceID: spaceID, DomainID: domainID, DomainIDs: []graph.DomainID{domainID}, TxnID: uuid.New(), TransactionID: uuid.New(), GraphRevision: 1, Revision: 1, UpdatedNodeIDs: []graph.NodeID{firstNode}, AffectedNodeIDs: []graph.NodeID{firstNode}, CommittedAt: time.Now().UTC()}
	second := graphchange.CommittedEvent{ID: uuid.New(), SpaceID: spaceID, DomainID: domainID, DomainIDs: []graph.DomainID{domainID}, TxnID: uuid.New(), TransactionID: uuid.New(), GraphRevision: 2, Revision: 2, UpdatedNodeIDs: []graph.NodeID{secondNode}, AffectedNodeIDs: []graph.NodeID{secondNode}, CommittedAt: time.Now().UTC()}
	if err := notifications.OnGraphCommitted(ctx, first); err != nil {
		t.Fatalf("OnGraphCommitted(first) error = %v", err)
	}
	if err := notifications.OnGraphCommitted(ctx, second); err != nil {
		t.Fatalf("OnGraphCommitted(second) error = %v", err)
	}
	consumer := &asyncSemanticDirtyConsumer{semantic: semantic, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if err := replayAsyncSemanticDirtyScope(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), notifications, semantic, consumer, graphchange.Scope{SpaceID: spaceID.String(), DomainID: domainID.String()}); err != nil {
		t.Fatalf("replayAsyncSemanticDirtyScope() error = %v", err)
	}
	mgr, err := semantic.MaintenanceManager(ctx, spaceID)
	if err != nil {
		t.Fatalf("MaintenanceManager() error = %v", err)
	}
	dirty, err := mgr.ListGraphDirtyEvents(ctx)
	if err != nil {
		t.Fatalf("ListGraphDirtyEvents() error = %v", err)
	}
	if len(dirty) != 2 {
		t.Fatalf("dirty events after first replay = %d, want 2", len(dirty))
	}
	if err := replayAsyncSemanticDirtyScope(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), notifications, semantic, consumer, graphchange.Scope{SpaceID: spaceID.String(), DomainID: domainID.String()}); err != nil {
		t.Fatalf("second replayAsyncSemanticDirtyScope() error = %v", err)
	}
	dirty, err = mgr.ListGraphDirtyEvents(ctx)
	if err != nil {
		t.Fatalf("ListGraphDirtyEvents() after second replay error = %v", err)
	}
	if len(dirty) != 2 {
		t.Fatalf("dirty events after second replay = %d, want deduplicated 2", len(dirty))
	}
	checkpoint, err := mgr.GetCheckpoint(ctx, asyncSemanticDirtyCheckpointConsumer(domainID.String()))
	if err != nil {
		t.Fatalf("GetCheckpoint() error = %v", err)
	}
	if checkpoint.LastGraphRevision != 2 {
		t.Fatalf("checkpoint revision = %d, want 2", checkpoint.LastGraphRevision)
	}
}

func newTestSemanticModule(t *testing.T) *semanticservice.Module {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := daemonconfig.Config{DataDir: t.TempDir(), Mode: daemonconfig.DefaultMode, LogLevel: daemonconfig.DefaultLogLevel, LogFormat: daemonconfig.DefaultLogFormat, GRPCAddr: "127.0.0.1:0"}
	rt := daemonruntime.New(cfg, logger, "", nil)
	semantic := semanticservice.NewModule()
	if result := semantic.Init(context.Background(), rt); !result.OK {
		t.Fatalf("semantic Init() failed: %+v", result)
	}
	return semantic
}
