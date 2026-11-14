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
	graphjournal "github.com/myceldb/mycel/internal/graph/journal"
	graph "github.com/myceldb/mycel/internal/graph/model"
	graphnotification "github.com/myceldb/mycel/internal/graph/notification"
	"github.com/myceldb/mycel/internal/runtime/quiesce"
	semanticservice "github.com/myceldb/mycel/internal/semantic/service"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAsyncSemanticDirtyConsumerAppendsEventAndCheckpoint(t *testing.T) {
	ctx := context.Background()
	semantic := newTestSemanticModule(t)
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	nodeID := graph.NodeID(uuid.New())
	event := semanticDirtyTestEvent(spaceID, domainID, nodeID, 7)
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

func TestAsyncSemanticDirtyConsumerRejectedDuringSemanticQuiesce(t *testing.T) {
	ctx := context.Background()
	semantic, coordinator := newTestSemanticModuleWithQuiesce(t)
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	event := semanticDirtyTestEvent(spaceID, domainID, graph.NodeID(uuid.New()), 11)
	lease, err := coordinator.QuiesceAll(ctx, quiesce.Request{Reason: "test backup", Mode: quiesce.ModeBackup, Source: "test"})
	if err != nil {
		t.Fatalf("QuiesceAll() error = %v", err)
	}
	consumer := &asyncSemanticDirtyConsumer{semantic: semantic, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	err = consumer.HandleGraphChange(ctx, event)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("HandleGraphChange() code = %v, want %v (err=%v)", status.Code(err), codes.Unavailable, err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatalf("release quiesce: %v", err)
	}
	mgr, err := semantic.MaintenanceManager(ctx, spaceID)
	if err != nil {
		t.Fatalf("MaintenanceManager() error = %v", err)
	}
	dirty, err := mgr.ListGraphDirtyEvents(ctx)
	if err != nil {
		t.Fatalf("ListGraphDirtyEvents() error = %v", err)
	}
	if len(dirty) != 0 {
		t.Fatalf("dirty events = %d, want 0 while semantic quiesced", len(dirty))
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

func TestAsyncSemanticDirtyJournalReplaySurvivesNotificationRetentionGap(t *testing.T) {
	ctx := context.Background()
	semantic := newTestSemanticModule(t)
	scope := graphchange.Scope{SpaceID: uuid.NewString(), DomainID: uuid.NewString()}
	spaceID := domainspace.SpaceID(uuid.MustParse(scope.SpaceID))
	domainID := graph.DomainID(uuid.MustParse(scope.DomainID))
	first := semanticDirtyTestEvent(spaceID, domainID, graph.NodeID(uuid.New()), 1)
	second := semanticDirtyTestEvent(spaceID, domainID, graph.NodeID(uuid.New()), 2)
	firstEntry, err := graphjournal.EntryFromEvent(first)
	if err != nil {
		t.Fatalf("EntryFromEvent(first) error = %v", err)
	}
	secondEntry, err := graphjournal.EntryFromEvent(second)
	if err != nil {
		t.Fatalf("EntryFromEvent(second) error = %v", err)
	}
	consumer := &asyncSemanticDirtyConsumer{semantic: semantic, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), journal: semanticJournalTestReader{entries: []graphjournal.Entry{firstEntry, secondEntry}, latest: 2}, partitionCount: 1}
	if err := replayAsyncSemanticDirtyJournalScope(ctx, consumer.logger, semantic, consumer, scope); err != nil {
		t.Fatalf("replayAsyncSemanticDirtyJournalScope() error = %v", err)
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
		t.Fatalf("dirty events after authoritative replay = %d, want 2", len(dirty))
	}
	checkpoint, err := mgr.GetCheckpoint(ctx, asyncSemanticDirtyCheckpointConsumer(domainID.String()))
	if err != nil {
		t.Fatalf("GetCheckpoint() error = %v", err)
	}
	if checkpoint.LastGraphRevision != 2 || checkpoint.RebuildRequired {
		t.Fatalf("unexpected authoritative replay checkpoint: %+v", checkpoint)
	}
}

type semanticJournalTestReader struct {
	entries []graphjournal.Entry
	latest  int64
}

func (r semanticJournalTestReader) GraphChangeJournalEntriesAfter(_ context.Context, _ uint32, _ graphchange.Scope, after uint64) ([]graphjournal.Entry, error) {
	var out []graphjournal.Entry
	for _, entry := range r.entries {
		if entry.Revision > after {
			out = append(out, entry)
		}
	}
	return out, nil
}

func (r semanticJournalTestReader) GraphChangeJournalScopes(context.Context, uint32) ([]graphchange.Scope, error) {
	return nil, nil
}

func (r semanticJournalTestReader) CurrentDomainRevision(context.Context, string, string) (int64, error) {
	return r.latest, nil
}

func TestAsyncSemanticDirtyJournalScopesRetryAfterEarlyEmptyRestore(t *testing.T) {
	ctx := context.Background()
	semantic := newTestSemanticModule(t)
	scope := graphchange.Scope{SpaceID: uuid.NewString(), DomainID: uuid.NewString()}
	spaceID := domainspace.SpaceID(uuid.MustParse(scope.SpaceID))
	domainID := graph.DomainID(uuid.MustParse(scope.DomainID))
	entry, err := graphjournal.EntryFromEvent(semanticDirtyTestEvent(spaceID, domainID, graph.NodeID(uuid.New()), 1))
	if err != nil {
		t.Fatalf("EntryFromEvent() error = %v", err)
	}
	reader := &delayedSemanticJournalTestReader{scope: scope, entry: entry}
	consumer := &asyncSemanticDirtyConsumer{semantic: semantic, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), journal: reader, partitionCount: 1}
	replayAsyncSemanticDirtyJournalScopes(ctx, consumer.logger, semantic, consumer)
	if reader.scopeCalls < 2 {
		t.Fatalf("journal scope calls = %d, want retry after early empty restore", reader.scopeCalls)
	}
	mgr, err := semantic.MaintenanceManager(ctx, spaceID)
	if err != nil {
		t.Fatalf("MaintenanceManager() error = %v", err)
	}
	checkpoint, err := mgr.GetCheckpoint(ctx, asyncSemanticDirtyCheckpointConsumer(domainID.String()))
	if err != nil {
		t.Fatalf("GetCheckpoint() error = %v", err)
	}
	if checkpoint.LastGraphRevision != 1 || checkpoint.RebuildRequired {
		t.Fatalf("checkpoint = %+v, want revision 1 without rebuild_required", checkpoint)
	}
}

type delayedSemanticJournalTestReader struct {
	scope      graphchange.Scope
	entry      graphjournal.Entry
	scopeCalls int
}

func (r *delayedSemanticJournalTestReader) GraphChangeJournalEntriesAfter(_ context.Context, _ uint32, _ graphchange.Scope, after uint64) ([]graphjournal.Entry, error) {
	if r.entry.Revision <= after {
		return nil, nil
	}
	return []graphjournal.Entry{r.entry}, nil
}

func (r *delayedSemanticJournalTestReader) GraphChangeJournalScopes(context.Context, uint32) ([]graphchange.Scope, error) {
	r.scopeCalls++
	if r.scopeCalls == 1 {
		return nil, nil
	}
	return []graphchange.Scope{r.scope}, nil
}

func (r *delayedSemanticJournalTestReader) CurrentDomainRevision(context.Context, string, string) (int64, error) {
	return int64(r.entry.Revision), nil
}

func TestAsyncSemanticDirtyJournalGapRequiresRebuild(t *testing.T) {
	ctx := context.Background()
	semantic := newTestSemanticModule(t)
	scope := graphchange.Scope{SpaceID: uuid.NewString(), DomainID: uuid.NewString()}
	spaceID := domainspace.SpaceID(uuid.MustParse(scope.SpaceID))
	domainID := graph.DomainID(uuid.MustParse(scope.DomainID))
	entry, err := graphjournal.EntryFromEvent(semanticDirtyTestEvent(spaceID, domainID, graph.NodeID(uuid.New()), 2))
	if err != nil {
		t.Fatalf("EntryFromEvent() error = %v", err)
	}
	consumer := &asyncSemanticDirtyConsumer{semantic: semantic, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), journal: semanticJournalTestReader{entries: []graphjournal.Entry{entry}, latest: 2}, partitionCount: 1}
	if err := replayAsyncSemanticDirtyJournalScope(ctx, consumer.logger, semantic, consumer, scope); err == nil {
		t.Fatal("replayAsyncSemanticDirtyJournalScope() error = nil, want rebuild-required gap")
	}
	mgr, err := semantic.MaintenanceManager(ctx, spaceID)
	if err != nil {
		t.Fatalf("MaintenanceManager() error = %v", err)
	}
	checkpoint, err := mgr.GetCheckpoint(ctx, asyncSemanticDirtyCheckpointConsumer(domainID.String()))
	if err != nil {
		t.Fatalf("GetCheckpoint() error = %v", err)
	}
	if !checkpoint.RebuildRequired || checkpoint.RebuildReason == "" {
		t.Fatalf("checkpoint = %+v, want rebuild_required", checkpoint)
	}
}

func semanticDirtyTestEvent(spaceID domainspace.SpaceID, domainID graph.DomainID, nodeID graph.NodeID, revision uint64) graphchange.CommittedEvent {
	return graphchange.CommittedEvent{ID: uuid.New(), SpaceID: spaceID, DomainID: domainID, DomainIDs: []graph.DomainID{domainID}, TxnID: uuid.New(), TransactionID: uuid.New(), GraphRevision: revision, Revision: revision, UpdatedNodeIDs: []graph.NodeID{nodeID}, AffectedNodeIDs: []graph.NodeID{nodeID}, CommittedAt: time.Now().UTC()}
}

func newTestSemanticModule(t *testing.T) *semanticservice.Module {
	t.Helper()
	semantic, _ := newTestSemanticModuleWithQuiesce(t)
	return semantic
}

func newTestSemanticModuleWithQuiesce(t *testing.T) (*semanticservice.Module, *quiesce.Coordinator) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := daemonconfig.Config{DataDir: t.TempDir(), Mode: daemonconfig.DefaultMode, LogLevel: daemonconfig.DefaultLogLevel, LogFormat: daemonconfig.DefaultLogFormat, GRPCAddr: "127.0.0.1:0"}
	rt := daemonruntime.New(cfg, logger, "", nil)
	semantic := semanticservice.NewModule()
	if result := semantic.Init(context.Background(), rt); !result.OK {
		t.Fatalf("semantic Init() failed: %+v", result)
	}
	return semantic, rt.Quiesce
}
