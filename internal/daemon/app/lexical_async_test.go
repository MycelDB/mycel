package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	adminapi "github.com/myceldb/mycel/internal/daemon/api/admin"
	clientapi "github.com/myceldb/mycel/internal/daemon/api/client"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	daemonconfig "github.com/myceldb/mycel/internal/daemon/config"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	graph "github.com/myceldb/mycel/internal/graph/model"
	graphnotification "github.com/myceldb/mycel/internal/graph/notification"
	graphservice "github.com/myceldb/mycel/internal/graph/service"
	identity "github.com/myceldb/mycel/internal/identity/model"
	principalservice "github.com/myceldb/mycel/internal/identity/service/principal"
	coreruntime "github.com/myceldb/mycel/internal/runtime"
	"github.com/myceldb/mycel/internal/search/lexical/analyzer"
	lexicalindex "github.com/myceldb/mycel/internal/search/lexical/index"
	lexicalservice "github.com/myceldb/mycel/internal/search/lexical/service"
	sessionservice "github.com/myceldb/mycel/internal/session/service"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	spaceservice "github.com/myceldb/mycel/internal/space/service"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestAsyncLexicalConsumerIndexesGraphChange(t *testing.T) {
	ctx := context.Background()
	lexical := newTestLexicalModule(t)
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	nodeID := graph.NodeID(uuid.New())
	event := lexicalNodeEvent(spaceID, domainID, nodeID, 7, "raft blossom")
	consumer := &asyncLexicalConsumer{lexical: lexical, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if err := consumer.HandleGraphChange(ctx, event); err != nil {
		t.Fatalf("HandleGraphChange() error = %v", err)
	}
	status := lexical.Status(ctx, spaceID.String(), domainID.String(), 7)
	if status.State != lexicalservice.StateFresh || status.IndexedGraphRevision != 7 || status.LiveDocumentCount != 1 || status.SegmentCount != 1 {
		t.Fatalf("unexpected lexical status: %+v", status)
	}
	if err := consumer.HandleGraphChange(ctx, event); err != nil {
		t.Fatalf("duplicate HandleGraphChange() error = %v", err)
	}
	status = lexical.Status(ctx, spaceID.String(), domainID.String(), 7)
	if status.IndexedGraphRevision != 7 || status.LiveDocumentCount != 1 || status.SegmentCount != 1 {
		t.Fatalf("unexpected lexical status after duplicate delivery: %+v", status)
	}
	res, err := lexical.Search(ctx, spaceID.String(), domainID.String(), "raft", lexicalindex.SearchOptions{PageSize: 10})
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(res.Results) != 1 || res.Results[0].NodeID != nodeID.String() || res.Results[0].IndexedGraphRevision != 7 {
		t.Fatalf("unexpected search results: %+v", res.Results)
	}
}

func TestAsyncLexicalConsumerAdvancesRevisionForNonDocumentChange(t *testing.T) {
	ctx := context.Background()
	lexical := newTestLexicalModule(t)
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	event := graphchange.CommittedEvent{
		ID:            uuid.New(),
		SpaceID:       spaceID,
		DomainID:      domainID,
		DomainIDs:     []graph.DomainID{domainID},
		GraphRevision: 1,
		Revision:      1,
		Changes:       []graphchange.Change{{Type: graphchange.ChangeTypeEdgeCreated, EdgeID: uuid.NewString()}},
		CommittedAt:   time.Now().UTC(),
	}
	consumer := &asyncLexicalConsumer{lexical: lexical, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if err := consumer.HandleGraphChange(ctx, event); err != nil {
		t.Fatalf("HandleGraphChange() error = %v", err)
	}
	status := lexical.Status(ctx, spaceID.String(), domainID.String(), 1)
	if status.State != lexicalservice.StateFresh || status.IndexedGraphRevision != 1 || status.LiveDocumentCount != 0 {
		t.Fatalf("non-document graph change status = %+v", status)
	}
}

func TestAsyncLexicalReplayUsesIndexedRevisionCursor(t *testing.T) {
	ctx := context.Background()
	lexical := newTestLexicalModule(t)
	notifications := graphnotification.NewModule()
	notifications.SetDataDirForTest(t.TempDir())
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	firstNode := graph.NodeID(uuid.New())
	secondNode := graph.NodeID(uuid.New())
	first := lexicalNodeEvent(spaceID, domainID, firstNode, 1, "alpha raft")
	second := lexicalNodeEvent(spaceID, domainID, secondNode, 2, "beta raft")
	if err := notifications.OnGraphCommitted(ctx, first); err != nil {
		t.Fatalf("OnGraphCommitted(first) error = %v", err)
	}
	if err := notifications.OnGraphCommitted(ctx, second); err != nil {
		t.Fatalf("OnGraphCommitted(second) error = %v", err)
	}
	consumer := &asyncLexicalConsumer{lexical: lexical, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	scope := graphchange.Scope{SpaceID: spaceID.String(), DomainID: domainID.String()}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := replayAsyncLexicalScope(ctx, logger, notifications, lexical, nil, consumer, scope); err != nil {
		t.Fatalf("replayAsyncLexicalScope() error = %v", err)
	}
	status := lexical.Status(ctx, spaceID.String(), domainID.String(), 2)
	if status.State != lexicalservice.StateFresh || status.IndexedGraphRevision != 2 || status.LiveDocumentCount != 2 || status.SegmentCount != 2 {
		t.Fatalf("unexpected lexical status after first replay: %+v", status)
	}
	if err := replayAsyncLexicalScope(ctx, logger, notifications, lexical, nil, consumer, scope); err != nil {
		t.Fatalf("second replayAsyncLexicalScope() error = %v", err)
	}
	status = lexical.Status(ctx, spaceID.String(), domainID.String(), 2)
	if status.IndexedGraphRevision != 2 || status.LiveDocumentCount != 2 || status.SegmentCount != 2 {
		t.Fatalf("unexpected lexical status after second replay: %+v", status)
	}
}

func TestAsyncLexicalReplayGapDoesNotAdvanceLaterRevisions(t *testing.T) {
	ctx := context.Background()
	lexical := newTestLexicalModule(t)
	notifications := graphnotification.NewModule()
	notifications.SetDataDirForTest(t.TempDir())
	notifications.SetRetentionForTest(1, time.Hour)
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	consumer := &asyncLexicalConsumer{lexical: lexical, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	first := lexicalNodeEvent(spaceID, domainID, graph.NodeID(uuid.New()), 1, "revision 1 raft")
	if err := consumer.HandleGraphChange(ctx, first); err != nil {
		t.Fatalf("HandleGraphChange(first) error = %v", err)
	}
	for revision := uint64(1); revision <= 3; revision++ {
		event := first
		if revision > 1 {
			event = lexicalNodeEvent(spaceID, domainID, graph.NodeID(uuid.New()), revision, "retained raft")
		}
		if err := notifications.OnGraphCommitted(ctx, event); err != nil {
			t.Fatalf("OnGraphCommitted(%d) error = %v", revision, err)
		}
	}
	scope := graphchange.Scope{SpaceID: spaceID.String(), DomainID: domainID.String()}
	if err := replayAsyncLexicalScope(ctx, consumer.logger, notifications, lexical, nil, consumer, scope); err == nil {
		t.Fatal("replayAsyncLexicalScope() error = nil, want retention gap")
	}
	fourth := lexicalNodeEvent(spaceID, domainID, graph.NodeID(uuid.New()), 4, "later raft")
	if err := notifications.OnGraphCommitted(ctx, fourth); err != nil {
		t.Fatalf("OnGraphCommitted(4) error = %v", err)
	}
	if err := replayAsyncLexicalScope(ctx, consumer.logger, notifications, lexical, nil, consumer, scope); err == nil {
		t.Fatal("second replayAsyncLexicalScope() error = nil, want retention gap")
	}
	status := lexical.Status(ctx, spaceID.String(), domainID.String(), 4)
	if status.State != lexicalservice.StateStale || status.IndexedGraphRevision != 1 {
		t.Fatalf("lexical cursor advanced across replay gap: %+v", status)
	}
}

func TestAsyncLexicalReplayStopsAtFailedRevision(t *testing.T) {
	ctx := context.Background()
	lexical := newTestLexicalModule(t)
	notifications := graphnotification.NewModule()
	notifications.SetDataDirForTest(t.TempDir())
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	for revision := uint64(1); revision <= 3; revision++ {
		if err := notifications.OnGraphCommitted(ctx, lexicalNodeEvent(spaceID, domainID, graph.NodeID(uuid.New()), revision, "ordered raft")); err != nil {
			t.Fatalf("OnGraphCommitted(%d) error = %v", revision, err)
		}
	}
	delegate := &asyncLexicalConsumer{lexical: lexical, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	consumer := &failingAsyncLexicalConsumer{delegate: delegate, failRevision: 2}
	scope := graphchange.Scope{SpaceID: spaceID.String(), DomainID: domainID.String()}
	if err := replayAsyncLexicalScope(ctx, delegate.logger, notifications, lexical, nil, consumer, scope); err == nil {
		t.Fatal("replayAsyncLexicalScope() error = nil, want revision 2 failure")
	}
	status := lexical.Status(ctx, spaceID.String(), domainID.String(), 3)
	if status.State != lexicalservice.StateStale || status.IndexedGraphRevision != 1 || status.LiveDocumentCount != 1 {
		t.Fatalf("lexical replay advanced beyond failed revision: %+v", status)
	}
	consumer.failRevision = 0
	if err := replayAsyncLexicalScope(ctx, delegate.logger, notifications, lexical, nil, consumer, scope); err != nil {
		t.Fatalf("replayAsyncLexicalScope(retry) error = %v", err)
	}
	status = lexical.Status(ctx, spaceID.String(), domainID.String(), 3)
	if status.State != lexicalservice.StateFresh || status.IndexedGraphRevision != 3 || status.LiveDocumentCount != 3 {
		t.Fatalf("lexical replay retry status = %+v", status)
	}
}

func TestValidateAsyncLexicalReplayRejectsHolesAndOutOfOrderEvents(t *testing.T) {
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	event := func(revision uint64) graphchange.CommittedEvent {
		return lexicalNodeEvent(spaceID, domainID, graph.NodeID(uuid.New()), revision, "raft")
	}
	if err := validateAsyncLexicalReplay([]graphchange.CommittedEvent{event(1), event(3)}, 0, 3); !errors.Is(err, graphnotification.ErrOutOfRange) {
		t.Fatalf("hole validation error = %v, want ErrOutOfRange", err)
	}
	if err := validateAsyncLexicalReplay([]graphchange.CommittedEvent{event(2), event(1)}, 0, 2); !errors.Is(err, graphnotification.ErrOutOfRange) {
		t.Fatalf("out-of-order validation error = %v, want ErrOutOfRange", err)
	}
}

func TestAsyncLexicalCatchupPrecedesRegisteredLiveDelivery(t *testing.T) {
	ctx := context.Background()
	lexical := newTestLexicalModule(t)
	notifications := graphnotification.NewModule()
	notifications.SetDataDirForTest(t.TempDir())
	t.Cleanup(func() {
		if err := notifications.Close(); err != nil {
			t.Errorf("close graph notification module: %v", err)
		}
	})
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	first := lexicalNodeEvent(spaceID, domainID, graph.NodeID(uuid.New()), 1, "alpha raft")
	second := lexicalNodeEvent(spaceID, domainID, graph.NodeID(uuid.New()), 2, "beta raft")
	if err := notifications.OnGraphCommitted(ctx, first); err != nil {
		t.Fatalf("OnGraphCommitted(first) error = %v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	registration, coordinator, err := registerAsyncLexicalConsumer(ctx, logger, notifications, lexical, nil)
	if err != nil {
		t.Fatalf("registerAsyncLexicalConsumer() error = %v", err)
	}
	t.Cleanup(func() {
		if err := registration.Close(); err != nil {
			t.Errorf("close lexical registration: %v", err)
		}
	})
	t.Cleanup(coordinator.Close)
	coordinator.schedule(graphchange.Scope{SpaceID: spaceID.String(), DomainID: domainID.String()})
	if err := notifications.OnGraphCommitted(ctx, second); err != nil {
		t.Fatalf("OnGraphCommitted(second) error = %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		status := lexical.Status(ctx, spaceID.String(), domainID.String(), 2)
		if status.State == lexicalservice.StateFresh && status.IndexedGraphRevision == 2 && status.LiveDocumentCount == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("lexical catch-up did not finish before live delivery: %+v", status)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestAsyncLexicalReplayRepairsCursorAheadOfDomainRevision(t *testing.T) {
	ctx := context.Background()
	lexical := newTestLexicalModule(t)
	notifications := graphnotification.NewModule()
	notifications.SetDataDirForTest(t.TempDir())
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	first := lexicalNodeEvent(spaceID, domainID, graph.NodeID(uuid.New()), 1, "alpha raft")
	second := lexicalNodeEvent(spaceID, domainID, graph.NodeID(uuid.New()), 2, "beta raft")
	if err := notifications.OnGraphCommitted(ctx, first); err != nil {
		t.Fatalf("OnGraphCommitted(first) error = %v", err)
	}
	if err := notifications.OnGraphCommitted(ctx, second); err != nil {
		t.Fatalf("OnGraphCommitted(second) error = %v", err)
	}
	if err := lexical.Rebuild(ctx, spaceID.String(), domainID.String(), nil, 9); err != nil {
		t.Fatalf("seed poisoned lexical cursor: %v", err)
	}
	if status := lexical.Status(ctx, spaceID.String(), domainID.String(), 2); status.State != lexicalservice.StateError {
		t.Fatalf("poisoned cursor status = %+v, want error", status)
	}

	consumer := &asyncLexicalConsumer{lexical: lexical, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if err := replayAsyncLexicalScope(ctx, consumer.logger, notifications, lexical, nil, consumer, graphchange.Scope{SpaceID: spaceID.String(), DomainID: domainID.String()}); err != nil {
		t.Fatalf("replayAsyncLexicalScope() error = %v", err)
	}
	status := lexical.Status(ctx, spaceID.String(), domainID.String(), 2)
	if status.State != lexicalservice.StateFresh || status.IndexedGraphRevision != 2 || status.LiveDocumentCount != 2 {
		t.Fatalf("repaired lexical status = %+v", status)
	}
	result, err := lexical.Search(ctx, spaceID.String(), domainID.String(), "raft", lexicalindex.SearchOptions{PageSize: 10})
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(result.Results) != 2 {
		t.Fatalf("Search() results = %+v, want both replayed nodes", result.Results)
	}
}

func TestAsyncLexicalReplayPreservesInvalidCursorWhenFullHistoryIsUnavailable(t *testing.T) {
	ctx := context.Background()
	lexical := newTestLexicalModule(t)
	notifications := graphnotification.NewModule()
	notifications.SetDataDirForTest(t.TempDir())
	notifications.SetRetentionForTest(1, time.Hour)
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	if err := notifications.OnGraphCommitted(ctx, lexicalNodeEvent(spaceID, domainID, graph.NodeID(uuid.New()), 1, "alpha raft")); err != nil {
		t.Fatalf("OnGraphCommitted(first) error = %v", err)
	}
	if err := notifications.OnGraphCommitted(ctx, lexicalNodeEvent(spaceID, domainID, graph.NodeID(uuid.New()), 2, "beta raft")); err != nil {
		t.Fatalf("OnGraphCommitted(second) error = %v", err)
	}
	if err := lexical.Rebuild(ctx, spaceID.String(), domainID.String(), nil, 9); err != nil {
		t.Fatalf("seed poisoned lexical cursor: %v", err)
	}
	consumer := &asyncLexicalConsumer{lexical: lexical, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	err := replayAsyncLexicalScope(ctx, consumer.logger, notifications, lexical, nil, consumer, graphchange.Scope{SpaceID: spaceID.String(), DomainID: domainID.String()})
	if err == nil {
		t.Fatal("replayAsyncLexicalScope() error = nil, want replay gap")
	}
	status := lexical.Status(ctx, spaceID.String(), domainID.String(), 2)
	if status.State != lexicalservice.StateError || status.IndexedGraphRevision != 9 {
		t.Fatalf("invalid cursor was reset without complete history: %+v", status)
	}
}

func TestAsyncLexicalDurableReplayStaysOrderedWithoutWriteBackpressure(t *testing.T) {
	ctx := context.Background()
	lexical := newTestLexicalModule(t)
	notifications := graphnotification.NewModule()
	notifications.SetDataDirForTest(t.TempDir())
	t.Cleanup(func() {
		if err := notifications.Close(); err != nil {
			t.Errorf("close graph notification module: %v", err)
		}
	})
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	first := lexicalNodeEvent(spaceID, domainID, graph.NodeID(uuid.New()), 1, "revision 1 raft")
	if err := notifications.OnGraphCommitted(ctx, first); err != nil {
		t.Fatalf("OnGraphCommitted(first) error = %v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	consumer := &asyncLexicalConsumer{lexical: lexical, logger: logger}
	if err := consumer.HandleGraphChange(ctx, first); err != nil {
		t.Fatalf("HandleGraphChange(first) error = %v", err)
	}
	registration, coordinator, err := registerAsyncLexicalConsumer(ctx, logger, notifications, lexical, nil)
	if err != nil {
		t.Fatalf("registerAsyncLexicalConsumer() error = %v", err)
	}
	t.Cleanup(func() {
		if err := registration.Close(); err != nil {
			t.Errorf("close lexical registration: %v", err)
		}
	})
	t.Cleanup(coordinator.Close)

	snapshotStarted := make(chan struct{})
	releaseSnapshot := make(chan struct{})
	rebuildDone := make(chan error, 1)
	go func() {
		rebuildDone <- lexical.RebuildFromSnapshot(ctx, spaceID.String(), domainID.String(), func() ([]lexicalindex.IndexedDocument, uint64, error) {
			close(snapshotStarted)
			<-releaseSnapshot
			doc := analyzer.ExtractNodeDocument(*first.Changes[0].Node)
			return []lexicalindex.IndexedDocument{{Document: doc, GraphRevision: 1}}, 1, nil
		})
	}()
	<-snapshotStarted

	const lastRevision = uint64(68)
	publishDone := make(chan error, 1)
	go func() {
		for revision := uint64(2); revision <= lastRevision; revision++ {
			event := lexicalNodeEvent(spaceID, domainID, graph.NodeID(uuid.New()), revision, "queued raft")
			if err := notifications.OnGraphCommitted(ctx, event); err != nil {
				publishDone <- err
				return
			}
		}
		publishDone <- nil
	}()
	select {
	case err := <-publishDone:
		if err != nil {
			t.Fatalf("publish queued graph changes: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("graph-change publication blocked behind lexical rebuild")
	}
	current, err := notifications.CurrentRevision(ctx, spaceID.String(), domainID.String())
	if err != nil {
		t.Fatalf("CurrentRevision() error = %v", err)
	}
	if current != lastRevision {
		t.Fatalf("notification revision = %d, want %d before rebuild release", current, lastRevision)
	}
	close(releaseSnapshot)
	if err := <-rebuildDone; err != nil {
		t.Fatalf("RebuildFromSnapshot() error = %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		status := lexical.Status(ctx, spaceID.String(), domainID.String(), lastRevision)
		if status.State == lexicalservice.StateFresh && status.IndexedGraphRevision == lastRevision && status.LiveDocumentCount == int(lastRevision) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("lexical delivery did not reach revision %d: %+v", lastRevision, status)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestUserOwnedDomainGraphWritesAreIndexedRebuiltAndPreserved(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dataDir := t.TempDir()
	cfg := daemonconfig.Config{DataDir: dataDir, Mode: daemonconfig.DefaultMode, LogLevel: daemonconfig.DefaultLogLevel, LogFormat: daemonconfig.DefaultLogFormat, GRPCAddr: "127.0.0.1:0"}
	rt := daemonruntime.New(cfg, logger, "", nil)

	spaces := spaceservice.NewModule().WithPrincipalAuthorizer(allowLexicalSpaceAccess{})
	sessions := sessionservice.NewModule()
	graphs := graphservice.NewModule()
	notifications := graphnotification.NewModule()
	lexical := lexicalservice.NewModule()
	services := []struct {
		name    string
		service interface {
			Init(context.Context, coreruntime.Host) coreruntime.InitResult
		}
	}{
		{name: "spaces", service: spaces},
		{name: "sessions", service: sessions},
		{name: "graphs", service: graphs},
		{name: "notifications", service: notifications},
		{name: "lexical", service: lexical},
	}
	for _, service := range services {
		if result := service.service.Init(ctx, rt); !result.OK {
			t.Fatalf("init %s service: %+v", service.name, result)
		}
	}
	t.Cleanup(func() {
		if err := notifications.Close(); err != nil {
			t.Errorf("close graph notification service: %v", err)
		}
	})
	graphs.SetChangeSink(graphchange.Named("graph_change_notification", notifications))

	userID := uuid.NewString()
	space, defaultDomain, err := spaces.CreateSpace(ctx, spaceservice.CreateSpaceInput{Name: "User Space", OwnerPrincipalID: identity.PrincipalID(userID)})
	if err != nil {
		t.Fatalf("CreateSpace() error = %v", err)
	}
	contentDomain, err := spaces.CreateDomain(ctx, userID, spaceservice.CreateDomainInput{SpaceID: space.SpaceID.String(), Key: "content", Name: "Content"})
	if err != nil {
		t.Fatalf("CreateDomain() error = %v", err)
	}
	userCtx := daemonauth.ContextWithPrincipal(ctx, daemonauth.Principal{Kind: daemonauth.PrincipalKindHuman, PrincipalID: userID, Username: "alice"})
	sessionAPI := clientapi.NewSessionService(sessions, spaces)
	transactionAPI := clientapi.NewTransactionService(sessions, graphs, spaces)
	graphAPI := clientapi.NewGraphService(sessions, graphs)
	defaultSessionID := openLexicalTestSession(t, userCtx, sessionAPI, space.SpaceID.String(), defaultDomain.ID.String())
	contentSessionID := openLexicalTestSession(t, userCtx, sessionAPI, space.SpaceID.String(), contentDomain.ID.String())

	// Keep another domain ahead of the content domain. Lexical cursors are
	// domain-scoped and must not be poisoned by this space-wide revision.
	for i := 0; i < 4; i++ {
		writeLexicalTestNode(t, userCtx, transactionAPI, graphAPI, defaultSessionID, "default domain revision")
	}
	firstNodeID := writeLexicalTestNode(t, userCtx, transactionAPI, graphAPI, contentSessionID, "Quasar lexical status")
	if revision, err := graphs.CurrentDomainRevision(ctx, space.SpaceID.String(), contentDomain.ID.String()); err != nil || revision != 1 {
		t.Fatalf("content domain revision = %d, %v; want 1, nil", revision, err)
	}

	operatorCtx := daemonauth.ContextWithPrincipal(ctx, daemonauth.Principal{Kind: daemonauth.PrincipalKindHuman, PrincipalID: uuid.NewString(), Username: "operator"})
	maintenance := adminapi.NewAdminLexicalMaintenanceService(lexical, graphs, spaces, allowLexicalMaintenance{})
	statusBeforeRebuild, err := maintenance.GetLexicalMaintenanceStatus(operatorCtx, &adminv1.GetLexicalMaintenanceStatusRequest{SpaceId: space.SpaceID.String(), DomainId: contentDomain.ID.String()})
	if err != nil {
		t.Fatalf("GetLexicalMaintenanceStatus(before rebuild) error = %v", err)
	}
	if got := statusBeforeRebuild.GetStatus(); got.GetState() != clientv1.SearchFreshnessState_SEARCH_FRESHNESS_STATE_UNAVAILABLE || got.GetLatestKnownGraphRevision() != 1 || got.GetRevisionLag() != 1 {
		t.Fatalf("unexpected pre-rebuild lexical status: %+v", got)
	}
	if _, err := maintenance.RebuildLexicalIndex(operatorCtx, &adminv1.RebuildLexicalIndexRequest{SpaceId: space.SpaceID.String(), DomainId: contentDomain.ID.String()}); err != nil {
		t.Fatalf("RebuildLexicalIndex() error = %v", err)
	}

	searchAPI := clientapi.NewSearchService(lexical, spaces, graphs)
	assertLexicalSearchResult(t, userCtx, searchAPI, space.SpaceID.String(), contentDomain.ID.String(), "quasar", firstNodeID)
	statusAfterRebuild, err := maintenance.GetLexicalMaintenanceStatus(operatorCtx, &adminv1.GetLexicalMaintenanceStatusRequest{SpaceId: space.SpaceID.String(), DomainId: contentDomain.ID.String()})
	if err != nil {
		t.Fatalf("GetLexicalMaintenanceStatus(after rebuild) error = %v", err)
	}
	if got := statusAfterRebuild.GetStatus(); got.GetIndexedGraphRevision() != 1 || got.GetLatestKnownGraphRevision() != 1 || got.GetLiveDocumentCount() != 1 {
		t.Fatalf("unexpected rebuilt lexical status: %+v", got)
	}

	registration, coordinator, err := registerAsyncLexicalConsumer(ctx, logger, notifications, lexical, graphs)
	if err != nil {
		t.Fatalf("registerAsyncLexicalConsumer() error = %v", err)
	}
	t.Cleanup(func() {
		if err := registration.Close(); err != nil {
			t.Errorf("close async lexical consumer registration: %v", err)
		}
	})
	t.Cleanup(coordinator.Close)
	scopes, err := notifications.StoredScopes(ctx)
	if err != nil {
		t.Fatalf("StoredScopes() error = %v", err)
	}
	if err := replayAsyncLexicalScopes(ctx, logger, notifications, lexical, graphs, coordinator.consumer, scopes); err != nil {
		t.Fatalf("replayAsyncLexicalScopes() error = %v", err)
	}
	secondNodeID := writeLexicalTestNode(t, userCtx, transactionAPI, graphAPI, contentSessionID, "Marmalade async indexing")
	eventuallyLexicalSearchResult(t, userCtx, searchAPI, space.SpaceID.String(), contentDomain.ID.String(), "marmalade", secondNodeID)
	statusAfterWrite, err := maintenance.GetLexicalMaintenanceStatus(operatorCtx, &adminv1.GetLexicalMaintenanceStatusRequest{SpaceId: space.SpaceID.String(), DomainId: contentDomain.ID.String()})
	if err != nil {
		t.Fatalf("GetLexicalMaintenanceStatus(after write) error = %v", err)
	}
	if got := statusAfterWrite.GetStatus(); got.GetIndexedGraphRevision() != 2 || got.GetLatestKnownGraphRevision() != 2 || got.GetLiveDocumentCount() != 2 {
		t.Fatalf("unexpected post-write lexical status: %+v", got)
	}

	restartRT := daemonruntime.New(cfg, logger, "", nil)
	restartedLexical := lexicalservice.NewModule()
	if result := restartedLexical.Init(ctx, restartRT); !result.OK {
		t.Fatalf("init restarted lexical service: %+v", result)
	}
	restartedSearchAPI := clientapi.NewSearchService(restartedLexical, spaces, graphs)
	assertLexicalSearchResult(t, userCtx, restartedSearchAPI, space.SpaceID.String(), contentDomain.ID.String(), "marmalade", secondNodeID)
}

type failingAsyncLexicalConsumer struct {
	delegate     graphnotification.Consumer
	failRevision uint64
}

func (c *failingAsyncLexicalConsumer) HandleGraphChange(ctx context.Context, event graphchange.CommittedEvent) error {
	if eventRevision(event) == c.failRevision {
		return errors.New("injected lexical indexing failure")
	}
	return c.delegate.HandleGraphChange(ctx, event)
}

func (c *failingAsyncLexicalConsumer) HandleGraphChangeGap(ctx context.Context, gap graphchange.Gap) error {
	return c.delegate.HandleGraphChangeGap(ctx, gap)
}

type allowLexicalSpaceAccess struct{}

func (allowLexicalSpaceAccess) Authorize(context.Context, string, string, principalservice.AccessScope) error {
	return nil
}

func (allowLexicalSpaceAccess) EffectiveAccess(context.Context, string, principalservice.AccessScope) (principalservice.EffectiveAccess, error) {
	return principalservice.EffectiveAccess{}, nil
}

type allowLexicalMaintenance struct{}

func (allowLexicalMaintenance) HasCapability(context.Context, string, string) (bool, error) {
	return true, nil
}

func openLexicalTestSession(t *testing.T, ctx context.Context, sessions *clientapi.SessionService, spaceID string, domainID string) string {
	t.Helper()
	opened, err := sessions.OpenSession(ctx, &clientv1.OpenSessionRequest{SpaceId: spaceID, DomainId: domainID})
	if err != nil {
		t.Fatalf("OpenSession() error = %v", err)
	}
	return opened.GetSession().GetSessionId()
}

func writeLexicalTestNode(t *testing.T, ctx context.Context, transactions *clientapi.TransactionService, graphs *clientapi.GraphService, sessionID string, text string) string {
	t.Helper()
	begun, err := transactions.BeginTransaction(ctx, &clientv1.BeginTransactionRequest{SessionId: sessionID, Mode: clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE})
	if err != nil {
		t.Fatalf("BeginTransaction() error = %v", err)
	}
	properties, err := structpb.NewStruct(map[string]any{"title": text})
	if err != nil {
		t.Fatalf("NewStruct() error = %v", err)
	}
	created, err := graphs.CreateNode(ctx, &clientv1.CreateNodeRequest{TransactionId: begun.GetTransaction().GetTransactionId(), Node: &clientv1.NodeCreate{Labels: []string{"Page"}, Properties: properties}})
	if err != nil {
		t.Fatalf("CreateNode() error = %v", err)
	}
	if _, err := transactions.CommitTransaction(ctx, &clientv1.CommitTransactionRequest{TransactionId: begun.GetTransaction().GetTransactionId()}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}
	return created.GetNode().GetNodeId()
}

func assertLexicalSearchResult(t *testing.T, ctx context.Context, search *clientapi.SearchService, spaceID string, domainID string, query string, nodeID string) {
	t.Helper()
	result, err := search.Search(ctx, &clientv1.SearchRequest{SpaceId: spaceID, DomainId: domainID, Query: query, Mode: clientv1.SearchMode_SEARCH_MODE_LEXICAL})
	if err != nil {
		t.Fatalf("Search(%q) error = %v", query, err)
	}
	if len(result.GetResults()) != 1 || result.GetResults()[0].GetNodeId() != nodeID {
		t.Fatalf("Search(%q) results = %+v, want node %s", query, result.GetResults(), nodeID)
	}
}

func eventuallyLexicalSearchResult(t *testing.T, ctx context.Context, search *clientapi.SearchService, spaceID string, domainID string, query string, nodeID string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		result, err := search.Search(ctx, &clientv1.SearchRequest{SpaceId: spaceID, DomainId: domainID, Query: query, Mode: clientv1.SearchMode_SEARCH_MODE_LEXICAL})
		if err == nil && len(result.GetResults()) == 1 && result.GetResults()[0].GetNodeId() == nodeID {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Search(%q) did not return node %s before deadline: result=%+v err=%v", query, nodeID, result, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func newTestLexicalModule(t *testing.T) *lexicalservice.Module {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := daemonconfig.Config{DataDir: t.TempDir(), Mode: daemonconfig.DefaultMode, LogLevel: daemonconfig.DefaultLogLevel, LogFormat: daemonconfig.DefaultLogFormat, GRPCAddr: "127.0.0.1:0"}
	rt := daemonruntime.New(cfg, logger, "", nil)
	lexical := lexicalservice.NewModule()
	if result := lexical.Init(context.Background(), rt); !result.OK {
		t.Fatalf("lexical Init() failed: %+v", result)
	}
	return lexical
}

func lexicalNodeEvent(spaceID domainspace.SpaceID, domainID graph.DomainID, nodeID graph.NodeID, revision uint64, text string) graphchange.CommittedEvent {
	node := graph.Node{ID: nodeID, DomainID: domainID, Labels: []string{"Document"}, Payload: map[string]any{"text": text}, Properties: map[string]any{"title": text}}
	return graphchange.CommittedEvent{
		ID:              uuid.New(),
		SpaceID:         spaceID,
		DomainID:        domainID,
		DomainIDs:       []graph.DomainID{domainID},
		TxnID:           uuid.New(),
		TransactionID:   uuid.New(),
		GraphRevision:   revision,
		Revision:        revision,
		Changes:         []graphchange.Change{{Type: graphchange.ChangeTypeNodeCreated, NodeID: nodeID.String(), Node: &node, AffectedNodeIDs: []string{nodeID.String()}}},
		CreatedNodeIDs:  []graph.NodeID{nodeID},
		AffectedNodeIDs: []graph.NodeID{nodeID},
		CommittedAt:     time.Now().UTC(),
	}
}
