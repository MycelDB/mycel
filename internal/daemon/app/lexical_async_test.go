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
	lexicalindex "github.com/myceldb/mycel/internal/search/lexical/index"
	lexicalservice "github.com/myceldb/mycel/internal/search/lexical/service"
	domainspace "github.com/myceldb/mycel/internal/space/model"
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
	if err := replayAsyncLexicalScope(ctx, logger, notifications, lexical, consumer, scope); err != nil {
		t.Fatalf("replayAsyncLexicalScope() error = %v", err)
	}
	status := lexical.Status(ctx, spaceID.String(), domainID.String(), 2)
	if status.State != lexicalservice.StateFresh || status.IndexedGraphRevision != 2 || status.LiveDocumentCount != 2 || status.SegmentCount != 2 {
		t.Fatalf("unexpected lexical status after first replay: %+v", status)
	}
	if err := replayAsyncLexicalScope(ctx, logger, notifications, lexical, consumer, scope); err != nil {
		t.Fatalf("second replayAsyncLexicalScope() error = %v", err)
	}
	status = lexical.Status(ctx, spaceID.String(), domainID.String(), 2)
	if status.IndexedGraphRevision != 2 || status.LiveDocumentCount != 2 || status.SegmentCount != 2 {
		t.Fatalf("unexpected lexical status after second replay: %+v", status)
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
