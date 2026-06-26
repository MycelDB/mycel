package maintenance

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/domain/graph"
	domainsemantic "github.com/myceldb/mycel/domain/semantic"
	domainspace "github.com/myceldb/mycel/domain/space"
	storesemantic "github.com/myceldb/mycel/store/semantic"
)

func TestAnalyzerCoalescesGraphDirtyEvents(t *testing.T) {
	ctx := context.Background()
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	nodeID := graph.NodeID(uuid.New())
	mgr := storesemantic.NewSpaceManager()
	if err := mgr.Init(ctx, filepath.Join(t.TempDir(), "semantic"), spaceID); err != nil {
		t.Fatalf("init failed: %v", err)
	}
	idx, err := mgr.UpsertSemanticIndex(ctx, domainsemantic.SemanticIndex{SpaceID: spaceID, DomainID: domainID, Key: "idx", Name: "idx", Purpose: domainsemantic.SemanticIndexPurposeSearch, SourcePolicy: domainsemantic.SemanticSourcePolicy{Extraction: domainsemantic.SourceExtractionSelf}, ModelEndpointID: uuid.New(), ModelID: uuid.New(), VectorStoreID: uuid.New(), Enabled: true})
	if err != nil {
		t.Fatalf("index upsert failed: %v", err)
	}
	txnID := uuid.New()
	event := domainsemantic.GraphDirtyEvent{TxnID: txnID, GraphRevision: 1, SpaceID: spaceID, DomainIDs: []graph.DomainID{domainID}, CreatedNodeIDs: []graph.NodeID{nodeID}, CommittedAt: time.Now().UTC()}
	if _, err := mgr.AppendGraphDirtyEvent(ctx, event); err != nil {
		t.Fatalf("append event failed: %v", err)
	}
	if _, err := mgr.AppendGraphDirtyEvent(ctx, event); err != nil {
		t.Fatalf("append duplicate failed: %v", err)
	}
	events, err := mgr.ListGraphDirtyEvents(ctx)
	if err != nil || len(events) != 1 {
		t.Fatalf("expected idempotent event list, events=%+v err=%v", events, err)
	}
	res, err := (Analyzer{SpaceManager: mgr}).AnalyzeOnce(ctx, AnalyzeInput{})
	if err != nil {
		t.Fatalf("analyze failed: %v", err)
	}
	if res.ProcessedEvents != 1 || res.EnqueuedItems != 1 {
		t.Fatalf("unexpected analyze result %+v", res)
	}
	items, err := mgr.ListDirtyWorkItems(ctx)
	if err != nil || len(items) != 1 {
		t.Fatalf("expected one dirty item, items=%+v err=%v", items, err)
	}
	if items[0].SemanticIndexID != idx.ID || items[0].TargetNodeID != nodeID || items[0].Reason != "node_created" || items[0].Status != domainsemantic.SemanticDirtyWorkStatusPending {
		t.Fatalf("unexpected dirty item %+v", items[0])
	}
	states, err := mgr.ListIndexStates(ctx)
	if err != nil || len(states) != 1 || states[0].GraphDirtyCheckpointRevision != 1 || states[0].DirtyCount != 1 {
		t.Fatalf("unexpected states %+v err=%v", states, err)
	}
	res, err = (Analyzer{SpaceManager: mgr}).AnalyzeOnce(ctx, AnalyzeInput{})
	if err != nil {
		t.Fatalf("second analyze failed: %v", err)
	}
	if res.ProcessedEvents != 0 {
		t.Fatalf("expected checkpoint skip, got %+v", res)
	}
}
