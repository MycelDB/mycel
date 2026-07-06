package maintenance

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/domain/graph"
	domainspace "github.com/myceldb/mycel/domain/space"
	"github.com/myceldb/mycel/internal/graphchange"
	storesemantic "github.com/myceldb/mycel/store/semantic"
)

func TestDirtyEventAppenderPersistsGraphChange(t *testing.T) {
	ctx := context.Background()
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	nodeID := graph.NodeID(uuid.New())
	mgr := storesemantic.NewMaintenanceManager()
	if err := mgr.Init(ctx, filepath.Join(t.TempDir(), "maintenance"), spaceID); err != nil {
		t.Fatalf("init maintenance manager: %v", err)
	}
	appender := DirtyEventAppender{MaintenanceManager: mgr}
	txnID := uuid.New()
	if err := appender.OnGraphCommitted(ctx, graphchange.CommittedEvent{TxnID: txnID, GraphRevision: 3, SpaceID: spaceID, DomainIDs: []graph.DomainID{domainID}, CreatedNodeIDs: []graph.NodeID{nodeID}}); err != nil {
		t.Fatalf("append graph change: %v", err)
	}
	events, err := mgr.ListGraphDirtyEvents(ctx)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(events) != 1 || events[0].TxnID != txnID || events[0].GraphRevision != 3 || events[0].CreatedNodeIDs[0] != nodeID {
		t.Fatalf("unexpected events: %+v", events)
	}
}

func TestDirtyEventAppenderIgnoresEmptyEvents(t *testing.T) {
	ctx := context.Background()
	if err := (DirtyEventAppender{}).OnGraphCommitted(ctx, graphchange.CommittedEvent{}); err != nil {
		t.Fatalf("empty event should not require manager: %v", err)
	}
}
