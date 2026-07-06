package maintenance

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/graph/change"
	"github.com/myceldb/mycel/internal/graph/model"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	domainspace "github.com/myceldb/mycel/internal/space/model"
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

func TestDirtyEventAppenderRequiresManagerForNonEmptyEvent(t *testing.T) {
	ctx := context.Background()
	err := (DirtyEventAppender{}).OnGraphCommitted(ctx, graphchange.CommittedEvent{SpaceID: domainspace.SpaceID(uuid.New()), CreatedNodeIDs: []graph.NodeID{graph.NodeID(uuid.New())}})
	if err != ErrMaintenanceManagerRequired {
		t.Fatalf("expected ErrMaintenanceManagerRequired, got %v", err)
	}
}

func TestDirtyEventAppenderConvertsFullGraphChangeContext(t *testing.T) {
	ctx := context.Background()
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	oldDomainID := graph.DomainID(uuid.New())
	nodeID := graph.NodeID(uuid.New())
	updatedID := graph.NodeID(uuid.New())
	deletedID := graph.NodeID(uuid.New())
	oldParentID := graph.NodeID(uuid.New())
	newParentID := graph.NodeID(uuid.New())
	edgeID := graph.EdgeID(uuid.New())
	txnID := uuid.New()
	mgr := &recordingMaintenanceManager{}
	appender := DirtyEventAppender{MaintenanceManager: mgr}

	err := appender.OnGraphCommitted(ctx, graphchange.CommittedEvent{
		TxnID:         txnID,
		GraphRevision: 42,
		SpaceID:       spaceID,
		DomainIDs:     []graph.DomainID{domainID},
		CreatedNodeIDs: []graph.NodeID{
			nodeID,
		},
		UpdatedNodeIDs: []graph.NodeID{updatedID},
		DeletedNodeIDs: []graph.NodeID{deletedID},
		ChangedEdges: []graphchange.EdgeChange{{
			EdgeID: edgeID,
			Kind:   graph.EdgeKindContains,
			Change: "updated",
			FromID: newParentID,
			ToID:   nodeID,
		}},
		OldParentByNodeID: map[graph.NodeID]graph.NodeID{nodeID: oldParentID},
		NewParentByNodeID: map[graph.NodeID]graph.NodeID{nodeID: newParentID},
		OldDomainByNodeID: map[graph.NodeID]graph.DomainID{nodeID: oldDomainID},
		NewDomainByNodeID: map[graph.NodeID]graph.DomainID{nodeID: domainID},
	})
	if err != nil {
		t.Fatalf("append graph change: %v", err)
	}
	if len(mgr.events) != 1 {
		t.Fatalf("expected one event, got %+v", mgr.events)
	}
	event := mgr.events[0]
	if event.ID == uuid.Nil || event.TxnID != txnID || event.GraphRevision != 42 || event.SpaceID != spaceID {
		t.Fatalf("unexpected event identity: %+v", event)
	}
	if len(event.DomainIDs) != 1 || event.DomainIDs[0] != domainID {
		t.Fatalf("unexpected domains: %+v", event.DomainIDs)
	}
	if len(event.CreatedNodeIDs) != 1 || event.CreatedNodeIDs[0] != nodeID || len(event.UpdatedNodeIDs) != 1 || event.UpdatedNodeIDs[0] != updatedID || len(event.DeletedNodeIDs) != 1 || event.DeletedNodeIDs[0] != deletedID {
		t.Fatalf("unexpected node changes: %+v", event)
	}
	if len(event.ChangedEdges) != 1 || event.ChangedEdges[0].EdgeID != edgeID || event.ChangedEdges[0].Kind != graph.EdgeKindContains || event.ChangedEdges[0].Change != "updated" {
		t.Fatalf("unexpected edge changes: %+v", event.ChangedEdges)
	}
	if event.OldParentByNodeID[nodeID] != oldParentID || event.NewParentByNodeID[nodeID] != newParentID {
		t.Fatalf("unexpected parent context: %+v %+v", event.OldParentByNodeID, event.NewParentByNodeID)
	}
	if event.OldDomainByNodeID[nodeID] != oldDomainID || event.NewDomainByNodeID[nodeID] != domainID {
		t.Fatalf("unexpected domain context: %+v %+v", event.OldDomainByNodeID, event.NewDomainByNodeID)
	}
}

type recordingMaintenanceManager struct {
	events []domainsemantic.GraphDirtyEvent
}

func (m *recordingMaintenanceManager) Init(context.Context, string, domainspace.SpaceID) error {
	return nil
}
func (m *recordingMaintenanceManager) AppendGraphDirtyEvent(_ context.Context, event domainsemantic.GraphDirtyEvent) (domainsemantic.GraphDirtyEvent, error) {
	m.events = append(m.events, event)
	return event, nil
}
func (m *recordingMaintenanceManager) ListGraphDirtyEvents(context.Context) ([]domainsemantic.GraphDirtyEvent, error) {
	return m.events, nil
}
func (m *recordingMaintenanceManager) GetCheckpoint(context.Context, string) (storesemantic.MaintenanceCheckpoint, error) {
	return storesemantic.MaintenanceCheckpoint{}, nil
}
func (m *recordingMaintenanceManager) SaveCheckpoint(context.Context, storesemantic.MaintenanceCheckpoint) error {
	return nil
}
func (m *recordingMaintenanceManager) UpsertDirtyWorkItem(_ context.Context, item domainsemantic.SemanticDirtyWorkItem) (domainsemantic.SemanticDirtyWorkItem, error) {
	return item, nil
}
func (m *recordingMaintenanceManager) ListDirtyWorkItems(context.Context) ([]domainsemantic.SemanticDirtyWorkItem, error) {
	return nil, nil
}
func (m *recordingMaintenanceManager) ClaimReadyWork(context.Context, storesemantic.ClaimReadyWorkInput) ([]domainsemantic.SemanticDirtyWorkItem, error) {
	return nil, nil
}
func (m *recordingMaintenanceManager) CompleteWork(context.Context, uuid.UUID, storesemantic.WorkResult) error {
	return nil
}
func (m *recordingMaintenanceManager) FailWork(context.Context, uuid.UUID, storesemantic.WorkFailure) error {
	return nil
}
