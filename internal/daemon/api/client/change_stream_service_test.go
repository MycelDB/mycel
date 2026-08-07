package client

import (
	"testing"
	"time"

	"github.com/google/uuid"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	graph "github.com/myceldb/mycel/internal/graph/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestMapGraphChangeEventFiltersBeforeApplyingProjection(t *testing.T) {
	spaceID := uuid.New()
	domainID := uuid.New()
	nodeID := uuid.New()
	event := graphchange.CommittedEvent{
		ID:          uuid.New(),
		SpaceID:     domainspace.SpaceID(spaceID),
		DomainID:    graph.DomainID(domainID),
		Revision:    7,
		CommittedAt: time.Now().UTC(),
		Origin:      graphchange.OriginMetadata{TransactionID: uuid.NewString(), OperationID: uuid.NewString()},
		Changes: []graphchange.Change{{
			Type:            graphchange.ChangeTypeNodeUpdated,
			NodeID:          nodeID.String(),
			Node:            &graph.Node{ID: graph.NodeID(nodeID), Labels: []string{"Note"}},
			ChangedFields:   []string{"payload.text"},
			AffectedNodeIDs: []string{nodeID.String()},
		}},
	}
	filter := graphChangeFilterFromProto(&clientv1.GraphChangeFilter{ChangedFields: []string{"payload.text"}})
	projection := graphchange.Projection{IncludeRevision: true}

	mapped := mapGraphChangeEvent(event, filter, projection)
	if mapped == nil || len(mapped.GetChanges()) != 1 {
		t.Fatalf("expected filtered change to survive projection, got %#v", mapped)
	}
	if len(mapped.GetChanges()[0].GetChangedFields()) != 0 || len(mapped.GetChanges()[0].GetAffectedNodeIds()) != 0 || mapped.GetChanges()[0].GetNewNode() != nil {
		t.Fatalf("projection leaked fields: %#v", mapped.GetChanges()[0])
	}
}

func TestMapGraphChangeEventPrunesLabelFilteredChanges(t *testing.T) {
	spaceID := uuid.New()
	domainID := uuid.New()
	matchingID := uuid.New()
	otherID := uuid.New()
	event := graphchange.CommittedEvent{
		ID:          uuid.New(),
		SpaceID:     domainspace.SpaceID(spaceID),
		DomainID:    graph.DomainID(domainID),
		Revision:    8,
		CommittedAt: time.Now().UTC(),
		Changes: []graphchange.Change{
			{Type: graphchange.ChangeTypeNodeUpdated, NodeID: matchingID.String(), Node: &graph.Node{ID: graph.NodeID(matchingID), Labels: []string{"Note"}}},
			{Type: graphchange.ChangeTypeNodeUpdated, NodeID: otherID.String(), Node: &graph.Node{ID: graph.NodeID(otherID), Labels: []string{"Task"}}},
		},
	}
	filter := graphChangeFilterFromProto(&clientv1.GraphChangeFilter{Labels: []string{"Note"}})
	projection := graphchange.Projection{IncludeRevision: true, IncludeNewNodeSnapshot: true}

	mapped := mapGraphChangeEvent(event, filter, projection)
	if mapped == nil || len(mapped.GetChanges()) != 1 || mapped.GetChanges()[0].GetNodeId() != matchingID.String() {
		t.Fatalf("expected only matching label change, got %#v", mapped)
	}
}
