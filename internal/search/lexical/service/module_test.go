package service

import (
	"context"
	"testing"

	"github.com/google/uuid"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	graph "github.com/myceldb/mycel/internal/graph/model"
	"github.com/myceldb/mycel/internal/runtime/quiesce"
	"github.com/myceldb/mycel/internal/runtime/runtimetest"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestModuleQuiesceRejectsIndexMutation(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	rt := runtimetest.New(t.TempDir(), nil)
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("Init() error=%v", result.Error)
	}
	participants := rt.QuiesceCoordinator().Participants()
	if len(participants) != 1 || participants[0].Name() != ModuleName {
		t.Fatalf("participants=%v, want lexical_search", participants)
	}
	lease, err := rt.QuiesceCoordinator().QuiesceAll(ctx, quiesce.Request{Reason: "test backup", Mode: quiesce.ModeBackup, Source: "test"})
	if err != nil {
		t.Fatalf("QuiesceAll() error=%v", err)
	}
	defer lease.Release(context.Background())
	spaceID := uuid.New()
	domainID := graph.DomainID(uuid.New())
	nodeID := graph.NodeID(uuid.New())
	err = m.OnGraphCommitted(ctx, graphchange.CommittedEvent{
		ID:            uuid.New(),
		SpaceID:       domainspace.SpaceID(spaceID),
		DomainID:      domainID,
		DomainIDs:     []graph.DomainID{domainID},
		GraphRevision: 1,
		Revision:      1,
		Changes: []graphchange.Change{{Type: graphchange.ChangeTypeNodeCreated, NodeID: nodeID.String(), Node: &graph.Node{
			ID:       nodeID,
			DomainID: domainID,
			Labels:   []string{"Page"},
			Payload:  map[string]any{"text": "backup quiesce test"},
		}}},
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("OnGraphCommitted() error=%v, want Unavailable", err)
	}
}
