package client

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	graph "github.com/myceldb/mycel/internal/graph/model"
	graphnotification "github.com/myceldb/mycel/internal/graph/notification"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestWatchGraphChangesFailsClosedWhenLocalNodeIsNotGraphLeader(t *testing.T) {
	spaceModule, _, userID, spaceID, domainID := initSessionServiceTestModules(t)
	svc := NewGraphChangeService(graphnotification.NewModule(), spaceModule).WithGraphWriteLeaderChecker(fakeGraphChangeLeaderChecker{err: status.Error(codes.Unavailable, "not local graph leader")})
	ctx := daemonauth.ContextWithPrincipal(context.Background(), daemonauth.Principal{Kind: daemonauth.PrincipalKindHuman, PrincipalID: userID, Username: "alice"})
	err := svc.WatchGraphChanges(&clientv1.WatchGraphChangesRequest{SpaceId: spaceID, DomainId: domainID}, fakeGraphChangeWatchServer{ctx: ctx})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("WatchGraphChanges() error = %v, want Unavailable", err)
	}
}

type fakeGraphChangeLeaderChecker struct{ err error }

func (f fakeGraphChangeLeaderChecker) RequireLocalGraphWriteLeader(ctx context.Context, spaceID string) error {
	return f.err
}

type fakeGraphChangeWatchServer struct {
	grpc.ServerStream
	ctx  context.Context
	sent []*clientv1.WatchGraphChangesResponse
}

func (s fakeGraphChangeWatchServer) Send(msg *clientv1.WatchGraphChangesResponse) error { return nil }
func (s fakeGraphChangeWatchServer) Context() context.Context                           { return s.ctx }
func (s fakeGraphChangeWatchServer) SetHeader(metadata.MD) error                        { return nil }
func (s fakeGraphChangeWatchServer) SendHeader(metadata.MD) error                       { return nil }
func (s fakeGraphChangeWatchServer) SetTrailer(metadata.MD)                             {}
func (s fakeGraphChangeWatchServer) SendMsg(any) error                                  { return nil }
func (s fakeGraphChangeWatchServer) RecvMsg(any) error                                  { return nil }

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

func TestMapGraphChangeEventRecomputesEnvelopeAffectedIDsAfterFiltering(t *testing.T) {
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
			{Type: graphchange.ChangeTypeNodeUpdated, NodeID: matchingID.String(), Node: &graph.Node{ID: graph.NodeID(matchingID), Labels: []string{"Note"}}, AffectedNodeIDs: []string{matchingID.String()}},
			{Type: graphchange.ChangeTypeNodeUpdated, NodeID: otherID.String(), Node: &graph.Node{ID: graph.NodeID(otherID), Labels: []string{"Task"}}, AffectedNodeIDs: []string{otherID.String()}},
		},
	}
	filter := graphChangeFilterFromProto(&clientv1.GraphChangeFilter{Labels: []string{"Note"}})
	projection := graphchange.Projection{IncludeRevision: true, IncludeAffectedNodeIDs: true, IncludeNewNodeSnapshot: true}

	mapped := mapGraphChangeEvent(event, filter, projection)
	if mapped == nil || len(mapped.GetChanges()) != 1 || mapped.GetChanges()[0].GetNodeId() != matchingID.String() {
		t.Fatalf("expected only matching label change, got %#v", mapped)
	}
	if got := mapped.GetAffectedNodeIds(); len(got) != 1 || got[0] != matchingID.String() {
		t.Fatalf("envelope affected node IDs leaked filtered changes: %#v", got)
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
