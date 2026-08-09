package graphchange

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/google/uuid"
	graph "github.com/myceldb/mycel/internal/graph/model"
)

func TestMultiSinkInvokesSinksInOrder(t *testing.T) {
	calls := []string{}
	sink := MultiSink{
		SinkFunc(func(context.Context, CommittedEvent) error {
			calls = append(calls, "first")
			return nil
		}),
		nil,
		SinkFunc(func(context.Context, CommittedEvent) error {
			calls = append(calls, "second")
			return nil
		}),
	}
	if err := sink.OnGraphCommitted(context.Background(), CommittedEvent{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := []string{"first", "second"}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("calls = %#v, want %#v", calls, want)
	}
}

func TestMultiSinkJoinsErrorsAndContinues(t *testing.T) {
	errA := errors.New("a")
	errB := errors.New("b")
	calls := 0
	sink := MultiSink{
		SinkFunc(func(context.Context, CommittedEvent) error {
			calls++
			return errA
		}),
		SinkFunc(func(context.Context, CommittedEvent) error {
			calls++
			return errB
		}),
	}
	err := sink.OnGraphCommitted(context.Background(), CommittedEvent{})
	if !errors.Is(err, errA) || !errors.Is(err, errB) {
		t.Fatalf("expected joined errors, got %v", err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2", calls)
	}
}

func TestCommittedEventNormalizeDerivesAffectedIDsAndRevisionAliases(t *testing.T) {
	fromID := graph.NodeID(uuid.MustParse("00000000-0000-0000-0000-000000000001"))
	toID := graph.NodeID(uuid.MustParse("00000000-0000-0000-0000-000000000003"))
	edgeID := graph.EdgeID(uuid.MustParse("00000000-0000-0000-0000-000000000004"))
	nodeID := graph.NodeID(uuid.MustParse("00000000-0000-0000-0000-000000000002"))
	txnID := uuid.New()
	event := CommittedEvent{
		TxnID:          txnID,
		GraphRevision:  42,
		CreatedNodeIDs: []graph.NodeID{nodeID},
		ChangedEdges:   []EdgeChange{{EdgeID: edgeID, FromID: fromID, ToID: toID, Change: "updated"}},
		Changes:        []Change{{Type: ChangeTypeEdgeUpdated, EdgeID: edgeID.String(), AffectedNodeIDs: []string{fromID.String()}}},
	}

	event.Normalize()

	if event.Revision != 42 || event.GraphRevision != 42 {
		t.Fatalf("revision aliases = (%d,%d), want 42", event.Revision, event.GraphRevision)
	}
	if event.TransactionID != txnID || event.TxnID != txnID {
		t.Fatalf("transaction aliases = (%s,%s), want %s", event.TransactionID, event.TxnID, txnID)
	}
	wantNodes := []graph.NodeID{fromID, nodeID, toID}
	if !reflect.DeepEqual(event.AffectedNodeIDs, wantNodes) {
		t.Fatalf("AffectedNodeIDs = %#v, want %#v", event.AffectedNodeIDs, wantNodes)
	}
	if wantEdges := []graph.EdgeID{edgeID}; !reflect.DeepEqual(event.AffectedEdgeIDs, wantEdges) {
		t.Fatalf("AffectedEdgeIDs = %#v, want %#v", event.AffectedEdgeIDs, wantEdges)
	}
}

func TestApplyProjectionDropsUnrequestedSnapshotsAndMetadata(t *testing.T) {
	nodeID := graph.NodeID(uuid.New())
	node := graph.Node{ID: nodeID, Labels: []string{"Note"}}
	event := CommittedEvent{
		GraphRevision: 7,
		Origin:        OriginMetadata{ClientID: "client"},
		Changes:       []Change{{Type: ChangeTypeNodeUpdated, NodeID: nodeID.String(), Node: &node, OldNode: &node, ChangedFields: []string{"title"}, AffectedNodeIDs: []string{nodeID.String()}}},
	}

	projected := event.ApplyProjection(Projection{IncludeRevision: true, IncludeAffectedNodeIDs: true, IncludeNewNodeSnapshot: true})

	if projected.Revision != 7 || projected.GraphRevision != 7 {
		t.Fatalf("projected revision = (%d,%d), want 7", projected.Revision, projected.GraphRevision)
	}
	if projected.Origin.ClientID != "" {
		t.Fatalf("origin was not projected out: %#v", projected.Origin)
	}
	if len(projected.AffectedNodeIDs) != 1 || projected.AffectedNodeIDs[0] != nodeID {
		t.Fatalf("projected affected nodes = %#v", projected.AffectedNodeIDs)
	}
	if got := projected.Changes[0]; got.Node == nil || got.OldNode != nil || len(got.ChangedFields) != 0 || len(got.AffectedNodeIDs) != 1 {
		t.Fatalf("projected change = %#v", got)
	}
}
