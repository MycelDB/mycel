package routing

import (
	"context"
	"testing"

	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/clustering/partitioning"
)

type fakeLeaderResolver struct {
	leaders map[partitioning.PartitionID]consensus.NodeID
}

func (f fakeLeaderResolver) LeaderForPartition(ctx context.Context, partitionID partitioning.PartitionID) (consensus.NodeID, error) {
	return f.leaders[partitionID], nil
}

type fakeForwarder struct {
	called bool
	route  Route
}

func (f *fakeForwarder) ForwardForSpace(ctx context.Context, route Route, fn func(context.Context) error) error {
	f.called = true
	f.route = route
	return fn(ctx)
}
func (f *fakeForwarder) ForwardForSpaceValue(ctx context.Context, route Route, fn SpaceFunc[any]) (any, error) {
	f.called = true
	f.route = route
	return fn(ctx)
}

func TestRaftExecutorExecutesLocalLeader(t *testing.T) {
	spaceID := "00000000-0000-0000-0000-000000000001"
	partitionID, err := partitioning.PartitionForSpace(spaceID, 64)
	if err != nil {
		t.Fatalf("PartitionForSpace() error = %v", err)
	}
	forwarder := &fakeForwarder{}
	exec := NewRaftExecutor(64, 2, fakeLeaderResolver{leaders: map[partitioning.PartitionID]consensus.NodeID{partitionID: 2}}, forwarder)
	called := false
	if err := exec.ForSpace(context.Background(), spaceID, func(ctx context.Context) error { called = true; return nil }); err != nil {
		t.Fatalf("ForSpace() error = %v", err)
	}
	if !called {
		t.Fatal("expected local callback")
	}
	if forwarder.called {
		t.Fatal("did not expect forwarding")
	}
}

func TestRaftExecutorForwardsRemoteLeader(t *testing.T) {
	spaceID := "00000000-0000-0000-0000-000000000001"
	partitionID, err := partitioning.PartitionForSpace(spaceID, 64)
	if err != nil {
		t.Fatalf("PartitionForSpace() error = %v", err)
	}
	forwarder := &fakeForwarder{}
	exec := NewRaftExecutor(64, 1, fakeLeaderResolver{leaders: map[partitioning.PartitionID]consensus.NodeID{partitionID: 2}}, forwarder)
	called := false
	if err := exec.ForSpace(context.Background(), spaceID, func(ctx context.Context) error { called = true; return nil }); err != nil {
		t.Fatalf("ForSpace() error = %v", err)
	}
	if !called || !forwarder.called {
		t.Fatalf("expected forwarded callback called=%v forwarded=%v", called, forwarder.called)
	}
	if forwarder.route.Leader != 2 || forwarder.route.LocalNode != 1 || forwarder.route.PartitionID != partitionID {
		t.Fatalf("unexpected route: %+v", forwarder.route)
	}
}

func TestRaftExecutorValue(t *testing.T) {
	spaceID := "00000000-0000-0000-0000-000000000001"
	partitionID, err := partitioning.PartitionForSpace(spaceID, 64)
	if err != nil {
		t.Fatalf("PartitionForSpace() error = %v", err)
	}
	exec := NewRaftExecutor(64, 2, fakeLeaderResolver{leaders: map[partitioning.PartitionID]consensus.NodeID{partitionID: 2}}, nil)
	got, err := RaftForSpaceValue[string](exec, context.Background(), spaceID, func(ctx context.Context) (string, error) { return "ok", nil })
	if err != nil {
		t.Fatalf("RaftForSpaceValue() error = %v", err)
	}
	if got != "ok" {
		t.Fatalf("got %q want ok", got)
	}
}

func TestRaftExecutorRejectsNoLeader(t *testing.T) {
	exec := NewRaftExecutor(64, 1, fakeLeaderResolver{leaders: map[partitioning.PartitionID]consensus.NodeID{}}, nil)
	if err := exec.ForSpace(context.Background(), "00000000-0000-0000-0000-000000000001", func(ctx context.Context) error { return nil }); err == nil {
		t.Fatal("expected no leader error")
	}
}
