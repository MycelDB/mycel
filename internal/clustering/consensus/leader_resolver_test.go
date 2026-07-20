package consensus

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/myceldb/mycel/internal/clustering/partitioning"
)

func TestMultiGroupLeaderResolverNoLeader(t *testing.T) {
	transport := newMemoryTransport()
	mg, err := StartMultiGroup(context.Background(), MultiGroupOptions{NodeID: 1, PeerNodeIDs: []NodeID{1, 2, 3}, PartitionCount: 2, Transport: transport, StateMachines: StateMachineFactoryFunc{System: func() StateMachine { return NewSystemStateMachine() }, Partition: func(uint32) StateMachine { return &MemoryStateMachine{} }}, ElectionTick: 50, HeartbeatTick: 1})
	if err != nil {
		t.Fatalf("StartMultiGroup() error = %v", err)
	}
	defer mg.Stop()
	_, err = NewMultiGroupLeaderResolver(mg).LeaderForPartition(context.Background(), partitioning.PartitionID(0))
	if !errors.Is(err, ErrNoLeader) {
		t.Fatalf("LeaderForPartition() error=%v want ErrNoLeader", err)
	}
}

func TestMultiGroupLeaderResolverReturnsLeader(t *testing.T) {
	transport := newMemoryTransport()
	peers := []NodeID{1, 2, 3}
	groups := map[NodeID]*MultiGroup{}
	defer func() {
		for _, mg := range groups {
			mg.Stop()
		}
	}()
	for _, id := range peers {
		mg, err := StartMultiGroup(context.Background(), MultiGroupOptions{NodeID: id, PeerNodeIDs: peers, PartitionCount: 2, Transport: transport, StateMachines: StateMachineFactoryFunc{System: func() StateMachine { return NewSystemStateMachine() }, Partition: func(uint32) StateMachine { return &MemoryStateMachine{} }}, ElectionTick: 5, HeartbeatTick: 1})
		if err != nil {
			t.Fatalf("StartMultiGroup(%d) error = %v", id, err)
		}
		groups[id] = mg
		for _, g := range mg.Groups() {
			transport.register(g)
		}
	}
	stopTick := make(chan struct{})
	defer close(stopTick)
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopTick:
				return
			case <-ticker.C:
				for _, mg := range groups {
					mg.Tick()
				}
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resolver := NewMultiGroupLeaderResolver(groups[1])
	if err := WaitUntil(ctx, 20*time.Millisecond, func() bool { leader, _ := resolver.LeaderForPartition(ctx, 0); return leader != 0 }); err != nil {
		t.Fatalf("leader not resolved: %v", err)
	}
	leader, err := resolver.LeaderForPartition(ctx, 0)
	if err != nil {
		t.Fatalf("LeaderForPartition() error = %v", err)
	}
	if leader == 0 {
		t.Fatal("expected non-zero leader")
	}
}
