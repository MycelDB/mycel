package space

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/daemon/config"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
)

func TestGetSpaceViaRaftLeaderNoLeader(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, Logger: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	created, err := m.CreateSpaceWithResult(ctx, CreateSpaceInput{Name: "main", OwnerUserID: testUserID(t)})
	if err != nil {
		t.Fatalf("CreateSpaceWithResult() error = %v", err)
	}
	transport := noopSpaceRaftTransport{}
	mg, err := consensus.StartMultiGroup(ctx, consensus.MultiGroupOptions{NodeID: 1, PeerNodeIDs: []consensus.NodeID{1, 2, 3}, PartitionCount: 64, Transport: transport, StateMachines: consensus.StateMachineFactoryFunc{System: func() consensus.StateMachine { return consensus.NewSystemStateMachine() }, Partition: func(uint32) consensus.StateMachine { return &consensus.MemoryStateMachine{} }}, ElectionTick: 50, HeartbeatTick: 1})
	if err != nil {
		t.Fatalf("StartMultiGroup() error = %v", err)
	}
	defer mg.Stop()
	if _, err := m.getSpaceViaRaftLeader(ctx, created.Space.SpaceID, mg, 1, nil); err == nil {
		t.Fatal("expected no leader error")
	}
}

func TestGetSpaceViaRaftLeaderLocalLeader(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, Logger: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	created, err := m.CreateSpaceWithResult(ctx, CreateSpaceInput{Name: "main", OwnerUserID: testUserID(t)})
	if err != nil {
		t.Fatalf("CreateSpaceWithResult() error = %v", err)
	}
	routers := map[consensus.NodeID]*consensus.LocalMessageRouter{1: consensus.NewLocalMessageRouter(), 2: consensus.NewLocalMessageRouter(), 3: consensus.NewLocalMessageRouter()}
	transport := consensus.RoutedTransport{Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) { r, ok := routers[nodeID]; return r, ok })}
	peers := []consensus.NodeID{1, 2, 3}
	groups := map[consensus.NodeID]*consensus.MultiGroup{}
	defer func() {
		for _, mg := range groups {
			mg.Stop()
		}
	}()
	for _, id := range peers {
		mg, err := consensus.StartMultiGroup(ctx, consensus.MultiGroupOptions{NodeID: id, PeerNodeIDs: peers, PartitionCount: 64, Transport: transport, StateMachines: consensus.StateMachineFactoryFunc{System: func() consensus.StateMachine { return consensus.NewSystemStateMachine() }, Partition: func(uint32) consensus.StateMachine { return &consensus.MemoryStateMachine{} }}, ElectionTick: 5, HeartbeatTick: 1})
		if err != nil {
			t.Fatalf("StartMultiGroup(%d) error = %v", id, err)
		}
		groups[id] = mg
		for _, g := range mg.Groups() {
			for _, router := range routers {
				router.Register(g)
			}
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
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var leader consensus.NodeID
	resolver := consensus.NewMultiGroupLeaderResolver(groups[1])
	if err := consensus.WaitUntil(waitCtx, 20*time.Millisecond, func() bool {
		var err error
		leader, err = resolver.LeaderForPartition(waitCtx, mustPartitionForSpace(t, created.Space.SpaceID))
		return err == nil && leader != 0
	}); err != nil {
		t.Fatalf("leader not resolved: %v", err)
	}
	loaded, err := m.getSpaceViaRaftLeader(ctx, created.Space.SpaceID, groups[leader], leader, nil)
	if err != nil {
		t.Fatalf("getSpaceViaRaftLeader() error = %v", err)
	}
	if loaded.SpaceID != created.Space.SpaceID {
		t.Fatalf("loaded=%s want %s", loaded.SpaceID, created.Space.SpaceID)
	}
}
