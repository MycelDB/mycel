package service

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/myceldb/mycel/internal/clustering/consensus"
	config "github.com/myceldb/mycel/internal/runtime/runtimetest"
	daemonruntime "github.com/myceldb/mycel/internal/runtime/runtimetest"
)

func TestModuleCreateSpaceWithResultUsesRaftProposalWhenEnabled(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir(), Cluster: config.ClusterConfig{RaftPartitionCount: 64}}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init module failed: %v", result.Error)
	}
	router := consensus.NewLocalMessageRouter()
	mg, err := consensus.StartMultiGroup(ctx, consensus.MultiGroupOptions{NodeID: 1, PeerNodeIDs: []consensus.NodeID{1}, PartitionCount: 64, Transport: consensus.RoutedTransport{Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) { return router, nodeID == 1 })}, StateMachines: consensus.StateMachineFactoryFunc{System: func() consensus.StateMachine { return consensus.NewSystemStateMachine() }, Partition: func(uint32) consensus.StateMachine { return RaftStateMachine{Module: m, PartitionCount: 64} }}, ElectionTick: 5, HeartbeatTick: 1})
	if err != nil {
		t.Fatalf("StartMultiGroup() error = %v", err)
	}
	defer mg.Stop()
	for _, group := range mg.Groups() {
		router.Register(group)
	}
	m.EnableExperimentalRaft(mg, 1, nil, "")
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
				mg.Tick()
			}
		}
	}()
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := consensus.WaitUntil(waitCtx, 20*time.Millisecond, func() bool {
		for _, status := range mg.Status() {
			if status.PartitionID != nil && status.Leader == 0 {
				return false
			}
		}
		return true
	}); err != nil {
		t.Fatalf("leaders not elected: %v", err)
	}
	ownerID := testPrincipalID(t)
	result, err := m.CreateSpaceWithResult(waitCtx, CreateSpaceInput{Name: "raft-service", OwnerPrincipalID: ownerID, CommandID: "create-space-idempotent-1"})
	if err != nil {
		t.Fatalf("CreateSpaceWithResult() error = %v", err)
	}
	retry, err := m.CreateSpaceWithResult(waitCtx, CreateSpaceInput{Name: "raft-service", OwnerPrincipalID: ownerID, CommandID: "create-space-idempotent-1"})
	if err != nil {
		t.Fatalf("CreateSpaceWithResult() retry error = %v", err)
	}
	if retry.Space.SpaceID != result.Space.SpaceID {
		t.Fatalf("retry created different space: %s != %s", retry.Space.SpaceID, result.Space.SpaceID)
	}
	spaces, err := m.spaces.List(ctx)
	if err != nil {
		t.Fatalf("spaces.List() error = %v", err)
	}
	if len(spaces) != 1 {
		t.Fatalf("idempotent retry created %d spaces, want 1", len(spaces))
	}
	if _, err := m.GetSpace(ctx, result.Space.SpaceID.String()); err != nil {
		t.Fatalf("GetSpace() after raft proposal error = %v", err)
	}
	listed, err := m.ListSpaces(ctx, true)
	if err != nil {
		t.Fatalf("ListSpaces() in experimental raft mode error = %v", err)
	}
	if len(listed) != 1 || listed[0].SpaceID != result.Space.SpaceID {
		t.Fatalf("ListSpaces() = %+v, want created space only", listed)
	}
	grant, err := m.GrantSpacePrincipal(waitCtx, result.Space.SpaceID.String(), string(testPrincipalID(t)), "reader")
	if err != nil {
		t.Fatalf("GrantSpacePrincipal() via raft error = %v", err)
	}
	if grant.SpaceID != result.Space.SpaceID.String() || grant.Role != "reader" {
		t.Fatalf("unexpected grant: %+v", grant)
	}
	domain, err := m.CreateDomain(waitCtx, string(ownerID), CreateDomainInput{SpaceID: result.Space.SpaceID.String(), Key: "docs", Name: "Docs"})
	if err != nil {
		t.Fatalf("CreateDomain() via raft error = %v", err)
	}
	loadedDomain, err := m.GetDomainByRef(ctx, result.Space.SpaceID.String(), "docs")
	if err != nil {
		t.Fatalf("GetDomainByRef() after raft domain create error = %v", err)
	}
	if loadedDomain.ID != domain.ID {
		t.Fatalf("loaded domain=%s want %s", loadedDomain.ID, domain.ID)
	}
	newName := "Docs Updated"
	updated, err := m.UpdateDomain(waitCtx, string(ownerID), UpdateDomainInput{SpaceID: result.Space.SpaceID.String(), DomainID: domain.ID.String(), Name: &newName})
	if err != nil {
		t.Fatalf("UpdateDomain() via raft error = %v", err)
	}
	if updated.Name != newName {
		t.Fatalf("updated domain name=%q want %q", updated.Name, newName)
	}
	if err := m.DeleteDomain(waitCtx, string(ownerID), result.Space.SpaceID.String(), domain.ID.String()); err != nil {
		t.Fatalf("DeleteDomain() via raft error = %v", err)
	}
	if _, err := m.GetDomainByRef(ctx, result.Space.SpaceID.String(), domain.ID.String()); err == nil {
		t.Fatal("expected deleted domain to be unavailable")
	}

}

func TestRaftCreateSpaceCommitAndLeaderFailoverHarness(t *testing.T) {
	ctx := context.Background()
	routers := map[consensus.NodeID]*consensus.LocalMessageRouter{1: consensus.NewLocalMessageRouter(), 2: consensus.NewLocalMessageRouter(), 3: consensus.NewLocalMessageRouter()}
	transport := consensus.RoutedTransport{Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) { r, ok := routers[nodeID]; return r, ok })}
	peers := []consensus.NodeID{1, 2, 3}
	groups := map[consensus.NodeID]*consensus.MultiGroup{}
	modules := map[consensus.NodeID]*Module{}
	defer func() {
		for _, mg := range groups {
			mg.Stop()
		}
	}()
	for _, id := range peers {
		m := NewModule()
		if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir(), Cluster: config.ClusterConfig{RaftPartitionCount: 64}}, LoggerValue: slog.Default()}); !result.OK {
			t.Fatalf("init module %d failed: %v", id, result.Error)
		}
		modules[id] = m
		localModule := m
		mg, err := consensus.StartMultiGroup(ctx, consensus.MultiGroupOptions{NodeID: id, PeerNodeIDs: peers, PartitionCount: 64, Transport: transport, StateMachines: consensus.StateMachineFactoryFunc{System: func() consensus.StateMachine { return consensus.NewSystemStateMachine() }, Partition: func(uint32) consensus.StateMachine { return RaftStateMachine{Module: localModule, PartitionCount: 64} }}, ElectionTick: 5, HeartbeatTick: 1})
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

	ownerID := testPrincipalID(t)
	seedRecord, cmd, err := modules[1].buildCreateSpaceRaftCommand(CreateSpaceInput{Name: "raft-main", OwnerPrincipalID: ownerID}, 64, "create-space-e2e-1")
	if err != nil {
		t.Fatalf("buildCreateSpaceRaftCommand() error = %v", err)
	}
	partition := mustPartitionForSpace(t, seedRecord.Space.SpaceID)
	resolver := consensus.NewMultiGroupLeaderResolver(groups[1])
	waitCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	var leader consensus.NodeID
	if err := consensus.WaitUntil(waitCtx, 20*time.Millisecond, func() bool {
		var err error
		leader, err = resolver.LeaderForPartition(waitCtx, partition)
		return err == nil && leader != 0
	}); err != nil {
		t.Fatalf("leader not resolved: %v", err)
	}
	group, ok := groups[leader].Group(consensus.PartitionGroupID(partition.Uint32()))
	if !ok {
		t.Fatalf("leader group not found")
	}
	if _, err := group.Propose(waitCtx, cmd); err != nil {
		t.Fatalf("Propose() error = %v", err)
	}
	if err := consensus.WaitUntil(waitCtx, 20*time.Millisecond, func() bool {
		for _, m := range modules {
			if _, err := m.GetSpace(ctx, seedRecord.Space.SpaceID.String()); err != nil {
				return false
			}
		}
		return true
	}); err != nil {
		t.Fatalf("space did not apply on all replicas: %v", err)
	}

	for _, router := range routers {
		router.UnregisterNode(leader)
	}
	groups[leader].Stop()
	delete(groups, leader)
	if err := consensus.WaitUntil(waitCtx, 20*time.Millisecond, func() bool {
		for nodeID, mg := range groups {
			if nodeID == leader {
				continue
			}
			newLeader, err := consensus.NewMultiGroupLeaderResolver(mg).LeaderForPartition(waitCtx, partition)
			if err == nil && newLeader != 0 && newLeader != leader {
				return true
			}
		}
		return false
	}); err != nil {
		t.Fatalf("new leader not elected after stop: %v", err)
	}
}
