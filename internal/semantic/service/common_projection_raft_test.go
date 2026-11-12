package service

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	graphmodel "github.com/myceldb/mycel/internal/graph/model"
	"github.com/myceldb/mycel/internal/projection"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestCommonSemanticProjectionFencesPromotedLeaderUntilItPublishes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	peers := []consensus.NodeID{1, 2, 3}
	const partitionCount = uint32(1)
	routers := map[consensus.NodeID]*consensus.LocalMessageRouter{}
	for _, nodeID := range peers {
		routers[nodeID] = consensus.NewLocalMessageRouter()
	}
	transport := consensus.RoutedTransport{Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) {
		router, ok := routers[nodeID]
		return router, ok
	})}
	managers := map[consensus.NodeID]*projection.RegistryManager{}
	modules := map[consensus.NodeID]*Module{}
	groups := map[consensus.NodeID]*consensus.MultiGroup{}
	for _, nodeID := range peers {
		manager := projection.NewRegistryManager(partitionCount)
		module := NewModule()
		module.projectionRegistries = manager
		module.raftPartitionCount = partitionCount
		module.raftLocalNode = nodeID
		managers[nodeID] = manager
		modules[nodeID] = module
		group, err := consensus.StartMultiGroup(ctx, consensus.MultiGroupOptions{
			NodeID:         nodeID,
			PeerNodeIDs:    peers,
			PartitionCount: partitionCount,
			Transport:      transport,
			StateMachines: consensus.StateMachineFactoryFunc{
				System: func() consensus.StateMachine { return consensus.NewSystemStateMachine() },
				Partition: func(partitionID uint32) consensus.StateMachine {
					return manager.NewRaftStateMachine(partitionID)
				},
			},
			ElectionTick:  5,
			HeartbeatTick: 1,
		})
		if err != nil {
			t.Fatalf("start node %d: %v", nodeID, err)
		}
		groups[nodeID] = group
		module.raftGroups = group
		for _, raftGroup := range group.Groups() {
			for _, router := range routers {
				router.Register(raftGroup)
			}
		}
	}
	defer func() {
		for _, group := range groups {
			group.Stop()
		}
	}()

	stopTick := make(chan struct{})
	tickDone := make(chan struct{})
	go func() {
		defer close(tickDone)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopTick:
				return
			case <-ticker.C:
				for _, group := range groups {
					group.Tick()
				}
			}
		}
	}()
	defer func() {
		close(stopTick)
		<-tickDone
	}()

	spaceID := domainspace.SpaceID(uuid.New())
	partitionForSpace, err := projection.NewPartitionCommand(spaceID, partitionCount, projection.NewDefinitionRecord(projection.Definition{ID: "probe", Kind: projection.KindSemantic, Scope: projection.Scope{SpaceID: spaceID.String(), DomainID: "probe"}, Key: "probe", ConfigurationEpoch: 1, MaterializerVersion: semanticProjectionMaterializerVersion}), "probe")
	if err != nil {
		t.Fatalf("build partition probe: %v", err)
	}
	partitionID := partitionForSpace.PartitionID
	leaderForPartition := func() consensus.NodeID {
		votes := map[consensus.NodeID]int{}
		for _, group := range groups {
			if partition, ok := group.Group(consensus.PartitionGroupID(partitionID)); ok && partition.Leader() != 0 {
				votes[partition.Leader()]++
			}
		}
		for nodeID, count := range votes {
			if count >= 2 {
				return nodeID
			}
		}
		return 0
	}
	if err := consensus.TickUntil(ctx, 20*time.Millisecond, func() {
		for _, group := range groups {
			group.Tick()
		}
	}, func() bool { return leaderForPartition() != 0 }); err != nil {
		t.Fatalf("leader election: %v", err)
	}

	for nodeID, module := range modules {
		module.semanticLeadershipProbe = func(space domainspace.SpaceID) (consensus.NodeID, uint64, bool, error) {
			partition, err := projection.NewPartitionCommand(space, partitionCount, projection.NewDefinitionRecord(projection.Definition{ID: "leadership-probe", Kind: projection.KindSemantic, Scope: projection.Scope{SpaceID: space.String(), DomainID: "probe"}, Key: "probe", ConfigurationEpoch: 1, MaterializerVersion: semanticProjectionMaterializerVersion}), "leadership-probe")
			if err != nil {
				return 0, 0, true, err
			}
			group, ok := groups[nodeID].Group(consensus.PartitionGroupID(partition.PartitionID))
			if !ok {
				return 0, 0, true, nil
			}
			_, term, _ := group.Progress()
			return group.Leader(), term, true, nil
		}
	}

	rule := domainsemantic.SemanticGenerationRule{ID: domainsemantic.SemanticRuleID(uuid.New()), SpaceID: spaceID, DomainID: graphmodel.DomainID(uuid.New()), Key: "common-lifecycle", Enabled: true, Embeddings: []domainsemantic.SemanticEmbeddingBinding{{Key: "search", Enabled: true}}}
	status := ReliabilityStatus{SpaceID: spaceID, SemanticRuleID: rule.ID, EmbeddingBindingKey: "search", LatestObservedGraphRevision: 5, FreshThroughGraphRevision: 5, SemanticGeneration: 1, Rebuild: domainsemantic.SemanticReliabilityState{EmbeddingBindingKey: "search", ResolvedConfigurationHash: "route-v1", RebuildEpoch: "epoch-1", RebuildState: "completed"}}
	oldLeader := leaderForPartition()
	if err := modules[oldLeader].publishCommonSemanticProjection(ctx, rule, rule.Embeddings[0], status, true); err != nil {
		t.Fatalf("publish initial common projection: %v", err)
	}
	if err := consensus.WaitUntil(ctx, 20*time.Millisecond, func() bool {
		for _, manager := range managers {
			reader, ok := manager.Reader(partitionID)
			if !ok {
				return false
			}
			projectionID, _, _, identityErr := semanticProjectionIdentity(rule, rule.Embeddings[0])
			if identityErr != nil {
				return false
			}
			checkpoint, checkpointOK := reader.Checkpoint(projectionID)
			if !checkpointOK || checkpoint.State != projection.StateReady || checkpoint.Published.Revision != 5 {
				return false
			}
		}
		return true
	}); err != nil {
		t.Fatalf("common projection did not replicate: %v", err)
	}
	projectionID, _, _, _ := semanticProjectionIdentity(rule, rule.Embeddings[0])
	if reader, ok := managers[oldLeader].Reader(partitionID); ok {
		checkpoint, checkpointOK := reader.Checkpoint(projectionID)
		if !checkpointOK || checkpoint.State != projection.StateReady || checkpoint.Published.Revision != 5 {
			t.Fatalf("initial common checkpoint=%+v ok=%v", checkpoint, checkpointOK)
		}
	}

	promoted := peers[0]
	if promoted == oldLeader {
		promoted = peers[1]
	}
	promotedGroup, ok := groups[promoted].Group(consensus.PartitionGroupID(partitionID))
	if !ok {
		t.Fatalf("promoted partition group unavailable")
	}
	if err := promotedGroup.Campaign(ctx); err != nil {
		t.Fatalf("campaign promoted node: %v", err)
	}
	if err := consensus.TickUntil(ctx, 20*time.Millisecond, func() {
		for _, group := range groups {
			group.Tick()
		}
	}, func() bool { return leaderForPartition() == promoted }); err != nil {
		t.Fatalf("leadership transfer: %v", err)
	}
	newGroup, ok := groups[promoted].Group(consensus.PartitionGroupID(partitionID))
	if !ok {
		t.Fatalf("new leader partition group unavailable")
	}
	_, newTerm, _ := newGroup.Progress()
	if err := modules[promoted].validateCommonSemanticProjection(ctx, rule, rule.Embeddings[0], status, promoted, newTerm); err == nil || !strings.Contains(err.Error(), ProjectionReadinessCommonNotReady) {
		t.Fatalf("promoted leader readiness error=%v, want common local claim fence", err)
	}
	if err := modules[promoted].publishCommonSemanticProjection(ctx, rule, rule.Embeddings[0], status, true); err != nil {
		t.Fatalf("promoted leader publication: %v", err)
	}
	if err := modules[promoted].validateCommonSemanticProjection(ctx, rule, rule.Embeddings[0], status, promoted, newTerm); err != nil {
		t.Fatalf("promoted leader remained fenced after publication: %v", err)
	}
}
