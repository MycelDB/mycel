package consensus

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestPreferredLeaderNodeRoundRobin(t *testing.T) {
	nodes := []NodeID{3, 1, 2}
	want := []NodeID{1, 2, 3, 1, 2, 3}
	for p, expected := range want {
		got, err := PreferredLeaderNode(uint32(p), nodes)
		if err != nil {
			t.Fatalf("PreferredLeaderNode() error = %v", err)
		}
		if got != expected {
			t.Fatalf("partition %d preferred leader=%d want %d", p, got, expected)
		}
	}
}

func TestStartMultiGroupStartsSystemAndPartitions(t *testing.T) {
	transport := newMemoryTransport()
	factory := StateMachineFactoryFunc{System: func() StateMachine { return NewSystemStateMachine() }, Partition: func(partitionID uint32) StateMachine { return &MemoryStateMachine{} }}
	mg, err := StartMultiGroup(context.Background(), MultiGroupOptions{NodeID: 1, PeerNodeIDs: []NodeID{1, 2, 3}, PartitionCount: 64, Transport: transport, StateMachines: factory, ElectionTick: 5, HeartbeatTick: 1})
	if err != nil {
		t.Fatalf("StartMultiGroup() error = %v", err)
	}
	defer mg.Stop()
	if mg.GroupCount() != 65 {
		t.Fatalf("GroupCount()=%d want 65", mg.GroupCount())
	}
	if _, ok := mg.Group(SystemGroupID); !ok {
		t.Fatal("expected system group")
	}
	if _, ok := mg.Group(PartitionGroupID(63)); !ok {
		t.Fatal("expected partition 63 group")
	}
	status := mg.Status()
	if len(status) != 65 {
		t.Fatalf("status count=%d want 65", len(status))
	}
	var p0, p1 GroupStatus
	for _, st := range status {
		if st.PartitionID != nil && *st.PartitionID == 0 {
			p0 = st
		}
		if st.PartitionID != nil && *st.PartitionID == 1 {
			p1 = st
		}
	}
	if p0.PreferredLeader != 1 || p1.PreferredLeader != 2 {
		t.Fatalf("unexpected preferred leaders p0=%d p1=%d", p0.PreferredLeader, p1.PreferredLeader)
	}
}

func TestStartMultiGroupDefersPartitionsUntilSystemMetadata(t *testing.T) {
	transport := newMemoryTransport()
	factory := StateMachineFactoryFunc{System: func() StateMachine { return NewSystemStateMachine() }, Partition: func(partitionID uint32) StateMachine { return &MemoryStateMachine{} }}
	mg, err := StartMultiGroup(context.Background(), MultiGroupOptions{NodeID: 1, PeerNodeIDs: []NodeID{1, 2, 3}, PartitionCount: 64, Transport: transport, StateMachines: factory, ElectionTick: 5, HeartbeatTick: 1, DeferPartitionGroups: true})
	if err != nil {
		t.Fatalf("StartMultiGroup() error = %v", err)
	}
	defer mg.Stop()
	if mg.GroupCount() != 1 {
		t.Fatalf("GroupCount()=%d want 1 system group", mg.GroupCount())
	}
	meta, err := buildBootstrapMetadata(BootstrapMetadataPayload{ClusterID: "cluster_00000000-0000-0000-0000-000000000099", NodeCount: 3, PartitionCount: 4, ReplicaFactor: 3, Nodes: []SystemNode{{NodeID: "node_1", RaftNodeID: 1, NodeName: "node-a"}, {NodeID: "node_2", RaftNodeID: 2, NodeName: "node-b"}, {NodeID: "node_3", RaftNodeID: 3, NodeName: "node-c"}}})
	if err != nil {
		t.Fatalf("buildBootstrapMetadata() error = %v", err)
	}
	if err := mg.StartPartitionGroups(context.Background(), meta); err != nil {
		t.Fatalf("StartPartitionGroups() error = %v", err)
	}
	if mg.GroupCount() != 5 {
		t.Fatalf("GroupCount()=%d want 5", mg.GroupCount())
	}
	status := mg.Status()
	var p3 GroupStatus
	for _, st := range status {
		if st.PartitionID != nil && *st.PartitionID == 3 {
			p3 = st
		}
	}
	if p3.PreferredLeader != 1 {
		t.Fatalf("partition 3 preferred leader=%d want 1 from metadata placement", p3.PreferredLeader)
	}
}

func TestStartPartitionGroupsCreatesPersistentPartitionStorage(t *testing.T) {
	transport := newMemoryTransport()
	factory := StateMachineFactoryFunc{System: func() StateMachine { return NewSystemStateMachine() }, Partition: func(partitionID uint32) StateMachine { return &MemoryStateMachine{} }}
	storageDir := t.TempDir()
	mg, err := StartMultiGroup(context.Background(), MultiGroupOptions{NodeID: 1, PeerNodeIDs: []NodeID{1, 2, 3}, PartitionCount: 2, Transport: transport, StateMachines: factory, ElectionTick: 5, HeartbeatTick: 1, StorageDir: storageDir, DeferPartitionGroups: true})
	if err != nil {
		t.Fatalf("StartMultiGroup() error = %v", err)
	}
	defer mg.Stop()
	meta, err := buildBootstrapMetadata(BootstrapMetadataPayload{ClusterID: "cluster_00000000-0000-0000-0000-000000000101", NodeCount: 3, PartitionCount: 2, ReplicaFactor: 3, Nodes: []SystemNode{{NodeID: "node_1", RaftNodeID: 1, NodeName: "node-a"}, {NodeID: "node_2", RaftNodeID: 2, NodeName: "node-b"}, {NodeID: "node_3", RaftNodeID: 3, NodeName: "node-c"}}})
	if err != nil {
		t.Fatalf("buildBootstrapMetadata() error = %v", err)
	}
	if err := mg.StartPartitionGroups(context.Background(), meta); err != nil {
		t.Fatalf("StartPartitionGroups() error = %v", err)
	}
	if _, err := NewPersistentStorage(filepath.Join(storageDir, string(PartitionGroupID(0)))); err != nil {
		t.Fatalf("partition 0 storage was not initialized: %v", err)
	}
	if _, err := NewPersistentStorage(filepath.Join(storageDir, string(PartitionGroupID(1)))); err != nil {
		t.Fatalf("partition 1 storage was not initialized: %v", err)
	}
}

func TestStartPartitionGroupsUsesMetadataReplicaPlacement(t *testing.T) {
	transport := newMemoryTransport()
	factory := StateMachineFactoryFunc{System: func() StateMachine { return NewSystemStateMachine() }, Partition: func(partitionID uint32) StateMachine { return &MemoryStateMachine{} }}
	mg, err := StartMultiGroup(context.Background(), MultiGroupOptions{NodeID: 3, PeerNodeIDs: []NodeID{1, 2, 3}, PartitionCount: 3, Transport: transport, StateMachines: factory, ElectionTick: 5, HeartbeatTick: 1, DeferPartitionGroups: true})
	if err != nil {
		t.Fatalf("StartMultiGroup() error = %v", err)
	}
	defer mg.Stop()
	meta, err := buildBootstrapMetadata(BootstrapMetadataPayload{ClusterID: "cluster_00000000-0000-0000-0000-000000000100", NodeCount: 3, PartitionCount: 3, ReplicaFactor: 2, Nodes: []SystemNode{{NodeID: "node_1", RaftNodeID: 1, NodeName: "node-a"}, {NodeID: "node_2", RaftNodeID: 2, NodeName: "node-b"}, {NodeID: "node_3", RaftNodeID: 3, NodeName: "node-c"}}})
	if err != nil {
		t.Fatalf("buildBootstrapMetadata() error = %v", err)
	}
	if err := mg.StartPartitionGroups(context.Background(), meta); err != nil {
		t.Fatalf("StartPartitionGroups() error = %v", err)
	}
	if _, ok := mg.Group(PartitionGroupID(0)); ok {
		t.Fatal("node 3 should not start partition 0; replicas are node 1 and node 2")
	}
	if _, ok := mg.Group(PartitionGroupID(1)); !ok {
		t.Fatal("node 3 should start partition 1; replicas include node 3")
	}
	if _, ok := mg.Group(PartitionGroupID(2)); !ok {
		t.Fatal("node 3 should start partition 2; replicas include node 3")
	}
}

func TestMultiGroupInMemoryLeaderDistributionSmoke(t *testing.T) {
	transport := newMemoryTransport()
	peers := []NodeID{1, 2, 3}
	groups := map[NodeID]*MultiGroup{}
	stopTick := make(chan struct{})
	defer close(stopTick)
	defer func() {
		for _, mg := range groups {
			mg.Stop()
		}
	}()
	factory := func() StateMachineFactory {
		return StateMachineFactoryFunc{System: func() StateMachine { return NewSystemStateMachine() }, Partition: func(partitionID uint32) StateMachine { return &MemoryStateMachine{} }}
	}
	for _, id := range peers {
		mg, err := StartMultiGroup(context.Background(), MultiGroupOptions{NodeID: id, PeerNodeIDs: peers, PartitionCount: 6, Transport: transport, StateMachines: factory(), ElectionTick: 5, HeartbeatTick: 1})
		if err != nil {
			t.Fatalf("StartMultiGroup(%d) error = %v", id, err)
		}
		groups[id] = mg
		for _, g := range mg.Groups() {
			transport.register(g)
		}
	}
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
	if err := WaitUntil(ctx, 20*time.Millisecond, func() bool {
		for p := uint32(0); p < 6; p++ {
			leaders := map[NodeID]int{}
			for _, mg := range groups {
				g, ok := mg.Group(PartitionGroupID(p))
				if !ok {
					return false
				}
				if l := g.Leader(); l != 0 {
					leaders[l]++
				}
			}
			found := false
			for _, count := range leaders {
				if count >= 2 {
					found = true
				}
			}
			if !found {
				return false
			}
		}
		return true
	}); err != nil {
		t.Fatalf("partition leaders not elected: %v", err)
	}
}
