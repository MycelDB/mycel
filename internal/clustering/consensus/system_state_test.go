package consensus

import (
	"context"
	"testing"
	"time"
)

func testBootstrapPayload() BootstrapMetadataPayload {
	return BootstrapMetadataPayload{
		ClusterID:      "cluster_00000000-0000-0000-0000-000000000001",
		NodeCount:      3,
		PartitionCount: 64,
		ReplicaFactor:  3,
		ClusterName:    "test-cluster",
		BootstrapEpoch: "epoch-1",
		Nodes: []SystemNode{
			{NodeID: "node_00000000-0000-0000-0000-000000000001", RaftNodeID: 1, NodeName: "node-a", BackendAdvertiseAddr: "127.0.0.1:19093"},
			{NodeID: "node_00000000-0000-0000-0000-000000000002", RaftNodeID: 2, NodeName: "node-b", BackendAdvertiseAddr: "127.0.0.1:19094"},
			{NodeID: "node_00000000-0000-0000-0000-000000000003", RaftNodeID: 3, NodeName: "node-c", BackendAdvertiseAddr: "127.0.0.1:19095"},
		},
	}
}

func TestSystemStateMachineBootstrapMetadata(t *testing.T) {
	sm := NewSystemStateMachine()
	cmd, err := NewBootstrapMetadataCommand(testBootstrapPayload(), "bootstrap-1")
	if err != nil {
		t.Fatalf("NewBootstrapMetadataCommand() error = %v", err)
	}
	if err := sm.ApplyCommand(context.Background(), ApplyContext{RaftIndex: 1, RaftTerm: 1}, cmd); err != nil {
		t.Fatalf("ApplyCommand() error = %v", err)
	}
	meta := sm.Metadata()
	if meta.ClusterID != "cluster_00000000-0000-0000-0000-000000000001" || meta.NodeCount != 3 || meta.PartitionCount != 64 || meta.ReplicaFactor != 3 {
		t.Fatalf("unexpected metadata: %+v", meta)
	}
	if len(meta.Nodes) != 3 || len(meta.Placement) != 64 {
		t.Fatalf("unexpected metadata counts: nodes=%d placement=%d", len(meta.Nodes), len(meta.Placement))
	}
	p0 := meta.Placement[0]
	if p0.PreferredLeader != "node_00000000-0000-0000-0000-000000000001" || len(p0.ReplicaNodeIDs) != 3 {
		t.Fatalf("unexpected partition 0 placement: %+v", p0)
	}
	p1 := meta.Placement[1]
	if p1.PreferredLeader != "node_00000000-0000-0000-0000-000000000002" {
		t.Fatalf("unexpected partition 1 leader: %+v", p1)
	}
}

func TestValidateSystemMetadataAgainstBootstrapConfig(t *testing.T) {
	meta, err := buildBootstrapMetadata(testBootstrapPayload())
	if err != nil {
		t.Fatalf("buildBootstrapMetadata() error = %v", err)
	}
	cfg := BootstrapConfig{ClusterID: meta.ClusterID, ClusterName: "test-cluster", NodeCount: 3, PartitionCount: 64, ReplicaFactor: 3, Nodes: testBootstrapPayload().Nodes}
	if err := ValidateSystemMetadataAgainstBootstrapConfig(meta, cfg); err != nil {
		t.Fatalf("ValidateSystemMetadataAgainstBootstrapConfig() error = %v", err)
	}
	cfg.PartitionCount = 32
	if err := ValidateSystemMetadataAgainstBootstrapConfig(meta, cfg); err == nil {
		t.Fatal("expected partition count mismatch")
	}
}

func TestSystemStateMachineRejectsDuplicateRaftNodeID(t *testing.T) {
	payload := testBootstrapPayload()
	payload.Nodes[1].RaftNodeID = payload.Nodes[0].RaftNodeID
	if _, err := buildBootstrapMetadata(payload); err == nil {
		t.Fatal("expected duplicate raft node id to fail")
	}
}

func TestSystemStateMachineSnapshotRestore(t *testing.T) {
	sm := NewSystemStateMachine()
	cmd, err := NewBootstrapMetadataCommand(testBootstrapPayload(), "bootstrap-1")
	if err != nil {
		t.Fatalf("NewBootstrapMetadataCommand() error = %v", err)
	}
	if err := sm.ApplyCommand(context.Background(), ApplyContext{RaftIndex: 1, RaftTerm: 1}, cmd); err != nil {
		t.Fatalf("ApplyCommand() error = %v", err)
	}
	data, err := sm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	restored := NewSystemStateMachine()
	if err := restored.RestoreSnapshot(data); err != nil {
		t.Fatalf("RestoreSnapshot() error = %v", err)
	}
	if restored.Metadata().ClusterID != sm.Metadata().ClusterID || len(restored.Metadata().Placement) != 64 {
		t.Fatalf("unexpected restored metadata: %+v", restored.Metadata())
	}
}

func TestSystemStateMachineRejectsInvalidBootstrap(t *testing.T) {
	payload := testBootstrapPayload()
	payload.ReplicaFactor = 4
	cmd, err := NewBootstrapMetadataCommand(payload, "bootstrap-1")
	if err != nil {
		t.Fatalf("NewBootstrapMetadataCommand() error = %v", err)
	}
	sm := NewSystemStateMachine()
	if err := sm.ApplyCommand(context.Background(), ApplyContext{RaftIndex: 1}, cmd); err == nil {
		t.Fatal("expected invalid bootstrap metadata to fail")
	}
}

func TestSystemMetadataReplaysFromSingleNodePersistentRaftStorage(t *testing.T) {
	ctx := context.Background()
	transport := newMemoryTransport()
	dir := t.TempDir()
	store, err := NewPersistentStorage(dir)
	if err != nil {
		t.Fatalf("NewPersistentStorage() error = %v", err)
	}
	sm := NewSystemStateMachine()
	g, err := StartGroup(ctx, GroupOptions{ID: SystemGroupID, NodeID: 1, Peers: []NodeID{1}, PartitionCount: 64, StateMachine: sm, Transport: transport, ElectionTick: 5, HeartbeatTick: 1, Storage: store})
	if err != nil {
		t.Fatalf("StartGroup() error = %v", err)
	}
	transport.register(g)
	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := g.Campaign(waitCtx); err != nil {
		t.Fatalf("Campaign() error = %v", err)
	}
	if err := WaitUntil(waitCtx, 10*time.Millisecond, func() bool { g.Tick(); return g.Leader() == 1 }); err != nil {
		t.Fatalf("leader election timed out: %v", err)
	}
	cmd, err := NewBootstrapMetadataCommand(testBootstrapPayload(), "bootstrap-persistent-1")
	if err != nil {
		t.Fatalf("NewBootstrapMetadataCommand() error = %v", err)
	}
	if _, err := g.Propose(waitCtx, cmd); err != nil {
		t.Fatalf("Propose() error = %v", err)
	}
	g.Stop()

	reopened, err := NewPersistentStorage(dir)
	if err != nil {
		t.Fatalf("reopen NewPersistentStorage() error = %v", err)
	}
	replayed := NewSystemStateMachine()
	g2, err := StartGroup(ctx, GroupOptions{ID: SystemGroupID, NodeID: 1, Peers: []NodeID{1}, PartitionCount: 64, StateMachine: replayed, Transport: transport, ElectionTick: 5, HeartbeatTick: 1, Storage: reopened, ReplayCommittedEntries: true})
	if err != nil {
		t.Fatalf("restart StartGroup() error = %v", err)
	}
	g2.Stop()
	if replayed.Metadata().ClusterID != testBootstrapPayload().ClusterID {
		t.Fatalf("replayed metadata cluster_id=%q want %q", replayed.Metadata().ClusterID, testBootstrapPayload().ClusterID)
	}
}

func TestSystemMetadataCommitsThroughThreeNodePersistentRaft(t *testing.T) {
	transport := newMemoryTransport()
	ctx := context.Background()
	groups := map[NodeID]*Group{}
	sms := map[NodeID]*SystemStateMachine{}
	dirs := map[NodeID]string{}
	peers := []NodeID{1, 2, 3}
	stopTick := make(chan struct{})
	defer close(stopTick)
	defer func() {
		for _, g := range groups {
			g.Stop()
		}
	}()
	for _, id := range peers {
		dir := t.TempDir()
		dirs[id] = dir
		store, err := NewPersistentStorage(dir)
		if err != nil {
			t.Fatalf("NewPersistentStorage(%d) error = %v", id, err)
		}
		sm := NewSystemStateMachine()
		g, err := StartGroup(ctx, GroupOptions{ID: SystemGroupID, NodeID: id, Peers: peers, PartitionCount: 64, StateMachine: sm, Transport: transport, ElectionTick: 5, HeartbeatTick: 1, Storage: store})
		if err != nil {
			t.Fatalf("StartGroup(%d) error = %v", id, err)
		}
		groups[id] = g
		sms[id] = sm
		transport.register(g)
	}
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopTick:
				return
			case <-ticker.C:
				for _, g := range groups {
					g.Tick()
				}
			}
		}
	}()
	leader := func() *Group {
		counts := map[NodeID]int{}
		for _, g := range groups {
			if l := g.Leader(); l != 0 {
				counts[l]++
			}
		}
		for id, count := range counts {
			if count >= 2 {
				return groups[id]
			}
		}
		return nil
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := WaitUntil(waitCtx, 20*time.Millisecond, func() bool { return leader() != nil }); err != nil {
		t.Fatalf("leader election timed out: %v", err)
	}
	cmd, err := NewBootstrapMetadataCommand(testBootstrapPayload(), "bootstrap-persistent-three-node-1")
	if err != nil {
		t.Fatalf("NewBootstrapMetadataCommand() error = %v", err)
	}
	if _, err := leader().Propose(waitCtx, cmd); err != nil {
		t.Fatalf("Propose() error = %v", err)
	}
	if err := WaitUntil(waitCtx, 20*time.Millisecond, func() bool {
		for _, sm := range sms {
			if sm.Metadata().ClusterID == "" {
				return false
			}
		}
		return true
	}); err != nil {
		t.Fatalf("metadata did not apply on all nodes: %v", err)
	}
	for _, g := range groups {
		g.Stop()
	}
	groups = map[NodeID]*Group{}

	for _, id := range peers {
		store, err := NewPersistentStorage(dirs[id])
		if err != nil {
			t.Fatalf("reopen NewPersistentStorage(%d) error = %v", id, err)
		}
		replayed := NewSystemStateMachine()
		g, err := StartGroup(ctx, GroupOptions{ID: SystemGroupID, NodeID: id, Peers: peers, PartitionCount: 64, StateMachine: replayed, Transport: transport, ElectionTick: 5, HeartbeatTick: 1, Storage: store, ReplayCommittedEntries: true})
		if err != nil {
			t.Fatalf("restart StartGroup(%d) error = %v", id, err)
		}
		groups[id] = g
		if replayed.Metadata().ClusterID != testBootstrapPayload().ClusterID {
			t.Fatalf("node %d replayed metadata cluster_id=%q want %q", id, replayed.Metadata().ClusterID, testBootstrapPayload().ClusterID)
		}
	}
}

func TestSystemMetadataCommitsThroughInMemoryRaft(t *testing.T) {
	transport := newMemoryTransport()
	ctx := context.Background()
	groups := map[NodeID]*Group{}
	sms := map[NodeID]*SystemStateMachine{}
	peers := []NodeID{1, 2, 3}
	stopTick := make(chan struct{})
	defer close(stopTick)
	defer func() {
		for _, g := range groups {
			g.Stop()
		}
	}()
	for _, id := range peers {
		sm := NewSystemStateMachine()
		g, err := StartGroup(ctx, GroupOptions{ID: "system", NodeID: id, Peers: peers, PartitionCount: 64, StateMachine: sm, Transport: transport, ElectionTick: 5, HeartbeatTick: 1})
		if err != nil {
			t.Fatalf("StartGroup(%d) error = %v", id, err)
		}
		groups[id] = g
		sms[id] = sm
		transport.register(g)
	}
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopTick:
				return
			case <-ticker.C:
				for _, g := range groups {
					g.Tick()
				}
			}
		}
	}()
	leader := func() *Group {
		counts := map[NodeID]int{}
		for _, g := range groups {
			if l := g.Leader(); l != 0 {
				counts[l]++
			}
		}
		for id, count := range counts {
			if count >= 2 {
				return groups[id]
			}
		}
		return nil
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := WaitUntil(waitCtx, 20*time.Millisecond, func() bool { return leader() != nil }); err != nil {
		t.Fatalf("leader election timed out: %v", err)
	}
	cmd, err := NewBootstrapMetadataCommand(testBootstrapPayload(), "bootstrap-raft-1")
	if err != nil {
		t.Fatalf("NewBootstrapMetadataCommand() error = %v", err)
	}
	if _, err := leader().Propose(waitCtx, cmd); err != nil {
		t.Fatalf("Propose() error = %v", err)
	}
	if err := WaitUntil(waitCtx, 20*time.Millisecond, func() bool {
		for _, sm := range sms {
			if sm.Metadata().ClusterID == "" {
				return false
			}
		}
		return true
	}); err != nil {
		t.Fatalf("metadata did not apply on all nodes: %v", err)
	}
}
