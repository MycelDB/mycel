package consensus

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/myceldb/mycel/internal/wal"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

type memoryTransport struct {
	mu     sync.RWMutex
	groups map[GroupID]map[NodeID]*Group
	drop   map[NodeID]bool
}

func newMemoryTransport() *memoryTransport {
	return &memoryTransport{groups: map[GroupID]map[NodeID]*Group{}, drop: map[NodeID]bool{}}
}

func (t *memoryTransport) register(g *Group) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.groups[g.id] == nil {
		t.groups[g.id] = map[NodeID]*Group{}
	}
	t.groups[g.id][g.nodeID] = g
}

func (t *memoryTransport) unregister(id NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for groupID := range t.groups {
		delete(t.groups[groupID], id)
	}
	t.drop[id] = true
}

func (t *memoryTransport) Send(ctx context.Context, groupID GroupID, from NodeID, messages []raftpb.Message) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.drop[from] {
		return
	}
	for _, msg := range messages {
		to := NodeID(msg.To)
		if t.drop[to] {
			continue
		}
		g := t.groups[groupID][to]
		if g == nil {
			continue
		}
		m := msg
		go func() { _ = g.Step(ctx, m) }()
	}
}

type memoryCluster struct {
	transport *memoryTransport
	groups    map[NodeID]*Group
	sms       map[NodeID]*MemoryStateMachine
	stopTick  chan struct{}
}

func newMemoryCluster(t *testing.T) *memoryCluster {
	t.Helper()
	ctx := context.Background()
	transport := newMemoryTransport()
	cluster := &memoryCluster{transport: transport, groups: map[NodeID]*Group{}, sms: map[NodeID]*MemoryStateMachine{}, stopTick: make(chan struct{})}
	peers := []NodeID{1, 2, 3}
	for _, id := range peers {
		sm := &MemoryStateMachine{}
		g, err := StartGroup(ctx, GroupOptions{ID: "test", NodeID: id, Peers: peers, PartitionCount: 64, StateMachine: sm, Transport: transport, ElectionTick: 5, HeartbeatTick: 1})
		if err != nil {
			t.Fatalf("StartGroup(%d) error = %v", id, err)
		}
		cluster.groups[id] = g
		cluster.sms[id] = sm
		transport.register(g)
	}
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-cluster.stopTick:
				return
			case <-ticker.C:
				for _, g := range cluster.groups {
					g.Tick()
				}
			}
		}
	}()
	return cluster
}

func (c *memoryCluster) close() {
	close(c.stopTick)
	for _, g := range c.groups {
		g.Stop()
	}
}

func (c *memoryCluster) leader() *Group {
	leaders := map[NodeID]int{}
	for _, g := range c.groups {
		if l := g.Leader(); l != 0 {
			leaders[l]++
		}
	}
	for id, count := range leaders {
		if count >= 2 {
			return c.groups[id]
		}
	}
	return nil
}

func TestInMemoryRaftGroupElectsLeaderAndCommits(t *testing.T) {
	cluster := newMemoryCluster(t)
	defer cluster.close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := WaitUntil(ctx, 20*time.Millisecond, func() bool { return cluster.leader() != nil }); err != nil {
		t.Fatalf("leader election timed out: %v", err)
	}
	leader := cluster.leader()
	cmd := NewCommand(CommandScopeSystem, wal.RecordType("system.test"), []byte(`{"ok":true}`), "cmd-1")
	result, err := leader.Propose(ctx, cmd)
	if err != nil {
		t.Fatalf("Propose() error = %v", err)
	}
	if result.Index == 0 || result.Term == 0 {
		t.Fatalf("unexpected proposal result: %+v", result)
	}
	if cluster.sms[leader.nodeID].AppliedCount() != 1 {
		t.Fatalf("leader apply count=%d want 1", cluster.sms[leader.nodeID].AppliedCount())
	}
}

func TestInMemoryRaftGroupRecoversAfterOneNodeStopped(t *testing.T) {
	cluster := newMemoryCluster(t)
	defer cluster.close()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if err := WaitUntil(ctx, 20*time.Millisecond, func() bool { return cluster.leader() != nil }); err != nil {
		t.Fatalf("leader election timed out: %v", err)
	}
	oldLeader := cluster.leader()
	cluster.transport.unregister(oldLeader.nodeID)
	oldLeader.Stop()
	delete(cluster.groups, oldLeader.nodeID)
	if err := WaitUntil(ctx, 20*time.Millisecond, func() bool { l := cluster.leader(); return l != nil && l.nodeID != oldLeader.nodeID }); err != nil {
		t.Fatalf("new leader election timed out: %v", err)
	}
	leader := cluster.leader()
	cmd := NewCommand(CommandScopeSystem, wal.RecordType("system.test"), []byte(`{"ok":true}`), "cmd-after-fail")
	if _, err := leader.Propose(ctx, cmd); err != nil {
		t.Fatalf("Propose() after one node stopped error = %v", err)
	}
}

func TestInMemoryRaftGroupUnavailableWithoutQuorum(t *testing.T) {
	cluster := newMemoryCluster(t)
	defer cluster.close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := WaitUntil(ctx, 20*time.Millisecond, func() bool { return cluster.leader() != nil }); err != nil {
		t.Fatalf("leader election timed out: %v", err)
	}
	leader := cluster.leader()
	stopped := 0
	for id, g := range cluster.groups {
		if id == leader.nodeID || stopped >= 2 {
			continue
		}
		cluster.transport.unregister(id)
		g.Stop()
		delete(cluster.groups, id)
		stopped++
	}
	proposalCtx, proposalCancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer proposalCancel()
	cmd := NewCommand(CommandScopeSystem, wal.RecordType("system.test"), []byte(`{"ok":true}`), "cmd-no-quorum")
	if _, err := leader.Propose(proposalCtx, cmd); err == nil {
		t.Fatal("expected proposal without quorum to fail")
	}
}
