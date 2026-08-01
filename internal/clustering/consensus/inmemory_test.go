package consensus

import (
	"context"
	"errors"
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

func TestGroupStepCapsHeartbeatCommitPastLocalLastIndex(t *testing.T) {
	transport := newMemoryTransport()
	g, err := StartGroup(context.Background(), GroupOptions{ID: "test", NodeID: 3, Peers: []NodeID{1, 2, 3}, PartitionCount: 64, StateMachine: &MemoryStateMachine{}, Transport: transport, ElectionTick: 50, HeartbeatTick: 1})
	if err != nil {
		t.Fatalf("StartGroup() error = %v", err)
	}
	defer g.Stop()
	if err := g.Step(context.Background(), raftpb.Message{Type: raftpb.MsgHeartbeat, From: 1, To: 3, Term: 2, Commit: 10}); err != nil {
		t.Fatalf("Step() error = %v", err)
	}
	time.Sleep(25 * time.Millisecond)
}

type blockingStateMachine struct {
	entered chan ApplyContext
	release chan struct{}
}

func (s *blockingStateMachine) ApplyCommand(ctx context.Context, apply ApplyContext, cmd RaftCommand) error {
	s.entered <- apply
	select {
	case <-s.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
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

func TestGroupLinearizableReadSucceedsOnLeader(t *testing.T) {
	cluster := newMemoryCluster(t)
	defer cluster.close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := WaitUntil(ctx, 20*time.Millisecond, func() bool { return cluster.leader() != nil }); err != nil {
		t.Fatalf("leader election timed out: %v", err)
	}
	leader := cluster.leader()
	cmd := NewCommand(CommandScopeSystem, wal.RecordType("system.test"), []byte(`{"ok":true}`), "cmd-before-read")
	proposal, err := leader.Propose(ctx, cmd)
	if err != nil {
		t.Fatalf("Propose() error = %v", err)
	}
	read, err := leader.LinearizableRead(ctx)
	if err != nil {
		t.Fatalf("LinearizableRead() error = %v", err)
	}
	if read.Index < proposal.Index || read.Term == 0 {
		t.Fatalf("LinearizableRead()=%#v proposal=%#v", read, proposal)
	}
	_, _, applied := leader.Progress()
	if applied < read.Index {
		t.Fatalf("applied=%d readIndex=%d; read returned before apply", applied, read.Index)
	}
	diag := leader.ReadDiagnostics()
	if diag.ReadIndexAttempts != 1 || diag.ReadIndexSuccesses != 1 || diag.ReadIndexFailures != 0 || diag.LastReadIndex != read.Index || diag.LastAppliedWaitSuccess != read.Index {
		t.Fatalf("ReadDiagnostics()=%#v", diag)
	}
}

func TestGroupLinearizableReadRejectsFollowerAndNoLeader(t *testing.T) {
	transport := newMemoryTransport()
	noLeader, err := StartGroup(context.Background(), GroupOptions{ID: "test", NodeID: 1, Peers: []NodeID{1, 2, 3}, PartitionCount: 64, StateMachine: &MemoryStateMachine{}, Transport: transport, ElectionTick: 50, HeartbeatTick: 1})
	if err != nil {
		t.Fatalf("StartGroup() error = %v", err)
	}
	defer noLeader.Stop()
	if _, err := noLeader.LinearizableRead(context.Background()); !errors.Is(err, ErrNoLeader) {
		t.Fatalf("LinearizableRead(no leader) error=%v want ErrNoLeader", err)
	}
	if diag := noLeader.ReadDiagnostics(); diag.ReadIndexNoLeader != 1 || diag.ReadIndexFailures != 1 {
		t.Fatalf("no leader ReadDiagnostics()=%#v", diag)
	}

	cluster := newMemoryCluster(t)
	defer cluster.close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := WaitUntil(ctx, 20*time.Millisecond, func() bool { return cluster.leader() != nil }); err != nil {
		t.Fatalf("leader election timed out: %v", err)
	}
	leader := cluster.leader()
	var follower *Group
	for id, g := range cluster.groups {
		if id != leader.nodeID {
			follower = g
			break
		}
	}
	if follower == nil {
		t.Fatal("expected follower")
	}
	if err := WaitUntil(ctx, 20*time.Millisecond, func() bool { return follower.Leader() == leader.nodeID }); err != nil {
		t.Fatalf("follower did not learn leader: %v", err)
	}
	if _, err := follower.LinearizableRead(ctx); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("LinearizableRead(follower) error=%v want ErrNotLeader", err)
	}
	if diag := follower.ReadDiagnostics(); diag.ReadIndexNotLeader != 1 || diag.ReadIndexFailures != 1 {
		t.Fatalf("follower ReadDiagnostics()=%#v", diag)
	}
}

func TestGroupWaitAppliedWaitsForStateMachineApplyCompletion(t *testing.T) {
	ctx := context.Background()
	transport := newMemoryTransport()
	sm := &blockingStateMachine{entered: make(chan ApplyContext, 1), release: make(chan struct{})}
	g, err := StartGroup(ctx, GroupOptions{ID: "test", NodeID: 1, Peers: []NodeID{1}, PartitionCount: 64, StateMachine: sm, Transport: transport, ElectionTick: 5, HeartbeatTick: 1})
	if err != nil {
		t.Fatalf("StartGroup() error = %v", err)
	}
	defer g.Stop()
	transport.register(g)
	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := g.Campaign(waitCtx); err != nil {
		t.Fatalf("Campaign() error = %v", err)
	}
	if err := WaitUntil(waitCtx, 10*time.Millisecond, func() bool { g.Tick(); return g.Leader() == 1 }); err != nil {
		t.Fatalf("leader election timed out: %v", err)
	}
	proposeErr := make(chan error, 1)
	go func() {
		_, err := g.Propose(waitCtx, NewCommand(CommandScopeSystem, wal.RecordType("system.test"), []byte(`{"ok":true}`), "cmd-blocked-apply"))
		proposeErr <- err
	}()
	var apply ApplyContext
	select {
	case apply = <-sm.entered:
	case <-waitCtx.Done():
		t.Fatalf("state machine apply did not start: %v", waitCtx.Err())
	}
	shortCtx, shortCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer shortCancel()
	if err := g.WaitApplied(shortCtx, apply.RaftIndex); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitApplied() while apply blocked error=%v want DeadlineExceeded", err)
	}
	close(sm.release)
	if err := <-proposeErr; err != nil {
		t.Fatalf("Propose() error = %v", err)
	}
	if err := g.WaitApplied(waitCtx, apply.RaftIndex); err != nil {
		t.Fatalf("WaitApplied() after apply release error=%v", err)
	}
}

func TestGroupLinearizableReadWithoutQuorumTimesOutAndCleansWaiter(t *testing.T) {
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
	readCtx, readCancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer readCancel()
	if _, err := leader.LinearizableRead(readCtx); err == nil {
		t.Fatal("expected LinearizableRead without quorum to fail")
	}
	leader.mu.Lock()
	waiters := len(leader.readWaiters)
	leader.mu.Unlock()
	if waiters != 0 {
		t.Fatalf("read waiters after timeout=%d want 0", waiters)
	}
	if diag := leader.ReadDiagnostics(); diag.ReadIndexAttempts != 1 || diag.ReadIndexFailures != 1 || diag.ReadIndexTimeouts != 1 {
		t.Fatalf("ReadDiagnostics()=%#v", diag)
	}
}
