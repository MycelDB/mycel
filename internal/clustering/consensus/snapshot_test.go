package consensus

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/myceldb/mycel/internal/wal"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

type countingSnapshotStateMachine struct {
	mu    sync.Mutex
	count int
}

func (s *countingSnapshotStateMachine) ApplyCommand(ctx context.Context, apply ApplyContext, cmd RaftCommand) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.count++
	return nil
}

func (s *countingSnapshotStateMachine) Snapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(struct {
		Count int `json:"count"`
	}{Count: s.count})
}

func (s *countingSnapshotStateMachine) RestoreSnapshot(data []byte) error {
	var payload struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.count = payload.Count
	return nil
}

func (s *countingSnapshotStateMachine) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

func TestStartGroupRestoresSnapshotWithoutReplayingSnapshottedEntries(t *testing.T) {
	transport := newMemoryTransport()
	store, err := NewPersistentStorage(t.TempDir())
	if err != nil {
		t.Fatalf("NewPersistentStorage() error = %v", err)
	}
	sm := &countingSnapshotStateMachine{}
	g, err := StartGroup(context.Background(), GroupOptions{ID: "snapshot-replay", NodeID: 1, Peers: []NodeID{1}, PartitionCount: 1, StateMachine: sm, Transport: transport, Storage: store, ElectionTick: 5, HeartbeatTick: 1})
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
	cmd := NewCommand(CommandScopeSystem, wal.RecordType("test.count"), []byte(`{}`), "count-1")
	if _, err := g.Propose(waitCtx, cmd); err != nil {
		t.Fatalf("Propose() error = %v", err)
	}
	if sm.Count() != 1 {
		t.Fatalf("count after propose=%d want 1", sm.Count())
	}
	if _, err := g.CreateSnapshot(0, false); err != nil {
		t.Fatalf("CreateSnapshot() error = %v", err)
	}
	g.Stop()

	reopened, err := NewPersistentStorage(store.dir)
	if err != nil {
		t.Fatalf("reopen NewPersistentStorage() error = %v", err)
	}
	restored := &countingSnapshotStateMachine{}
	g2, err := StartGroup(context.Background(), GroupOptions{ID: "snapshot-replay", NodeID: 1, Peers: []NodeID{1}, PartitionCount: 1, StateMachine: restored, Transport: transport, Storage: reopened, ReplayCommittedEntries: true, ElectionTick: 5, HeartbeatTick: 1})
	if err != nil {
		t.Fatalf("restart StartGroup() error = %v", err)
	}
	defer g2.Stop()
	transport.register(g2)
	for i := 0; i < 5; i++ {
		g2.Tick()
		time.Sleep(10 * time.Millisecond)
	}
	if restored.Count() != 1 {
		t.Fatalf("restored count=%d want 1; snapshotted entries were replayed", restored.Count())
	}
}

func TestStartGroupRejectsNonEmptySnapshotForApplyOnlyStateMachine(t *testing.T) {
	store, err := NewPersistentStorage(t.TempDir())
	if err != nil {
		t.Fatalf("NewPersistentStorage() error = %v", err)
	}
	if err := store.ApplySnapshot(raftpb.Snapshot{Data: []byte("application-state"), Metadata: raftpb.SnapshotMetadata{Index: 1, Term: 1, ConfState: raftpb.ConfState{Voters: []uint64{1}}}}); err != nil {
		t.Fatalf("ApplySnapshot() error = %v", err)
	}
	_, err = StartGroup(context.Background(), GroupOptions{ID: "snapshot-restore", NodeID: 1, Peers: []NodeID{1}, PartitionCount: 1, StateMachine: &MemoryStateMachine{}, Transport: newMemoryTransport(), Storage: store, ReplayCommittedEntries: true})
	if err == nil || !strings.Contains(err.Error(), "cannot restore non-empty raft snapshot") {
		t.Fatalf("StartGroup() error = %v, want non-empty snapshot restore failure", err)
	}
}
