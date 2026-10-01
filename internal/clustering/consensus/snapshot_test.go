package consensus

import (
	"context"
	"encoding/json"
	"os"
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

func TestPersistentRaftGroupEmptyStorageRejoinInstallsSnapshotAndTail(t *testing.T) {
	transport := newMemoryTransport()
	ctx := context.Background()
	peers := []NodeID{1, 2, 3}
	dirs := map[NodeID]string{}
	groups := map[NodeID]*Group{}
	sms := map[NodeID]*countingSnapshotStateMachine{}
	var groupsMu sync.RWMutex
	stopTick := make(chan struct{})
	defer func() {
		close(stopTick)
		groupsMu.Lock()
		defer groupsMu.Unlock()
		for _, g := range groups {
			g.Stop()
		}
	}()
	for _, id := range peers {
		dirs[id] = t.TempDir()
		store, err := NewPersistentStorage(dirs[id])
		if err != nil {
			t.Fatalf("NewPersistentStorage(%d) error = %v", id, err)
		}
		sm := &countingSnapshotStateMachine{}
		g, err := StartGroup(ctx, GroupOptions{ID: "snapshot-rejoin", NodeID: id, Peers: peers, PartitionCount: 64, StateMachine: sm, Transport: transport, Storage: store, ReplayCommittedEntries: true, ElectionTick: 5, HeartbeatTick: 1})
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
				groupsMu.RLock()
				for _, g := range groups {
					g.Tick()
				}
				groupsMu.RUnlock()
			}
		}
	}()
	leader := func() *Group {
		groupsMu.RLock()
		defer groupsMu.RUnlock()
		leaders := map[NodeID]int{}
		for _, g := range groups {
			if l := g.Leader(); l != 0 {
				leaders[l]++
			}
		}
		for id, count := range leaders {
			if count >= 2 {
				return groups[id]
			}
		}
		return nil
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	waitLeader := func() *Group {
		t.Helper()
		var out *Group
		if err := WaitUntil(waitCtx, 20*time.Millisecond, func() bool { out = leader(); return out != nil }); err != nil {
			t.Fatalf("leader election timed out: %v", err)
		}
		return out
	}
	proposeCount := func(prefix string, count int) {
		t.Helper()
		for i := 0; i < count; i++ {
			cmd := NewCommand(CommandScopeSystem, wal.RecordType("system.test"), []byte(`{"ok":true}`), prefix+"-"+string(rune('0'+i)))
			if _, err := waitLeader().Propose(waitCtx, cmd); err != nil {
				t.Fatalf("Propose(%s %d) error = %v", prefix, i, err)
			}
		}
	}
	safeCount := func(id NodeID) int {
		if sm := sms[id]; sm != nil {
			return sm.Count()
		}
		return -1
	}
	waitCounts := func(ids []NodeID, want int, label string) {
		t.Helper()
		if err := WaitUntil(waitCtx, 20*time.Millisecond, func() bool {
			for _, id := range ids {
				if safeCount(id) < want {
					return false
				}
			}
			return true
		}); err != nil {
			t.Fatalf("%s counts did not reach %d: node1=%d node2=%d node3=%d err=%v", label, want, safeCount(1), safeCount(2), safeCount(3), err)
		}
	}

	proposeCount("before-wipe", 3)
	waitCounts(peers, 3, "initial")

	wipedID := NodeID(3)
	transport.unregister(wipedID)
	groupsMu.Lock()
	groups[wipedID].Stop()
	delete(groups, wipedID)
	delete(sms, wipedID)
	groupsMu.Unlock()
	if err := os.RemoveAll(dirs[wipedID]); err != nil {
		t.Fatalf("RemoveAll(wiped dir) error = %v", err)
	}
	if err := os.MkdirAll(dirs[wipedID], 0o755); err != nil {
		t.Fatalf("MkdirAll(wiped dir) error = %v", err)
	}

	activeIDs := []NodeID{1, 2}
	proposeCount("snapshotted-while-wiped", 5)
	waitCounts(activeIDs, 8, "pre-snapshot active quorum")
	var snapshotIndex uint64
	groupsMu.RLock()
	for _, id := range activeIDs {
		idx, err := groups[id].CreateSnapshot(0, true)
		if err != nil {
			groupsMu.RUnlock()
			t.Fatalf("CreateSnapshot(%d) error = %v", id, err)
		}
		if idx > snapshotIndex {
			snapshotIndex = idx
		}
	}
	groupsMu.RUnlock()
	if snapshotIndex == 0 {
		t.Fatal("expected non-zero active quorum snapshot index")
	}

	proposeCount("tail-while-wiped", 2)
	waitCounts(activeIDs, 10, "tail active quorum")

	store, err := NewPersistentStorage(dirs[wipedID])
	if err != nil {
		t.Fatalf("rejoin NewPersistentStorage() error = %v", err)
	}
	rejoinedSM := &countingSnapshotStateMachine{}
	rejoined, err := StartGroup(ctx, GroupOptions{ID: "snapshot-rejoin", NodeID: wipedID, Peers: peers, PartitionCount: 64, StateMachine: rejoinedSM, Transport: transport, Storage: store, ReplayCommittedEntries: true, JoinExisting: true, ElectionTick: 5, HeartbeatTick: 1})
	if err != nil {
		t.Fatalf("rejoin StartGroup() error = %v", err)
	}
	groupsMu.Lock()
	groups[wipedID] = rejoined
	sms[wipedID] = rejoinedSM
	groupsMu.Unlock()
	transport.mu.Lock()
	delete(transport.drop, wipedID)
	transport.mu.Unlock()
	transport.register(rejoined)

	proposeCount("after-rejoin", 1)
	if err := WaitUntil(waitCtx, 20*time.Millisecond, func() bool { return rejoinedSM.Count() >= 11 }); err != nil {
		term, commit, applied := rejoined.Progress()
		last, snap := rejoined.StorageProgress()
		t.Fatalf("wiped same-ID rejoin did not restore snapshot plus tail: count=%d term=%d commit=%d applied=%d last=%d snapshot=%d want_snapshot>=%d err=%v", rejoinedSM.Count(), term, commit, applied, last, snap, snapshotIndex, err)
	}
	_, installedSnapshot := rejoined.StorageProgress()
	if installedSnapshot < snapshotIndex {
		t.Fatalf("rejoined snapshot index=%d want at least active quorum snapshot index=%d", installedSnapshot, snapshotIndex)
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
