package consensus

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/myceldb/mycel/internal/diagnostics"
	"go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

type GroupID string
type NodeID uint64

var ErrNoLeader = errors.New("raft group has no leader")
var ErrNotLeader = errors.New("raft group local node is not leader")

const slowRaftReadApplyWait = 100 * time.Millisecond

type ProposalResult struct {
	Index uint64
	Term  uint64
}

type ReadBarrierResult struct {
	Index uint64
	Term  uint64
}

type ReadDiagnostics struct {
	ReadIndexAttempts       uint64
	ReadIndexSuccesses      uint64
	ReadIndexFailures       uint64
	ReadIndexTimeouts       uint64
	ReadIndexNoLeader       uint64
	ReadIndexNotLeader      uint64
	ApplyWaitFailures       uint64
	LastFailureAt           time.Time
	LastFailureReason       string
	LastReadIndex           uint64
	LastAppliedWaitIndex    uint64
	LastAppliedWaitSuccess  uint64
	LastAppliedWaitDuration time.Duration
}

type Group struct {
	id             GroupID
	nodeID         NodeID
	subsystem      string
	partitionCount uint32
	peers          []NodeID
	node           raft.Node
	storage        raftStorage
	transport      Transport
	sm             StateMachine

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	mu              sync.Mutex
	backupMu        sync.RWMutex
	leader          NodeID
	term            uint64
	commitIndex     uint64
	appliedIndex    uint64
	waiters         map[string]chan proposalOutcome
	readSeq         uint64
	readWaiters     map[string]chan readIndexOutcome
	readDiagnostics ReadDiagnostics
}

type proposalOutcome struct {
	result ProposalResult
	err    error
}

type readIndexOutcome struct {
	result ReadBarrierResult
	err    error
}

type Transport interface {
	Send(ctx context.Context, groupID GroupID, from NodeID, messages []raftpb.Message)
}

type GroupOptions struct {
	ID             GroupID
	NodeID         NodeID
	Peers          []NodeID
	PartitionCount uint32
	StateMachine   StateMachine
	Transport      Transport
	ElectionTick   int
	HeartbeatTick  int

	// Storage optionally supplies durable raft storage. When nil, StartGroup uses
	// in-memory storage for tests and non-durable experimental groups.
	Storage raftStorage

	// ReplayCommittedEntries rebuilds a volatile state machine by restoring any
	// persisted raft snapshot first, then replaying committed entries from Storage
	// before raft restarts. Enable this only for state machines whose snapshot
	// restore and ApplyCommand paths are safe to run into a fresh instance.
	ReplayCommittedEntries bool
}

func StartGroup(ctx context.Context, opts GroupOptions) (*Group, error) {
	if opts.ID == "" || opts.NodeID == 0 || len(opts.Peers) == 0 || opts.StateMachine == nil || opts.Transport == nil {
		return nil, fmt.Errorf("group id, node id, peers, state machine, and transport are required")
	}
	storage := opts.Storage
	if storage == nil {
		storage = raft.NewMemoryStorage()
	}
	subsystem := ""
	if named, ok := opts.StateMachine.(interface{ RaftStateMachineName() string }); ok {
		subsystem = named.RaftStateMachineName()
	}
	if labeler, ok := storage.(interface {
		SetDiagnosticsLabels(GroupID, NodeID)
	}); ok {
		labeler.SetDiagnosticsLabels(opts.ID, opts.NodeID)
	}
	if labeler, ok := storage.(interface{ SetDiagnosticsSubsystem(string) }); ok {
		labeler.SetDiagnosticsSubsystem(subsystem)
	}
	initialAppliedIndex := uint64(0)
	if opts.ReplayCommittedEntries {
		applied, err := restoreSnapshotAndReplayCommittedEntries(ctx, storage, opts.StateMachine, opts.PartitionCount)
		if err != nil {
			return nil, fmt.Errorf("restore raft snapshot and replay committed entries: %w", err)
		}
		initialAppliedIndex = applied
	}
	hs, _, err := storage.InitialState()
	if err != nil {
		return nil, fmt.Errorf("read initial raft state: %w", err)
	}
	peers := make([]raft.Peer, 0, len(opts.Peers))
	for _, peer := range opts.Peers {
		if peer == 0 {
			return nil, fmt.Errorf("peer node id must be positive")
		}
		peers = append(peers, raft.Peer{ID: uint64(peer)})
	}
	electionTick := opts.ElectionTick
	if electionTick == 0 {
		electionTick = 10
	}
	heartbeatTick := opts.HeartbeatTick
	if heartbeatTick == 0 {
		heartbeatTick = 1
	}
	cfg := &raft.Config{ID: uint64(opts.NodeID), ElectionTick: electionTick, HeartbeatTick: heartbeatTick, Storage: storage, Applied: initialAppliedIndex, MaxSizePerMsg: 1024 * 1024, MaxInflightMsgs: 256}
	ctx, cancel := context.WithCancel(ctx)
	var node raft.Node
	if hasPersistentRaftState(storage) {
		node = raft.RestartNode(cfg)
	} else {
		node = raft.StartNode(cfg, peers)
	}
	g := &Group{id: opts.ID, nodeID: opts.NodeID, subsystem: subsystem, partitionCount: opts.PartitionCount, peers: append([]NodeID(nil), opts.Peers...), node: node, storage: storage, transport: opts.Transport, sm: opts.StateMachine, ctx: ctx, cancel: cancel, done: make(chan struct{}), term: hs.Term, commitIndex: hs.Commit, appliedIndex: initialAppliedIndex, waiters: map[string]chan proposalOutcome{}, readWaiters: map[string]chan readIndexOutcome{}}
	go g.run()
	return g, nil
}

func hasPersistentRaftState(storage raftStorage) bool {
	hs, _, err := storage.InitialState()
	if err == nil && !raft.IsEmptyHardState(hs) {
		return true
	}
	last, err := storage.LastIndex()
	return err == nil && last > 0
}

func restoreSnapshotAndReplayCommittedEntries(ctx context.Context, storage raftStorage, sm StateMachine, partitionCount uint32) (uint64, error) {
	hs, _, err := storage.InitialState()
	if err != nil {
		return 0, err
	}
	applied := uint64(0)
	if snap, err := storage.Snapshot(); err == nil && !raft.IsEmptySnap(snap) {
		if err := restoreStateMachineSnapshot(sm, snap); err != nil {
			return 0, err
		}
		applied = snap.Metadata.Index
	} else if err != nil && !errors.Is(err, raft.ErrSnapshotTemporarilyUnavailable) {
		return 0, err
	}
	if hs.Commit == 0 {
		return applied, nil
	}
	first, err := storage.FirstIndex()
	if err != nil {
		return applied, err
	}
	if applied > 0 && first <= applied {
		first = applied + 1
	}
	last, err := storage.LastIndex()
	if err != nil {
		return applied, err
	}
	commit := hs.Commit
	if commit > last {
		commit = last
	}
	if commit < first {
		return applied, nil
	}
	entries, err := storage.Entries(first, commit+1, ^uint64(0))
	if err != nil {
		return applied, err
	}
	for _, entry := range entries {
		if entry.Type == raftpb.EntryNormal && len(entry.Data) > 0 {
			cmd, err := DecodeCommand(entry.Data)
			if err != nil {
				return applied, err
			}
			if err := cmd.Validate(partitionCount); err != nil {
				return applied, err
			}
			if err := sm.ApplyCommand(ctx, ApplyContext{RaftIndex: entry.Index, RaftTerm: entry.Term}, cmd); err != nil {
				return applied, err
			}
		}
		if entry.Index > applied {
			applied = entry.Index
		}
	}
	if commit > applied {
		applied = commit
	}
	return applied, nil
}

func restoreStateMachineSnapshot(sm StateMachine, snap raftpb.Snapshot) error {
	if raft.IsEmptySnap(snap) || len(snap.Data) == 0 {
		return nil
	}
	restorer, ok := sm.(StateMachineSnapshotRestorer)
	if !ok {
		return fmt.Errorf("state machine cannot restore non-empty raft snapshot at index %d", snap.Metadata.Index)
	}
	if err := restorer.RestoreSnapshot(snap.Data); err != nil {
		return fmt.Errorf("restore state machine snapshot at index %d: %w", snap.Metadata.Index, err)
	}
	return nil
}

func emptyConfState(cs raftpb.ConfState) bool {
	return len(cs.Voters) == 0 && len(cs.Learners) == 0 && len(cs.VotersOutgoing) == 0 && len(cs.LearnersNext) == 0 && !cs.AutoLeave
}

func (g *Group) Stop() {
	g.cancel()
	<-g.done
}

func (g *Group) Tick() { g.node.Tick() }

func (g *Group) Step(ctx context.Context, msg raftpb.Message) error {
	if msg.Type == raftpb.MsgHeartbeat && msg.Commit > 0 && g.storage != nil {
		last, err := g.storage.LastIndex()
		if err == nil && msg.Commit > last {
			msg.Commit = last
		}
	}
	return g.node.Step(ctx, msg)
}

func (g *Group) Leader() NodeID {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.leader
}

func (g *Group) Progress() (term uint64, commitIndex uint64, appliedIndex uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.term, g.commitIndex, g.appliedIndex
}

type BackupCheckpoint struct {
	GroupID       GroupID
	NodeID        NodeID
	Leader        NodeID
	Term          uint64
	BarrierIndex  uint64
	CommitIndex   uint64
	AppliedIndex  uint64
	LastIndex     uint64
	SnapshotIndex uint64
	FrozenAt      time.Time
}

type BackupFreezeLease struct {
	Checkpoint BackupCheckpoint
	release    func()
	once       sync.Once
}

func (l *BackupFreezeLease) Release() {
	if l == nil {
		return
	}
	l.once.Do(func() {
		if l.release != nil {
			l.release()
		}
	})
}

func (g *Group) AcquireBackupFreeze(ctx context.Context, barrier uint64) (*BackupFreezeLease, error) {
	if g == nil || g.storage == nil {
		return nil, fmt.Errorf("raft group and storage are required")
	}
	if err := g.WaitApplied(ctx, barrier); err != nil {
		return nil, err
	}
	locked := make(chan struct{})
	go func() {
		g.backupMu.Lock()
		close(locked)
	}()
	select {
	case <-locked:
	case <-ctx.Done():
		go func() {
			<-locked
			g.backupMu.Unlock()
		}()
		return nil, ctx.Err()
	}
	if flusher, ok := g.storage.(interface{ Flush() error }); ok {
		if err := flusher.Flush(); err != nil {
			g.backupMu.Unlock()
			return nil, err
		}
	}
	term, commit, applied := g.Progress()
	last, snap := g.StorageProgress()
	if applied < barrier {
		g.backupMu.Unlock()
		return nil, fmt.Errorf("raft group %s applied index %d is behind backup barrier %d", g.id, applied, barrier)
	}
	lease := &BackupFreezeLease{Checkpoint: BackupCheckpoint{GroupID: g.id, NodeID: g.nodeID, Leader: g.Leader(), Term: term, BarrierIndex: barrier, CommitIndex: commit, AppliedIndex: applied, LastIndex: last, SnapshotIndex: snap, FrozenAt: time.Now().UTC()}}
	lease.release = g.backupMu.Unlock
	return lease, nil
}

func (g *Group) StorageProgress() (lastIndex uint64, snapshotIndex uint64) {
	if g == nil || g.storage == nil {
		return 0, 0
	}
	if last, err := g.storage.LastIndex(); err == nil {
		lastIndex = last
	}
	if snap, err := g.storage.Snapshot(); err == nil {
		snapshotIndex = snap.Metadata.Index
	}
	return lastIndex, snapshotIndex
}

func (g *Group) CreateSnapshot(index uint64, compact bool) (uint64, error) {
	if g == nil || g.storage == nil || g.sm == nil {
		return 0, fmt.Errorf("raft group, storage, and state machine are required")
	}
	snapshotter, ok := g.sm.(StateMachineSnapshotter)
	if !ok {
		return 0, fmt.Errorf("state machine cannot create raft snapshots")
	}
	g.mu.Lock()
	applied := g.appliedIndex
	g.mu.Unlock()
	if index == 0 {
		index = applied
	}
	if index == 0 {
		return 0, fmt.Errorf("cannot snapshot before any applied entries")
	}
	if index != applied {
		return 0, fmt.Errorf("cannot snapshot index %d with current state applied at index %d", index, applied)
	}
	data, err := snapshotter.Snapshot()
	if err != nil {
		return 0, fmt.Errorf("create state machine snapshot: %w", err)
	}
	_, cs, err := g.storage.InitialState()
	if err != nil {
		return 0, err
	}
	if emptyConfState(cs) {
		cs.Voters = make([]uint64, 0, len(g.peers))
		for _, peer := range g.peers {
			cs.Voters = append(cs.Voters, uint64(peer))
		}
	}
	snap, err := g.storage.CreateSnapshot(index, &cs, data)
	if err != nil {
		return 0, err
	}
	if compact {
		if err := g.storage.Compact(index); err != nil {
			return 0, err
		}
	}
	return snap.Metadata.Index, nil
}

func (g *Group) ReadDiagnostics() ReadDiagnostics {
	if g == nil {
		return ReadDiagnostics{}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.readDiagnostics
}

func (g *Group) LinearizableRead(ctx context.Context) (ReadBarrierResult, error) {
	if err := ctx.Err(); err != nil {
		return ReadBarrierResult{}, err
	}
	readCtx, ch, err := g.beginReadIndex(ctx)
	if err != nil {
		return ReadBarrierResult{}, err
	}
	if err := g.node.ReadIndex(ctx, []byte(readCtx)); err != nil {
		g.forgetReadWaiter(readCtx, readIndexOutcome{err: err})
		g.recordReadFailure(err)
		return ReadBarrierResult{}, err
	}
	select {
	case out := <-ch:
		if out.err != nil {
			g.recordReadFailure(out.err)
			return ReadBarrierResult{}, out.err
		}
		waitStarted := time.Now()
		if err := g.WaitApplied(ctx, out.result.Index); err != nil {
			g.recordApplyWaitFailure(out.result.Index, time.Since(waitStarted), err)
			return ReadBarrierResult{}, err
		}
		g.recordReadSuccess(out.result.Index, time.Since(waitStarted))
		return out.result, nil
	case <-ctx.Done():
		g.forgetReadWaiter(readCtx, readIndexOutcome{err: ctx.Err()})
		g.recordReadFailure(ctx.Err())
		return ReadBarrierResult{}, ctx.Err()
	case <-g.ctx.Done():
		err := g.ctx.Err()
		if err == nil {
			err = context.Canceled
		}
		g.forgetReadWaiter(readCtx, readIndexOutcome{err: err})
		g.recordReadFailure(err)
		return ReadBarrierResult{}, err
	}
}

func (g *Group) WaitApplied(ctx context.Context, index uint64) error {
	if index == 0 {
		return nil
	}
	return WaitUntil(ctx, time.Millisecond, func() bool {
		g.mu.Lock()
		defer g.mu.Unlock()
		return g.appliedIndex >= index
	})
}

func (g *Group) beginReadIndex(ctx context.Context) (string, chan readIndexOutcome, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.readDiagnostics.ReadIndexAttempts++
	if g.leader == 0 {
		g.recordReadFailureLocked(ErrNoLeader)
		slog.Default().Warn("raft read-index failed", "group_id", string(g.id), "local_node_id", uint64(g.nodeID), "leader_node_id", uint64(g.leader), "reason", g.readDiagnostics.LastFailureReason)
		return "", nil, ErrNoLeader
	}
	if g.leader != g.nodeID {
		err := fmt.Errorf("%w: leader is %d", ErrNotLeader, g.leader)
		g.recordReadFailureLocked(err)
		slog.Default().Warn("raft read-index failed", "group_id", string(g.id), "local_node_id", uint64(g.nodeID), "leader_node_id", uint64(g.leader), "reason", g.readDiagnostics.LastFailureReason)
		return "", nil, err
	}
	g.readSeq++
	readCtx := fmt.Sprintf("read-%d-%d", g.nodeID, g.readSeq)
	ch := make(chan readIndexOutcome, 1)
	g.readWaiters[readCtx] = ch
	return readCtx, ch, nil
}

func (g *Group) Campaign(ctx context.Context) error { return g.node.Campaign(ctx) }

func (g *Group) Propose(ctx context.Context, cmd RaftCommand) (ProposalResult, error) {
	diag := diagnostics.CommitTimingEnabled()
	var started time.Time
	if diag {
		started = time.Now()
	}
	if err := cmd.Validate(g.partitionCount); err != nil {
		return ProposalResult{}, err
	}
	data, err := EncodeCommand(cmd)
	if err != nil {
		return ProposalResult{}, err
	}
	ch := make(chan proposalOutcome, 1)
	g.mu.Lock()
	if _, exists := g.waiters[cmd.CommandID]; exists {
		g.mu.Unlock()
		return ProposalResult{}, fmt.Errorf("duplicate in-flight command_id %q", cmd.CommandID)
	}
	g.waiters[cmd.CommandID] = ch
	waiterCount := len(g.waiters)
	leader := g.leader
	g.mu.Unlock()
	var proposeStarted time.Time
	if diag {
		proposeStarted = time.Now()
	}
	if err := g.node.Propose(ctx, data); err != nil {
		g.forgetWaiter(cmd.CommandID, proposalOutcome{err: err})
		if diag {
			diagnostics.LogCommitTiming("raft proposal failed before wait",
				"subsystem", g.subsystem,
				"group_id", string(g.id),
				"local_node_id", uint64(g.nodeID),
				"leader_node_id", uint64(leader),
				"command_id", cmd.CommandID,
				"scope", string(cmd.Scope),
				"record_type", string(cmd.RecordType),
				"partition_id", cmd.PartitionID,
				"payload_bytes", len(cmd.Payload),
				"encoded_bytes", len(data),
				"inflight_waiters", waiterCount,
				"node_propose_ms", time.Since(proposeStarted).Milliseconds(),
				"duration_ms", time.Since(started).Milliseconds(),
				"error", err.Error())
		}
		return ProposalResult{}, err
	}
	var nodeProposeDuration time.Duration
	var waitStarted time.Time
	if diag {
		nodeProposeDuration = time.Since(proposeStarted)
		waitStarted = time.Now()
	}
	select {
	case out := <-ch:
		if diag {
			diagnostics.LogCommitTiming("raft proposal completed",
				"subsystem", g.subsystem,
				"group_id", string(g.id),
				"local_node_id", uint64(g.nodeID),
				"leader_node_id", uint64(leader),
				"command_id", cmd.CommandID,
				"scope", string(cmd.Scope),
				"record_type", string(cmd.RecordType),
				"partition_id", cmd.PartitionID,
				"payload_bytes", len(cmd.Payload),
				"encoded_bytes", len(data),
				"inflight_waiters", waiterCount,
				"node_propose_ms", nodeProposeDuration.Milliseconds(),
				"wait_ms", time.Since(waitStarted).Milliseconds(),
				"duration_ms", time.Since(started).Milliseconds(),
				"result_index", out.result.Index,
				"result_term", out.result.Term,
				"error", errorString(out.err))
		}
		return out.result, out.err
	case <-ctx.Done():
		g.forgetWaiter(cmd.CommandID, proposalOutcome{err: ctx.Err()})
		if diag {
			diagnostics.LogCommitTiming("raft proposal wait failed",
				"subsystem", g.subsystem,
				"group_id", string(g.id),
				"local_node_id", uint64(g.nodeID),
				"leader_node_id", uint64(leader),
				"command_id", cmd.CommandID,
				"scope", string(cmd.Scope),
				"record_type", string(cmd.RecordType),
				"partition_id", cmd.PartitionID,
				"payload_bytes", len(cmd.Payload),
				"encoded_bytes", len(data),
				"inflight_waiters", waiterCount,
				"node_propose_ms", nodeProposeDuration.Milliseconds(),
				"wait_ms", time.Since(waitStarted).Milliseconds(),
				"duration_ms", time.Since(started).Milliseconds(),
				"error", ctx.Err().Error())
		}
		return ProposalResult{}, ctx.Err()
	}
}

func (g *Group) run() {
	defer close(g.done)
	defer g.node.Stop()
	defer g.failReadWaiters(context.Canceled)
	for {
		select {
		case <-g.ctx.Done():
			return
		case rd := <-g.node.Ready():
			if !g.processReady(rd) {
				return
			}
		}
	}
}

func (g *Group) processReady(rd raft.Ready) bool {
	diag := diagnostics.CommitTimingEnabled()
	var started time.Time
	var snapshotDuration, hardStateDuration, appendDuration, sendDuration, applyDuration, advanceDuration time.Duration
	if diag {
		started = time.Now()
	}
	g.backupMu.RLock()
	defer g.backupMu.RUnlock()
	if rd.SoftState != nil {
		g.mu.Lock()
		previousLeader := g.leader
		g.leader = NodeID(rd.SoftState.Lead)
		localLeader := g.leader == g.nodeID
		currentLeader := g.leader
		term := g.term
		g.mu.Unlock()
		if diag && previousLeader != currentLeader {
			diagnostics.LogCommitTiming("raft leader changed",
				"subsystem", g.subsystem,
				"group_id", string(g.id),
				"local_node_id", uint64(g.nodeID),
				"previous_leader_node_id", uint64(previousLeader),
				"leader_node_id", uint64(currentLeader),
				"term", term)
		}
		if !localLeader {
			g.failReadWaiters(ErrNotLeader)
		}
	}
	if !raft.IsEmptySnap(rd.Snapshot) {
		var snapshotStarted time.Time
		if diag {
			snapshotStarted = time.Now()
		}
		if err := g.storage.ApplySnapshot(rd.Snapshot); err != nil {
			g.failWaiters(fmt.Errorf("apply raft snapshot: %w", err))
			return false
		}
		if err := restoreStateMachineSnapshot(g.sm, rd.Snapshot); err != nil {
			g.failWaiters(err)
			return false
		}
		g.markApplied(rd.Snapshot.Metadata.Index)
		if diag {
			snapshotDuration = time.Since(snapshotStarted)
		}
	}
	if !raft.IsEmptyHardState(rd.HardState) {
		var hardStarted time.Time
		if diag {
			hardStarted = time.Now()
		}
		if err := g.storage.SetHardState(rd.HardState); err != nil {
			g.failWaiters(fmt.Errorf("persist raft hard state: %w", err))
			return false
		}
		g.mu.Lock()
		if rd.HardState.Term != 0 {
			g.term = rd.HardState.Term
		}
		if rd.HardState.Commit != 0 {
			g.commitIndex = rd.HardState.Commit
		}
		g.mu.Unlock()
		if diag {
			hardStateDuration = time.Since(hardStarted)
		}
	}
	if len(rd.Entries) > 0 {
		var appendStarted time.Time
		if diag {
			appendStarted = time.Now()
		}
		if err := g.storage.Append(rd.Entries); err != nil {
			g.failWaiters(fmt.Errorf("persist raft entries: %w", err))
			return false
		}
		if diag {
			appendDuration = time.Since(appendStarted)
		}
	}
	var sendStarted time.Time
	if diag {
		sendStarted = time.Now()
	}
	g.transport.Send(g.ctx, g.id, g.nodeID, rd.Messages)
	if diag {
		sendDuration = time.Since(sendStarted)
	}
	var applyStarted time.Time
	if diag {
		applyStarted = time.Now()
	}
	for _, entry := range rd.CommittedEntries {
		g.applyEntry(entry)
	}
	if diag {
		applyDuration = time.Since(applyStarted)
	}
	for _, readState := range rd.ReadStates {
		g.completeReadWaiter(string(readState.RequestCtx), readIndexOutcome{result: ReadBarrierResult{Index: readState.Index, Term: g.currentTerm()}})
	}
	var advanceStarted time.Time
	if diag {
		advanceStarted = time.Now()
	}
	g.node.Advance()
	if diag {
		advanceDuration = time.Since(advanceStarted)
		leader, term, commitIndex, appliedIndex := g.readyDiagnosticsState()
		diagnostics.LogCommitTiming("raft ready processed",
			"subsystem", g.subsystem,
			"group_id", string(g.id),
			"local_node_id", uint64(g.nodeID),
			"leader_node_id", uint64(leader),
			"term", term,
			"commit_index", commitIndex,
			"applied_index", appliedIndex,
			"entries", len(rd.Entries),
			"entry_record_types", entryRecordTypes(rd.Entries),
			"committed_entries", len(rd.CommittedEntries),
			"committed_record_types", entryRecordTypes(rd.CommittedEntries),
			"messages", len(rd.Messages),
			"read_states", len(rd.ReadStates),
			"has_hard_state", !raft.IsEmptyHardState(rd.HardState),
			"has_snapshot", !raft.IsEmptySnap(rd.Snapshot),
			"snapshot_ms", snapshotDuration.Milliseconds(),
			"hard_state_ms", hardStateDuration.Milliseconds(),
			"append_ms", appendDuration.Milliseconds(),
			"send_ms", sendDuration.Milliseconds(),
			"apply_ms", applyDuration.Milliseconds(),
			"advance_ms", advanceDuration.Milliseconds(),
			"duration_ms", time.Since(started).Milliseconds())
	}
	return true
}

func (g *Group) applyEntry(entry raftpb.Entry) {
	if g.alreadyApplied(entry.Index) {
		return
	}
	if entry.Type == raftpb.EntryConfChange || entry.Type == raftpb.EntryConfChangeV2 {
		var cc raftpb.ConfChangeI
		if entry.Type == raftpb.EntryConfChange {
			var v1 raftpb.ConfChange
			if err := v1.Unmarshal(entry.Data); err != nil {
				g.failWaiters(fmt.Errorf("decode raft conf change: %w", err))
				return
			}
			cc = v1
		} else {
			var v2 raftpb.ConfChangeV2
			if err := v2.Unmarshal(entry.Data); err != nil {
				g.failWaiters(fmt.Errorf("decode raft conf change v2: %w", err))
				return
			}
			cc = v2
		}
		cs := g.node.ApplyConfChange(cc)
		if store, ok := g.storage.(interface{ SetConfState(raftpb.ConfState) error }); ok {
			if err := store.SetConfState(*cs); err != nil {
				g.failWaiters(fmt.Errorf("persist raft conf state: %w", err))
				return
			}
		}
		g.markApplied(entry.Index)
		return
	}
	if entry.Type != raftpb.EntryNormal || len(entry.Data) == 0 {
		g.markApplied(entry.Index)
		return
	}
	cmd, err := DecodeCommand(entry.Data)
	if err == nil {
		err = g.sm.ApplyCommand(g.ctx, ApplyContext{RaftIndex: entry.Index, RaftTerm: entry.Term}, cmd)
	}
	g.markApplied(entry.Index)
	if err != nil {
		g.failReadWaiters(err)
	}
	if cmd.CommandID != "" {
		g.completeWaiter(cmd.CommandID, proposalOutcome{result: ProposalResult{Index: entry.Index, Term: entry.Term}, err: err})
	}
}

func (g *Group) alreadyApplied(index uint64) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return index != 0 && index <= g.appliedIndex
}

func (g *Group) markApplied(index uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if index > g.appliedIndex {
		g.appliedIndex = index
	}
}

func (g *Group) completeWaiter(commandID string, out proposalOutcome) {
	if commandID == "" {
		return
	}
	g.mu.Lock()
	ch := g.waiters[commandID]
	delete(g.waiters, commandID)
	g.mu.Unlock()
	if ch != nil {
		ch <- out
	}
}

func (g *Group) forgetWaiter(commandID string, out proposalOutcome) { g.completeWaiter(commandID, out) }

func (g *Group) failWaiters(err error) {
	g.mu.Lock()
	waiters := g.waiters
	g.waiters = map[string]chan proposalOutcome{}
	readWaiters := g.readWaiters
	g.readWaiters = map[string]chan readIndexOutcome{}
	g.mu.Unlock()
	for _, ch := range waiters {
		ch <- proposalOutcome{err: err}
	}
	for _, ch := range readWaiters {
		ch <- readIndexOutcome{err: err}
	}
}

func (g *Group) currentTerm() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.term
}

func (g *Group) readyDiagnosticsState() (leader NodeID, term uint64, commitIndex uint64, appliedIndex uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.leader, g.term, g.commitIndex, g.appliedIndex
}

func (g *Group) completeReadWaiter(readCtx string, out readIndexOutcome) {
	if readCtx == "" {
		return
	}
	g.mu.Lock()
	ch := g.readWaiters[readCtx]
	delete(g.readWaiters, readCtx)
	g.mu.Unlock()
	if ch != nil {
		ch <- out
	}
}

func (g *Group) forgetReadWaiter(readCtx string, out readIndexOutcome) {
	g.completeReadWaiter(readCtx, out)
}

func (g *Group) failReadWaiters(err error) {
	g.mu.Lock()
	readWaiters := g.readWaiters
	g.readWaiters = map[string]chan readIndexOutcome{}
	g.mu.Unlock()
	for _, ch := range readWaiters {
		ch <- readIndexOutcome{err: err}
	}
}

func (g *Group) recordReadSuccess(index uint64, applyWait time.Duration) {
	g.mu.Lock()
	g.readDiagnostics.ReadIndexSuccesses++
	g.readDiagnostics.LastReadIndex = index
	g.readDiagnostics.LastAppliedWaitSuccess = index
	g.readDiagnostics.LastAppliedWaitDuration = applyWait
	groupID, nodeID := g.id, g.nodeID
	g.mu.Unlock()
	if applyWait >= slowRaftReadApplyWait {
		slog.Default().Warn("raft read apply wait was slow", "group_id", string(groupID), "local_node_id", uint64(nodeID), "read_index", index, "duration_ms", applyWait.Milliseconds())
	}
}

func (g *Group) recordReadFailure(err error) {
	g.mu.Lock()
	g.recordReadFailureLocked(err)
	groupID, nodeID, leader, reason := g.id, g.nodeID, g.leader, g.readDiagnostics.LastFailureReason
	g.mu.Unlock()
	slog.Default().Warn("raft read-index failed", "group_id", string(groupID), "local_node_id", uint64(nodeID), "leader_node_id", uint64(leader), "reason", reason)
}

func (g *Group) recordReadFailureLocked(err error) {
	g.readDiagnostics.ReadIndexFailures++
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		g.readDiagnostics.ReadIndexTimeouts++
	}
	if errors.Is(err, ErrNoLeader) {
		g.readDiagnostics.ReadIndexNoLeader++
	}
	if errors.Is(err, ErrNotLeader) {
		g.readDiagnostics.ReadIndexNotLeader++
	}
	g.readDiagnostics.LastFailureAt = time.Now().UTC()
	g.readDiagnostics.LastFailureReason = readFailureReason(err)
}

func (g *Group) recordApplyWaitFailure(index uint64, applyWait time.Duration, err error) {
	g.mu.Lock()
	g.readDiagnostics.ReadIndexFailures++
	g.readDiagnostics.ApplyWaitFailures++
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		g.readDiagnostics.ReadIndexTimeouts++
	}
	g.readDiagnostics.LastAppliedWaitIndex = index
	g.readDiagnostics.LastAppliedWaitDuration = applyWait
	g.readDiagnostics.LastFailureAt = time.Now().UTC()
	g.readDiagnostics.LastFailureReason = "apply_wait_" + readFailureReason(err)
	groupID, nodeID, leader, reason := g.id, g.nodeID, g.leader, g.readDiagnostics.LastFailureReason
	g.mu.Unlock()
	slog.Default().Warn("raft read apply wait failed", "group_id", string(groupID), "local_node_id", uint64(nodeID), "leader_node_id", uint64(leader), "read_index", index, "duration_ms", applyWait.Milliseconds(), "reason", reason)
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func readFailureReason(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, ErrNoLeader):
		return "no_leader"
	case errors.Is(err, ErrNotLeader):
		return "not_leader"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return "error"
	}
}

func WaitUntil(ctx context.Context, interval time.Duration, fn func() bool) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if fn() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
