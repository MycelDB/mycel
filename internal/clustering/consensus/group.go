package consensus

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

type GroupID string
type NodeID uint64

var ErrNoLeader = errors.New("raft group has no leader")

type ProposalResult struct {
	Index uint64
	Term  uint64
}

type Group struct {
	id             GroupID
	nodeID         NodeID
	partitionCount uint32
	node           raft.Node
	storage        raftStorage
	transport      Transport
	sm             StateMachine

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	mu           sync.Mutex
	leader       NodeID
	term         uint64
	commitIndex  uint64
	appliedIndex uint64
	waiters      map[string]chan proposalOutcome
}

type proposalOutcome struct {
	result ProposalResult
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

	// ReplayCommittedEntries rebuilds a volatile state machine by replaying
	// committed entries from Storage before raft restarts. Enable this only for
	// state machines whose ApplyCommand path is safe to replay into a fresh
	// in-memory instance.
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
	if opts.ReplayCommittedEntries {
		if err := replayCommittedEntries(ctx, storage, opts.StateMachine, opts.PartitionCount); err != nil {
			return nil, fmt.Errorf("replay committed raft entries: %w", err)
		}
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
	cfg := &raft.Config{ID: uint64(opts.NodeID), ElectionTick: electionTick, HeartbeatTick: heartbeatTick, Storage: storage, MaxSizePerMsg: 1024 * 1024, MaxInflightMsgs: 256}
	ctx, cancel := context.WithCancel(ctx)
	var node raft.Node
	if hasPersistentRaftState(storage) {
		node = raft.RestartNode(cfg)
	} else {
		node = raft.StartNode(cfg, peers)
	}
	g := &Group{id: opts.ID, nodeID: opts.NodeID, partitionCount: opts.PartitionCount, node: node, storage: storage, transport: opts.Transport, sm: opts.StateMachine, ctx: ctx, cancel: cancel, done: make(chan struct{}), waiters: map[string]chan proposalOutcome{}}
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

func replayCommittedEntries(ctx context.Context, storage raftStorage, sm StateMachine, partitionCount uint32) error {
	hs, _, err := storage.InitialState()
	if err != nil {
		return err
	}
	if hs.Commit == 0 {
		return nil
	}
	first, err := storage.FirstIndex()
	if err != nil {
		return err
	}
	last, err := storage.LastIndex()
	if err != nil {
		return err
	}
	commit := hs.Commit
	if commit > last {
		commit = last
	}
	if commit < first {
		return nil
	}
	entries, err := storage.Entries(first, commit+1, ^uint64(0))
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Type != raftpb.EntryNormal || len(entry.Data) == 0 {
			continue
		}
		cmd, err := DecodeCommand(entry.Data)
		if err != nil {
			return err
		}
		if err := cmd.Validate(partitionCount); err != nil {
			return err
		}
		if err := sm.ApplyCommand(ctx, ApplyContext{RaftIndex: entry.Index, RaftTerm: entry.Term}, cmd); err != nil {
			return err
		}
	}
	return nil
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

func (g *Group) Campaign(ctx context.Context) error { return g.node.Campaign(ctx) }

func (g *Group) Propose(ctx context.Context, cmd RaftCommand) (ProposalResult, error) {
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
	g.mu.Unlock()
	if err := g.node.Propose(ctx, data); err != nil {
		g.forgetWaiter(cmd.CommandID, proposalOutcome{err: err})
		return ProposalResult{}, err
	}
	select {
	case out := <-ch:
		return out.result, out.err
	case <-ctx.Done():
		g.forgetWaiter(cmd.CommandID, proposalOutcome{err: ctx.Err()})
		return ProposalResult{}, ctx.Err()
	}
}

func (g *Group) run() {
	defer close(g.done)
	defer g.node.Stop()
	for {
		select {
		case <-g.ctx.Done():
			return
		case rd := <-g.node.Ready():
			if !raft.IsEmptySnap(rd.Snapshot) {
				if err := g.storage.ApplySnapshot(rd.Snapshot); err != nil {
					g.failWaiters(fmt.Errorf("apply raft snapshot: %w", err))
					return
				}
			}
			if !raft.IsEmptyHardState(rd.HardState) {
				if err := g.storage.SetHardState(rd.HardState); err != nil {
					g.failWaiters(fmt.Errorf("persist raft hard state: %w", err))
					return
				}
				g.mu.Lock()
				if rd.HardState.Term != 0 {
					g.term = rd.HardState.Term
				}
				if rd.HardState.Commit != 0 {
					g.commitIndex = rd.HardState.Commit
				}
				g.mu.Unlock()
			}
			if len(rd.Entries) > 0 {
				if err := g.storage.Append(rd.Entries); err != nil {
					g.failWaiters(fmt.Errorf("persist raft entries: %w", err))
					return
				}
			}
			g.transport.Send(g.ctx, g.id, g.nodeID, rd.Messages)
			for _, entry := range rd.CommittedEntries {
				g.applyEntry(entry)
			}
			if rd.SoftState != nil {
				g.mu.Lock()
				g.leader = NodeID(rd.SoftState.Lead)
				g.mu.Unlock()
			}
			g.node.Advance()
		}
	}
}

func (g *Group) applyEntry(entry raftpb.Entry) {
	g.mu.Lock()
	if entry.Index > g.appliedIndex {
		g.appliedIndex = entry.Index
	}
	g.mu.Unlock()
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
		return
	}
	if entry.Type != raftpb.EntryNormal || len(entry.Data) == 0 {
		return
	}
	cmd, err := DecodeCommand(entry.Data)
	if err == nil {
		err = g.sm.ApplyCommand(g.ctx, ApplyContext{RaftIndex: entry.Index, RaftTerm: entry.Term}, cmd)
	}
	g.completeWaiter(cmd.CommandID, proposalOutcome{result: ProposalResult{Index: entry.Index, Term: entry.Term}, err: err})
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
	g.mu.Unlock()
	for _, ch := range waiters {
		ch <- proposalOutcome{err: err}
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
