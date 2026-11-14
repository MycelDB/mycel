package consensus

import (
	"context"
	"errors"
	"testing"

	"github.com/myceldb/mycel/internal/wal"
	"go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

type switchApplyStateMachine struct {
	err   error
	calls int
}

type failingAppendStorage struct {
	*raft.MemoryStorage
	err error
}

func (s failingAppendStorage) Append([]raftpb.Entry) error { return s.err }

func (s *switchApplyStateMachine) ApplyCommand(context.Context, ApplyContext, RaftCommand) error {
	s.calls++
	return s.err
}

func TestGroupFatalApplyDoesNotAdvanceAndCanReplay(t *testing.T) {
	persistenceErr := errors.New("injected persistence failure")
	stateMachine := &switchApplyStateMachine{err: NewFatalApplyError(persistenceErr)}
	group := &Group{ctx: context.Background(), sm: stateMachine, waiters: map[string]chan proposalOutcome{}, readWaiters: map[string]chan readIndexOutcome{}, proposalTimings: map[string]*ProposalTiming{}}
	command := NewCommand(CommandScopeSystem, wal.RecordType("test.mutation.v1"), []byte(`{"ok":true}`), "fatal-apply-1")
	encoded, err := EncodeCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	entry := raftpb.Entry{Type: raftpb.EntryNormal, Index: 7, Term: 3, Data: encoded}
	if err := group.applyEntry(entry); !IsFatalApplyError(err) {
		t.Fatalf("applyEntry() error=%v, want fatal apply error", err)
	}
	if group.appliedIndex != 0 {
		t.Fatalf("appliedIndex=%d, want 0 after fatal apply", group.appliedIndex)
	}
	stateMachine.err = nil
	if err := group.applyEntry(entry); err != nil {
		t.Fatalf("replay applyEntry() error=%v", err)
	}
	if group.appliedIndex != entry.Index || stateMachine.calls != 2 {
		t.Fatalf("replayed state: appliedIndex=%d calls=%d", group.appliedIndex, stateMachine.calls)
	}
}

func TestGroupInfrastructureApplicationErrorDoesNotAdvanceAppliedIndex(t *testing.T) {
	stateMachine := &switchApplyStateMachine{err: errors.New("injected persistence failure")}
	group := &Group{ctx: context.Background(), sm: stateMachine, waiters: map[string]chan proposalOutcome{}, readWaiters: map[string]chan readIndexOutcome{}, proposalTimings: map[string]*ProposalTiming{}}
	command := NewCommand(CommandScopeSystem, wal.RecordType("test.mutation.v1"), []byte(`{"ok":true}`), "nonfatal-apply-1")
	encoded, err := EncodeCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	entry := raftpb.Entry{Type: raftpb.EntryNormal, Index: 8, Term: 3, Data: encoded}
	if err := group.applyEntry(entry); !IsFatalApplyError(err) {
		t.Fatalf("applyEntry() error=%v, want fatal apply error", err)
	}
	if group.appliedIndex != 0 {
		t.Fatalf("appliedIndex=%d, want 0 after infrastructure failure", group.appliedIndex)
	}
}

func TestGroupMalformedCommandDoesNotAdvanceAppliedIndex(t *testing.T) {
	group := &Group{ctx: context.Background(), sm: &switchApplyStateMachine{}, waiters: map[string]chan proposalOutcome{}, readWaiters: map[string]chan readIndexOutcome{}, proposalTimings: map[string]*ProposalTiming{}}
	entry := raftpb.Entry{Type: raftpb.EntryNormal, Index: 9, Term: 3, Data: []byte("not-a-raft-command")}
	if err := group.applyEntry(entry); !IsFatalApplyError(err) {
		t.Fatalf("applyEntry() error=%v, want fatal decode error", err)
	}
	if group.appliedIndex != 0 {
		t.Fatalf("appliedIndex=%d, want 0 after malformed command", group.appliedIndex)
	}
}

func TestGroupCommandRejectionAdvancesAppliedIndex(t *testing.T) {
	stateMachine := &switchApplyStateMachine{err: NewCommandRejection(errors.New("conditional command rejected"))}
	group := &Group{ctx: context.Background(), sm: stateMachine, waiters: map[string]chan proposalOutcome{}, readWaiters: map[string]chan readIndexOutcome{}, proposalTimings: map[string]*ProposalTiming{}}
	command := NewCommand(CommandScopeSystem, wal.RecordType("test.mutation.v1"), []byte(`{"ok":true}`), "rejected-apply-1")
	encoded, err := EncodeCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	entry := raftpb.Entry{Type: raftpb.EntryNormal, Index: 8, Term: 3, Data: encoded}
	if err := group.applyEntry(entry); err != nil {
		t.Fatalf("applyEntry() error=%v", err)
	}
	if group.appliedIndex != entry.Index {
		t.Fatalf("appliedIndex=%d, want %d", group.appliedIndex, entry.Index)
	}
}

func TestCommittedReplayDoesNotSkipEntriesAfterFailedApply(t *testing.T) {
	first := NewCommand(CommandScopeSystem, wal.RecordType("test.mutation.v1"), []byte(`{"entry":1}`), "replay-apply-failed")
	firstEncoded, err := EncodeCommand(first)
	if err != nil {
		t.Fatal(err)
	}
	second := NewCommand(CommandScopeSystem, wal.RecordType("test.mutation.v1"), []byte(`{"entry":2}`), "replay-apply-later")
	secondEncoded, err := EncodeCommand(second)
	if err != nil {
		t.Fatal(err)
	}
	storage := raft.NewMemoryStorage()
	if err := storage.Append([]raftpb.Entry{
		{Type: raftpb.EntryNormal, Index: 1, Term: 1, Data: firstEncoded},
		{Type: raftpb.EntryNormal, Index: 2, Term: 1, Data: secondEncoded},
	}); err != nil {
		t.Fatal(err)
	}
	if err := storage.SetHardState(raftpb.HardState{Term: 1, Commit: 2}); err != nil {
		t.Fatal(err)
	}
	stateMachine := &switchApplyStateMachine{err: errors.New("first entry unavailable")}
	applied, err := restoreSnapshotAndReplayCommittedEntries(context.Background(), storage, stateMachine, 1)
	if !IsFatalApplyError(err) || applied != 0 || stateMachine.calls != 1 {
		t.Fatalf("failed replay applied=%d calls=%d err=%v, want stop before later entry", applied, stateMachine.calls, err)
	}
}

func TestProcessReadyDoesNotPersistHardStateWhenEntryAppendFails(t *testing.T) {
	appendErr := errors.New("raft log append unavailable")
	storage := failingAppendStorage{MemoryStorage: raft.NewMemoryStorage(), err: appendErr}
	group := &Group{id: SystemGroupID, nodeID: 1, leader: 1, ctx: context.Background(), sm: &switchApplyStateMachine{}, storage: storage, transport: newMemoryTransport(), waiters: map[string]chan proposalOutcome{}, readWaiters: map[string]chan readIndexOutcome{}, proposalTimings: map[string]*ProposalTiming{}}
	entry := raftpb.Entry{Type: raftpb.EntryNormal, Index: 4, Term: 2, Data: []byte("entry")}
	if group.processReady(raft.Ready{HardState: raftpb.HardState{Term: 2, Commit: 4}, Entries: []raftpb.Entry{entry}}) {
		t.Fatal("processReady() succeeded after log append failure")
	}
	hs, _, err := storage.InitialState()
	if err != nil {
		t.Fatal(err)
	}
	if hs.Term != 0 || hs.Commit != 0 || group.appliedIndex != 0 {
		t.Fatalf("partial durable state hardState=%+v applied=%d", hs, group.appliedIndex)
	}
}

func TestProcessReadyStopsAfterFailedApplyWithoutAdvancing(t *testing.T) {
	command := NewCommand(CommandScopeSystem, wal.RecordType("test.mutation.v1"), []byte(`{"ok":true}`), "process-ready-failure")
	encoded, err := EncodeCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	stateMachine := &switchApplyStateMachine{err: errors.New("graph journal unavailable")}
	group := &Group{id: SystemGroupID, nodeID: 1, leader: 1, ctx: context.Background(), sm: stateMachine, storage: raft.NewMemoryStorage(), transport: newMemoryTransport(), waiters: map[string]chan proposalOutcome{}, readWaiters: map[string]chan readIndexOutcome{}, proposalTimings: map[string]*ProposalTiming{}}
	entry := raftpb.Entry{Type: raftpb.EntryNormal, Index: 4, Term: 2, Data: encoded}
	if group.processReady(raft.Ready{CommittedEntries: []raftpb.Entry{entry}}) {
		t.Fatal("processReady() succeeded after state-machine failure")
	}
	if group.appliedIndex != 0 || group.Leader() != 0 || group.ApplyFailure() == nil {
		t.Fatalf("failed processReady state applied=%d leader=%d failure=%v", group.appliedIndex, group.Leader(), group.ApplyFailure())
	}
}

func TestFailedGroupReportsNoLeaderAndRejectsNewProposals(t *testing.T) {
	failure := errors.New("journal storage unavailable")
	group := &Group{id: SystemGroupID, nodeID: 1, leader: 1, applyFailure: nil, waiters: map[string]chan proposalOutcome{}, readWaiters: map[string]chan readIndexOutcome{}, proposalTimings: map[string]*ProposalTiming{}}
	group.fail(failure)
	if group.Leader() != 0 || !errors.Is(group.ApplyFailure(), failure) {
		t.Fatalf("failed group leader=%d failure=%v", group.Leader(), group.ApplyFailure())
	}
	cmd := NewCommand(CommandScopeSystem, wal.RecordType("test.mutation.v1"), []byte(`{"ok":true}`), "proposal-after-failure")
	if _, err := group.Propose(context.Background(), cmd); !errors.Is(err, failure) {
		t.Fatalf("proposal after failed apply error=%v, want %v", err, failure)
	}
}

func TestCommittedReplayStopsForInfrastructureApplyErrors(t *testing.T) {
	command := NewCommand(CommandScopeSystem, wal.RecordType("test.mutation.v1"), []byte(`{"ok":true}`), "replay-apply-1")
	encoded, err := EncodeCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	newStorage := func() *raft.MemoryStorage {
		storage := raft.NewMemoryStorage()
		if err := storage.Append([]raftpb.Entry{{Type: raftpb.EntryNormal, Index: 1, Term: 1, Data: encoded}}); err != nil {
			t.Fatal(err)
		}
		if err := storage.SetHardState(raftpb.HardState{Term: 1, Commit: 1}); err != nil {
			t.Fatal(err)
		}
		return storage
	}

	nonfatal := &switchApplyStateMachine{err: errors.New("persistence failed")}
	applied, err := restoreSnapshotAndReplayCommittedEntries(context.Background(), newStorage(), nonfatal, 1)
	if !IsFatalApplyError(err) || applied != 0 {
		t.Fatalf("infrastructure replay applied=%d err=%v", applied, err)
	}

	fatal := &switchApplyStateMachine{err: NewCommandRejection(errors.New("conditional command rejected"))}
	applied, err = restoreSnapshotAndReplayCommittedEntries(context.Background(), newStorage(), fatal, 1)
	if err != nil || applied != 1 {
		t.Fatalf("command rejection replay applied=%d err=%v", applied, err)
	}
	fatal.err = nil
	applied, err = restoreSnapshotAndReplayCommittedEntries(context.Background(), newStorage(), fatal, 1)
	if err != nil || applied != 1 || fatal.calls != 2 {
		t.Fatalf("successful replay applied=%d calls=%d err=%v", applied, fatal.calls, err)
	}
}
