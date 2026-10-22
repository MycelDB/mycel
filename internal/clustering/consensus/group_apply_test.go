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

func TestGroupNonfatalApplicationErrorAdvancesAppliedIndex(t *testing.T) {
	stateMachine := &switchApplyStateMachine{err: errors.New("conditional command rejected")}
	group := &Group{ctx: context.Background(), sm: stateMachine, waiters: map[string]chan proposalOutcome{}, readWaiters: map[string]chan readIndexOutcome{}, proposalTimings: map[string]*ProposalTiming{}}
	command := NewCommand(CommandScopeSystem, wal.RecordType("test.mutation.v1"), []byte(`{"ok":true}`), "nonfatal-apply-1")
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

func TestCommittedReplayStopsOnlyForFatalApplyErrors(t *testing.T) {
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

	nonfatal := &switchApplyStateMachine{err: errors.New("conditional command rejected")}
	applied, err := restoreSnapshotAndReplayCommittedEntries(context.Background(), newStorage(), nonfatal, 1)
	if err != nil || applied != 1 {
		t.Fatalf("nonfatal replay applied=%d err=%v", applied, err)
	}

	fatal := &switchApplyStateMachine{err: NewFatalApplyError(errors.New("persistence failed"))}
	applied, err = restoreSnapshotAndReplayCommittedEntries(context.Background(), newStorage(), fatal, 1)
	if !IsFatalApplyError(err) || applied != 0 {
		t.Fatalf("fatal replay applied=%d err=%v", applied, err)
	}
	fatal.err = nil
	applied, err = restoreSnapshotAndReplayCommittedEntries(context.Background(), newStorage(), fatal, 1)
	if err != nil || applied != 1 || fatal.calls != 2 {
		t.Fatalf("successful replay applied=%d calls=%d err=%v", applied, fatal.calls, err)
	}
}
