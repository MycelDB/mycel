package consensus

import (
	"bytes"
	"testing"

	raftpb "go.etcd.io/raft/v3/raftpb"
)

func TestPersistentStorageRecoversHardStateAndEntries(t *testing.T) {
	dir := t.TempDir()
	store, err := NewPersistentStorage(dir)
	if err != nil {
		t.Fatalf("NewPersistentStorage() error = %v", err)
	}
	if err := store.SetHardState(raftpb.HardState{Term: 3, Vote: 2, Commit: 1}); err != nil {
		t.Fatalf("SetHardState() error = %v", err)
	}
	entries := []raftpb.Entry{{Term: 1, Index: 1, Type: raftpb.EntryNormal, Data: []byte("a")}, {Term: 1, Index: 2, Type: raftpb.EntryNormal, Data: []byte("b")}}
	if err := store.Append(entries); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	reopened, err := NewPersistentStorage(dir)
	if err != nil {
		t.Fatalf("reopen NewPersistentStorage() error = %v", err)
	}
	hs, _, err := reopened.InitialState()
	if err != nil {
		t.Fatalf("InitialState() error = %v", err)
	}
	if hs.Term != 3 || hs.Vote != 2 || hs.Commit != 1 {
		t.Fatalf("unexpected hard state: %+v", hs)
	}
	last, err := reopened.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex() error = %v", err)
	}
	if last != 2 {
		t.Fatalf("LastIndex()=%d want 2", last)
	}
	got, err := reopened.Entries(1, 3, ^uint64(0))
	if err != nil {
		t.Fatalf("Entries() error = %v", err)
	}
	if len(got) != 2 || !bytes.Equal(got[0].Data, []byte("a")) || !bytes.Equal(got[1].Data, []byte("b")) {
		t.Fatalf("unexpected entries: %+v", got)
	}
}

func TestPersistentStorageRecoversSnapshot(t *testing.T) {
	dir := t.TempDir()
	store, err := NewPersistentStorage(dir)
	if err != nil {
		t.Fatalf("NewPersistentStorage() error = %v", err)
	}
	entries := []raftpb.Entry{{Term: 1, Index: 1, Type: raftpb.EntryNormal, Data: []byte("a")}, {Term: 1, Index: 2, Type: raftpb.EntryNormal, Data: []byte("b")}}
	if err := store.Append(entries); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	snap, err := store.CreateSnapshot(2, &raftpb.ConfState{Voters: []uint64{1, 2, 3}}, []byte("snapshot-state"))
	if err != nil {
		t.Fatalf("CreateSnapshot() error = %v", err)
	}
	if err := store.Compact(2); err != nil {
		t.Fatalf("Compact() error = %v", err)
	}

	reopened, err := NewPersistentStorage(dir)
	if err != nil {
		t.Fatalf("reopen NewPersistentStorage() error = %v", err)
	}
	got, err := reopened.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if got.Metadata.Index != snap.Metadata.Index || got.Metadata.Term != snap.Metadata.Term || !bytes.Equal(got.Data, []byte("snapshot-state")) {
		t.Fatalf("unexpected snapshot: %+v", got)
	}
	first, err := reopened.FirstIndex()
	if err != nil {
		t.Fatalf("FirstIndex() error = %v", err)
	}
	if first != 3 {
		t.Fatalf("FirstIndex()=%d want 3", first)
	}
}

func TestPersistentStorageApplySnapshotPersists(t *testing.T) {
	dir := t.TempDir()
	store, err := NewPersistentStorage(dir)
	if err != nil {
		t.Fatalf("NewPersistentStorage() error = %v", err)
	}
	snap := raftpb.Snapshot{Data: []byte("installed"), Metadata: raftpb.SnapshotMetadata{Index: 7, Term: 4, ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}}}}
	if err := store.ApplySnapshot(snap); err != nil {
		t.Fatalf("ApplySnapshot() error = %v", err)
	}
	reopened, err := NewPersistentStorage(dir)
	if err != nil {
		t.Fatalf("reopen NewPersistentStorage() error = %v", err)
	}
	got, err := reopened.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if got.Metadata.Index != 7 || got.Metadata.Term != 4 || !bytes.Equal(got.Data, []byte("installed")) {
		t.Fatalf("unexpected snapshot: %+v", got)
	}
}
