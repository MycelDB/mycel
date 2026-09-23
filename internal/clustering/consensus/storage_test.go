package consensus

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/myceldb/mycel/internal/encryption"

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

func TestPersistentStorageRecoversConfState(t *testing.T) {
	dir := t.TempDir()
	store, err := NewPersistentStorage(dir)
	if err != nil {
		t.Fatalf("NewPersistentStorage() error = %v", err)
	}
	if err := store.SetConfState(raftpb.ConfState{Voters: []uint64{1, 2, 3}}); err != nil {
		t.Fatalf("SetConfState() error = %v", err)
	}
	reopened, err := NewPersistentStorage(dir)
	if err != nil {
		t.Fatalf("reopen NewPersistentStorage() error = %v", err)
	}
	_, cs, err := reopened.InitialState()
	if err != nil {
		t.Fatalf("InitialState() error = %v", err)
	}
	if len(cs.Voters) != 3 || cs.Voters[0] != 1 || cs.Voters[1] != 2 || cs.Voters[2] != 3 {
		t.Fatalf("unexpected conf state: %+v", cs)
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

func TestPersistentStorageEncryptedFilesReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	enc, err := encryption.NewService(ctx, encryption.Config{AtRest: encryption.ModeEnabled, KEKProvider: encryption.ProviderStaticEnv, StaticKeyB64: "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	store, err := NewPersistentStorageWithOptions(PersistentStorageOptions{Dir: dir, GroupID: "test-group", Encryption: enc})
	if err != nil {
		t.Fatalf("NewPersistentStorageWithOptions() error = %v", err)
	}
	secret := []byte("raft-secret-plaintext")
	if err := store.Append([]raftpb.Entry{{Term: 1, Index: 1, Type: raftpb.EntryNormal, Data: secret}}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "entries.log"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, secret) {
		t.Fatal("raft entries log contains plaintext entry data")
	}
	snapshotSecret := []byte("raft-snapshot-secret-plaintext")
	if _, err := store.CreateSnapshot(1, &raftpb.ConfState{Voters: []uint64{1}}, snapshotSecret); err != nil {
		t.Fatalf("CreateSnapshot() error = %v", err)
	}
	snapshotData, err := os.ReadFile(filepath.Join(dir, "snapshot.pb"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(snapshotData, snapshotSecret) {
		t.Fatal("raft snapshot file contains plaintext snapshot data")
	}
	reopened, err := NewPersistentStorageWithOptions(PersistentStorageOptions{Dir: dir, GroupID: "test-group", Encryption: enc})
	if err != nil {
		t.Fatalf("reopen encrypted storage: %v", err)
	}
	snap, err := reopened.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if !bytes.Equal(snap.Data, snapshotSecret) {
		t.Fatalf("snapshot data=%q", snap.Data)
	}
}

func TestPersistentStorageUsesAppendOnlyEntryLog(t *testing.T) {
	dir := t.TempDir()
	store, err := NewPersistentStorage(dir)
	if err != nil {
		t.Fatalf("NewPersistentStorage() error = %v", err)
	}
	first := make([]raftpb.Entry, 0, 50)
	for i := 1; i <= 50; i++ {
		first = append(first, raftpb.Entry{Term: 1, Index: uint64(i), Type: raftpb.EntryNormal, Data: bytes.Repeat([]byte("x"), 128)})
	}
	if err := store.Append(first); err != nil {
		t.Fatalf("Append(first) error = %v", err)
	}
	infoBefore, err := os.Stat(filepath.Join(dir, "entries.log"))
	if err != nil {
		t.Fatalf("stat entries.log: %v", err)
	}
	if err := store.Append([]raftpb.Entry{{Term: 1, Index: 51, Type: raftpb.EntryNormal, Data: []byte("tail")}}); err != nil {
		t.Fatalf("Append(tail) error = %v", err)
	}
	infoAfter, err := os.Stat(filepath.Join(dir, "entries.log"))
	if err != nil {
		t.Fatalf("stat entries.log after append: %v", err)
	}
	if grew := infoAfter.Size() - infoBefore.Size(); grew <= 0 || grew >= infoBefore.Size()/2 {
		t.Fatalf("entries.log growth = %d, want small append-only growth below half prior size %d", grew, infoBefore.Size()/2)
	}
	reopened, err := NewPersistentStorage(dir)
	if err != nil {
		t.Fatalf("reopen storage: %v", err)
	}
	last, err := reopened.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex() error = %v", err)
	}
	if last != 51 {
		t.Fatalf("LastIndex()=%d want 51", last)
	}
}

func TestPersistentStorageMigratesLegacyEntriesFileToAppendLog(t *testing.T) {
	dir := t.TempDir()
	store, err := NewPersistentStorage(dir)
	if err != nil {
		t.Fatalf("NewPersistentStorage() error = %v", err)
	}
	legacyEntries := []raftpb.Entry{{Term: 1, Index: 1, Type: raftpb.EntryNormal, Data: []byte("legacy")}}
	if err := store.writeEntriesAtomic("entries.pb", legacyEntries); err != nil {
		t.Fatalf("write legacy entries: %v", err)
	}
	reopened, err := NewPersistentStorage(dir)
	if err != nil {
		t.Fatalf("reopen legacy storage: %v", err)
	}
	if err := reopened.Append([]raftpb.Entry{{Term: 1, Index: 2, Type: raftpb.EntryNormal, Data: []byte("new")}}); err != nil {
		t.Fatalf("Append(new) error = %v", err)
	}
	migrated, err := NewPersistentStorage(dir)
	if err != nil {
		t.Fatalf("reopen migrated storage: %v", err)
	}
	got, err := migrated.Entries(1, 3, ^uint64(0))
	if err != nil {
		t.Fatalf("Entries() error = %v", err)
	}
	if len(got) != 2 || !bytes.Equal(got[0].Data, []byte("legacy")) || !bytes.Equal(got[1].Data, []byte("new")) {
		t.Fatalf("unexpected migrated entries: %+v", got)
	}
}
