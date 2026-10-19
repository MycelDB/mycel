package graphstorage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/graph/model"
)

func TestLocalStoreIdempotencyReplaysAtomicallyAndSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := Open(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	committedAt := time.Now().UTC().Truncate(time.Microsecond)
	record := IdempotencyRecord{
		Scope:       "principal-1",
		Key:         "capture-1",
		Digest:      "digest-1",
		Metadata:    json.RawMessage(`{"commit_id":"commit-1"}`),
		CommittedAt: committedAt,
		ExpiresAt:   committedAt.Add(30 * 24 * time.Hour),
	}
	firstNode := graph.Node{ID: graph.NodeID(uuid.New()), Content: "first"}
	first, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	first.SetIdempotency(record)
	if err := first.PutNode(firstNode); err != nil {
		t.Fatal(err)
	}
	firstInfo, err := first.CommitWithInfo()
	if err != nil {
		t.Fatal(err)
	}
	if firstInfo.IdempotentReplay || firstInfo.NextRevision != 1 || firstInfo.Idempotency == nil {
		t.Fatalf("first commit info = %#v", firstInfo)
	}
	if firstInfo.Idempotency.StorageTxnID != firstInfo.TxnID || firstInfo.Idempotency.CommittedRevision != 1 {
		t.Fatalf("first idempotency record = %#v", firstInfo.Idempotency)
	}

	secondNode := graph.Node{ID: graph.NodeID(uuid.New()), Content: "must not be written"}
	replay, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	replay.SetIdempotency(record)
	if err := replay.PutNode(secondNode); err != nil {
		t.Fatal(err)
	}
	replayInfo, err := replay.CommitWithInfo()
	if err != nil {
		t.Fatal(err)
	}
	if !replayInfo.IdempotentReplay || replayInfo.NextRevision != firstInfo.NextRevision || replayInfo.TxnID != firstInfo.TxnID {
		t.Fatalf("replay commit info = %#v, first = %#v", replayInfo, firstInfo)
	}
	if store.Revision() != 1 {
		t.Fatalf("revision after replay = %d, want 1", store.Revision())
	}
	if _, err := store.GetNode(ctx, secondNode.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("replay node lookup error = %v, want ErrNotFound", err)
	}

	conflict, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	conflictingRecord := record
	conflictingRecord.Digest = "different-digest"
	conflict.SetIdempotency(conflictingRecord)
	if _, err := conflict.CommitWithInfo(); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflicting commit error = %v, want ErrIdempotencyConflict", err)
	}
	if err := store.MarkIdempotencyDelivered(ctx, record.Scope, record.Key, record.Digest); err != nil {
		t.Fatal(err)
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	loaded, ok, err := store.GetIdempotencyRecord(ctx, record.Scope, record.Key)
	if err != nil || !ok {
		t.Fatalf("GetIdempotencyRecord() = %#v, %v, %v", loaded, ok, err)
	}
	if loaded.Digest != record.Digest || loaded.CommittedRevision != 1 || string(loaded.Metadata) != string(record.Metadata) || !loaded.Delivered {
		t.Fatalf("loaded idempotency record = %#v", loaded)
	}
}

func TestLocalStoreIdempotencyCapacityFailsBeforeGraphWrites(t *testing.T) {
	ctx := context.Background()
	store, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Now().UTC()
	store.mu.Lock()
	for i := 0; i < MaxIdempotencyRecordsPerStore; i++ {
		record := IdempotencyRecord{Scope: "principal", Key: fmt.Sprintf("key-%d", i), Digest: "digest", CommittedAt: now, ExpiresAt: now.Add(time.Hour)}
		store.idempotencyRecords[idempotencyRecordIdentity(record.Scope, record.Key)] = record
	}
	store.mu.Unlock()
	node := graph.Node{ID: graph.NodeID(uuid.New()), Content: "must not commit"}
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tx.SetIdempotency(IdempotencyRecord{Scope: "principal", Key: "overflow", Digest: "digest", Metadata: json.RawMessage(`{}`), CommittedAt: now, ExpiresAt: now.Add(time.Hour)})
	if err := tx.PutNode(node); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.CommitWithInfo(); !errors.Is(err, ErrIdempotencyCapacity) {
		t.Fatalf("capacity commit error = %v, want ErrIdempotencyCapacity", err)
	}
	if store.Revision() != 0 {
		t.Fatalf("revision = %d, want 0", store.Revision())
	}
	if _, err := store.GetNode(ctx, node.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("node lookup error = %v, want ErrNotFound", err)
	}
}

func TestLocalStoreIdempotencyExpiryIsEvicted(t *testing.T) {
	ctx := context.Background()
	store, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Now().UTC()
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tx.SetIdempotency(IdempotencyRecord{Scope: "principal", Key: "short", Digest: "digest", Metadata: json.RawMessage(`{}`), CommittedAt: now, ExpiresAt: now.Add(20 * time.Millisecond)})
	if _, err := tx.CommitWithInfo(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if _, found, err := store.GetIdempotencyRecord(ctx, "principal", "short"); err != nil || found {
		t.Fatalf("expired lookup found=%v err=%v", found, err)
	}
	if got := store.IdempotencyEvictions(); got != 1 {
		t.Fatalf("IdempotencyEvictions() = %d, want 1", got)
	}
}
