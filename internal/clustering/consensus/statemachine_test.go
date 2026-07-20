package consensus

import (
	"context"
	"testing"
	"time"

	"github.com/myceldb/mycel/internal/wal"
)

func TestWALApplierStateMachineAppliesThroughRegistry(t *testing.T) {
	registry := wal.NewRegistry()
	var got wal.Record
	if err := registry.Register(wal.RecordType("space.create"), wal.ApplierFunc(func(ctx context.Context, rec wal.Record) error {
		got = rec
		return nil
	})); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	cmd := NewCommand(CommandScopeSystem, wal.RecordType("space.create"), []byte(`{"name":"main"}`), "cmd-1")
	sm := WALApplierStateMachine{Registry: registry, PartitionCount: 64, Now: func() time.Time { return time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC) }}
	if err := sm.ApplyCommand(context.Background(), ApplyContext{RaftIndex: 42, RaftTerm: 7}, cmd); err != nil {
		t.Fatalf("ApplyCommand() error = %v", err)
	}
	if got.LSN != wal.LSN(42) || got.Type != cmd.RecordType || got.SchemaVersion != cmd.SchemaVersion || got.Encoding != cmd.Encoding || string(got.Payload) != string(cmd.Payload) {
		t.Fatalf("unexpected applied record: %+v", got)
	}
	if !got.Timestamp.Equal(time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("unexpected timestamp: %s", got.Timestamp)
	}
}

func TestWALApplierStateMachineRejectsInvalidCommand(t *testing.T) {
	registry := wal.NewRegistry()
	cmd := NewCommand(CommandScopeSystem, wal.RecordType("space.create"), []byte(`{}`), "cmd-1")
	cmd.CommandID = ""
	sm := WALApplierStateMachine{Registry: registry, PartitionCount: 64}
	if err := sm.ApplyCommand(context.Background(), ApplyContext{RaftIndex: 1}, cmd); err == nil {
		t.Fatal("expected invalid command to fail")
	}
}

func TestMemoryStateMachineRecordsCommands(t *testing.T) {
	sm := &MemoryStateMachine{}
	cmd := NewCommand(CommandScopeSystem, wal.RecordType("system.op"), []byte(`{}`), "cmd-1")
	if err := sm.ApplyCommand(context.Background(), ApplyContext{RaftIndex: 3, RaftTerm: 2}, cmd); err != nil {
		t.Fatalf("ApplyCommand() error = %v", err)
	}
	if sm.AppliedCount() != 1 {
		t.Fatalf("AppliedCount()=%d want 1", sm.AppliedCount())
	}
	if sm.Contexts[0].RaftIndex != 3 || sm.Contexts[0].RaftTerm != 2 || sm.Applied[0].CommandID != "cmd-1" {
		t.Fatalf("unexpected memory state: %+v %+v", sm.Contexts, sm.Applied)
	}
}
