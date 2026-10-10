package principal

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/myceldb/mycel/internal/clustering/consensus"
)

func TestIssue172ReplayDuplicatePrincipalAfterSnapshotIsStartupSafe(t *testing.T) {
	m, ctx := newTestModule(t)
	snapshot, err := (RaftStateMachine{Module: m}).Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}

	restored, _ := newTestModule(t)
	if err := (RaftStateMachine{Module: restored}).RestoreSnapshot(snapshot); err != nil {
		t.Fatalf("RestoreSnapshot() error = %v", err)
	}
	cmd := duplicateAdminPrincipalCommand(t, restored, "issue172-duplicate-admin")
	if err := (RaftStateMachine{Module: restored}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 42, RaftTerm: 7, Replay: true}, cmd); err != nil {
		t.Fatalf("replay duplicate principal after snapshot error = %v, want startup-safe nil", err)
	}
	if _, ok := restored.raftAppliedCommandRecord(cmd.CommandID); !ok {
		t.Fatalf("replay duplicate principal did not record command outcome")
	}
}

func TestIssue172LiveDuplicatePrincipalStillReturnsDuplicate(t *testing.T) {
	m, ctx := newTestModule(t)
	cmd := duplicateAdminPrincipalCommand(t, m, "issue172-live-duplicate-admin")
	if err := (RaftStateMachine{Module: m}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 10, RaftTerm: 1}, cmd); !errors.Is(err, ErrDuplicatePrincipal) {
		t.Fatalf("live duplicate principal error = %v, want ErrDuplicatePrincipal", err)
	}
	if rec, ok := m.raftAppliedCommandRecord(cmd.CommandID); !ok || rec.Status != raftCommandStatusApplicationError || rec.ErrorCode != raftCommandErrorDuplicatePrincipal {
		t.Fatalf("live duplicate outcome record = %#v ok=%v, want duplicate application error", rec, ok)
	}
}

func TestIssue172CommandIDReusedWithDifferentHashFails(t *testing.T) {
	m, ctx := newTestModule(t)
	cmd1 := principalPutCommand(t, m, Principal{ID: "principal-hash-1", Username: "hash-one", Kind: PrincipalKindHuman, State: PrincipalStateActive, LoginEnabled: true, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}, "issue172-hash")
	if err := (RaftStateMachine{Module: m}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 11, RaftTerm: 1}, cmd1); err != nil {
		t.Fatalf("first ApplyCommand() error = %v", err)
	}
	cmd2 := principalPutCommand(t, m, Principal{ID: "principal-hash-2", Username: "hash-two", Kind: PrincipalKindHuman, State: PrincipalStateActive, LoginEnabled: true, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}, "issue172-hash")
	if err := (RaftStateMachine{Module: m}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 12, RaftTerm: 1, Replay: true}, cmd2); err == nil || !strings.Contains(err.Error(), "different command hash") {
		t.Fatalf("second ApplyCommand() error = %v, want command hash mismatch", err)
	}
}

func TestIssue172SnapshotPreservesStructuredAppliedCommandRecords(t *testing.T) {
	m, ctx := newTestModule(t)
	cmd := duplicateAdminPrincipalCommand(t, m, "issue172-snapshot-duplicate-admin")
	if err := (RaftStateMachine{Module: m}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 13, RaftTerm: 2}, cmd); !errors.Is(err, ErrDuplicatePrincipal) {
		t.Fatalf("live duplicate principal error = %v, want ErrDuplicatePrincipal", err)
	}
	snapshot, err := (RaftStateMachine{Module: m}).Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	restored, _ := newTestModule(t)
	if err := (RaftStateMachine{Module: restored}).RestoreSnapshot(snapshot); err != nil {
		t.Fatalf("RestoreSnapshot() error = %v", err)
	}
	rec, ok := restored.raftAppliedCommandRecord(cmd.CommandID)
	if !ok {
		t.Fatalf("restored snapshot missing applied command record")
	}
	if rec.Status != raftCommandStatusApplicationError || rec.ErrorCode != raftCommandErrorDuplicatePrincipal || rec.RaftIndex != 13 || rec.RaftTerm != 2 || rec.CommandHash == "" {
		t.Fatalf("restored applied command record = %#v", rec)
	}
	if err := (RaftStateMachine{Module: restored}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 14, RaftTerm: 2, Replay: true}, cmd); err != nil {
		t.Fatalf("replay of snapshot-recorded duplicate error = %v, want nil", err)
	}
}

func duplicateAdminPrincipalCommand(t *testing.T, m *Module, commandID string) consensus.RaftCommand {
	t.Helper()
	return principalPutCommand(t, m, Principal{ID: "different-admin-id-" + commandID, Username: "admin", Kind: PrincipalKindHuman, State: PrincipalStateActive, LoginEnabled: true, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}, commandID)
}

func principalPutCommand(t *testing.T, m *Module, p Principal, commandID string) consensus.RaftCommand {
	t.Helper()
	cmd, err := m.buildPrincipalPutRaftCommand(p, commandID)
	if err != nil {
		t.Fatalf("buildPrincipalPutRaftCommand() error = %v", err)
	}
	return cmd
}
