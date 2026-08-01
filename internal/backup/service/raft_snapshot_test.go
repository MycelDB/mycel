package service

import (
	"context"
	"log/slog"
	"testing"
	"time"

	backupcore "github.com/myceldb/mycel/internal/backup"
	config "github.com/myceldb/mycel/internal/runtime/runtimetest"
	daemonruntime "github.com/myceldb/mycel/internal/runtime/runtimetest"
)

func TestBackupRaftStateMachineSnapshotRestorePolicyOnly(t *testing.T) {
	ctx := context.Background()
	source := NewModule()
	if result := source.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("source init failed: %v", result.Error)
	}
	policy := backupcore.Policy{Enabled: false, BackupDir: "backups", ScheduleKind: backupcore.ScheduleKindInterval, Interval: 2 * time.Hour, RetentionCount: 3, Compression: "zip"}
	if _, err := source.UpdatePolicy(ctx, policy); err != nil {
		t.Fatalf("UpdatePolicy() error = %v", err)
	}
	snapshot, err := (RaftStateMachine{Module: source}).Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}

	restored := NewModule()
	if result := restored.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("restored init failed: %v", result.Error)
	}
	if err := (RaftStateMachine{Module: restored}).RestoreSnapshot(snapshot); err != nil {
		t.Fatalf("RestoreSnapshot() error = %v", err)
	}
	got := restored.Policy()
	if got.Interval != 2*time.Hour || got.RetentionCount != 3 || got.Enabled {
		t.Fatalf("restored policy=%+v", got)
	}
	if restored.Running() {
		t.Fatal("backup restore should not resurrect a running local execution")
	}
}
