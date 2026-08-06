package service

import (
	"context"
	"encoding/json"
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

func TestBackupRaftSnapshotRestoreClusterBackupState(t *testing.T) {
	ctx := context.Background()
	source := NewModule()
	if result := source.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("source init failed: %v", result.Error)
	}
	source.restoreClusterBackupState("backup-set-active", map[string]clusterBackupRun{
		"backup-set-active": {BackupSetID: "backup-set-active", ClusterID: "cluster-1", Phase: clusterBackupPhaseBarrierWait, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(), Barriers: map[string]uint64{"system": 10}},
		"backup-set-done":   {BackupSetID: "backup-set-done", ClusterID: "cluster-1", Phase: clusterBackupPhaseSucceeded, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()},
	})
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
	active, runs := restored.clusterBackupSnapshot()
	if active != "backup-set-active" {
		t.Fatalf("active=%q want backup-set-active", active)
	}
	if runs["backup-set-active"].Barriers["system"] != 10 {
		t.Fatalf("restored active run=%#v", runs["backup-set-active"])
	}
	if runs["backup-set-done"].Phase != clusterBackupPhaseSucceeded {
		t.Fatalf("restored terminal run=%#v", runs["backup-set-done"])
	}
}

func TestBackupRaftSnapshotRestoreVersionOneClearsClusterBackupState(t *testing.T) {
	ctx := context.Background()
	legacy := backupRaftSnapshot{Version: 1, Policy: backupcore.Policy{Enabled: false, BackupDir: "backups", ScheduleKind: backupcore.ScheduleKindInterval, Interval: time.Hour, RetentionCount: 2, Compression: "zip"}}
	raw, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	restored := NewModule()
	if result := restored.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("restored init failed: %v", result.Error)
	}
	restored.restoreClusterBackupState("stale", map[string]clusterBackupRun{"stale": {BackupSetID: "stale", Phase: clusterBackupPhaseRequested}})
	if err := (RaftStateMachine{Module: restored}).RestoreSnapshot(raw); err != nil {
		t.Fatalf("RestoreSnapshot(v1) error = %v", err)
	}
	active, runs := restored.clusterBackupSnapshot()
	if active != "" || len(runs) != 0 {
		t.Fatalf("active=%q runs=%#v, want empty v1 cluster backup state", active, runs)
	}
}
