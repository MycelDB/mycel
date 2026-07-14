package backup

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRetentionKeepsNewestCompleteBackups(t *testing.T) {
	dataDir := fixtureDataDir(t)
	backupDir := t.TempDir()
	clock := incrementingClock(time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC), time.Minute)
	mgr := NewManager(ManagerConfig{DataDir: dataDir, Policy: Policy{BackupDir: backupDir, RetentionCount: 2}, Now: clock})
	created := []string{}
	for i := 0; i < 3; i++ {
		res, err := mgr.Trigger(context.Background(), TriggerInput{Source: "test"})
		if err != nil {
			t.Fatalf("Trigger(%d) error = %v", i, err)
		}
		created = append(created, res.BackupID)
	}
	backups, err := mgr.ListBackups(context.Background())
	if err != nil {
		t.Fatalf("ListBackups() error = %v", err)
	}
	if len(backups) != 2 {
		t.Fatalf("backup count = %d, want 2 (%#v)", len(backups), backups)
	}
	if backups[0].BackupID != created[2] || backups[1].BackupID != created[1] {
		t.Fatalf("retention kept wrong backups: got %q, %q want %q, %q", backups[0].BackupID, backups[1].BackupID, created[2], created[1])
	}
	if _, err := os.Stat(filepath.Join(backupDir, created[0]+".manifest.json")); !os.IsNotExist(err) {
		t.Fatalf("old manifest still exists or unexpected stat error: %v", err)
	}
	if _, err := os.Stat(filepath.Join(backupDir, created[0]+".zip")); !os.IsNotExist(err) {
		t.Fatalf("old archive still exists or unexpected stat error: %v", err)
	}
}

func TestRetentionIgnoresIncompleteTmpFiles(t *testing.T) {
	dataDir := fixtureDataDir(t)
	backupDir := t.TempDir()
	writeFile(t, filepath.Join(backupDir, "backup-incomplete.zip.tmp"), "zip")
	writeFile(t, filepath.Join(backupDir, "backup-incomplete.manifest.json.tmp"), "{}")
	mgr := NewManager(ManagerConfig{DataDir: dataDir, Policy: Policy{BackupDir: backupDir, RetentionCount: 1}})
	backups, err := mgr.ListBackups(context.Background())
	if err != nil {
		t.Fatalf("ListBackups() error = %v", err)
	}
	if len(backups) != 0 {
		t.Fatalf("incomplete tmp files returned as backups: %#v", backups)
	}
	if err := mgr.ApplyRetention(context.Background()); err != nil {
		t.Fatalf("ApplyRetention() error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(backupDir, "backup-incomplete.zip.tmp")); err != nil {
		t.Fatalf("tmp archive should be ignored and retained, stat err=%v", err)
	}
}

func incrementingClock(start time.Time, step time.Duration) func() time.Time {
	current := start.Add(-step)
	return func() time.Time {
		current = current.Add(step)
		return current
	}
}
