package replication

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCleanupStaleSnapshotStaging(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "meta", "clustering", "replication", "snapshot-staging")
	oldDir := filepath.Join(root, "old")
	newDir := filepath.Join(root, "new")
	if err := os.MkdirAll(oldDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(newDir, 0o700); err != nil {
		t.Fatal(err)
	}
	oldTime := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(oldDir, oldTime, oldTime); err != nil {
		t.Fatal(err)
	}
	if err := CleanupStaleSnapshotStaging(context.Background(), dir, 24*time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(oldDir); !os.IsNotExist(err) {
		t.Fatalf("old dir still exists err=%v", err)
	}
	if _, err := os.Stat(newDir); err != nil {
		t.Fatalf("new dir removed: %v", err)
	}
}
