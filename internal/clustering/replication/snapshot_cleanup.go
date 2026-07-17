package replication

import (
	"context"
	"os"
	"path/filepath"
	"time"
)

func CleanupStaleSnapshotStaging(ctx context.Context, dataDir string, olderThan time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if olderThan <= 0 {
		olderThan = 24 * time.Hour
	}
	root := filepath.Join(dataDir, "meta", "clustering", "replication", "snapshot-staging")
	entries, err := os.ReadDir(root)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	cutoff := time.Now().Add(-olderThan)
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !entry.IsDir() {
			continue
		}
		path := filepath.Join(root, entry.Name())
		info, err := entry.Info()
		if err != nil {
			continue
		}
		if info.ModTime().Before(cutoff) {
			_ = os.RemoveAll(path)
		}
	}
	return nil
}
