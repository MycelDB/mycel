package backup

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/myceldb/mycel/internal/projection"
)

func TestBackupManifestIncludesProjectionCatalog(t *testing.T) {
	dataDir := t.TempDir()
	backupDir := t.TempDir()
	catalog := projection.BackupCatalog{Version: projection.BackupCatalogVersion, PartitionCount: 1, Partitions: []projection.BackupPartitionCatalog{{PartitionID: 0, SnapshotHash: "sha256:" + strings.Repeat("0", 64)}}}
	manager := NewManager(ManagerConfig{DataDir: dataDir, Policy: Policy{BackupDir: backupDir, IncludeLogs: false}, ProjectionCatalog: func(context.Context) (projection.BackupCatalog, error) { return catalog, nil }})
	result, err := manager.Trigger(context.Background(), TriggerInput{Source: "projection-catalog-test"})
	if err != nil {
		t.Fatalf("Trigger() error: %v", err)
	}
	raw, err := os.ReadFile(result.ManifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var manifest Manifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	if manifest.ProjectionCatalog == nil || len(manifest.ProjectionCatalog.Partitions) != 1 || manifest.ProjectionCatalog.Partitions[0].SnapshotHash != catalog.Partitions[0].SnapshotHash {
		t.Fatalf("projection catalog missing from manifest: %+v", manifest.ProjectionCatalog)
	}
}
