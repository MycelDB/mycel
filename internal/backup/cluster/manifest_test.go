package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	backupcore "github.com/myceldb/mycel/internal/backup"
)

func TestManifestValidateRestoreHappyPath(t *testing.T) {
	manifest := validManifest(t)
	if err := Validate(manifest, ValidationModeRestore); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestManifestValidateRejectsIncompleteForRestore(t *testing.T) {
	manifest := validManifest(t)
	manifest.Complete = false
	if err := Validate(manifest, ValidationModeRestore); err == nil || !strings.Contains(err.Error(), "complete") {
		t.Fatalf("Validate() error = %v, want complete rejection", err)
	}
}

func TestManifestValidateRejectsDuplicateOrdinal(t *testing.T) {
	manifest := validManifest(t)
	manifest.Nodes[1].Ordinal = manifest.Nodes[0].Ordinal
	if err := Validate(manifest, ValidationModeBackupSet); err == nil || !strings.Contains(err.Error(), "duplicate ordinal") {
		t.Fatalf("Validate() error = %v, want duplicate ordinal", err)
	}
}

func TestManifestValidateRejectsMissingExpectedNode(t *testing.T) {
	manifest := validManifest(t)
	manifest.Nodes = manifest.Nodes[:1]
	if err := Validate(manifest, ValidationModeBackupSet); err == nil || !strings.Contains(err.Error(), "expected_nodes") {
		t.Fatalf("Validate() error = %v, want expected_nodes mismatch", err)
	}
}

func TestManifestValidateRejectsFilenameWithoutPodName(t *testing.T) {
	manifest := validManifest(t)
	manifest.Nodes[0].ArchiveName = strings.ReplaceAll(manifest.Nodes[0].ArchiveName, "myceld-0", "node-zero")
	if err := Validate(manifest, ValidationModeBackupSet); err == nil || !strings.Contains(err.Error(), "archive_name must match") {
		t.Fatalf("Validate() error = %v, want pod name filename rejection", err)
	}
}

func TestManifestValidateRejectsMalformedTimestamp(t *testing.T) {
	manifest := validManifest(t)
	manifest.Nodes[0].ArchiveName = strings.Replace(manifest.Nodes[0].ArchiveName, "20260803T183500Z", "2026-08-03T18:35:00Z", 1)
	manifest.Nodes[0].ManifestName, _ = NewNodeManifestName(manifest.Nodes[0].ArchiveName)
	if err := Validate(manifest, ValidationModeBackupSet); err == nil || !strings.Contains(err.Error(), "archive_name must match") {
		t.Fatalf("Validate() error = %v, want malformed timestamp rejection", err)
	}
}

func TestManifestValidateRejectsManifestNameMismatch(t *testing.T) {
	manifest := validManifest(t)
	manifest.Nodes[0].ManifestName = strings.Replace(manifest.Nodes[0].ManifestName, "myceld-0", "myceld-1", 1)
	if err := Validate(manifest, ValidationModeBackupSet); err == nil || !strings.Contains(err.Error(), "manifest_name must match archive_name") {
		t.Fatalf("Validate() error = %v, want manifest name mismatch", err)
	}
}

func TestManifestValidateRejectsWrongArchiveExtension(t *testing.T) {
	manifest := validManifest(t)
	manifest.Nodes[0].ArchiveName = strings.TrimSuffix(manifest.Nodes[0].ArchiveName, ".tar.zst") + ".zip"
	if err := Validate(manifest, ValidationModeBackupSet); err == nil || !strings.Contains(err.Error(), "extension") {
		t.Fatalf("Validate() error = %v, want extension rejection", err)
	}
}

func TestMarshalDeterministicSortsNodes(t *testing.T) {
	manifest := validManifest(t)
	manifest.Nodes[0], manifest.Nodes[1] = manifest.Nodes[1], manifest.Nodes[0]
	raw, err := manifest.MarshalDeterministic()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Index(string(raw), "myceld-0") > strings.Index(string(raw), "myceld-1") {
		t.Fatalf("nodes were not sorted by ordinal:\n%s", raw)
	}
}

func TestValidateArchiveFilesRejectsChecksumMismatch(t *testing.T) {
	dir := t.TempDir()
	archive := filepath.Join(dir, "archive.tar.zst")
	if err := os.WriteFile(archive, []byte("archive"), 0o600); err != nil {
		t.Fatal(err)
	}
	manifest := validManifest(t)
	manifest.ExpectedNodes = 1
	manifest.Nodes = manifest.Nodes[:1]
	manifest.Nodes[0].ArchiveURI = archive
	manifest.Nodes[0].SizeBytes = int64(len("archive"))
	manifest.Nodes[0].ChecksumSHA256 = strings.Repeat("0", 64)
	if err := ValidateArchiveFiles(context.Background(), manifest); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("ValidateArchiveFiles() error = %v, want checksum mismatch", err)
	}
}

func TestValidateArchiveFilesAcceptsLocalPath(t *testing.T) {
	dir := t.TempDir()
	archive := filepath.Join(dir, "archive.tar.zst")
	payload := []byte("archive")
	if err := os.WriteFile(archive, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	manifest := validManifest(t)
	manifest.ExpectedNodes = 1
	manifest.Nodes = manifest.Nodes[:1]
	manifest.Nodes[0].ArchiveURI = archive
	manifest.Nodes[0].SizeBytes = int64(len(payload))
	manifest.Nodes[0].ChecksumSHA256 = sumHex(payload)
	if err := ValidateArchiveFiles(context.Background(), manifest); err != nil {
		t.Fatalf("ValidateArchiveFiles() error = %v", err)
	}
}

func validManifest(t *testing.T) Manifest {
	t.Helper()
	ts := time.Date(2026, 8, 3, 18, 35, 0, 0, time.UTC)
	backupSetID := "backup-set-20260803T183500Z-cluster_7d42d6ab"
	node0Archive, err := NewArchiveName(ts, "myceld-0", backupSetID, backupcore.ArchiveFormatTarZst)
	if err != nil {
		t.Fatal(err)
	}
	node0Manifest, err := NewNodeManifestName(node0Archive)
	if err != nil {
		t.Fatal(err)
	}
	node1Archive, err := NewArchiveName(ts, "myceld-1", backupSetID, backupcore.ArchiveFormatTarZst)
	if err != nil {
		t.Fatal(err)
	}
	node1Manifest, err := NewNodeManifestName(node1Archive)
	if err != nil {
		t.Fatal(err)
	}
	return Manifest{
		Version:       ManifestVersion,
		BackupSetID:   backupSetID,
		CreatedAt:     ts,
		CompletedAt:   ts.Add(time.Minute),
		ClusterID:     "cluster_7d42d6ab-9a97-447e-b8e5-1b1ecf0abb93",
		Complete:      true,
		State:         StateSucceeded,
		ExpectedNodes: 2,
		DataDir:       "/data/mycel",
		ArchiveFormat: backupcore.ArchiveFormatTarZst,
		Nodes: []NodeArtifact{
			{PodName: "myceld-0", NodeID: "node_1", Ordinal: 0, RaftNodeID: 1, ArchiveName: node0Archive, ArchiveURI: "file:///mnt/backups/myceld-0/" + node0Archive, ManifestName: node0Manifest, ManifestURI: "file:///mnt/backups/myceld-0/" + node0Manifest, SizeBytes: 10, ChecksumSHA256: sumHex([]byte("node0"))},
			{PodName: "myceld-1", NodeID: "node_2", Ordinal: 1, RaftNodeID: 2, ArchiveName: node1Archive, ArchiveURI: "file:///mnt/backups/myceld-1/" + node1Archive, ManifestName: node1Manifest, ManifestURI: "file:///mnt/backups/myceld-1/" + node1Manifest, SizeBytes: 10, ChecksumSHA256: sumHex([]byte("node1"))},
		},
	}
}

func sumHex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
