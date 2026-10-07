package cluster

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	backupcore "github.com/myceldb/mycel/internal/backup"
)

func TestBuildRestorePlanValidatesLocalSetAndMapsOrdinals(t *testing.T) {
	dir := t.TempDir()
	manifest := writeLocalBackupSet(t, dir)

	plan, _, err := BuildRestorePlan(context.Background(), dir)
	if err != nil {
		t.Fatalf("BuildRestorePlan() error = %v", err)
	}
	if plan.BackupSetID != manifest.BackupSetID || plan.ExpectedNodes != 2 {
		t.Fatalf("unexpected plan metadata: %+v", plan)
	}
	if len(plan.Nodes) != 2 {
		t.Fatalf("plan nodes = %d, want 2", len(plan.Nodes))
	}
	if plan.Nodes[0].Ordinal != 0 || plan.Nodes[1].Ordinal != 1 {
		t.Fatalf("unexpected ordinal mapping: %+v", plan.Nodes)
	}
	if plan.Nodes[1].ArchivePath != filepath.Join(dir, manifest.Nodes[1].ArchiveName) {
		t.Fatalf("unexpected archive path for ordinal 1: %s", plan.Nodes[1].ArchivePath)
	}
}

func TestBuildRestorePlanRejectsInvalidSet(t *testing.T) {
	dir := t.TempDir()
	manifest := writeLocalBackupSet(t, dir)
	manifest.Complete = false
	writeSetManifest(t, dir, manifest)

	_, _, err := BuildRestorePlan(context.Background(), dir)
	if err == nil || !strings.Contains(err.Error(), "complete") {
		t.Fatalf("BuildRestorePlan() error = %v, want complete rejection", err)
	}
}

func TestBuildRestorePlanRejectsChecksumMismatch(t *testing.T) {
	dir := t.TempDir()
	manifest := writeLocalBackupSet(t, dir)
	manifest.Nodes[0].ChecksumSHA256 = strings.Repeat("0", 64)
	writeSetManifest(t, dir, manifest)

	_, _, err := BuildRestorePlan(context.Background(), dir)
	if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("BuildRestorePlan() error = %v, want checksum mismatch", err)
	}
}

func TestBuildRestorePlanRejectsMissingNodeManifest(t *testing.T) {
	dir := t.TempDir()
	manifest := writeLocalBackupSet(t, dir)
	if err := os.Remove(filepath.Join(dir, manifest.Nodes[0].ManifestName)); err != nil {
		t.Fatal(err)
	}

	_, _, err := BuildRestorePlan(context.Background(), dir)
	if err == nil || !strings.Contains(err.Error(), "manifest") {
		t.Fatalf("BuildRestorePlan() error = %v, want missing manifest rejection", err)
	}
}

func TestRestoreLocalExtractsOnlyRequestedOrdinal(t *testing.T) {
	dir := t.TempDir()
	manifest := writeLocalBackupSet(t, dir)
	dest := filepath.Join(t.TempDir(), "data")

	result, err := RestoreLocal(context.Background(), RestoreLocalInput{BackupSet: dir, Ordinal: 1, DataDir: dest})
	if err != nil {
		t.Fatalf("RestoreLocal() error = %v", err)
	}
	if result.Ordinal != 1 || result.PodName != "myceld-1" {
		t.Fatalf("unexpected result: %+v", result)
	}
	if got := string(mustReadFile(t, filepath.Join(dest, "meta", "clustering", "node.json"))); !strings.Contains(got, manifest.Nodes[1].NodeID) {
		t.Fatalf("restored node identity %q does not contain node id %s", got, manifest.Nodes[1].NodeID)
	}
	if got := string(mustReadFile(t, filepath.Join(dest, "ordinal.txt"))); strings.TrimSpace(got) != "1" {
		t.Fatalf("restored ordinal.txt = %q, want 1", got)
	}
}

func TestRestoreLocalRejectsNonEmptyDataDir(t *testing.T) {
	dir := t.TempDir()
	writeLocalBackupSet(t, dir)
	dest := t.TempDir()
	if err := os.WriteFile(filepath.Join(dest, "existing"), []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := RestoreLocal(context.Background(), RestoreLocalInput{BackupSet: dir, Ordinal: 0, DataDir: dest})
	if err == nil || !strings.Contains(err.Error(), "not empty") {
		t.Fatalf("RestoreLocal() error = %v, want non-empty rejection", err)
	}
}

func TestExtractArchiveRejectsTraversal(t *testing.T) {
	dir := t.TempDir()
	archivePath := filepath.Join(dir, "bad.tar")
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := tw.WriteHeader(&tar.Header{Name: "../escape", Mode: 0o600, Size: int64(len("bad"))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte("bad")); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(archivePath, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	err := ExtractArchive(context.Background(), backupcore.ArchiveFormatTar, archivePath, filepath.Join(dir, "data"))
	if err == nil || !strings.Contains(err.Error(), "unsafe archive entry") {
		t.Fatalf("ExtractArchive() error = %v, want unsafe entry", err)
	}
}

func writeLocalBackupSet(t *testing.T, dir string) Manifest {
	t.Helper()
	manifest := validManifest(t)
	manifest.Nodes[0].ArchiveURI = ""
	manifest.Nodes[0].ManifestURI = ""
	manifest.Nodes[1].ArchiveURI = ""
	manifest.Nodes[1].ManifestURI = ""
	for i := range manifest.Nodes {
		writeNodeArchive(t, dir, &manifest.Nodes[i])
	}
	writeSetManifest(t, dir, manifest)
	return manifest
}

func writeNodeArchive(t *testing.T, dir string, node *NodeArtifact) {
	t.Helper()
	source := t.TempDir()
	files := map[string]string{
		filepath.Join("meta", "clustering", "node.json"):         `{"node_id":"` + node.NodeID + `"}` + "\n",
		filepath.Join("meta", "raft", "system", "hard_state.pb"): "raft-state",
		"ordinal.txt": strings.TrimPrefix(node.PodName, "myceld-") + "\n",
	}
	for rel, content := range files {
		path := filepath.Join(source, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	archivePath := filepath.Join(dir, node.ArchiveName)
	if err := backupcore.WriteArchive(context.Background(), backupcore.ArchiveFormatTarZst, source, archivePath); err != nil {
		t.Fatalf("write archive: %v", err)
	}
	checksum, size, err := fileSHA256(context.Background(), archivePath)
	if err != nil {
		t.Fatal(err)
	}
	node.ChecksumSHA256 = checksum
	node.SizeBytes = size
	writeNodeManifest(t, dir, *node)
}

func writeNodeManifest(t *testing.T, dir string, node NodeArtifact) {
	t.Helper()
	manifest := backupcore.Manifest{Version: backupcore.ManifestVersion, BackupID: node.ArchiveName, ArchiveName: node.ArchiveName, SizeBytes: node.SizeBytes, ChecksumSHA256: node.ChecksumSHA256, Policy: backupcore.PolicySummary{ArchiveFormat: string(backupcore.ArchiveFormatTarZst), Compression: string(backupcore.ArchiveFormatTarZst)}}
	raw := marshalJSON(t, manifest)
	if err := os.WriteFile(filepath.Join(dir, node.ManifestName), raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeSetManifest(t *testing.T, dir string, manifest Manifest) {
	t.Helper()
	raw, err := manifest.MarshalDeterministic()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, BackupSetManifestName), raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func marshalJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := jsonMarshalIndent(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func jsonMarshalIndent(value any) ([]byte, error) {
	return json.MarshalIndent(value, "", "  ")
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
