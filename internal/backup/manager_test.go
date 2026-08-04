package backup

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/myceldb/mycel/internal/runtime/quiesce"
)

func TestManagerRejectsBackupDirInsideDataDir(t *testing.T) {
	dataDir := t.TempDir()
	mgr := NewManager(ManagerConfig{DataDir: dataDir, Policy: Policy{BackupDir: filepath.Join(dataDir, "backups")}})
	_, err := mgr.Trigger(context.Background(), TriggerInput{Source: "test"})
	if err == nil || !contains(err.Error(), "under data dir") {
		t.Fatalf("Trigger() error = %v, want backup dir under data dir rejection", err)
	}
}

func TestManagerRejectsBackupDirSymlinkIntoDataDir(t *testing.T) {
	dataDir := t.TempDir()
	target := filepath.Join(dataDir, "linked-backups")
	if err := os.MkdirAll(target, 0o700); err != nil {
		t.Fatalf("mkdir target: %v", err)
	}
	link := filepath.Join(t.TempDir(), "backup-link")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink not supported: %v", err)
	}
	mgr := NewManager(ManagerConfig{DataDir: dataDir, Policy: Policy{BackupDir: link}})
	_, err := mgr.Trigger(context.Background(), TriggerInput{Source: "test"})
	if err == nil || !contains(err.Error(), "under data dir") {
		t.Fatalf("Trigger() error = %v, want symlink-under-data rejection", err)
	}
}

func TestManagerUpdatePolicyRejectsNegativeAndPersists(t *testing.T) {
	dataDir := fixtureDataDir(t)
	backupDir := t.TempDir()
	mgr := NewManager(ManagerConfig{DataDir: dataDir, Policy: Policy{BackupDir: backupDir}})
	if _, err := mgr.UpdatePolicy(context.Background(), Policy{BackupDir: backupDir, Interval: -time.Second}); err == nil || !contains(err.Error(), "negative") {
		t.Fatalf("UpdatePolicy() error = %v, want negative rejection", err)
	}
	updated, err := mgr.UpdatePolicy(context.Background(), Policy{Enabled: true, BackupDir: backupDir, Interval: time.Hour, RetentionCount: 2, Compression: "zip", QuiesceDrainTimeout: time.Second, BackupTimeout: time.Minute, RetryAfter: time.Second, StatusHistoryLimit: 3})
	if err != nil {
		t.Fatalf("UpdatePolicy() error = %v", err)
	}
	if !updated.Enabled || updated.RetentionCount != 2 {
		t.Fatalf("unexpected updated policy: %#v", updated)
	}
	restarted := NewManager(ManagerConfig{DataDir: dataDir, Policy: Policy{BackupDir: t.TempDir()}})
	if got := restarted.Policy(); !got.Enabled || got.BackupDir != backupDir || got.RetentionCount != 2 {
		t.Fatalf("persisted policy not loaded: %#v", got)
	}
}

func TestManagerUpdatePolicyRejectsUnsupportedArchiveFormat(t *testing.T) {
	dataDir := fixtureDataDir(t)
	backupDir := t.TempDir()
	mgr := NewManager(ManagerConfig{DataDir: dataDir, Policy: Policy{BackupDir: backupDir}})
	_, err := mgr.UpdatePolicy(context.Background(), Policy{Enabled: true, BackupDir: backupDir, Interval: time.Hour, RetentionCount: 2, ArchiveFormat: ArchiveFormat("rar"), QuiesceDrainTimeout: time.Second, BackupTimeout: time.Minute, RetryAfter: time.Second, StatusHistoryLimit: 3})
	if err == nil || !contains(err.Error(), "archive_format") {
		t.Fatalf("UpdatePolicy() error = %v, want unsupported archive_format", err)
	}
}

func TestManagerDeleteBackupRemovesConfiguredArchiveFormats(t *testing.T) {
	formats := []ArchiveFormat{ArchiveFormatZip, ArchiveFormatTar, ArchiveFormatTarGz, ArchiveFormatTarZst}
	for _, format := range formats {
		t.Run(string(format), func(t *testing.T) {
			dataDir := fixtureDataDir(t)
			backupDir := t.TempDir()
			mgr := NewManager(ManagerConfig{DataDir: dataDir, Policy: Policy{BackupDir: backupDir, ArchiveFormat: format}})
			res, err := mgr.Trigger(context.Background(), TriggerInput{Source: "test"})
			if err != nil {
				t.Fatalf("Trigger() error = %v", err)
			}
			if err := mgr.DeleteBackup(context.Background(), res.BackupID); err != nil {
				t.Fatalf("DeleteBackup() error = %v", err)
			}
			if _, err := os.Stat(res.ArchivePath); !os.IsNotExist(err) {
				t.Fatalf("archive still exists or unexpected stat error: %v", err)
			}
			if _, err := os.Stat(res.ManifestPath); !os.IsNotExist(err) {
				t.Fatalf("manifest still exists or unexpected stat error: %v", err)
			}
		})
	}
}

func TestManagerDeleteBackupRejectsEscapingManifestArchiveName(t *testing.T) {
	dataDir := fixtureDataDir(t)
	backupDir := t.TempDir()
	backupID := "backup-escape"
	manifest := Manifest{Version: ManifestVersion, BackupID: backupID, ArchiveName: "../outside.zip", CreatedAt: time.Now().UTC(), CompletedAt: time.Now().UTC()}
	if err := writeManifestAtomic(filepath.Join(backupDir, backupID+".manifest.json"), manifest); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	mgr := NewManager(ManagerConfig{DataDir: dataDir, Policy: Policy{BackupDir: backupDir}})
	if err := mgr.DeleteBackup(context.Background(), backupID); err == nil || !contains(err.Error(), "invalid backup manifest") {
		t.Fatalf("DeleteBackup() error = %v, want invalid manifest rejection", err)
	}
}

func TestManagerCreateLocalArchiveUsesCustomNamesAndDestination(t *testing.T) {
	dataDir := fixtureDataDir(t)
	backupDir := t.TempDir()
	customDir := t.TempDir()
	mgr := NewManager(ManagerConfig{DataDir: dataDir, Policy: Policy{BackupDir: backupDir, ArchiveFormat: ArchiveFormatZip, IncludeLogs: false}, Version: "test-version", Now: fixedClock()})
	res, err := mgr.CreateLocalArchive(context.Background(), LocalArchiveInput{BackupID: "mycel-system-20260803T183500Z-myceld-0-backup-set-1", ArchiveName: "mycel-system-20260803T183500Z-myceld-0-backup-set-1.tar.zst", BackupDir: customDir, ArchiveFormat: string(ArchiveFormatTarZst), Source: "cluster-backup", Reason: "test", CreatedAt: fixedClock()()})
	if err != nil {
		t.Fatalf("CreateLocalArchive() error = %v", err)
	}
	if filepath.Base(res.ArchivePath) != "mycel-system-20260803T183500Z-myceld-0-backup-set-1.tar.zst" {
		t.Fatalf("archive path=%s", res.ArchivePath)
	}
	archiveDir, err := os.Stat(filepath.Dir(res.ArchivePath))
	if err != nil {
		t.Fatalf("stat archive dir: %v", err)
	}
	customDirInfo, err := os.Stat(customDir)
	if err != nil {
		t.Fatalf("stat custom dir: %v", err)
	}
	if !os.SameFile(archiveDir, customDirInfo) {
		t.Fatalf("archive path=%s, want dir %s", res.ArchivePath, customDir)
	}
	if filepath.Base(res.ManifestPath) != "mycel-system-20260803T183500Z-myceld-0-backup-set-1.manifest.json" {
		t.Fatalf("manifest path=%s", res.ManifestPath)
	}
	manifest := readManifest(t, res.ManifestPath)
	if manifest.BackupID != res.BackupID || manifest.ArchiveName != filepath.Base(res.ArchivePath) || manifest.Policy.ArchiveFormat != string(ArchiveFormatTarZst) {
		t.Fatalf("unexpected manifest: %#v", manifest)
	}
	if _, err := os.Stat(filepath.Join(backupDir, filepath.Base(res.ArchivePath))); !os.IsNotExist(err) {
		t.Fatalf("archive unexpectedly written to policy backup dir: %v", err)
	}
}

func TestManagerCreatesArchiveManifestAndChecksum(t *testing.T) {
	dataDir := fixtureDataDir(t)
	backupDir := t.TempDir()
	mgr := NewManager(ManagerConfig{DataDir: dataDir, Policy: Policy{BackupDir: backupDir, IncludeLogs: false}, Version: "test-version", Now: fixedClock()})
	res, err := mgr.Trigger(context.Background(), TriggerInput{Source: "test"})
	if err != nil {
		t.Fatalf("Trigger() error = %v", err)
	}
	if _, err := os.Stat(res.ArchivePath); err != nil {
		t.Fatalf("archive not created: %v", err)
	}
	manifest := readManifest(t, res.ManifestPath)
	if manifest.BackupID != res.BackupID || manifest.ArchiveName != filepath.Base(res.ArchivePath) || manifest.DaemonVersion != "test-version" {
		t.Fatalf("unexpected manifest: %#v", manifest)
	}
	checksum, size, err := fileSHA256(res.ArchivePath)
	if err != nil {
		t.Fatalf("fileSHA256() error = %v", err)
	}
	if manifest.ChecksumSHA256 != checksum || manifest.SizeBytes != size {
		t.Fatalf("manifest checksum/size = %s/%d, want %s/%d", manifest.ChecksumSHA256, manifest.SizeBytes, checksum, size)
	}
	if manifest.Policy.ArchiveFormat != string(ArchiveFormatZip) || manifest.Policy.Compression != string(ArchiveFormatZip) {
		t.Fatalf("manifest archive format summary = %#v", manifest.Policy)
	}
	entries := zipEntries(t, res.ArchivePath)
	if !entries["meta/spaces.json"] || !entries["graphs/space/nodes.json"] {
		t.Fatalf("archive missing expected entries: %#v", entries)
	}
	if entries["log/myceld.log"] {
		t.Fatalf("archive included log despite IncludeLogs=false: %#v", entries)
	}
}

func TestManagerCreatesConfiguredArchiveFormats(t *testing.T) {
	formats := []ArchiveFormat{ArchiveFormatZip, ArchiveFormatTar, ArchiveFormatTarGz, ArchiveFormatTarZst}
	for _, format := range formats {
		t.Run(string(format), func(t *testing.T) {
			dataDir := fixtureDataDir(t)
			backupDir := t.TempDir()
			mgr := NewManager(ManagerConfig{DataDir: dataDir, Policy: Policy{BackupDir: backupDir, ArchiveFormat: format, IncludeLogs: false}})
			res, err := mgr.Trigger(context.Background(), TriggerInput{Source: "test"})
			if err != nil {
				t.Fatalf("Trigger() error = %v", err)
			}
			ext, err := ArchiveExtension(format)
			if err != nil {
				t.Fatalf("ArchiveExtension() error = %v", err)
			}
			if filepath.Ext(res.ArchivePath) == "" || filepath.Base(res.ArchivePath) != res.BackupID+ext {
				t.Fatalf("archive path = %s, want backup_id%s", res.ArchivePath, ext)
			}
			entries := archiveEntries(t, format, res.ArchivePath)
			if !entries["meta/spaces.json"] || !entries["graphs/space/nodes.json"] {
				t.Fatalf("archive missing expected entries: %#v", entries)
			}
			manifest := readManifest(t, res.ManifestPath)
			if manifest.Policy.ArchiveFormat != string(format) || manifest.Policy.Compression != string(format) {
				t.Fatalf("manifest archive format summary = %#v, want %s", manifest.Policy, format)
			}
		})
	}
}

func TestManagerIncludesLogsWhenConfigured(t *testing.T) {
	dataDir := fixtureDataDir(t)
	mgr := NewManager(ManagerConfig{DataDir: dataDir, Policy: Policy{BackupDir: t.TempDir(), IncludeLogs: true}})
	res, err := mgr.Trigger(context.Background(), TriggerInput{Source: "test"})
	if err != nil {
		t.Fatalf("Trigger() error = %v", err)
	}
	entries := zipEntries(t, res.ArchivePath)
	if !entries["log/myceld.log"] {
		t.Fatalf("archive did not include logs with IncludeLogs=true: %#v", entries)
	}
}

func TestManagerDoesNotFollowSymlinks(t *testing.T) {
	dataDir := fixtureDataDir(t)
	outside := filepath.Join(t.TempDir(), "outside-secret.txt")
	if err := os.WriteFile(outside, []byte("secret"), 0o600); err != nil {
		t.Fatalf("write outside: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(dataDir, "meta", "linked-secret.txt")); err != nil {
		t.Skipf("symlink not supported: %v", err)
	}
	mgr := NewManager(ManagerConfig{DataDir: dataDir, Policy: Policy{BackupDir: t.TempDir(), IncludeLogs: true}})
	res, err := mgr.Trigger(context.Background(), TriggerInput{Source: "test"})
	if err != nil {
		t.Fatalf("Trigger() error = %v", err)
	}
	entries := zipEntries(t, res.ArchivePath)
	if entries["meta/linked-secret.txt"] {
		t.Fatalf("archive included symlink entry: %#v", entries)
	}
}

func TestManagerReleasesQuiesceLeaseWhenSnapshotFails(t *testing.T) {
	dataDir := fixtureDataDir(t)
	backupDir := t.TempDir()
	participant := &removeDataParticipant{name: "test-participant", dataDir: dataDir}
	coord := quiesce.NewCoordinator()
	if err := coord.Register(participant); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	mgr := NewManager(ManagerConfig{DataDir: dataDir, Policy: Policy{BackupDir: backupDir}, Quiesce: coord})
	_, err := mgr.Trigger(context.Background(), TriggerInput{Source: "test"})
	if err == nil {
		t.Fatal("expected snapshot failure")
	}
	if !participant.released {
		t.Fatal("quiesce lease was not released after snapshot failure")
	}
}

func TestManagerConcurrentTriggerReturnsAlreadyRunning(t *testing.T) {
	dataDir := fixtureDataDir(t)
	participant := &blockingParticipant{name: "block", entered: make(chan struct{}), release: make(chan struct{})}
	coord := quiesce.NewCoordinator()
	if err := coord.Register(participant); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	mgr := NewManager(ManagerConfig{DataDir: dataDir, Policy: Policy{BackupDir: t.TempDir()}, Quiesce: coord})
	done := make(chan error, 1)
	go func() {
		_, err := mgr.Trigger(context.Background(), TriggerInput{Source: "first"})
		done <- err
	}()
	select {
	case <-participant.entered:
	case <-time.After(time.Second):
		t.Fatal("first trigger did not reach quiesce participant")
	}
	_, err := mgr.Trigger(context.Background(), TriggerInput{Source: "second"})
	if !errors.Is(err, ErrBackupRunning) {
		t.Fatalf("second Trigger() error = %v, want ErrBackupRunning", err)
	}
	close(participant.release)
	if err := <-done; err != nil {
		t.Fatalf("first Trigger() error = %v", err)
	}
}

func TestManagerUsesTemporaryArchiveBeforeAtomicRename(t *testing.T) {
	dataDir := fixtureDataDir(t)
	backupDir := t.TempDir()
	mgr := NewManager(ManagerConfig{DataDir: dataDir, Policy: Policy{BackupDir: backupDir}})
	res, err := mgr.Trigger(context.Background(), TriggerInput{Source: "test"})
	if err != nil {
		t.Fatalf("Trigger() error = %v", err)
	}
	if _, err := os.Stat(res.ArchivePath + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("temporary archive still visible after completion: %v", err)
	}
	if _, err := os.Stat(res.ArchivePath); err != nil {
		t.Fatalf("final archive missing: %v", err)
	}
}

func fixtureDataDir(t *testing.T) string {
	t.Helper()
	dataDir := t.TempDir()
	writeFile(t, filepath.Join(dataDir, "meta", "spaces.json"), `[{"id":"space"}]`)
	writeFile(t, filepath.Join(dataDir, "graphs", "space", "nodes.json"), `[{"id":"node"}]`)
	writeFile(t, filepath.Join(dataDir, "log", "myceld.log"), "log line")
	return dataDir
}

func writeFile(t *testing.T, path string, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir %s: %v", path, err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func archiveEntries(t *testing.T, format ArchiveFormat, path string) map[string]bool {
	t.Helper()
	switch format {
	case ArchiveFormatZip:
		return zipEntries(t, path)
	case ArchiveFormatTar:
		file, err := os.Open(path)
		if err != nil {
			t.Fatalf("open tar: %v", err)
		}
		defer file.Close()
		return tarEntries(t, tar.NewReader(file))
	case ArchiveFormatTarGz:
		file, err := os.Open(path)
		if err != nil {
			t.Fatalf("open tar.gz: %v", err)
		}
		defer file.Close()
		gz, err := gzip.NewReader(file)
		if err != nil {
			t.Fatalf("open gzip: %v", err)
		}
		defer gz.Close()
		return tarEntries(t, tar.NewReader(gz))
	case ArchiveFormatTarZst:
		file, err := os.Open(path)
		if err != nil {
			t.Fatalf("open tar.zst: %v", err)
		}
		defer file.Close()
		zr, err := zstd.NewReader(file)
		if err != nil {
			t.Fatalf("open zstd: %v", err)
		}
		defer zr.Close()
		return tarEntries(t, tar.NewReader(zr))
	default:
		t.Fatalf("unsupported test archive format %q", format)
		return nil
	}
}

func zipEntries(t *testing.T, path string) map[string]bool {
	t.Helper()
	reader, err := zip.OpenReader(path)
	if err != nil {
		t.Fatalf("open zip: %v", err)
	}
	defer reader.Close()
	entries := map[string]bool{}
	for _, file := range reader.File {
		entries[file.Name] = true
	}
	return entries
}

func tarEntries(t *testing.T, reader *tar.Reader) map[string]bool {
	t.Helper()
	entries := map[string]bool{}
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return entries
		}
		if err != nil {
			t.Fatalf("read tar entry: %v", err)
		}
		entries[header.Name] = true
	}
}

func readManifest(t *testing.T, path string) Manifest {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var manifest Manifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	return manifest
}

func fixedClock() func() time.Time {
	return func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) }
}

func contains(value string, substr string) bool {
	return len(substr) == 0 || (len(value) >= len(substr) && (value == substr || contains(value[1:], substr) || value[:len(substr)] == substr))
}

type removeDataParticipant struct {
	name     string
	dataDir  string
	released bool
}

func (p *removeDataParticipant) Name() string { return p.name }
func (p *removeDataParticipant) Status() quiesce.ParticipantStatus {
	return quiesce.ParticipantStatus{Name: p.name}
}
func (p *removeDataParticipant) Quiesce(context.Context, quiesce.Request) (quiesce.Lease, error) {
	_ = os.RemoveAll(p.dataDir)
	return quiesce.LeaseFunc(func(context.Context) error { p.released = true; return nil }), nil
}

type blockingParticipant struct {
	name    string
	entered chan struct{}
	release chan struct{}
}

func (p *blockingParticipant) Name() string { return p.name }
func (p *blockingParticipant) Status() quiesce.ParticipantStatus {
	return quiesce.ParticipantStatus{Name: p.name}
}
func (p *blockingParticipant) Quiesce(ctx context.Context, req quiesce.Request) (quiesce.Lease, error) {
	close(p.entered)
	select {
	case <-p.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return quiesce.LeaseFunc(func(context.Context) error { return nil }), nil
}
