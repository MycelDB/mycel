package app

import (
	"archive/zip"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	backupcore "github.com/myceldb/mycel/internal/backup"
	"github.com/myceldb/mycel/internal/daemon/config"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	daemonadmin "github.com/myceldb/mycel/internal/identity/service/admin"
	"github.com/myceldb/mycel/internal/wal"
)

func TestInitializeCreatesDataAndLogDirs(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "mycel-data")
	cfg := config.Config{DataDir: dataDir, Mode: "mesh", LogLevel: "debug", LogFormat: "text", GRPCAddr: "127.0.0.1:0"}

	rt, err := Initialize(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}
	if _, ok := rt.Service("admin"); !ok {
		t.Fatalf("expected admin service to be registered, got services: %#v", rt.ServicesByName)
	}
	if _, ok := rt.Service("user"); !ok {
		t.Fatalf("expected user service to be registered, got services: %#v", rt.ServicesByName)
	}
	statuses := rt.ServiceStatuses(context.Background())
	if len(statuses) == 0 {
		t.Fatal("expected service statuses to be collected")
	}
	if !hasServiceStatus(statuses, "semantic") || !hasServiceStatus(statuses, "backup") {
		t.Fatalf("expected semantic and backup service statuses, got %#v", statuses)
	}
	if err := rt.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	assertDir(t, dataDir)
	assertDir(t, filepath.Join(dataDir, "log"))
	assertDir(t, filepath.Join(dataDir, "users"))
	assertDir(t, filepath.Join(dataDir, "users", "sessions"))
	assertFile(t, filepath.Join(dataDir, "log", LogFilename))
	assertFile(t, filepath.Join(dataDir, "users", "users.json"))

	logContent := readFile(t, rt.LogPath)
	for _, want := range []string{"daemon startup begins", "data directory ready", "log directory ready", "initializing service", "daemon initialization complete"} {
		if !strings.Contains(logContent, want) {
			t.Fatalf("expected log %q, got:\n%s", want, logContent)
		}
	}
}

func TestPhase8OfflineRestoreArchiveBootsDaemonAndListsResources(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "mycel-data")
	backupDir := filepath.Join(t.TempDir(), "backups")
	cfg := config.Config{DataDir: dataDir, Mode: "standalone", LogLevel: "debug", LogFormat: "text", GRPCAddr: "127.0.0.1:0"}
	rt, err := Initialize(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}
	mgr := backupcore.NewManager(backupcore.ManagerConfig{DataDir: dataDir, Policy: backupcore.Policy{BackupDir: backupDir, IncludeLogs: true}, Quiesce: rt.Quiesce})
	result, err := mgr.Trigger(context.Background(), backupcore.TriggerInput{Source: "restore-test"})
	if err != nil {
		_ = rt.Close()
		t.Fatalf("Trigger() error = %v", err)
	}
	if err := rt.Close(); err != nil {
		t.Fatalf("Close() original runtime error = %v", err)
	}

	restoredDir := filepath.Join(t.TempDir(), "restored")
	unzipArchive(t, result.ArchivePath, restoredDir)
	restored, err := Initialize(context.Background(), config.Config{DataDir: restoredDir, Mode: "standalone", LogLevel: "debug", LogFormat: "text", GRPCAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("Initialize(restored) error = %v", err)
	}
	defer restored.Close()
	adminService, ok := daemonruntime.ServiceAs[*daemonadmin.Module](restored, daemonadmin.ModuleName)
	if !ok {
		t.Fatal("restored admin service is not registered")
	}
	admins, err := adminService.ListAdmins(context.Background())
	if err != nil {
		t.Fatalf("ListAdmins(restored) error = %v", err)
	}
	if len(admins) == 0 || admins[0].Username != "admin" {
		t.Fatalf("unexpected restored admins: %#v", admins)
	}
}

func TestInitializeWithWALEnabledOpensAndRecoversEmptyLog(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "mycel-data")
	cfg := config.Config{DataDir: dataDir, Mode: "mesh", LogLevel: "debug", LogFormat: "text", GRPCAddr: "127.0.0.1:0", WAL: config.WALConfig{Enabled: true, SegmentBytes: 1024, SyncPolicy: "always"}}
	rt, err := Initialize(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}
	if rt.WAL == nil || rt.WALRegistry == nil || rt.WALRecovery == nil || rt.WALWaiter == nil {
		t.Fatalf("expected WAL runtime components to be initialized")
	}
	if got := rt.WAL.LastCommittedLSN(); got != 0 {
		t.Fatalf("LastCommittedLSN() = %v, want 0", got)
	}
	if err := rt.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	assertDir(t, filepath.Join(dataDir, "wal"))
}

func TestInitializeWithWALCorruptionFailsStartup(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "mycel-data")
	walDir := filepath.Join(dataDir, "wal")
	mgr, err := wal.Open(context.Background(), wal.Options{Dir: walDir, SegmentBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	lsn, err := mgr.Append(context.Background(), wal.PendingRecord{Type: "unknown.v1", SchemaVersion: 1, Payload: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if err := mgr.Sync(context.Background(), lsn); err != nil {
		t.Fatal(err)
	}
	_ = mgr.Close()
	cfg := config.Config{DataDir: dataDir, Mode: "mesh", LogLevel: "debug", LogFormat: "text", GRPCAddr: "127.0.0.1:0", WAL: config.WALConfig{Enabled: true, SegmentBytes: 1024, SyncPolicy: "always"}}
	if _, err := Initialize(context.Background(), cfg); err == nil {
		t.Fatal("expected Initialize() to fail on unreplayable WAL record")
	}
}

func TestRunLogsStartupAndShutdown(t *testing.T) {
	dataDir := t.TempDir()
	cfg := config.Config{DataDir: dataDir, Mode: "mesh", LogLevel: "debug", LogFormat: "text", GRPCAddr: "127.0.0.1:0"}
	rt, err := Initialize(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	rt.Logger.Info("daemon ready")
	logRuntimeConfiguration(rt.Logger, cfg, rt.LogPath, "127.0.0.1:12345")
	cancel()
	waitForShutdown(ctx, rt.Logger)
	rt.Logger.Info("daemon shutdown complete")
	if err := rt.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	logContent := readFile(t, filepath.Join(dataDir, "log", LogFilename))
	for _, want := range []string{"daemon startup begins", "daemon ready", "daemon runtime configuration", "data_dir=" + dataDir, "mode=mesh", "grpc_addr=127.0.0.1:12345", "log_path=" + filepath.Join(dataDir, "log", LogFilename), "log_level=debug", "log_format=text", "daemon shutdown begins", "daemon shutdown complete"} {
		if !strings.Contains(logContent, want) {
			t.Fatalf("expected log %q, got:\n%s", want, logContent)
		}
	}
}

func hasServiceStatus(statuses []daemonruntime.ServiceStatus, name string) bool {
	for _, status := range statuses {
		if status.Name == name {
			return true
		}
	}
	return false
}

func assertDir(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat dir %s: %v", path, err)
	}
	if !info.IsDir() {
		t.Fatalf("%s is not a directory", path)
	}
}

func assertFile(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat file %s: %v", path, err)
	}
	if info.IsDir() {
		t.Fatalf("%s is a directory", path)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file %s: %v", path, err)
	}
	return string(data)
}

func unzipArchive(t *testing.T, archivePath string, dst string) {
	t.Helper()
	reader, err := zip.OpenReader(archivePath)
	if err != nil {
		t.Fatalf("open archive %s: %v", archivePath, err)
	}
	defer reader.Close()
	for _, file := range reader.File {
		path := filepath.Join(dst, file.Name)
		if !strings.HasPrefix(filepath.Clean(path), filepath.Clean(dst)+string(filepath.Separator)) {
			t.Fatalf("archive entry escapes restore dir: %s", file.Name)
		}
		if file.FileInfo().IsDir() {
			if err := os.MkdirAll(path, file.Mode()); err != nil {
				t.Fatalf("mkdir restored dir %s: %v", path, err)
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatalf("mkdir restored parent %s: %v", path, err)
		}
		in, err := file.Open()
		if err != nil {
			t.Fatalf("open archive file %s: %v", file.Name, err)
		}
		out, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, file.Mode())
		if err != nil {
			_ = in.Close()
			t.Fatalf("create restored file %s: %v", path, err)
		}
		_, copyErr := io.Copy(out, in)
		closeInErr := in.Close()
		closeOutErr := out.Close()
		if copyErr != nil || closeInErr != nil || closeOutErr != nil {
			t.Fatalf("restore file %s failed: copy=%v closeIn=%v closeOut=%v", file.Name, copyErr, closeInErr, closeOutErr)
		}
	}
}
