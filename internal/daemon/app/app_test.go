package app

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/myceldb/mycel/internal/daemon/config"
)

func TestInitializeCreatesDataAndLogDirs(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "mycel-data")
	cfg := config.Config{DataDir: dataDir, Mode: "mesh", LogLevel: "debug", LogFormat: "text", GRPCAddr: "127.0.0.1:0"}

	rt, err := Initialize(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}
	if _, ok := rt.Modules["admin"]; !ok {
		t.Fatalf("expected admin module to be registered, got modules: %#v", rt.Modules)
	}
	if err := rt.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	assertDir(t, dataDir)
	assertDir(t, filepath.Join(dataDir, "log"))
	assertFile(t, filepath.Join(dataDir, "log", LogFilename))

	logContent := readFile(t, rt.LogPath)
	for _, want := range []string{"daemon startup begins", "data directory ready", "log directory ready", "initializing module", "daemon initialization complete"} {
		if !strings.Contains(logContent, want) {
			t.Fatalf("expected log %q, got:\n%s", want, logContent)
		}
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
