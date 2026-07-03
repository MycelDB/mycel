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
	cfg := config.Config{DataDir: dataDir, Mode: "mesh", LogLevel: "debug", LogFormat: "text"}

	initialized, err := Initialize(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}
	if err := initialized.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	assertDir(t, dataDir)
	assertDir(t, filepath.Join(dataDir, "log"))
	assertFile(t, filepath.Join(dataDir, "log", LogFilename))

	logContent := readFile(t, initialized.LogPath)
	for _, want := range []string{"daemon startup begins", "data directory ready", "log directory ready", "initializing module", "daemon initialization complete"} {
		if !strings.Contains(logContent, want) {
			t.Fatalf("expected log %q, got:\n%s", want, logContent)
		}
	}
}

func TestRunLogsStartupAndShutdown(t *testing.T) {
	dataDir := t.TempDir()
	cfg := config.Config{DataDir: dataDir, Mode: "mesh", LogLevel: "debug", LogFormat: "text"}
	initialized, err := Initialize(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	initialized.Runtime.Logger.Info("daemon ready")
	cancel()
	waitForShutdown(ctx, initialized.Runtime.Logger)
	initialized.Runtime.Logger.Info("daemon shutdown complete")
	if err := initialized.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	logContent := readFile(t, filepath.Join(dataDir, "log", LogFilename))
	for _, want := range []string{"daemon startup begins", "daemon ready", "daemon shutdown begins", "daemon shutdown complete"} {
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
