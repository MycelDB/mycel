package app

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/myceldb/mycel/internal/daemon/config"
	"github.com/myceldb/mycel/internal/daemon/modules/admin"
)

func TestInitializeStandaloneCreatesDefaultAdminAndLogsCredentials(t *testing.T) {
	dataDir := t.TempDir()
	cfg := config.Config{DataDir: dataDir, Mode: "standalone", LogLevel: "debug", LogFormat: "text"}

	initialized, err := Initialize(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}
	admins, err := initialized.AdminModule.ListAdmins(context.Background())
	if err != nil {
		t.Fatalf("ListAdmins() error = %v", err)
	}
	if len(admins) != 1 {
		t.Fatalf("expected 1 admin, got %d", len(admins))
	}
	if admins[0].Username != "admin" {
		t.Fatalf("expected default admin username, got %q", admins[0].Username)
	}
	if admins[0].PasswordHash == "" || strings.Contains(admins[0].PasswordHash, "admin") {
		t.Fatalf("expected non-empty password hash without plaintext username, got %q", admins[0].PasswordHash)
	}
	if err := initialized.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	assertDir(t, filepath.Join(dataDir, "log"))
	assertDir(t, filepath.Join(dataDir, "admins"))
	assertFile(t, filepath.Join(dataDir, "admins", admin.StoreFilename))

	logContent := readFile(t, initialized.LogPath)
	if !strings.Contains(logContent, "daemon startup begins") {
		t.Fatalf("expected startup log, got:\n%s", logContent)
	}
	if !strings.Contains(logContent, "default standalone admin created") {
		t.Fatalf("expected default admin creation log, got:\n%s", logContent)
	}
	if !strings.Contains(logContent, "username=admin") {
		t.Fatalf("expected admin username in log, got:\n%s", logContent)
	}
	password := extractLoggedPassword(t, logContent)
	if password == "" {
		t.Fatalf("expected logged password, got:\n%s", logContent)
	}
	if err := admin.VerifyPassword(admins[0].PasswordHash, password); err != nil {
		t.Fatalf("logged password does not match stored hash: %v", err)
	}
	storeContent := readFile(t, filepath.Join(dataDir, "admins", admin.StoreFilename))
	if strings.Contains(storeContent, password) {
		t.Fatalf("admin store contains plaintext password %q", password)
	}
}

func TestInitializeStandaloneIsIdempotent(t *testing.T) {
	dataDir := t.TempDir()
	cfg := config.Config{DataDir: dataDir, Mode: "standalone", LogLevel: "debug", LogFormat: "text"}

	first, err := Initialize(context.Background(), cfg)
	if err != nil {
		t.Fatalf("first Initialize() error = %v", err)
	}
	firstAdmins, err := first.AdminModule.ListAdmins(context.Background())
	if err != nil {
		t.Fatalf("first ListAdmins() error = %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}

	second, err := Initialize(context.Background(), cfg)
	if err != nil {
		t.Fatalf("second Initialize() error = %v", err)
	}
	secondAdmins, err := second.AdminModule.ListAdmins(context.Background())
	if err != nil {
		t.Fatalf("second ListAdmins() error = %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}

	if len(firstAdmins) != 1 || len(secondAdmins) != 1 {
		t.Fatalf("expected one admin after both initializations, got first=%d second=%d", len(firstAdmins), len(secondAdmins))
	}
	if firstAdmins[0].ID != secondAdmins[0].ID {
		t.Fatalf("expected same admin after reinitialize, got %q and %q", firstAdmins[0].ID, secondAdmins[0].ID)
	}
	logContent := readFile(t, second.LogPath)
	if count := strings.Count(logContent, "default standalone admin created"); count != 1 {
		t.Fatalf("expected default admin to be logged exactly once, got %d logs:\n%s", count, logContent)
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

func TestInitializeMeshDoesNotCreateDefaultAdmin(t *testing.T) {
	dataDir := t.TempDir()
	cfg := config.Config{DataDir: dataDir, Mode: "mesh", LogLevel: "debug", LogFormat: "text"}

	initialized, err := Initialize(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}
	admins, err := initialized.AdminModule.ListAdmins(context.Background())
	if err != nil {
		t.Fatalf("ListAdmins() error = %v", err)
	}
	if err := initialized.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if len(admins) != 0 {
		t.Fatalf("expected no default admins in mesh mode, got %d", len(admins))
	}
	logContent := readFile(t, initialized.LogPath)
	if strings.Contains(logContent, "default standalone admin created") {
		t.Fatalf("did not expect standalone admin log in mesh mode, got:\n%s", logContent)
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

func extractLoggedPassword(t *testing.T, logContent string) string {
	t.Helper()
	re := regexp.MustCompile(`password=([^\s]+)`)
	match := re.FindStringSubmatch(logContent)
	if len(match) != 2 {
		return ""
	}
	return strings.Trim(match[1], `"`)
}
