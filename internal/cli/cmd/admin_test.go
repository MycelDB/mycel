package cmd

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	daemonapp "github.com/myceldb/mycel/internal/daemon/app"
	daemonconfig "github.com/myceldb/mycel/internal/daemon/config"
	daemonadmin "github.com/myceldb/mycel/internal/daemon/modules/admin"
)

func TestAdminListCommandJSON(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "myceld")
	initialized, err := daemonapp.Initialize(context.Background(), daemonconfig.Config{DataDir: dataDir, Mode: "standalone", LogLevel: "debug", LogFormat: "text"})
	if err != nil {
		t.Fatalf("initialize daemon admin store failed: %v", err)
	}
	if err := initialized.Close(); err != nil {
		t.Fatalf("close daemon init failed: %v", err)
	}

	out, err := runCLI(t, "--data-dir", dataDir, "--output", "json", "admin", "list")
	if err != nil {
		t.Fatalf("admin list failed: %v\n%s", err, out)
	}
	var admins []daemonadmin.AdminSummary
	if err := json.Unmarshal([]byte(out), &admins); err != nil {
		t.Fatalf("decode admin list output failed: %v\n%s", err, out)
	}
	if len(admins) != 1 {
		t.Fatalf("expected 1 admin, got %#v", admins)
	}
	if admins[0].Username != "admin" || admins[0].ID == "" || admins[0].CreatedAt.IsZero() {
		t.Fatalf("unexpected admin summary: %#v", admins[0])
	}
	if strings.Contains(out, "password") || strings.Contains(out, "hash") {
		t.Fatalf("admin list leaked password/hash material: %s", out)
	}
}

func TestAdminListCommandUsesMyceldDataDir(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "myceld")
	t.Setenv("MYCELD_DATA_DIR", dataDir)
	initialized, err := daemonapp.Initialize(context.Background(), daemonconfig.Config{DataDir: dataDir, Mode: "standalone", LogLevel: "debug", LogFormat: "text"})
	if err != nil {
		t.Fatalf("initialize daemon admin store failed: %v", err)
	}
	if err := initialized.Close(); err != nil {
		t.Fatalf("close daemon init failed: %v", err)
	}

	out, err := runCLI(t, "--output", "json", "admin", "list")
	if err != nil {
		t.Fatalf("admin list failed: %v\n%s", err, out)
	}
	if !strings.Contains(out, "admin") {
		t.Fatalf("expected admin in output, got %s", out)
	}
}

func TestAdminListCommandDoesNotInitializeStore(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "uninitialized-myceld")
	out, err := runCLI(t, "--data-dir", dataDir, "--output", "json", "admin", "list")
	if err == nil {
		t.Fatalf("expected admin list to fail for uninitialized store, got output %s", out)
	}
	if _, statErr := os.Stat(filepath.Join(dataDir, "admins", daemonadmin.StoreFilename)); !os.IsNotExist(statErr) {
		t.Fatalf("admin list should not create admin store, stat err = %v", statErr)
	}
}
