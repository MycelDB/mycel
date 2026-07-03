package cmd

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	adminv1 "github.com/myceldb/mycel/gen/go/mycel/admin/v1"
	daemonapp "github.com/myceldb/mycel/internal/daemon/app"
	daemonconfig "github.com/myceldb/mycel/internal/daemon/config"
	"github.com/myceldb/mycel/internal/daemon/server"
)

func TestAdminListCommandJSONUsesGRPC(t *testing.T) {
	_, addr, password, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()

	out, err := runCLI(t, "--daemon-addr", addr, "--username", "admin", "--password", password, "--output", "json", "admin", "list")
	if err != nil {
		t.Fatalf("admin list failed: %v\n%s", err, out)
	}
	var admins []*adminv1.Operator
	if err := json.Unmarshal([]byte(out), &admins); err != nil {
		t.Fatalf("decode admin list output failed: %v\n%s", err, out)
	}
	if len(admins) != 1 {
		t.Fatalf("expected 1 admin, got %#v", admins)
	}
	if admins[0].GetUsername() != "admin" || admins[0].GetOperatorId() == "" || admins[0].GetCreateTime().AsTime().IsZero() {
		t.Fatalf("unexpected admin operator: %#v", admins[0])
	}
	if strings.Contains(out, "password") || strings.Contains(out, "hash") {
		t.Fatalf("admin list leaked password/hash material: %s", out)
	}
}

func TestAdminPasswordSetCommandChangesPassword(t *testing.T) {
	_, addr, password, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()

	out, err := runCLI(t, "--daemon-addr", addr, "--username", "admin", "--password", password, "admin", "password", "set", "--new-password", "new-cli-password")
	if err != nil {
		t.Fatalf("admin password set failed: %v\n%s", err, out)
	}
	if !strings.Contains(out, "admin password changed: admin") {
		t.Fatalf("unexpected password set output: %s", out)
	}
	if out, err := runCLI(t, "--daemon-addr", addr, "--username", "admin", "--password", password, "--output", "json", "admin", "list"); err == nil {
		t.Fatalf("expected old password to fail after password change, got output %s", out)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "--username", "admin", "--password", "new-cli-password", "--output", "json", "admin", "list")
	if err != nil {
		t.Fatalf("expected new password to work: %v\n%s", err, out)
	}
	if !strings.Contains(out, "admin") {
		t.Fatalf("expected admin in output, got %s", out)
	}
}

func TestAdminPasswordSetCommandRequiresNewPassword(t *testing.T) {
	_, addr, password, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	out, err := runCLI(t, "--daemon-addr", addr, "--username", "admin", "--password", password, "admin", "password", "set")
	if err == nil {
		t.Fatalf("expected password set to require new password, got output %s", out)
	}
	if !strings.Contains(err.Error(), "--new-password is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAdminListCommandRequiresCredentials(t *testing.T) {
	_, addr, _, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	out, err := runCLI(t, "--daemon-addr", addr, "--output", "json", "admin", "list")
	if err == nil {
		t.Fatalf("expected admin list to require credentials, got output %s", out)
	}
	if !strings.Contains(err.Error(), "--username/-u and --password/-p") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAdminListCommandRejectsBadPassword(t *testing.T) {
	_, addr, _, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	out, err := runCLI(t, "--daemon-addr", addr, "--username", "admin", "--password", "wrong", "--output", "json", "admin", "list")
	if err == nil {
		t.Fatalf("expected admin list to reject bad password, got output %s", out)
	}
}

func TestAdminListCommandUsesMyceldGRPCAddr(t *testing.T) {
	_, addr, password, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	t.Setenv("MYCELD_GRPC_ADDR", addr)

	out, err := runCLI(t, "--username", "admin", "--password", password, "--output", "json", "admin", "list")
	if err != nil {
		t.Fatalf("admin list failed: %v\n%s", err, out)
	}
	if !strings.Contains(out, "admin") {
		t.Fatalf("expected admin in output, got %s", out)
	}
}

func TestAdminListCommandFailsWhenDaemonUnavailable(t *testing.T) {
	out, err := runCLI(t, "--daemon-addr", "127.0.0.1:1", "--username", "admin", "--password", "pass", "--output", "json", "admin", "list")
	if err == nil {
		t.Fatalf("expected admin list to fail when daemon is unavailable, got output %s", out)
	}
}

func startDaemonAdminGRPC(t *testing.T) (string, string, string, func()) {
	t.Helper()
	dataDir := filepath.Join(t.TempDir(), "myceld")
	initialized, err := daemonapp.Initialize(context.Background(), daemonconfig.Config{DataDir: dataDir, Mode: "standalone", LogLevel: "debug", LogFormat: "text", GRPCAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("initialize daemon admin store failed: %v", err)
	}
	password := bootstrapPasswordFromLog(t, initialized.LogPath)
	ctx, cancel := context.WithCancel(context.Background())
	srv, errCh, err := server.Start(ctx, server.Config{Addr: "127.0.0.1:0", AdminLister: initialized.AdminModule, AdminAuthenticator: initialized.AdminModule, PasswordManager: initialized.AdminModule, Logger: initialized.Runtime.Logger})
	if err != nil {
		_ = initialized.Close()
		t.Fatalf("start grpc server failed: %v", err)
	}
	cleanup := func() {
		cancel()
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("grpc server stopped with error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for grpc server shutdown")
		}
		if err := initialized.Close(); err != nil {
			t.Fatalf("close daemon init failed: %v", err)
		}
	}
	return dataDir, srv.Addr(), password, cleanup
}

func bootstrapPasswordFromLog(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read bootstrap log: %v", err)
	}
	re := regexp.MustCompile(`password=([^\s]+)`)
	match := re.FindStringSubmatch(string(data))
	if len(match) != 2 {
		t.Fatalf("bootstrap password not found in log:\n%s", data)
	}
	return strings.Trim(match[1], `"`)
}
