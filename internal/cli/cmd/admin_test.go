package cmd

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	adminv1 "github.com/myceldb/mycel/gen/go/mycel/admin/v1"
	daemonapp "github.com/myceldb/mycel/internal/daemon/app"
	daemonconfig "github.com/myceldb/mycel/internal/daemon/config"
	"github.com/myceldb/mycel/internal/daemon/server"
)

func TestAdminListCommandJSONUsesGRPC(t *testing.T) {
	_, addr, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()

	out, err := runCLI(t, "--daemon-addr", addr, "--output", "json", "admin", "list")
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

func TestAdminListCommandUsesMyceldGRPCAddr(t *testing.T) {
	_, addr, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	t.Setenv("MYCELD_GRPC_ADDR", addr)

	out, err := runCLI(t, "--output", "json", "admin", "list")
	if err != nil {
		t.Fatalf("admin list failed: %v\n%s", err, out)
	}
	if !strings.Contains(out, "admin") {
		t.Fatalf("expected admin in output, got %s", out)
	}
}

func TestAdminListCommandFailsWhenDaemonUnavailable(t *testing.T) {
	out, err := runCLI(t, "--daemon-addr", "127.0.0.1:1", "--output", "json", "admin", "list")
	if err == nil {
		t.Fatalf("expected admin list to fail when daemon is unavailable, got output %s", out)
	}
}

func startDaemonAdminGRPC(t *testing.T) (string, string, func()) {
	t.Helper()
	dataDir := filepath.Join(t.TempDir(), "myceld")
	initialized, err := daemonapp.Initialize(context.Background(), daemonconfig.Config{DataDir: dataDir, Mode: "standalone", LogLevel: "debug", LogFormat: "text", GRPCAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("initialize daemon admin store failed: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	srv, errCh, err := server.Start(ctx, server.Config{Addr: "127.0.0.1:0", AdminLister: initialized.AdminModule, Logger: initialized.Runtime.Logger})
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
	return dataDir, srv.Addr(), cleanup
}
