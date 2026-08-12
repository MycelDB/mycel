package cmd

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/myceldb/mycel/internal/cli/app"
	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
)

func TestAuthLoginWhoAmIAndSessionListUseDaemonGRPC(t *testing.T) {
	_, addr, adminPassword, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	createTestUser(t, addr, adminPassword, "alice", "alice-pass")

	out, err := runCLI(t, "--daemon-addr", addr, "--username", "alice", "--password", "alice-pass", "--output", "json", "auth", "login")
	if err != nil {
		t.Fatalf("auth login failed: %v\n%s", err, out)
	}
	var login commonv1.LoginResponse
	if err := json.Unmarshal([]byte(out), &login); err != nil {
		t.Fatalf("decode login output: %v\n%s", err, out)
	}
	if login.GetAccessToken() == "" || login.GetRefreshToken() == "" || login.GetPrincipal().GetUsername() != "alice" {
		t.Fatalf("unexpected login response: %#v", &login)
	}

	out, err = runCLI(t, "--daemon-addr", addr, "--username", "alice", "--password", "alice-pass", "--output", "json", "auth", "whoami")
	if err != nil || !strings.Contains(out, "alice") {
		t.Fatalf("auth whoami failed: %v\n%s", err, out)
	}

	out, err = runCLI(t, "--daemon-addr", addr, "--username", "alice", "--password", "alice-pass", "--output", "json", "auth", "session", "list")
	if err != nil {
		t.Fatalf("auth session list failed: %v\n%s", err, out)
	}
	var sessions []*commonv1.AuthSessionSummary
	if err := json.Unmarshal([]byte(out), &sessions); err != nil {
		t.Fatalf("decode session list: %v\n%s", err, out)
	}
	if len(sessions) == 0 {
		t.Fatalf("expected at least one session, got %s", out)
	}
	if strings.Contains(out, login.GetRefreshToken()) || strings.Contains(out, "refresh_token_hash") {
		t.Fatalf("session list leaked token material: %s", out)
	}
}

func TestAuthRefreshAndRevokeOtherUseDaemonGRPC(t *testing.T) {
	_, addr, adminPassword, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	createTestUser(t, addr, adminPassword, "bob", "bob-pass")

	out, err := runCLI(t, "--daemon-addr", addr, "--username", "bob", "--password", "bob-pass", "--output", "json", "auth", "login")
	if err != nil {
		t.Fatalf("auth login failed: %v\n%s", err, out)
	}
	var login commonv1.LoginResponse
	if err := json.Unmarshal([]byte(out), &login); err != nil {
		t.Fatalf("decode login output: %v\n%s", err, out)
	}

	out, err = runCLI(t, "--daemon-addr", addr, "--output", "json", "auth", "refresh", "--refresh-token", login.GetRefreshToken())
	if err != nil {
		t.Fatalf("auth refresh failed: %v\n%s", err, out)
	}
	var refreshed commonv1.RefreshResponse
	if err := json.Unmarshal([]byte(out), &refreshed); err != nil {
		t.Fatalf("decode refresh output: %v\n%s", err, out)
	}
	if refreshed.GetRefreshToken() == "" || refreshed.GetRefreshToken() == login.GetRefreshToken() || refreshed.GetPrincipal().GetUsername() != "bob" {
		t.Fatalf("unexpected refresh response: %#v", &refreshed)
	}

	out, err = runCLI(t, "--daemon-addr", addr, "--username", "bob", "--password", "bob-pass", "--output", "json", "auth", "session", "revoke-other")
	if err != nil {
		t.Fatalf("auth revoke-other failed: %v\n%s", err, out)
	}
	if !strings.Contains(out, "revoked_count") && strings.TrimSpace(out) != "{}" {
		t.Fatalf("unexpected revoke-other output: %s", out)
	}
}

func TestAuthSessionCleanupUnavailableOverDaemon(t *testing.T) {
	out, err := runCLI(t, "auth", "session", "cleanup")
	if err == nil {
		t.Fatalf("expected cleanup to be unavailable, got %s", out)
	}
	if !strings.Contains(err.Error(), "not available over daemon gRPC") {
		t.Fatalf("unexpected cleanup error: %v", err)
	}
}

func createTestUser(t *testing.T, addr, adminPassword, username, password string) {
	t.Helper()
	out, err := runCLI(t, "--daemon-addr", addr, "--username", "admin", "--password", adminPassword, "user", "add", "--user-username", username, "--new-password", password)
	if err != nil {
		t.Fatalf("create test user failed: %v\n%s", err, out)
	}
}

func runCLI(t *testing.T, args ...string) (string, error) {
	t.Helper()
	a := &app.App{}
	cmd := NewRootCommand(a, false)
	cmd.SetArgs(args)
	var errBuf bytes.Buffer
	cmd.SetErr(&errBuf)
	out, err := captureStdout(func() error { return cmd.Execute() })
	if err != nil && errBuf.Len() > 0 {
		return out + errBuf.String(), err
	}
	return out, err
}

func captureStdout(fn func() error) (string, error) {
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		return "", err
	}
	os.Stdout = w
	defer func() { os.Stdout = old }()

	runErr := fn()
	_ = w.Close()
	var buf bytes.Buffer
	_, copyErr := io.Copy(&buf, r)
	_ = r.Close()
	if runErr != nil {
		return buf.String(), runErr
	}
	return buf.String(), copyErr
}
