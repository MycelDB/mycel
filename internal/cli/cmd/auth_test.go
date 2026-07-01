package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	mycelengine "github.com/myceldb/mycel/engine"
	"github.com/myceldb/mycel/internal/cli/app"
)

func TestAuthSessionListCommandJSONOmitsTokenMaterial(t *testing.T) {
	dataDir, sessions := setupAuthSessionCLIData(t, 2)
	out, err := runCLI(t, "--data-dir", dataDir, "--username", "admin@example.com", "--password", "change-me-now", "--output", "json", "auth", "session", "list")
	if err != nil {
		t.Fatalf("auth session list failed: %v", err)
	}
	var listed []mycelengine.RefreshSessionInfo
	if err := json.Unmarshal([]byte(out), &listed); err != nil {
		t.Fatalf("decode list output failed: %v\n%s", err, out)
	}
	if len(listed) != 2 {
		t.Fatalf("expected 2 sessions, got %#v", listed)
	}
	if strings.Contains(out, "refresh_token_hash") || strings.Contains(out, string(sessions[0].RefreshToken)) || strings.Contains(out, string(sessions[1].RefreshToken)) {
		t.Fatalf("session list leaked token material: %s", out)
	}
}

func TestAuthSessionRevokeCommand(t *testing.T) {
	dataDir, sessions := setupAuthSessionCLIData(t, 2)
	out, err := runCLI(t, "--data-dir", dataDir, "--username", "admin@example.com", "--password", "change-me-now", "--output", "json", "auth", "session", "revoke", sessions[0].RefreshSession.ID.String(), "--reason", "cli revoke")
	if err != nil {
		t.Fatalf("auth session revoke failed: %v", err)
	}
	if !strings.Contains(out, "\"revoked\": true") {
		t.Fatalf("expected revoke confirmation, got %s", out)
	}

	eng := reopenEngine(t, dataDir)
	defer eng.Close()
	if _, err := eng.RefreshSession(context.Background(), mycelengine.RefreshSessionInput{RefreshToken: sessions[0].RefreshToken}); err == nil {
		t.Fatal("expected revoked refresh token to fail")
	}
	if _, err := eng.RefreshSession(context.Background(), mycelengine.RefreshSessionInput{RefreshToken: sessions[1].RefreshToken}); err != nil {
		t.Fatalf("expected other refresh token to remain valid: %v", err)
	}
}

func TestAuthSessionRevokeOtherCommand(t *testing.T) {
	dataDir, sessions := setupAuthSessionCLIData(t, 3)
	out, err := runCLI(t, "--data-dir", dataDir, "--username", "admin@example.com", "--password", "change-me-now", "--output", "json", "auth", "session", "revoke-other", "--current-session-id", sessions[2].RefreshSession.ID.String(), "--reason", "cli revoke other")
	if err != nil {
		t.Fatalf("auth session revoke-other failed: %v", err)
	}
	if !strings.Contains(out, "\"revoked_count\": 2") {
		t.Fatalf("expected revoke-other count, got %s", out)
	}

	eng := reopenEngine(t, dataDir)
	defer eng.Close()
	if _, err := eng.RefreshSession(context.Background(), mycelengine.RefreshSessionInput{RefreshToken: sessions[0].RefreshToken}); err == nil {
		t.Fatal("expected first refresh token to fail")
	}
	if _, err := eng.RefreshSession(context.Background(), mycelengine.RefreshSessionInput{RefreshToken: sessions[1].RefreshToken}); err == nil {
		t.Fatal("expected second refresh token to fail")
	}
	if _, err := eng.RefreshSession(context.Background(), mycelengine.RefreshSessionInput{RefreshToken: sessions[2].RefreshToken}); err != nil {
		t.Fatalf("expected current refresh token to remain valid: %v", err)
	}
}

func TestAuthSessionCleanupCommand(t *testing.T) {
	dataDir, _ := setupAuthSessionCLIData(t, 1)
	out, err := runCLI(t, "--data-dir", dataDir, "--username", "admin@example.com", "--password", "change-me-now", "--output", "json", "auth", "session", "cleanup")
	if err != nil {
		t.Fatalf("auth session cleanup failed: %v", err)
	}
	var result mycelengine.CleanupRefreshSessionsResult
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("decode cleanup output failed: %v\n%s", err, out)
	}
	if result.ChangedCount != 0 {
		t.Fatalf("expected no cleanup changes for fresh sessions, got %d", result.ChangedCount)
	}
}

func setupAuthSessionCLIData(t *testing.T, count int) (string, []mycelengine.LoginSessionResult) {
	t.Helper()
	dataDir := filepath.Join(t.TempDir(), "mycel-cli-auth-session")
	eng, err := mycelengine.NewEngine(mycelengine.EngineConfig{DataDir: dataDir, Mode: mycelengine.EngineModeStandalone, CreateIfMissing: true, AdminUsername: "admin@example.com", AdminPassword: "change-me-now", RefreshAuditRetentionTTL: time.Hour}, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("create engine failed: %v", err)
	}
	sessions := make([]mycelengine.LoginSessionResult, 0, count)
	for i := 0; i < count; i++ {
		res, err := eng.LoginSession(context.Background(), mycelengine.LoginSessionInput{UserRef: "admin@example.com", Password: "change-me-now", Metadata: mycelengine.RefreshSessionMetadata{ClientName: "test-cli"}})
		if err != nil {
			_ = eng.Close()
			t.Fatalf("login session %d failed: %v", i, err)
		}
		sessions = append(sessions, res)
	}
	if err := eng.Close(); err != nil {
		t.Fatalf("close engine failed: %v", err)
	}
	return dataDir, sessions
}

func reopenEngine(t *testing.T, dataDir string) mycelengine.Engine {
	t.Helper()
	eng, err := mycelengine.NewEngine(mycelengine.EngineConfig{DataDir: dataDir, Mode: mycelengine.EngineModeStandalone, CreateIfMissing: false}, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("reopen engine failed: %v", err)
	}
	return eng
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
