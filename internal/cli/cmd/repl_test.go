package cmd

import (
	"bytes"
	"strings"
	"testing"

	"github.com/myceldb/mycel/internal/cli/app"
)

func TestREPLLoginUsesDaemon(t *testing.T) {
	_, addr, adminPassword, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	createTestUser(t, addr, adminPassword, "repl-user", "repl-pass")

	a := &app.App{DaemonAddr: addr}
	input := strings.NewReader("login repl-user repl-pass\nexit\n")
	var out bytes.Buffer
	if err := RunREPL(t.Context(), a, input, &out); err != nil {
		t.Fatalf("RunREPL() error = %v\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "logged in as repl-user") {
		t.Fatalf("expected daemon login output, got:\n%s", out.String())
	}
}

func TestREPLSpaceSetUsesDaemon(t *testing.T) {
	_, addr, adminPassword, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	createTestUser(t, addr, adminPassword, "repl-space", "repl-pass")
	spaceID := createTemplateTestSpace(t, addr, adminPassword, "repl-space")

	a := &app.App{DaemonAddr: addr}
	input := strings.NewReader("login repl-space repl-pass\nspace set " + spaceID + "\nexit\n")
	var out bytes.Buffer
	if err := RunREPL(t.Context(), a, input, &out); err != nil {
		t.Fatalf("RunREPL() error = %v\n%s", err, out.String())
	}
	if a.CurrentSpaceID == nil || a.CurrentSpaceID.String() != spaceID {
		t.Fatalf("expected current space %s, got %v; output:\n%s", spaceID, a.CurrentSpaceID, out.String())
	}
}
