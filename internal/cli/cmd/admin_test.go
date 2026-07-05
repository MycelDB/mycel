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

	adminv1 "github.com/myceldb/mycel-api/gen/go/mycel/admin/v1"
	daemonapp "github.com/myceldb/mycel/internal/daemon/app"
	daemonconfig "github.com/myceldb/mycel/internal/daemon/config"
	daemonadmin "github.com/myceldb/mycel/internal/daemon/modules/admin"
	daemonblob "github.com/myceldb/mycel/internal/daemon/modules/blob"
	daemonchange "github.com/myceldb/mycel/internal/daemon/modules/changestream"
	daegraph "github.com/myceldb/mycel/internal/daemon/modules/graph"
	daemonsemantic "github.com/myceldb/mycel/internal/daemon/modules/semantic"
	daemonsession "github.com/myceldb/mycel/internal/daemon/modules/session"
	daemonspace "github.com/myceldb/mycel/internal/daemon/modules/space"
	daemonuser "github.com/myceldb/mycel/internal/daemon/modules/user"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
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

func TestAdminCRUDCommands(t *testing.T) {
	_, addr, password, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	base := []string{"--daemon-addr", addr, "--username", "admin", "--password", password, "--output", "json"}

	out, err := runCLI(t, append(base, "admin", "create", "--operator-username", "bob", "--new-password", "bob-pass", "--email", "bob@example.com", "--role", "user-admin")...)
	if err != nil {
		t.Fatalf("admin create failed: %v\n%s", err, out)
	}
	var bob adminv1.Operator
	if err := json.Unmarshal([]byte(out), &bob); err != nil {
		t.Fatalf("decode create output: %v\n%s", err, out)
	}
	if bob.GetUsername() != "bob" || bob.GetEmail() != "bob@example.com" {
		t.Fatalf("unexpected created operator: %#v", &bob)
	}

	out, err = runCLI(t, append(base, "admin", "get", "--operator-id", bob.GetOperatorId())...)
	if err != nil || !strings.Contains(out, "bob") {
		t.Fatalf("admin get failed: %v\n%s", err, out)
	}
	out, err = runCLI(t, append(base, "admin", "find", "--operator-username", "bob")...)
	if err != nil || !strings.Contains(out, "bob") {
		t.Fatalf("admin find failed: %v\n%s", err, out)
	}
	out, err = runCLI(t, append(base, "admin", "update", "--operator-id", bob.GetOperatorId(), "--email", "new-bob@example.com")...)
	if err != nil || !strings.Contains(out, "new-bob@example.com") {
		t.Fatalf("admin update failed: %v\n%s", err, out)
	}
	out, err = runCLI(t, append(base, "admin", "disable", "--operator-id", bob.GetOperatorId())...)
	if err != nil {
		t.Fatalf("admin disable failed: %v\n%s", err, out)
	}
	var disabled adminv1.Operator
	if err := json.Unmarshal([]byte(out), &disabled); err != nil || disabled.GetState() != adminv1.OperatorState_OPERATOR_STATE_DISABLED {
		t.Fatalf("unexpected disabled output err=%v operator=%#v raw=%s", err, &disabled, out)
	}
	out, err = runCLI(t, append(base, "admin", "enable", "--operator-id", bob.GetOperatorId())...)
	if err != nil {
		t.Fatalf("admin enable failed: %v\n%s", err, out)
	}
	var enabled adminv1.Operator
	if err := json.Unmarshal([]byte(out), &enabled); err != nil || enabled.GetState() != adminv1.OperatorState_OPERATOR_STATE_ACTIVE {
		t.Fatalf("unexpected enabled output err=%v operator=%#v raw=%s", err, &enabled, out)
	}
	out, err = runCLI(t, append(base, "admin", "delete", "--operator-id", bob.GetOperatorId())...)
	if err != nil {
		t.Fatalf("admin delete failed: %v\n%s", err, out)
	}
	var deleted adminv1.Operator
	if err := json.Unmarshal([]byte(out), &deleted); err != nil || deleted.GetState() != adminv1.OperatorState_OPERATOR_STATE_DELETED {
		t.Fatalf("unexpected delete output err=%v operator=%#v raw=%s", err, &deleted, out)
	}
}

func TestAdminGrantAndSessionCommands(t *testing.T) {
	_, addr, password, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	base := []string{"--daemon-addr", addr, "--username", "admin", "--password", password, "--output", "json"}

	out, err := runCLI(t, append(base, "admin", "create", "--operator-username", "carol", "--new-password", "carol-pass")...)
	if err != nil {
		t.Fatalf("admin create failed: %v\n%s", err, out)
	}
	var carol adminv1.Operator
	if err := json.Unmarshal([]byte(out), &carol); err != nil {
		t.Fatalf("decode create output: %v\n%s", err, out)
	}

	out, err = runCLI(t, append(base, "admin", "role", "grant", "--operator-id", carol.GetOperatorId(), "--role", "space-admin")...)
	if err != nil {
		t.Fatalf("role grant failed: %v\n%s", err, out)
	}
	var roleGrant adminv1.GrantOperatorRoleResponse
	if err := json.Unmarshal([]byte(out), &roleGrant); err != nil {
		t.Fatalf("decode role grant: %v\n%s", err, out)
	}
	if roleGrant.GetGrant().GetRoleGrantId() == "" {
		t.Fatalf("expected role grant id: %#v", &roleGrant)
	}
	out, err = runCLI(t, append(base, "admin", "role", "list", "--operator-id", carol.GetOperatorId())...)
	if err != nil {
		t.Fatalf("role list failed: %v\n%s", err, out)
	}
	var roles adminv1.ListOperatorRolesResponse
	if err := json.Unmarshal([]byte(out), &roles); err != nil || len(roles.GetGrants()) != 1 || roles.GetGrants()[0].GetRole() != adminv1.OperatorRole_OPERATOR_ROLE_SPACE_ADMIN {
		t.Fatalf("unexpected roles output err=%v roles=%#v raw=%s", err, &roles, out)
	}
	out, err = runCLI(t, append(base, "admin", "role", "revoke", "--operator-id", carol.GetOperatorId(), "--grant-id", roleGrant.GetGrant().GetRoleGrantId())...)
	if err != nil {
		t.Fatalf("role revoke failed: %v\n%s", err, out)
	}

	out, err = runCLI(t, append(base, "admin", "capability", "grant", "--operator-id", carol.GetOperatorId(), "--capability", "operator-manage")...)
	if err != nil {
		t.Fatalf("capability grant failed: %v\n%s", err, out)
	}
	var capGrant adminv1.GrantOperatorCapabilityResponse
	if err := json.Unmarshal([]byte(out), &capGrant); err != nil {
		t.Fatalf("decode capability grant: %v\n%s", err, out)
	}
	out, err = runCLI(t, append(base, "admin", "capability", "list", "--operator-id", carol.GetOperatorId())...)
	if err != nil {
		t.Fatalf("capability list failed: %v\n%s", err, out)
	}
	var capabilities adminv1.ListOperatorCapabilitiesResponse
	if err := json.Unmarshal([]byte(out), &capabilities); err != nil || len(capabilities.GetEffectiveCapabilities()) != 1 || capabilities.GetEffectiveCapabilities()[0] != 143 {
		t.Fatalf("unexpected capability output err=%v capabilities=%#v raw=%s", err, &capabilities, out)
	}
	out, err = runCLI(t, append(base, "admin", "capability", "revoke", "--operator-id", carol.GetOperatorId(), "--grant-id", capGrant.GetGrant().GetCapabilityGrantId())...)
	if err != nil {
		t.Fatalf("capability revoke failed: %v\n%s", err, out)
	}

	out, err = runCLI(t, append(base, "admin", "session", "list", "--operator-id", carol.GetOperatorId())...)
	if err != nil || strings.TrimSpace(out) != "[]" {
		t.Fatalf("session list failed: %v\n%s", err, out)
	}
	out, err = runCLI(t, append(base, "admin", "session", "revoke", "--operator-id", carol.GetOperatorId(), "--session-id", "missing")...)
	if err != nil {
		t.Fatalf("session revoke failed: %v\n%s", err, out)
	}
	out, err = runCLI(t, append(base, "admin", "session", "revoke-all", "--operator-id", carol.GetOperatorId())...)
	if err != nil || strings.TrimSpace(out) != "{}" {
		t.Fatalf("session revoke-all failed: %v\n%s", err, out)
	}
}

func TestUserCommandsUseDaemonGRPC(t *testing.T) {
	_, addr, password, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	base := []string{"--daemon-addr", addr, "--username", "admin", "--password", password, "--output", "json"}

	out, err := runCLI(t, append(base, "user", "add", "--user-username", "alice", "--new-password", "alice-pass")...)
	if err != nil {
		t.Fatalf("user add failed: %v\n%s", err, out)
	}
	var alice adminv1.User
	if err := json.Unmarshal([]byte(out), &alice); err != nil {
		t.Fatalf("decode user add output: %v\n%s", err, out)
	}
	if alice.GetUserId() == "" || alice.GetUsername() != "alice" || alice.GetState() != adminv1.UserState_USER_STATE_ACTIVE {
		t.Fatalf("unexpected created user: %#v", &alice)
	}
	if strings.Contains(out, "alice-pass") || strings.Contains(out, "password_hash") {
		t.Fatalf("user add leaked password material: %s", out)
	}

	out, err = runCLI(t, append(base, "user", "list")...)
	if err != nil || !strings.Contains(out, "alice") {
		t.Fatalf("user list failed: %v\n%s", err, out)
	}
	out, err = runCLI(t, append(base, "user", "find", "--user-username", "alice")...)
	if err != nil || !strings.Contains(out, "alice") {
		t.Fatalf("user find failed: %v\n%s", err, out)
	}
	out, err = runCLI(t, append(base, "user", "password", "set", "--user-id", alice.GetUserId(), "--new-password", "new-alice-pass")...)
	if err != nil || !strings.Contains(out, "alice") || strings.Contains(out, "new-alice-pass") {
		t.Fatalf("user password set failed/leaked: %v\n%s", err, out)
	}
	out, err = runCLI(t, append(base, "user", "disable", "--user-id", alice.GetUserId())...)
	if err != nil {
		t.Fatalf("user disable failed: %v\n%s", err, out)
	}
	var disabled adminv1.User
	if err := json.Unmarshal([]byte(out), &disabled); err != nil || disabled.GetState() != adminv1.UserState_USER_STATE_DISABLED {
		t.Fatalf("unexpected disabled user err=%v user=%#v raw=%s", err, &disabled, out)
	}
	out, err = runCLI(t, append(base, "user", "enable", "--user-id", alice.GetUserId())...)
	if err != nil {
		t.Fatalf("user enable failed: %v\n%s", err, out)
	}
	var enabled adminv1.User
	if err := json.Unmarshal([]byte(out), &enabled); err != nil || enabled.GetState() != adminv1.UserState_USER_STATE_ACTIVE {
		t.Fatalf("unexpected enabled user err=%v user=%#v raw=%s", err, &enabled, out)
	}
	out, err = runCLI(t, append(base, "user", "delete", alice.GetUserId())...)
	if err != nil {
		t.Fatalf("user delete failed: %v\n%s", err, out)
	}
	var deleted adminv1.User
	if err := json.Unmarshal([]byte(out), &deleted); err != nil || deleted.GetState() != adminv1.UserState_USER_STATE_DELETED {
		t.Fatalf("unexpected deleted user err=%v user=%#v raw=%s", err, &deleted, out)
	}
}

func TestUserSessionCommandsUseDaemonGRPC(t *testing.T) {
	_, addr, password, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	base := []string{"--daemon-addr", addr, "--username", "admin", "--password", password, "--output", "json"}
	out, err := runCLI(t, append(base, "user", "add", "--ref", "bob", "--new-password", "bob-pass")...)
	if err != nil {
		t.Fatalf("user add failed: %v\n%s", err, out)
	}
	var bob adminv1.User
	if err := json.Unmarshal([]byte(out), &bob); err != nil {
		t.Fatalf("decode user add output: %v\n%s", err, out)
	}
	out, err = runCLI(t, append(base, "user", "session", "list", "--user-id", bob.GetUserId())...)
	if err != nil || strings.TrimSpace(out) != "[]" {
		t.Fatalf("user session list failed: %v\n%s", err, out)
	}
	out, err = runCLI(t, append(base, "user", "session", "revoke-all", "--user-id", bob.GetUserId())...)
	if err != nil || strings.TrimSpace(out) != "{}" {
		t.Fatalf("user session revoke-all failed: %v\n%s", err, out)
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
	rt, err := daemonapp.Initialize(context.Background(), daemonconfig.Config{DataDir: dataDir, Mode: "standalone", LogLevel: "debug", LogFormat: "text", GRPCAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("initialize daemon admin store failed: %v", err)
	}
	adminModule, ok := daemonruntime.ModuleAs[*daemonadmin.Module](rt, daemonadmin.ModuleName)
	if !ok {
		t.Fatal("admin module was not registered")
	}
	userModule, ok := daemonruntime.ModuleAs[*daemonuser.Module](rt, daemonuser.ModuleName)
	if !ok {
		t.Fatal("user module was not registered")
	}
	spaceModule, ok := daemonruntime.ModuleAs[*daemonspace.Module](rt, daemonspace.ModuleName)
	if !ok {
		t.Fatal("space module was not registered")
	}
	sessionModule, ok := daemonruntime.ModuleAs[*daemonsession.Module](rt, daemonsession.ModuleName)
	if !ok {
		t.Fatal("session module was not registered")
	}
	graphModule, ok := daemonruntime.ModuleAs[*daegraph.Module](rt, daegraph.ModuleName)
	if !ok {
		t.Fatal("graph module was not registered")
	}
	blobModule, ok := daemonruntime.ModuleAs[*daemonblob.Module](rt, daemonblob.ModuleName)
	if !ok {
		t.Fatal("blob module was not registered")
	}
	semanticModule, ok := daemonruntime.ModuleAs[*daemonsemantic.Module](rt, daemonsemantic.ModuleName)
	if !ok {
		t.Fatal("semantic module was not registered")
	}
	changeModule, ok := daemonruntime.ModuleAs[*daemonchange.Module](rt, daemonchange.ModuleName)
	if !ok {
		t.Fatal("change stream module was not registered")
	}
	password := bootstrapPasswordFromLog(t, rt.LogPath)
	ctx, cancel := context.WithCancel(context.Background())
	srv, errCh, err := server.Start(ctx, server.Config{Addr: "127.0.0.1:0", AdminLister: adminModule, AdminAuthenticator: adminModule, OperatorManager: adminModule, UserManager: userModule, SpaceManager: spaceModule, SessionManager: sessionModule, GraphManager: graphModule, BlobManager: blobModule, SemanticManager: semanticModule, ChangeManager: changeModule, Logger: rt.Logger})
	if err != nil {
		_ = rt.Close()
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
		if err := rt.Close(); err != nil {
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
