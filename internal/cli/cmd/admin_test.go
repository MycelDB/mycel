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

	daemonbackup "github.com/myceldb/mycel/internal/backup/service"
	daemonblob "github.com/myceldb/mycel/internal/blob/service"
	daemonapp "github.com/myceldb/mycel/internal/daemon/app"
	daemonconfig "github.com/myceldb/mycel/internal/daemon/config"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	"github.com/myceldb/mycel/internal/daemon/server"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
	graphnotification "github.com/myceldb/mycel/internal/graph/notification"
	daegraph "github.com/myceldb/mycel/internal/graph/service"
	identityservice "github.com/myceldb/mycel/internal/identity/service"
	inferenceservice "github.com/myceldb/mycel/internal/inference/service"
	daemonsemantic "github.com/myceldb/mycel/internal/semantic/service"
	daemonsession "github.com/myceldb/mycel/internal/session/service"
	daemonspace "github.com/myceldb/mycel/internal/space/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAdminListCommandJSONUsesGRPC(t *testing.T) {
	_, addr, password, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()

	out, err := runCLI(t, "--daemon-addr", addr, "--username", "admin", "--password", password, "--output", "json", "admin", "list")
	if err != nil {
		t.Fatalf("admin list failed: %v\n%s", err, out)
	}
	var admins []*adminv1.Principal
	if err := json.Unmarshal([]byte(out), &admins); err != nil {
		t.Fatalf("decode admin list output failed: %v\n%s", err, out)
	}
	var admin *adminv1.Principal
	for _, principal := range admins {
		if principal.GetUsername() == "admin" {
			admin = principal
			break
		}
	}
	if admin == nil {
		t.Fatalf("expected admin principal, got %#v", admins)
	}
	if admin.GetPrincipalId() == "" || admin.GetCreateTime().AsTime().IsZero() {
		t.Fatalf("unexpected admin operator: %#v", admin)
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
	var bob adminv1.Principal
	if err := json.Unmarshal([]byte(out), &bob); err != nil {
		t.Fatalf("decode create output: %v\n%s", err, out)
	}
	if bob.GetUsername() != "bob" || bob.GetEmail() != "bob@example.com" {
		t.Fatalf("unexpected created operator: %#v", &bob)
	}

	out, err = runCLI(t, append(base, "admin", "get", "--operator-id", bob.GetPrincipalId())...)
	if err != nil || !strings.Contains(out, "bob") {
		t.Fatalf("admin get failed: %v\n%s", err, out)
	}
	out, err = runCLI(t, append(base, "admin", "find", "--operator-username", "bob")...)
	if err != nil || !strings.Contains(out, "bob") {
		t.Fatalf("admin find failed: %v\n%s", err, out)
	}
	out, err = runCLI(t, append(base, "admin", "update", "--operator-id", bob.GetPrincipalId(), "--email", "new-bob@example.com")...)
	if err != nil || !strings.Contains(out, "new-bob@example.com") {
		t.Fatalf("admin update failed: %v\n%s", err, out)
	}
	out, err = runCLI(t, append(base, "admin", "disable", "--operator-id", bob.GetPrincipalId())...)
	if err != nil {
		t.Fatalf("admin disable failed: %v\n%s", err, out)
	}
	var disabled adminv1.Principal
	if err := json.Unmarshal([]byte(out), &disabled); err != nil || disabled.GetState() != adminv1.PrincipalState_PRINCIPAL_STATE_DISABLED {
		t.Fatalf("unexpected disabled output err=%v operator=%#v raw=%s", err, &disabled, out)
	}
	out, err = runCLI(t, append(base, "admin", "enable", "--operator-id", bob.GetPrincipalId())...)
	if err != nil {
		t.Fatalf("admin enable failed: %v\n%s", err, out)
	}
	var enabled adminv1.Principal
	if err := json.Unmarshal([]byte(out), &enabled); err != nil || enabled.GetState() != adminv1.PrincipalState_PRINCIPAL_STATE_ACTIVE {
		t.Fatalf("unexpected enabled output err=%v operator=%#v raw=%s", err, &enabled, out)
	}
	out, err = runCLI(t, append(base, "admin", "delete", "--operator-id", bob.GetPrincipalId())...)
	if err != nil {
		t.Fatalf("admin delete failed: %v\n%s", err, out)
	}
	var deleted adminv1.Principal
	if err := json.Unmarshal([]byte(out), &deleted); err != nil || deleted.GetState() != adminv1.PrincipalState_PRINCIPAL_STATE_DELETED {
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
	var carol adminv1.Principal
	if err := json.Unmarshal([]byte(out), &carol); err != nil {
		t.Fatalf("decode create output: %v\n%s", err, out)
	}

	out, err = runCLI(t, append(base, "admin", "role", "grant", "--operator-id", carol.GetPrincipalId(), "--role", "space-admin")...)
	if err != nil {
		t.Fatalf("role grant failed: %v\n%s", err, out)
	}
	var roleGrant adminv1.GrantPrincipalRoleResponse
	if err := json.Unmarshal([]byte(out), &roleGrant); err != nil {
		t.Fatalf("decode role grant: %v\n%s", err, out)
	}
	if roleGrant.GetGrant().GetRoleGrantId() == "" {
		t.Fatalf("expected role grant id: %#v", &roleGrant)
	}
	out, err = runCLI(t, append(base, "admin", "role", "list", "--operator-id", carol.GetPrincipalId())...)
	if err != nil {
		t.Fatalf("role list failed: %v\n%s", err, out)
	}
	var roles adminv1.ListPrincipalRolesResponse
	if err := json.Unmarshal([]byte(out), &roles); err != nil || len(roles.GetGrants()) != 1 || roles.GetGrants()[0].GetRole() != "space.admin" {
		t.Fatalf("unexpected roles output err=%v roles=%#v raw=%s", err, &roles, out)
	}
	out, err = runCLI(t, append(base, "admin", "role", "revoke", "--operator-id", carol.GetPrincipalId(), "--grant-id", roleGrant.GetGrant().GetRoleGrantId())...)
	if err != nil {
		t.Fatalf("role revoke failed: %v\n%s", err, out)
	}

	out, err = runCLI(t, append(base, "admin", "capability", "grant", "--operator-id", carol.GetPrincipalId(), "--capability", "identity-principal-update")...)
	if err != nil {
		t.Fatalf("capability grant failed: %v\n%s", err, out)
	}
	var capGrant adminv1.GrantPrincipalCapabilityResponse
	if err := json.Unmarshal([]byte(out), &capGrant); err != nil {
		t.Fatalf("decode capability grant: %v\n%s", err, out)
	}
	out, err = runCLI(t, append(base, "admin", "capability", "list", "--operator-id", carol.GetPrincipalId())...)
	if err != nil {
		t.Fatalf("capability list failed: %v\n%s", err, out)
	}
	var capabilities adminv1.ListPrincipalCapabilitiesResponse
	if err := json.Unmarshal([]byte(out), &capabilities); err != nil || len(capabilities.GetEffectiveCapabilities()) != 1 || capabilities.GetEffectiveCapabilities()[0] != commonv1.Capability_CAPABILITY_IDENTITY_PRINCIPAL_UPDATE {
		t.Fatalf("unexpected capability output err=%v capabilities=%#v raw=%s", err, &capabilities, out)
	}
	out, err = runCLI(t, append(base, "admin", "capability", "revoke", "--operator-id", carol.GetPrincipalId(), "--grant-id", capGrant.GetGrant().GetCapabilityGrantId())...)
	if err != nil {
		t.Fatalf("capability revoke failed: %v\n%s", err, out)
	}

	out, err = runCLI(t, append(base, "admin", "session", "list", "--operator-id", carol.GetPrincipalId())...)
	if err != nil || strings.TrimSpace(out) != "[]" {
		t.Fatalf("session list failed: %v\n%s", err, out)
	}
	out, err = runCLI(t, append(base, "admin", "session", "revoke", "--operator-id", carol.GetPrincipalId(), "--session-id", "missing")...)
	if err != nil {
		t.Fatalf("session revoke failed: %v\n%s", err, out)
	}
	out, err = runCLI(t, append(base, "admin", "session", "revoke-all", "--operator-id", carol.GetPrincipalId())...)
	if err != nil || strings.TrimSpace(out) != "{}" {
		t.Fatalf("session revoke-all failed: %v\n%s", err, out)
	}
}

func TestAdminBackupCommandsUseDaemonGRPC(t *testing.T) {
	dataDir, addr, password, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	backupDir := filepath.Join(filepath.Dir(dataDir), "cli-backups")
	base := []string{"--daemon-addr", addr, "--username", "admin", "--password", password, "--output", "json"}

	out, err := runCLI(t, append(base, "admin", "backup", "policy", "get")...)
	if err != nil {
		t.Fatalf("backup policy get failed: %v\n%s", err, out)
	}
	var policy adminv1.BackupPolicy
	if err := json.Unmarshal([]byte(out), &policy); err != nil {
		t.Fatalf("decode policy: %v\n%s", err, out)
	}
	if policy.GetCompression() != "zip" || policy.GetArchiveFormat() != adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_ZIP {
		t.Fatalf("unexpected default policy: %#v", &policy)
	}

	out, err = runCLI(t, append(base, "admin", "backup", "policy", "set", "--enabled", "--dir", backupDir, "--interval-hours", "1", "--keep", "2", "--archive-format", "zip", "--include-logs", "--schedule", "weekly", "--time-of-day", "22:00", "--timezone", "America/Toronto", "--weekday", "sun", "--weekday", "wed", "--run-missed")...)
	if err != nil {
		t.Fatalf("backup policy set failed: %v\n%s", err, out)
	}
	if err := json.Unmarshal([]byte(out), &policy); err != nil {
		t.Fatalf("decode updated policy: %v\n%s", err, out)
	}
	if !policy.GetEnabled() || policy.GetBackupDir() != backupDir || policy.GetIntervalHours() != 1 || policy.GetRetentionCount() != 2 || policy.GetArchiveFormat() != adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_ZIP || policy.GetCompression() != "zip" || !policy.GetIncludeLogs() || policy.GetScheduleKind() != "weekly" || policy.GetTimeOfDay() != "22:00" || policy.GetTimezone() != "America/Toronto" || len(policy.GetWeekdays()) != 2 || policy.GetWeekdays()[0] != 0 || policy.GetWeekdays()[1] != 3 || !policy.GetRunMissed() {
		t.Fatalf("unexpected updated policy: %#v", &policy)
	}

	out, err = runCLI(t, append(base, "admin", "backup", "trigger", "--reason", "cli-test")...)
	if err != nil {
		t.Fatalf("backup trigger failed: %v\n%s", err, out)
	}
	var trigger adminv1.TriggerBackupResponse
	if err := json.Unmarshal([]byte(out), &trigger); err != nil {
		t.Fatalf("decode trigger: %v\n%s", err, out)
	}
	backupID := trigger.GetBackup().GetBackupId()
	if backupID == "" || trigger.GetStatus().GetState() != "succeeded" {
		t.Fatalf("unexpected trigger response: %#v", &trigger)
	}

	out, err = runCLI(t, append(base, "admin", "backup", "status")...)
	if err != nil {
		t.Fatalf("backup status failed: %v\n%s", err, out)
	}
	var statusRes adminv1.GetBackupStatusResponse
	if err := json.Unmarshal([]byte(out), &statusRes); err != nil {
		t.Fatalf("decode status: %v\n%s", err, out)
	}
	if statusRes.GetStatus().GetBackupId() != backupID || statusRes.GetStatus().GetLastSuccessAt() == "" {
		t.Fatalf("unexpected status: %#v", &statusRes)
	}

	out, err = runCLI(t, append(base, "admin", "backup", "list")...)
	if err != nil {
		t.Fatalf("backup list failed: %v\n%s", err, out)
	}
	var backups adminv1.ListBackupsResponse
	if err := json.Unmarshal([]byte(out), &backups); err != nil {
		t.Fatalf("decode list: %v\n%s", err, out)
	}
	if len(backups.GetBackups()) != 1 || backups.GetBackups()[0].GetBackupId() != backupID {
		t.Fatalf("unexpected backups: %#v", &backups)
	}

	out, err = runCLI(t, append(base, "admin", "backup", "delete", backupID)...)
	if err != nil {
		t.Fatalf("backup delete failed: %v\n%s", err, out)
	}
	var deleted adminv1.DeleteBackupResponse
	if err := json.Unmarshal([]byte(out), &deleted); err != nil || deleted.GetBackupId() != backupID {
		t.Fatalf("unexpected delete output err=%v deleted=%#v raw=%s", err, &deleted, out)
	}
}

func TestBackupCLIArchiveFormatParsing(t *testing.T) {
	cases := map[string]adminv1.BackupArchiveFormat{
		"zip":     adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_ZIP,
		"TAR":     adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_TAR,
		"tar.gz":  adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_TAR_GZ,
		"tgz":     adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_TAR_GZ,
		"tar.zst": adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_TAR_ZST,
		"tzst":    adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_TAR_ZST,
	}
	for raw, want := range cases {
		got, err := parseBackupArchiveFormat(raw)
		if err != nil || got != want {
			t.Fatalf("parseBackupArchiveFormat(%q) = %v, %v; want %v, nil", raw, got, err, want)
		}
	}
	if _, err := parseBackupArchiveFormat("rar"); err == nil {
		t.Fatal("expected invalid archive format to fail")
	}
}

func TestBackupCLIWeekdayParsing(t *testing.T) {
	weekdays, err := parseBackupWeekdays([]string{"sun", "Wednesday", "3", "fri"})
	if err != nil {
		t.Fatalf("parseBackupWeekdays() error = %v", err)
	}
	if len(weekdays) != 3 || weekdays[0] != 0 || weekdays[1] != 3 || weekdays[2] != 5 {
		t.Fatalf("unexpected weekdays: %#v", weekdays)
	}
	if _, err := parseBackupWeekdays([]string{"noday"}); err == nil {
		t.Fatal("expected invalid weekday to fail")
	}
}

func TestBackupCLIErrorSurfacesUnavailableAsTemporary(t *testing.T) {
	err := backupCLIError("trigger backup", status.Error(codes.Unavailable, "service is quiesced"))
	if err == nil || !strings.Contains(err.Error(), "temporarily unavailable") {
		t.Fatalf("unexpected backupCLIError: %v", err)
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
	var alice adminv1.Principal
	if err := json.Unmarshal([]byte(out), &alice); err != nil {
		t.Fatalf("decode user add output: %v\n%s", err, out)
	}
	if alice.GetPrincipalId() == "" || alice.GetUsername() != "alice" || alice.GetState() != adminv1.PrincipalState_PRINCIPAL_STATE_ACTIVE {
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
	out, err = runCLI(t, append(base, "user", "password", "set", "--user-id", alice.GetPrincipalId(), "--new-password", "new-alice-pass")...)
	if err != nil || !strings.Contains(out, "alice") || strings.Contains(out, "new-alice-pass") {
		t.Fatalf("user password set failed/leaked: %v\n%s", err, out)
	}
	out, err = runCLI(t, append(base, "user", "disable", "--user-id", alice.GetPrincipalId())...)
	if err != nil {
		t.Fatalf("user disable failed: %v\n%s", err, out)
	}
	var disabled adminv1.Principal
	if err := json.Unmarshal([]byte(out), &disabled); err != nil || disabled.GetState() != adminv1.PrincipalState_PRINCIPAL_STATE_DISABLED {
		t.Fatalf("unexpected disabled user err=%v user=%#v raw=%s", err, &disabled, out)
	}
	out, err = runCLI(t, append(base, "user", "enable", "--user-id", alice.GetPrincipalId())...)
	if err != nil {
		t.Fatalf("user enable failed: %v\n%s", err, out)
	}
	var enabled adminv1.Principal
	if err := json.Unmarshal([]byte(out), &enabled); err != nil || enabled.GetState() != adminv1.PrincipalState_PRINCIPAL_STATE_ACTIVE {
		t.Fatalf("unexpected enabled user err=%v user=%#v raw=%s", err, &enabled, out)
	}
	out, err = runCLI(t, append(base, "user", "delete", alice.GetPrincipalId())...)
	if err != nil {
		t.Fatalf("user delete failed: %v\n%s", err, out)
	}
	var deleted adminv1.Principal
	if err := json.Unmarshal([]byte(out), &deleted); err != nil || deleted.GetState() != adminv1.PrincipalState_PRINCIPAL_STATE_DELETED {
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
	var bob adminv1.Principal
	if err := json.Unmarshal([]byte(out), &bob); err != nil {
		t.Fatalf("decode user add output: %v\n%s", err, out)
	}
	out, err = runCLI(t, append(base, "user", "session", "list", "--user-id", bob.GetPrincipalId())...)
	if err != nil || strings.TrimSpace(out) != "[]" {
		t.Fatalf("user session list failed: %v\n%s", err, out)
	}
	out, err = runCLI(t, append(base, "user", "session", "revoke-all", "--user-id", bob.GetPrincipalId())...)
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
	rt, err := daemonapp.Initialize(context.Background(), daemonconfig.Config{DataDir: dataDir, Mode: "standalone", LogLevel: "debug", LogFormat: "text", GRPCAddr: "127.0.0.1:0", NodeName: "node-a", Cluster: daemonconfig.ClusterConfig{Name: "dev", BackendAdvertiseAddr: "127.0.0.1:9093"}})
	if err != nil {
		t.Fatalf("initialize daemon admin store failed: %v", err)
	}
	principalModule, ok := daemonruntime.ServiceAs[*identityservice.PrincipalModule](rt, identityservice.PrincipalModuleName)
	if !ok {
		t.Fatal("identity service was not registered")
	}
	spaceModule, ok := daemonruntime.ServiceAs[*daemonspace.Module](rt, daemonspace.ModuleName)
	if !ok {
		t.Fatal("space service was not registered")
	}
	sessionModule, ok := daemonruntime.ServiceAs[*daemonsession.Module](rt, daemonsession.ModuleName)
	if !ok {
		t.Fatal("session service was not registered")
	}
	graphModule, ok := daemonruntime.ServiceAs[*daegraph.Module](rt, daegraph.ModuleName)
	if !ok {
		t.Fatal("graph service was not registered")
	}
	graphNotificationModule, ok := daemonruntime.ServiceAs[*graphnotification.Module](rt, graphnotification.ModuleName)
	if !ok {
		t.Fatal("graph change notification service was not registered")
	}
	blobModule, ok := daemonruntime.ServiceAs[*daemonblob.Module](rt, daemonblob.ModuleName)
	if !ok {
		t.Fatal("blob service was not registered")
	}
	semanticModule, ok := daemonruntime.ServiceAs[*daemonsemantic.Module](rt, daemonsemantic.ModuleName)
	if !ok {
		t.Fatal("semantic service was not registered")
	}
	inferenceModule, ok := daemonruntime.ServiceAs[*inferenceservice.Module](rt, inferenceservice.ModuleName)
	if !ok {
		t.Fatal("inference service was not registered")
	}
	backupModule, ok := daemonruntime.ServiceAs[*daemonbackup.Module](rt, daemonbackup.ModuleName)
	if !ok {
		t.Fatal("backup service was not registered")
	}
	password := bootstrapPasswordFromLog(t, rt.LogPath)
	ctx, cancel := context.WithCancel(context.Background())
	srv, errCh, err := server.Start(ctx, server.Config{Addr: "127.0.0.1:0", PrincipalManager: principalModule, BackupManager: backupModule, SpaceManager: spaceModule, SessionManager: sessionModule, GraphManager: graphModule, GraphChangeManager: graphNotificationModule, BlobManager: blobModule, InferenceManager: inferenceModule, SemanticManager: semanticModule, Logger: rt.Logger, Quiesce: rt.Quiesce, ClusteringManager: rt.ClusterManager, ClusteringServer: rt.ClusterManager.BackendService()})
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
