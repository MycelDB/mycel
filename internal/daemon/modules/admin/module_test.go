package admin

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/myceldb/mycel/internal/daemon/config"
	"github.com/myceldb/mycel/internal/daemon/quiesce"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestModuleInitStandaloneCreatesDefaultAdminAndLogsCredentials(t *testing.T) {
	dataDir := t.TempDir()
	var logs bytes.Buffer
	module := initModule(t, dataDir, "standalone", &logs)

	summaries, err := module.ListAdmins(context.Background())
	if err != nil {
		t.Fatalf("ListAdmins() error = %v", err)
	}
	if len(summaries) != 1 {
		t.Fatalf("expected 1 admin summary, got %d", len(summaries))
	}
	if summaries[0].Username != "admin" {
		t.Fatalf("expected default admin username, got %q", summaries[0].Username)
	}
	if summaries[0].ID == "" || summaries[0].CreatedAt.IsZero() {
		t.Fatalf("expected admin summary identity and timestamp, got %+v", summaries[0])
	}
	assertAdminSummaryDoesNotExposePasswordHash(t)

	admins := listPersistedAdmins(t, module)
	if len(admins) != 1 {
		t.Fatalf("expected 1 persisted admin, got %d", len(admins))
	}
	if admins[0].PasswordHash == "" || strings.Contains(admins[0].PasswordHash, "admin") {
		t.Fatalf("expected non-empty password hash without plaintext username, got %q", admins[0].PasswordHash)
	}

	assertDir(t, filepath.Join(dataDir, "admins"))
	assertFile(t, filepath.Join(dataDir, "admins", StoreFilename))

	logContent := logs.String()
	if !strings.Contains(logContent, "default standalone admin created") {
		t.Fatalf("expected default admin creation log, got:\n%s", logContent)
	}
	if !strings.Contains(logContent, "username=admin") {
		t.Fatalf("expected admin username in log, got:\n%s", logContent)
	}
	if !strings.Contains(logContent, "change this password immediately") || !strings.Contains(logContent, "change_password_required=true") {
		t.Fatalf("expected change-password warning in log, got:\n%s", logContent)
	}
	password := extractLoggedPassword(t, logContent)
	if password == "" {
		t.Fatalf("expected logged password, got:\n%s", logContent)
	}
	if err := VerifyPassword(admins[0].PasswordHash, password); err != nil {
		t.Fatalf("logged password does not match stored hash: %v", err)
	}
	storeContent := readFile(t, filepath.Join(dataDir, "admins", StoreFilename))
	if strings.Contains(storeContent, password) {
		t.Fatalf("admin store contains plaintext password %q", password)
	}
}

func TestModuleInitStandaloneUsesConfiguredBootstrapAdminCredentials(t *testing.T) {
	dataDir := t.TempDir()
	var logs bytes.Buffer
	module := initModuleWithConfig(t, config.Config{DataDir: dataDir, Mode: "standalone", LogLevel: "debug", LogFormat: "text", BootstrapAdminUsername: "compose-admin", BootstrapAdminPassword: "compose-password"}, &logs)

	summaries, err := module.ListAdmins(context.Background())
	if err != nil {
		t.Fatalf("ListAdmins() error = %v", err)
	}
	if len(summaries) != 1 || summaries[0].Username != "compose-admin" {
		t.Fatalf("expected configured bootstrap admin, got %+v", summaries)
	}
	if _, err := module.AuthenticateOperator(context.Background(), "compose-admin", "compose-password"); err != nil {
		t.Fatalf("expected configured bootstrap credentials to authenticate: %v", err)
	}
	admins := listPersistedAdmins(t, module)
	if len(admins) != 1 {
		t.Fatalf("expected 1 persisted admin, got %d", len(admins))
	}
	if err := VerifyPassword(admins[0].PasswordHash, "compose-password"); err != nil {
		t.Fatalf("stored password hash does not match configured bootstrap password: %v", err)
	}

	logContent := logs.String()
	if !strings.Contains(logContent, "default standalone admin created from configured bootstrap credentials") {
		t.Fatalf("expected configured bootstrap log, got:\n%s", logContent)
	}
	if !strings.Contains(logContent, "username=compose-admin") || !strings.Contains(logContent, "password_configured=true") {
		t.Fatalf("expected configured username/password marker in log, got:\n%s", logContent)
	}
	if strings.Contains(logContent, "compose-password") || extractLoggedPassword(t, logContent) != "" {
		t.Fatalf("configured bootstrap password must not be logged, got:\n%s", logContent)
	}
	storeContent := readFile(t, filepath.Join(dataDir, "admins", StoreFilename))
	if strings.Contains(storeContent, "compose-password") {
		t.Fatalf("admin store contains plaintext configured password")
	}
}

func TestModuleInitStandaloneDoesNotOverwriteExistingAdminWithBootstrapCredentials(t *testing.T) {
	dataDir := t.TempDir()
	var logs bytes.Buffer
	first := initModuleWithConfig(t, config.Config{DataDir: dataDir, Mode: "standalone", LogLevel: "debug", LogFormat: "text", BootstrapAdminUsername: "initial-admin", BootstrapAdminPassword: "initial-password"}, &logs)
	firstAdmins, err := first.ListAdmins(context.Background())
	if err != nil || len(firstAdmins) != 1 {
		t.Fatalf("first ListAdmins() = %v, %v", firstAdmins, err)
	}

	second := initModuleWithConfig(t, config.Config{DataDir: dataDir, Mode: "standalone", LogLevel: "debug", LogFormat: "text", BootstrapAdminUsername: "replacement-admin", BootstrapAdminPassword: "replacement-password"}, &logs)
	secondAdmins, err := second.ListAdmins(context.Background())
	if err != nil || len(secondAdmins) != 1 {
		t.Fatalf("second ListAdmins() = %v, %v", secondAdmins, err)
	}
	if secondAdmins[0].ID != firstAdmins[0].ID || secondAdmins[0].Username != "initial-admin" {
		t.Fatalf("expected existing admin to remain unchanged, first=%+v second=%+v", firstAdmins[0], secondAdmins[0])
	}
	if _, err := second.AuthenticateOperator(context.Background(), "initial-admin", "initial-password"); err != nil {
		t.Fatalf("expected original credentials to remain valid: %v", err)
	}
	if _, err := second.AuthenticateOperator(context.Background(), "replacement-admin", "replacement-password"); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("expected replacement bootstrap credentials to be ignored, got %v", err)
	}
}

func TestModuleInitStandalonePromotesExistingAdminWhenNoSystemAdmin(t *testing.T) {
	dataDir := t.TempDir()
	adminDir := filepath.Join(dataDir, "admins")
	if err := os.MkdirAll(adminDir, 0o700); err != nil {
		t.Fatalf("mkdir admin dir: %v", err)
	}
	store, _, err := OpenStore(adminDir)
	if err != nil {
		t.Fatalf("OpenStore() error = %v", err)
	}
	hash, err := HashPassword("old-pass")
	if err != nil {
		t.Fatalf("HashPassword() error = %v", err)
	}
	if err := store.Create(context.Background(), Admin{ID: "old-admin", Username: "admin", PasswordHash: hash}); err != nil {
		t.Fatalf("Create() old admin error = %v", err)
	}

	var logs bytes.Buffer
	module := initModule(t, dataDir, "standalone", &logs)
	admins, err := module.ListAdmins(context.Background())
	if err != nil || len(admins) != 1 {
		t.Fatalf("ListAdmins() = %v, %v", admins, err)
	}
	if len(admins[0].RoleGrants) != 1 || admins[0].RoleGrants[0].Role != OperatorRoleSystemAdmin {
		t.Fatalf("expected existing admin to be promoted to system admin, got %+v", admins[0])
	}
	if !strings.Contains(logs.String(), "existing standalone admin promoted to system admin") {
		t.Fatalf("expected migration warning, got logs:\n%s", logs.String())
	}
}

func TestModuleAuthenticateOperator(t *testing.T) {
	dataDir := t.TempDir()
	var logs bytes.Buffer
	module := initModule(t, dataDir, "standalone", &logs)
	password := extractLoggedPassword(t, logs.String())

	admin, err := module.AuthenticateOperator(context.Background(), "admin", password)
	if err != nil {
		t.Fatalf("AuthenticateOperator() error = %v", err)
	}
	if admin.Username != "admin" || admin.ID == "" || admin.CreatedAt.IsZero() {
		t.Fatalf("unexpected authenticated admin: %+v", admin)
	}
}

func TestModuleQuiesceRejectsSetOperatorPassword(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	var logs bytes.Buffer
	module := initModule(t, dataDir, "standalone", &logs)
	admins, err := module.ListAdmins(ctx)
	if err != nil || len(admins) != 1 {
		t.Fatalf("ListAdmins() = %v, %v", admins, err)
	}
	lease, err := module.gate.Quiesce(ctx, quiesce.Request{Reason: "test backup", Source: "test"})
	if err != nil {
		t.Fatalf("Quiesce() error = %v", err)
	}
	defer lease.Release(ctx)
	_, err = module.SetOperatorPassword(ctx, admins[0].ID, "blocked-password")
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("SetOperatorPassword() code = %v, want %v (err=%v)", status.Code(err), codes.Unavailable, err)
	}
}

func TestModuleSetOperatorPassword(t *testing.T) {
	dataDir := t.TempDir()
	var logs bytes.Buffer
	module := initModule(t, dataDir, "standalone", &logs)
	oldPassword := extractLoggedPassword(t, logs.String())
	admin, err := module.AuthenticateOperator(context.Background(), "admin", oldPassword)
	if err != nil {
		t.Fatalf("AuthenticateOperator() error = %v", err)
	}
	updated, err := module.SetOperatorPassword(context.Background(), admin.ID, "new-password")
	if err != nil {
		t.Fatalf("SetOperatorPassword() error = %v", err)
	}
	if updated.ID != admin.ID || updated.Username != "admin" {
		t.Fatalf("unexpected updated admin: %+v", updated)
	}
	if _, err := module.AuthenticateOperator(context.Background(), "admin", oldPassword); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("expected old password to fail, got %v", err)
	}
	if _, err := module.AuthenticateOperator(context.Background(), "admin", "new-password"); err != nil {
		t.Fatalf("expected new password to authenticate: %v", err)
	}
	storeContent := readFile(t, filepath.Join(dataDir, "admins", StoreFilename))
	if strings.Contains(storeContent, "new-password") || strings.Contains(storeContent, oldPassword) {
		t.Fatalf("admin store contains plaintext password material: %s", storeContent)
	}
}

func TestModuleAuthenticateOperatorRejectsInvalidCredentials(t *testing.T) {
	dataDir := t.TempDir()
	var logs bytes.Buffer
	module := initModule(t, dataDir, "standalone", &logs)
	if _, err := module.AuthenticateOperator(context.Background(), "admin", "wrong"); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("expected ErrInvalidCredentials for bad password, got %v", err)
	}
	if _, err := module.AuthenticateOperator(context.Background(), "missing", extractLoggedPassword(t, logs.String())); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("expected ErrInvalidCredentials for missing user, got %v", err)
	}
}

func TestModuleRejectsRemovingLastActiveSystemAdmin(t *testing.T) {
	dataDir := t.TempDir()
	var logs bytes.Buffer
	module := initModule(t, dataDir, "standalone", &logs)
	admins, err := module.ListAdmins(context.Background())
	if err != nil || len(admins) != 1 {
		t.Fatalf("ListAdmins() = %v, %v", admins, err)
	}
	adminID := admins[0].ID
	roleGrantID := admins[0].RoleGrants[0].ID

	if _, err := module.DisableOperator(context.Background(), adminID); !errors.Is(err, ErrLastSystemAdmin) {
		t.Fatalf("expected disable last system admin to fail, got %v", err)
	}
	if _, err := module.DeleteOperator(context.Background(), adminID); !errors.Is(err, ErrLastSystemAdmin) {
		t.Fatalf("expected delete last system admin to fail, got %v", err)
	}
	if _, err := module.RevokeRole(context.Background(), adminID, roleGrantID); !errors.Is(err, ErrLastSystemAdmin) {
		t.Fatalf("expected revoke last system admin role to fail, got %v", err)
	}
}

func TestModuleAllowsRemovingSystemAdminWhenAnotherActiveSystemAdminExists(t *testing.T) {
	dataDir := t.TempDir()
	var logs bytes.Buffer
	module := initModule(t, dataDir, "standalone", &logs)
	admins, err := module.ListAdmins(context.Background())
	if err != nil || len(admins) != 1 {
		t.Fatalf("ListAdmins() = %v, %v", admins, err)
	}
	bootstrap := admins[0]

	second, err := module.CreateOperator(context.Background(), CreateOperatorInput{Username: "second", Password: "second-pass", Roles: []RoleGrant{{Role: OperatorRoleSystemAdmin, Scope: systemScope(), GrantedByOperatorID: bootstrap.ID}}})
	if err != nil {
		t.Fatalf("CreateOperator() error = %v", err)
	}
	if _, err := module.DisableOperator(context.Background(), bootstrap.ID); err != nil {
		t.Fatalf("expected bootstrap disable to succeed while second system admin is active: %v", err)
	}
	if _, err := module.DeleteOperator(context.Background(), second.ID); !errors.Is(err, ErrLastSystemAdmin) {
		t.Fatalf("expected deleting remaining active system admin to fail, got %v", err)
	}
}

func TestModuleInitStandaloneIsIdempotent(t *testing.T) {
	dataDir := t.TempDir()
	var logs bytes.Buffer

	first := initModule(t, dataDir, "standalone", &logs)
	firstAdmins, err := first.ListAdmins(context.Background())
	if err != nil {
		t.Fatalf("first ListAdmins() error = %v", err)
	}

	second := initModule(t, dataDir, "standalone", &logs)
	secondAdmins, err := second.ListAdmins(context.Background())
	if err != nil {
		t.Fatalf("second ListAdmins() error = %v", err)
	}

	if len(firstAdmins) != 1 || len(secondAdmins) != 1 {
		t.Fatalf("expected one admin after both initializations, got first=%d second=%d", len(firstAdmins), len(secondAdmins))
	}
	if firstAdmins[0].ID != secondAdmins[0].ID {
		t.Fatalf("expected same admin after reinitialize, got %q and %q", firstAdmins[0].ID, secondAdmins[0].ID)
	}
	logContent := logs.String()
	if count := strings.Count(logContent, "default standalone admin created"); count != 1 {
		t.Fatalf("expected default admin to be logged exactly once, got %d logs:\n%s", count, logContent)
	}
}

func TestModuleInitMeshDoesNotCreateDefaultAdmin(t *testing.T) {
	dataDir := t.TempDir()
	var logs bytes.Buffer
	module := initModule(t, dataDir, "mesh", &logs)

	admins, err := module.ListAdmins(context.Background())
	if err != nil {
		t.Fatalf("ListAdmins() error = %v", err)
	}
	if len(admins) != 0 {
		t.Fatalf("expected no default admins in mesh mode, got %d", len(admins))
	}
	assertDir(t, filepath.Join(dataDir, "admins"))
	assertFile(t, filepath.Join(dataDir, "admins", StoreFilename))
	logContent := logs.String()
	if strings.Contains(logContent, "default standalone admin created") {
		t.Fatalf("did not expect standalone admin log in mesh mode, got:\n%s", logContent)
	}
}

func listPersistedAdmins(t *testing.T, module *Module) []Admin {
	t.Helper()
	admins, err := module.store.List(context.Background())
	if err != nil {
		t.Fatalf("store.List() error = %v", err)
	}
	return admins
}

func assertAdminSummaryDoesNotExposePasswordHash(t *testing.T) {
	t.Helper()
	if _, ok := reflect.TypeOf(AdminSummary{}).FieldByName("PasswordHash"); ok {
		t.Fatalf("AdminSummary must not expose PasswordHash")
	}
}

func initModule(t *testing.T, dataDir, mode string, logs *bytes.Buffer) *Module {
	t.Helper()
	return initModuleWithConfig(t, config.Config{DataDir: dataDir, Mode: mode, LogLevel: "debug", LogFormat: "text"}, logs)
}

func initModuleWithConfig(t *testing.T, cfg config.Config, logs *bytes.Buffer) *Module {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	rt := &daemonruntime.Runtime{
		Config: cfg,
		Logger: logger,
	}
	module := NewModule()
	result := module.Init(context.Background(), rt)
	if !result.OK {
		t.Fatalf("Init() result = %+v", result.Error)
	}
	return module
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
