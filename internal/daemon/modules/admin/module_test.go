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
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
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
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	rt := &daemonruntime.Runtime{
		Config: config.Config{DataDir: dataDir, Mode: mode, LogLevel: "debug", LogFormat: "text"},
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
