package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/pflag"
)

func TestLoadDefaults(t *testing.T) {
	clearConfigEnv(t)

	cfg, err := Load(Options{})
	if err != nil {
		t.Fatalf("load defaults failed: %v", err)
	}
	if cfg.Output != "text" || cfg.AccessTokenTTL != time.Hour || cfg.BlobStaleTmpAge != time.Hour {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if cfg.RefreshIdleTTL != 720*time.Hour || cfg.RefreshAbsoluteTTL != 2160*time.Hour || cfg.RefreshAuditRetentionTTL != 720*time.Hour || cfg.RefreshTokenBytes != 32 {
		t.Fatalf("unexpected refresh defaults: %+v", cfg)
	}
	if cfg.BlobLimits.MaxSizeBytes != -1 || cfg.BlobLimits.MaxPDFBytes != -1 {
		t.Fatalf("expected unlimited blob defaults, got %+v", cfg.BlobLimits)
	}
	if cfg.AdvancedSemanticEnabled {
		t.Fatal("advanced semantic support must default to disabled during phase 0")
	}
}

func TestLoadPrecedenceFileEnvFlags(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("MYCELDB_DATA_DIR", "/from-env")
	t.Setenv("MYCELDB_AUTH_ACCESS_TOKEN_TTL", "45m")
	t.Setenv("MYCELDB_AUTH_REFRESH_IDLE_TTL", "24h")
	configPath := filepath.Join(t.TempDir(), "mycel.yaml")
	if err := os.WriteFile(configPath, []byte(`
data_dir: /from-file
output: json
auth:
  access_token_ttl: 30m
  refresh_idle_ttl: 48h
  refresh_absolute_ttl: 96h
  refresh_audit_retention_ttl: 12h
  refresh_token_bytes: 40
storage:
  blobs:
    max_size_bytes: 100
    max_pdf_bytes: 80
    mime_type_limits:
      application/pdf: 60
`), 0o600); err != nil {
		t.Fatalf("write config failed: %v", err)
	}
	flags := testFlags(t,
		"--config", configPath,
		"--data-dir", "/from-flag",
		"--auth-token-ttl", "15m",
		"--auth-refresh-absolute-ttl", "72h",
		"--auth-refresh-token-bytes", "48",
		"--blob-max-pdf-bytes", "50",
		"--semantic-advanced-enabled",
	)

	cfg, err := Load(Options{Flags: flags})
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if cfg.DataDir != "/from-flag" {
		t.Fatalf("expected flag data dir, got %q", cfg.DataDir)
	}
	if cfg.AccessTokenTTL != 15*time.Minute {
		t.Fatalf("expected flag token ttl, got %s", cfg.AccessTokenTTL)
	}
	if cfg.RefreshIdleTTL != 24*time.Hour {
		t.Fatalf("expected env refresh idle ttl, got %s", cfg.RefreshIdleTTL)
	}
	if cfg.RefreshAbsoluteTTL != 72*time.Hour {
		t.Fatalf("expected flag refresh absolute ttl, got %s", cfg.RefreshAbsoluteTTL)
	}
	if cfg.RefreshAuditRetentionTTL != 12*time.Hour {
		t.Fatalf("expected file refresh audit retention ttl, got %s", cfg.RefreshAuditRetentionTTL)
	}
	if cfg.RefreshTokenBytes != 48 {
		t.Fatalf("expected flag refresh token bytes, got %d", cfg.RefreshTokenBytes)
	}
	if cfg.Output != "json" {
		t.Fatalf("expected file output json, got %q", cfg.Output)
	}
	if cfg.BlobLimits.MaxPDFBytes != 50 || cfg.BlobLimits.MimeTypeLimits["application/pdf"] != 60 {
		t.Fatalf("unexpected blob limits: %+v", cfg.BlobLimits)
	}
	if !cfg.AdvancedSemanticEnabled {
		t.Fatal("expected semantic advanced flag override")
	}
}

func TestLoadRejectsBlobLimitAboveGlobalCap(t *testing.T) {
	clearConfigEnv(t)
	flags := testFlags(t, "--blob-max-size-bytes", "10", "--blob-max-image-bytes", "11")
	if _, err := Load(Options{Flags: flags}); err == nil {
		t.Fatal("expected validation error")
	}
}

func TestLoadRejectsInvalidRefreshConfig(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "idle ttl", args: []string{"--auth-refresh-idle-ttl", "0s"}},
		{name: "absolute ttl", args: []string{"--auth-refresh-absolute-ttl", "0s"}},
		{name: "absolute before idle", args: []string{"--auth-refresh-idle-ttl", "2h", "--auth-refresh-absolute-ttl", "1h"}},
		{name: "audit retention ttl", args: []string{"--auth-refresh-audit-retention-ttl", "0s"}},
		{name: "token bytes", args: []string{"--auth-refresh-token-bytes", "31"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearConfigEnv(t)
			if _, err := Load(Options{Flags: testFlags(t, tt.args...)}); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func clearConfigEnv(t *testing.T) {
	t.Helper()
	envNames := []string{
		EnvConfig,
		"MYCELDB_DATA_DIR",
		"MYCELDB_AUTH_ACCESS_TOKEN_TTL",
		"MYCELDB_AUTH_REFRESH_IDLE_TTL",
		"MYCELDB_AUTH_REFRESH_ABSOLUTE_TTL",
		"MYCELDB_AUTH_REFRESH_AUDIT_RETENTION_TTL",
		"MYCELDB_AUTH_REFRESH_TOKEN_BYTES",
	}
	for _, name := range envNames {
		t.Setenv(name, "")
	}
}

func testFlags(t *testing.T, args ...string) *pflag.FlagSet {
	t.Helper()
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	flags.String("config", "", "")
	flags.String("data-dir", "", "")
	flags.String("output", "", "")
	flags.String("username", "", "")
	flags.String("password", "", "")
	flags.String("user-store-encryption-key-b64", "", "")
	flags.String("auth-token-ttl", "", "")
	flags.String("auth-refresh-idle-ttl", "", "")
	flags.String("auth-refresh-absolute-ttl", "", "")
	flags.String("auth-refresh-audit-retention-ttl", "", "")
	flags.Int("auth-refresh-token-bytes", 0, "")
	flags.String("blob-stale-tmp-age", "", "")
	flags.Int64("blob-max-size-bytes", 0, "")
	flags.Int64("blob-max-image-bytes", 0, "")
	flags.Int64("blob-max-pdf-bytes", 0, "")
	flags.Int64("blob-max-audio-bytes", 0, "")
	flags.Int64("blob-max-video-bytes", 0, "")
	flags.Int64("blob-max-other-bytes", 0, "")
	flags.Bool("semantic-advanced-enabled", false, "")
	if err := flags.Parse(args); err != nil {
		t.Fatalf("parse flags failed: %v", err)
	}
	return flags
}
