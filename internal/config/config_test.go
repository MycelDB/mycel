package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/pflag"
)

func TestLoadDefaults(t *testing.T) {
	t.Setenv(EnvConfig, "")
	t.Setenv("MYCELDB_DATA_DIR", "")

	cfg, err := Load(Options{})
	if err != nil {
		t.Fatalf("load defaults failed: %v", err)
	}
	if cfg.Output != "text" || cfg.AccessTokenTTL != time.Hour || cfg.BlobStaleTmpAge != time.Hour {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if cfg.BlobLimits.MaxSizeBytes != -1 || cfg.BlobLimits.MaxPDFBytes != -1 {
		t.Fatalf("expected unlimited blob defaults, got %+v", cfg.BlobLimits)
	}
	if cfg.AdvancedSemanticEnabled {
		t.Fatal("advanced semantic support must default to disabled during phase 0")
	}
}

func TestLoadPrecedenceFileEnvFlags(t *testing.T) {
	t.Setenv(EnvConfig, "")
	t.Setenv("MYCELDB_DATA_DIR", "/from-env")
	t.Setenv("MYCELDB_AUTH_ACCESS_TOKEN_TTL", "45m")
	configPath := filepath.Join(t.TempDir(), "mycel.yaml")
	if err := os.WriteFile(configPath, []byte(`
data_dir: /from-file
output: json
auth:
  access_token_ttl: 30m
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
	t.Setenv(EnvConfig, "")
	flags := testFlags(t, "--blob-max-size-bytes", "10", "--blob-max-image-bytes", "11")
	if _, err := Load(Options{Flags: flags}); err == nil {
		t.Fatal("expected validation error")
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
