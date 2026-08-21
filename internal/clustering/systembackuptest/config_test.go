package systembackuptest

import (
	"strings"
	"testing"
	"time"
)

func TestDefaultConfigValid(t *testing.T) {
	cfg := DefaultConfig(time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC))
	cfg.ConfirmDestructive = true
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if cfg.ClusterName != "mycel-sbr-20260820-120000" {
		t.Fatalf("cluster name = %q", cfg.ClusterName)
	}
}

func TestValidateRequiresDestructiveConfirmation(t *testing.T) {
	cfg := DefaultConfig(time.Now())
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "--confirm-destructive") {
		t.Fatalf("Validate() error = %v", err)
	}
	cfg.DryRun = true
	if err := cfg.Validate(); err != nil {
		t.Fatalf("dry-run Validate() error = %v", err)
	}
}

func TestValidateRejectsNonTarArchiveFormat(t *testing.T) {
	cfg := DefaultConfig(time.Now())
	cfg.ConfirmDestructive = true
	cfg.ArchiveFormat = "tar.zst"
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "supports tar") {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestResolveProfile(t *testing.T) {
	profile, err := ResolveProfile("backup-smoke")
	if err != nil {
		t.Fatal(err)
	}
	if profile.Writes <= 0 {
		t.Fatalf("profile writes = %d", profile.Writes)
	}
	if _, err := ResolveProfile("unknown"); err == nil {
		t.Fatal("expected unsupported profile error")
	}
}
