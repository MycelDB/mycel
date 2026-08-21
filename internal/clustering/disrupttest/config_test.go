package disrupttest

import (
	"strings"
	"testing"
	"time"
)

func TestDefaultConfigIsDeterministic(t *testing.T) {
	now := time.Date(2026, 8, 20, 1, 2, 3, 0, time.UTC)
	cfg := DefaultConfig(now)
	if cfg.ClusterName != "mycel-rdt-20260820-010203" {
		t.Fatalf("cluster name = %q", cfg.ClusterName)
	}
	if cfg.Driver != "k3s" || cfg.Provisioner != "k3d" || cfg.Profile != "smoke" || cfg.NodeCount != 3 {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestValidateRequiresDestructiveConfirmation(t *testing.T) {
	cfg := DefaultConfig(time.Now())
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "confirm-destructive") {
		t.Fatalf("Validate() error = %v", err)
	}
	cfg.ConfirmDestructive = true
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() with confirmation error = %v", err)
	}
}

func TestValidateAllowsDryRunWithoutConfirmation(t *testing.T) {
	cfg := DefaultConfig(time.Now())
	cfg.DryRun = true
	if err := cfg.Validate(); err != nil {
		t.Fatalf("dry run Validate() error = %v", err)
	}
}

func TestValidateRejectsUnsupportedSelector(t *testing.T) {
	cfg := DefaultConfig(time.Now())
	cfg.ConfirmDestructive = true
	cfg.Selector = "component=myceld"
	assertValidationContains(t, cfg, "selector must be")
}

func TestValidateRejectsLongK3DClusterName(t *testing.T) {
	cfg := DefaultConfig(time.Now())
	cfg.ConfirmDestructive = true
	cfg.ClusterName = strings.Repeat("a", 33)
	assertValidationContains(t, cfg, "cluster name must be <= 32")
}

func assertValidationContains(t *testing.T, cfg Config, want string) {
	t.Helper()
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("Validate() error = %v, want substring %q", err, want)
	}
}

func TestResolveProfile(t *testing.T) {
	profile, err := ResolveProfile("small")
	if err != nil {
		t.Fatal(err)
	}
	if profile.Writers != 2 || profile.Rate != 20 || profile.Duration != 2*time.Minute {
		t.Fatalf("small profile = %+v", profile)
	}
	if _, err := ResolveProfile("huge"); err == nil {
		t.Fatal("expected unsupported profile error")
	}
}
