package app

import (
	"testing"

	"github.com/myceldb/mycel/internal/daemon/config"
)

func TestAutomationProviderFromConfigNone(t *testing.T) {
	p, err := automationProviderFromConfig(config.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if p != nil {
		t.Fatalf("provider = %#v, want nil", p)
	}
}

func TestAutomationProviderFromConfigFake(t *testing.T) {
	p, err := automationProviderFromConfig(config.Config{Automation: config.AutomationConfig{Provider: "fake"}})
	if err != nil {
		t.Fatal(err)
	}
	if p == nil {
		t.Fatal("expected provider")
	}
}

func TestAutomationProviderFromConfigRejectsUnknown(t *testing.T) {
	_, err := automationProviderFromConfig(config.Config{Automation: config.AutomationConfig{Provider: "bogus"}})
	if err == nil {
		t.Fatal("expected error")
	}
}
