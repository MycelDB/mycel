package app

import (
	"fmt"
	"strings"

	autoprovider "github.com/myceldb/mycel/internal/automation/provider"
	"github.com/myceldb/mycel/internal/daemon/config"
)

func automationProviderFromConfig(cfg config.Config) (autoprovider.Provider, error) {
	providerID := strings.ToLower(strings.TrimSpace(cfg.Automation.Provider))
	if providerID == "" || providerID == "none" {
		return nil, nil
	}
	switch providerID {
	case "openai", "openai-compatible":
		return autoprovider.NewOpenAICompatibleProvider(autoprovider.OpenAIConfig{BaseURL: cfg.Automation.BaseURL, APIKey: cfg.Automation.APIKey, Timeout: cfg.Automation.Timeout})
	case "fake":
		return autoprovider.FakeProvider{}, nil
	default:
		return nil, fmt.Errorf("unsupported automation provider %q", cfg.Automation.Provider)
	}
}
