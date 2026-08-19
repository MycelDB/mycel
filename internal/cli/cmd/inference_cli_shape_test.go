package cmd

import (
	"testing"

	"github.com/myceldb/mycel/internal/cli/app"
)

func TestInferenceCommandTreeUsesSingularResourceNouns(t *testing.T) {
	root := NewRootCommand(&app.App{}, false)
	for _, path := range [][]string{
		{"inference", "package", "list"},
		{"inference", "packages", "list"},
		{"inference", "endpoint", "list"},
		{"inference", "model-endpoint", "list"},
		{"inference", "model", "list"},
		{"inference", "capability", "list"},
		{"inference", "vector-store", "list"},
		{"inference", "credential", "create"},
		{"inference", "credential", "add"},
		{"inference", "credential", "rotate"},
		{"inference", "credential", "revoke"},
		{"inference", "grant", "list"},
		{"inference", "credential", "grant", "list"},
		{"inference", "policy", "list"},
		{"inference", "decision", "get"},
		{"inference", "policy", "decision", "get"},
		{"inference", "usage", "list"},
		{"inference", "usage", "summarize"},
	} {
		if _, _, err := root.Find(path); err != nil {
			t.Fatalf("command path %v not found: %v", path, err)
		}
	}
}

func TestInferenceCredentialCommandsUseDirectSecretProvisioning(t *testing.T) {
	root := NewRootCommand(&app.App{}, false)
	for _, path := range [][]string{{"inference", "credential", "create"}, {"inference", "credential", "rotate"}} {
		cmd, _, err := root.Find(path)
		if err != nil {
			t.Fatalf("command path %v not found: %v", path, err)
		}
		if cmd.Flags().Lookup("secret-stdin") == nil || cmd.Flags().Lookup("secret-value") == nil {
			t.Fatalf("command path %v missing direct secret flags", path)
		}
		if cmd.Flags().Lookup("external-ref") != nil || cmd.Flags().Lookup("api-key-env") != nil {
			t.Fatalf("command path %v exposes removed external/environment provisioning flags", path)
		}
	}
}

func TestAutomationCommandTreeSupportsSpaceDomainRefs(t *testing.T) {
	root := NewRootCommand(&app.App{}, false)
	for _, path := range [][]string{
		{"automation", "create"},
		{"automation", "update"},
		{"automation", "put"},
		{"automation", "list"},
		{"automation", "get"},
		{"automation", "runs"},
		{"automation", "invocation", "retry"},
	} {
		cmd, _, err := root.Find(path)
		if err != nil {
			t.Fatalf("command path %v not found: %v", path, err)
		}
		if cmd.Flags().Lookup("space-id") == nil || cmd.Flags().Lookup("domain") == nil || cmd.Flags().Lookup("domain-id") == nil {
			t.Fatalf("command path %v missing space/domain flags", path)
		}
	}
}
