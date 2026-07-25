package cmd

import (
	"testing"

	"github.com/myceldb/mycel/internal/cli/app"
)

func TestSchemaCommandRegistered(t *testing.T) {
	root := NewRootCommand(&app.App{}, false)
	cmd, _, err := root.Find([]string{"schema", "validate"})
	if err != nil {
		t.Fatalf("find schema validate: %v", err)
	}
	if cmd == nil || cmd.Name() != "validate" {
		t.Fatalf("schema validate command not registered: %#v", cmd)
	}
}
