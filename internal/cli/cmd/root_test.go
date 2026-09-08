package cmd

import (
	"testing"

	"github.com/myceldb/mycel/internal/cli/app"
)

func TestRootCommandDoesNotRegisterRemovedDeprecatedCommands(t *testing.T) {
	root := NewRootCommand(&app.App{}, false)
	removed := []string{"init", "acl", "accounting"}
	for _, name := range removed {
		name := name
		t.Run(name, func(t *testing.T) {
			cmd, _, err := root.Find([]string{name})
			if err == nil && cmd != nil && cmd.Name() == name {
				t.Fatalf("deprecated command %q is still registered", name)
			}
		})
	}
}
