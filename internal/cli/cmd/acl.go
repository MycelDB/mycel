package cmd

import (
	"fmt"

	"github.com/myceldb/mycel/internal/cli/app"
	"github.com/spf13/cobra"
)

func NewACLCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:        "acl",
		Short:      "Deprecated embedded ACL commands",
		Deprecated: "embedded ACL commands have been removed; use daemon Admin APIs when access-management commands are available",
		RunE: func(cmd *cobra.Command, args []string) error {
			return fmt.Errorf("embedded ACL commands are no longer supported; daemon ACL management API/CLI is not implemented yet")
		},
	}
	return cmd
}
