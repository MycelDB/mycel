package cmd

import (
	"fmt"

	"github.com/myceldb/mycel/internal/cli/app"
	"github.com/spf13/cobra"
)

func NewAccountingCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:        "accounting",
		Short:      "Deprecated embedded accounting commands",
		Deprecated: "embedded accounting commands have been removed; use daemon Admin APIs when accounting commands are available",
		RunE: func(cmd *cobra.Command, args []string) error {
			return fmt.Errorf("embedded accounting commands are no longer supported; daemon accounting API/CLI is not implemented yet")
		},
	}
	return cmd
}
