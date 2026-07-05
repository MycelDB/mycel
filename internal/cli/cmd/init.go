package cmd

import (
	"fmt"

	"github.com/myceldb/mycel/internal/cli/app"
	"github.com/spf13/cobra"
)

func NewInitCommand(a *app.App) *cobra.Command {
	return &cobra.Command{
		Use:        "init",
		Short:      "Deprecated: myceld initializes daemon storage at startup",
		Deprecated: "embedded local initialization has been removed; start myceld instead",
		RunE: func(cmd *cobra.Command, args []string) error {
			return fmt.Errorf("mycel init is no longer supported; run myceld with MYCELD_DATA_DIR to initialize daemon storage")
		},
	}
}
