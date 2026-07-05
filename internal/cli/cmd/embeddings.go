package cmd

import (
	"fmt"

	"github.com/myceldb/mycel/internal/cli/app"
	"github.com/spf13/cobra"
)

func NewEmbeddingsCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:        "embeddings",
		Short:      "Deprecated legacy embedded embedding-profile commands",
		Deprecated: "legacy embedded embedding commands have been removed; use semantic and inference daemon commands",
		RunE: func(cmd *cobra.Command, args []string) error {
			return fmt.Errorf("legacy embedded embeddings commands are no longer supported; use `semantic search`, `semantic index`, `semantic maintenance`, and `inference` daemon commands")
		},
	}
	return cmd
}
