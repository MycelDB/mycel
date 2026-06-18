package cmd

import (
	"fmt"
	"strings"

	"github.com/myceldb/mycel/domain/identity"
	mycelengine "github.com/myceldb/mycel/engine"
	"github.com/myceldb/mycel/internal/cli/app"
	"github.com/spf13/cobra"
)

func NewInitCommand(a *app.App) *cobra.Command {
	return &cobra.Command{
		Use:   "init",
		Short: "Initialize a MycelDB data directory",
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(a.DataDir) == "" {
				return fmt.Errorf("--data-dir/-d is required")
			}
			if strings.TrimSpace(a.UserRef) == "" || strings.TrimSpace(a.Password) == "" {
				return fmt.Errorf("--username/-u and --password/-p are required")
			}
			engineCfg := a.Config.EngineConfig()
			engineCfg.DataDir = a.DataDir
			engineCfg.CreateIfMissing = true
			engineCfg.AdminUsername = a.UserRef
			engineCfg.AdminPassword = a.Password
			eng, err := mycelengine.NewEngine(engineCfg, nil, nil, nil, nil)
			if err != nil {
				return err
			}
			if _, err := eng.Authenticate(cmd.Context(), mycelengine.AuthInput{UserRef: identity.UserRef(a.UserRef), Password: a.Password}); err != nil {
				_ = eng.Close()
				return err
			}
			_ = eng.Close()
			return a.Print(map[string]any{"data_dir": a.DataDir}, fmt.Sprintf("initialized: %s\n", a.DataDir))
		},
	}
}
