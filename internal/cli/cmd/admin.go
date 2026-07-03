package cmd

import (
	"fmt"
	"strings"

	"github.com/myceldb/mycel/internal/cli/app"
	daemonconfig "github.com/myceldb/mycel/internal/daemon/config"
	daemonadmin "github.com/myceldb/mycel/internal/daemon/modules/admin"
	"github.com/spf13/cobra"
)

func NewAdminCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "admin", Aliases: []string{"admins"}, Short: "Manage daemon admins"}
	list := NewListAdminsCommand(a)
	list.Use = "list"
	list.Aliases = []string{"ls"}
	list.Short = "List daemon admins"
	cmd.AddCommand(list)
	return cmd
}

func NewListAdminsCommand(a *app.App) *cobra.Command {
	return &cobra.Command{
		Use:   "admins",
		Short: "List daemon admins",
		RunE: func(cmd *cobra.Command, args []string) error {
			dataDir, err := resolveDaemonDataDir(a)
			if err != nil {
				return err
			}
			lister, err := daemonadmin.OpenLister(dataDir)
			if err != nil {
				return fmt.Errorf("open daemon admin store at %s: %w", dataDir, err)
			}
			admins, err := lister.ListAdmins(cmd.Context())
			if err != nil {
				return err
			}
			if a.Output == "json" {
				return a.Print(admins, "")
			}
			app.RenderDaemonAdminsTable(admins)
			return nil
		},
	}
}

func resolveDaemonDataDir(a *app.App) (string, error) {
	if strings.TrimSpace(a.DataDir) != "" {
		return a.DataDir, nil
	}
	cfg, err := daemonconfig.LoadFromEnv()
	if err != nil {
		return "", err
	}
	return cfg.DataDir, nil
}
