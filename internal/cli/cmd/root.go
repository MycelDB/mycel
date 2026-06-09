package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	knotengine "martinbeauvais.com/mbgit/knotbase/knotdb/engine"
	"martinbeauvais.com/mbgit/knotbase/knotdb/internal/cli/app"
)

func Execute() error {
	a := &app.App{}
	cmd := NewRootCommand(a, false)
	if err := cmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return err
	}
	return nil
}

func NewRootCommand(a *app.App, repl bool) *cobra.Command {
	root := &cobra.Command{
		Use:           "knotdb",
		Short:         "KnotDB embedded engine CLI",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	a.DataDir = knotengine.ResolveDataDir(a.DataDir)
	root.PersistentFlags().StringVarP(&a.DataDir, "data-dir", "d", a.DataDir, "KnotDB data directory (defaults to KNOTDB_DATA_DIR)")
	root.PersistentFlags().StringVarP(&a.UserRef, "username", "u", a.UserRef, "username/user_ref for non-REPL authentication")
	root.PersistentFlags().StringVarP(&a.Password, "password", "p", a.Password, "password for non-REPL authentication")
	root.PersistentFlags().StringVar(&a.Output, "output", app.DefaultOutput(a.Output), "output format: text or json")

	root.AddCommand(NewInitCommand(a), NewAddCommand(a), NewDeleteCommand(a), NewListCommand(a), NewACLCommand(a), NewReplCommand(a))
	if repl {
		root.Use = ""
	}
	return root
}
