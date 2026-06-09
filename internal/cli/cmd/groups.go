package cmd

import (
	"github.com/spf13/cobra"
	"martinbeauvais.com/mbgit/knotbase/knotdb/internal/cli/app"
)

func NewAddCommand(a *app.App) *cobra.Command {
	add := &cobra.Command{Use: "add", Short: "Add/create resources"}
	add.AddCommand(NewAddUserCommand(a), NewAddACLCommand(a), NewAddSpaceCommand(a), NewAddNodeCommand(a), NewAddTemplateCommand(a))
	return add
}

func NewDeleteCommand(a *app.App) *cobra.Command {
	del := &cobra.Command{Use: "delete", Aliases: []string{"del", "remove", "rm"}, Short: "Hard-delete resources"}
	del.AddCommand(NewDeleteUserCommand(a), NewDeleteACLCommand(a), NewDeleteSpaceCommand(a), NewDeleteNodeCommand(a))
	return del
}

func NewListCommand(a *app.App) *cobra.Command {
	list := &cobra.Command{Use: "list", Aliases: []string{"ls"}, Short: "List resources"}
	list.AddCommand(NewListUsersCommand(a), NewListSpacesCommand(a), NewListTemplatesCommand(a), NewListACLCommand(a))
	return list
}
