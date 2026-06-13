package cmd

import (
	"github.com/spf13/cobra"
	"martinbeauvais.com/mbgit/knotbase/knotdb/internal/cli/app"
)

func NewUserCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "user", Aliases: []string{"users"}, Short: "Manage users"}
	add := NewAddUserCommand(a)
	add.Use = "add"
	add.Short = "Add a user"
	list := NewListUsersCommand(a)
	list.Use = "list"
	list.Aliases = []string{"ls"}
	list.Short = "List users"
	del := NewDeleteUserCommand(a)
	del.Use = "delete USER_ID"
	del.Aliases = []string{"del", "remove", "rm"}
	cmd.AddCommand(add, list, del)
	return cmd
}

func NewSpaceCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "space", Aliases: []string{"spaces"}, Short: "Manage spaces"}
	add := NewAddSpaceCommand(a)
	add.Use = "add [NAME]"
	add.Short = "Add a space"
	list := NewListSpacesCommand(a)
	list.Use = "list"
	list.Aliases = []string{"ls"}
	list.Short = "List spaces"
	del := NewDeleteSpaceCommand(a)
	del.Use = "delete [SPACE_ID]"
	del.Aliases = []string{"del", "remove", "rm"}
	cmd.AddCommand(add, list, NewSetSpaceCommand(a), NewUnsetSpaceCommand(a), del)
	return cmd
}

func NewNodeCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "node", Aliases: []string{"nodes"}, Short: "Manage graph nodes"}
	add := NewAddNodeCommand(a)
	add.Use = "add"
	add.Short = "Add a node"
	del := NewDeleteNodeCommand(a)
	del.Use = "delete NODE_ID"
	del.Aliases = []string{"del", "remove", "rm"}
	get := NewGetNodeCommand(a)
	get.Use = "get NODE_ID"
	list := NewListNodesCommand(a)
	list.Use = "list"
	list.Aliases = []string{"ls"}
	cmd.AddCommand(add, get, list, del)
	return cmd
}

func NewBlobCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "blob", Aliases: []string{"blobs"}, Short: "Manage blob nodes and content"}
	add := NewAddBlobCommand(a)
	add.Use = "add FILE"
	add.Short = "Add a blob node from a file"
	get := NewGetBlobCommand(a)
	get.Use = "get NODE_ID"
	get.Aliases = []string{"download"}
	get.Short = "Download blob content attached to a node"
	cmd.AddCommand(add, get)
	return cmd
}

func NewTemplateCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "template", Aliases: []string{"templates"}, Short: "Manage graph templates"}
	imp := NewAddTemplateCommand(a)
	imp.Use = "import"
	imp.Aliases = []string{"add"}
	imp.Short = "Import templates from a JSON file or stdin"
	list := NewListTemplatesCommand(a)
	list.Use = "list"
	list.Aliases = []string{"ls"}
	list.Short = "List templates for a space"
	cmd.AddCommand(imp, list)
	return cmd
}
