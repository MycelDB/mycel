package cmd

import (
	"github.com/myceldb/mycel/internal/cli/app"
	"github.com/spf13/cobra"
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
	cmd.AddCommand(add, list, NewGetUserCommand(a), NewFindUserCommand(a), NewDisableUserCommand(a), NewEnableUserCommand(a), del, NewUserPasswordCommand(a), NewUserSessionCommand(a))
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
	get := NewGetSpaceCommand(a)
	cmd.AddCommand(add, list, get, NewSetSpaceCommand(a), NewUnsetSpaceCommand(a), del)
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
	cmd := &cobra.Command{Use: "blob", Aliases: []string{"blobs"}, Short: "Manage raw blob content through daemon gRPC"}
	cmd.AddCommand(NewUploadBlobCommand(a), NewGetRawBlobCommand(a), NewDownloadRawBlobCommand(a), NewDeleteRawBlobCommand(a))
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
	cmd.AddCommand(imp, list, NewGetTemplateCommand(a), NewFindTemplateCommand(a), NewCreateTemplateCommand(a), NewUpdateTemplateCommand(a), NewArchiveTemplateCommand(a), NewDeleteTemplateCommand(a))
	return cmd
}
