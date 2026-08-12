package cmd

import (
	"fmt"

	"github.com/myceldb/mycel/internal/cli/app"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"github.com/spf13/cobra"
)

func NewAddSpaceCommand(a *app.App) *cobra.Command {
	var name, ownerUserIDText, ownerUsername, defaultDomainKey, defaultDomainName string
	cmd := &cobra.Command{
		Use:   "space [NAME]",
		Short: "Add a space through the daemon Admin API",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 1 {
				name = args[0]
			}
			if name == "" {
				return fmt.Errorf("space name is required")
			}
			if ownerUserIDText == "" && ownerUsername == "" {
				return fmt.Errorf("--owner-principal-id or --owner-username is required")
			}
			conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
			if err != nil {
				return err
			}
			defer conn.Close()
			res, err := adminv1.NewAdminSpaceServiceClient(conn).CreateSpace(authCtx, &adminv1.CreateSpaceRequest{Name: name, OwnerPrincipalId: ownerUserIDText, OwnerUsername: ownerUsername, DefaultDomainKey: defaultDomainKey, DefaultDomainName: defaultDomainName})
			if err != nil {
				return err
			}
			return a.Print(res, fmt.Sprintf("space added: %s (%s)\n", res.GetSpace().GetName(), res.GetSpace().GetSpaceId()))
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "space name")
	cmd.Flags().StringVar(&ownerUserIDText, "owner-principal-id", "", "target daemon principal ID")
	cmd.Flags().StringVar(&ownerUserIDText, "owner-user-id", "", "deprecated; use --owner-principal-id")
	cmd.Flags().StringVar(&ownerUsername, "owner-username", "", "target daemon username")
	cmd.Flags().StringVar(&defaultDomainKey, "default-domain-key", "", "initial default domain key")
	cmd.Flags().StringVar(&defaultDomainName, "default-domain-name", "", "initial default domain name")
	cmd.Flags().String("owner-ref", "", "deprecated; use --owner-username")
	return cmd
}

func NewDeleteSpaceCommand(a *app.App) *cobra.Command {
	return &cobra.Command{
		Use:   "space [SPACE_ID]",
		Short: "Hard-delete a space through the daemon Admin API",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			spaceIDText := ""
			if len(args) == 1 {
				spaceIDText = args[0]
			}
			id, err := a.ResolveSpaceID(spaceIDText)
			if err != nil {
				return err
			}
			conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
			if err != nil {
				return err
			}
			defer conn.Close()
			if _, err := adminv1.NewAdminSpaceServiceClient(conn).DeleteSpace(authCtx, &adminv1.DeleteSpaceRequest{SpaceId: id.String()}); err != nil {
				return err
			}
			return a.Print(map[string]any{"deleted_space_id": id}, fmt.Sprintf("space deleted: %s\n", id))
		},
	}
}

func NewGetSpaceCommand(a *app.App) *cobra.Command {
	return &cobra.Command{Use: "get SPACE_ID", Short: "Get a visible space through the daemon Client API", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewSpaceServiceClient(conn).GetSpace(authCtx, &clientv1.GetSpaceRequest{SpaceId: args[0]})
		if err != nil {
			return err
		}
		return a.Print(res.GetSpace(), fmt.Sprintf("space: %s (%s)\n", res.GetSpace().GetName(), res.GetSpace().GetSpaceId()))
	}}
}

func NewSetSpaceCommand(a *app.App) *cobra.Command {
	return &cobra.Command{
		Use:   "set SPACE_ID",
		Short: "Set the current REPL space",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := app.ParseUUID[domainspace.SpaceID](args[0])
			if err != nil {
				return err
			}
			conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
			if err != nil {
				return err
			}
			defer conn.Close()
			if _, err := clientv1.NewSpaceServiceClient(conn).GetSpace(authCtx, &clientv1.GetSpaceRequest{SpaceId: id.String()}); err != nil {
				return err
			}
			a.CurrentSpaceID = &id
			return a.Print(map[string]any{"current_space_id": id}, fmt.Sprintf("space set: %s\n", id))
		},
	}
}

func NewUnsetSpaceCommand(a *app.App) *cobra.Command {
	return &cobra.Command{
		Use:   "unset",
		Short: "Clear the current REPL space",
		RunE: func(cmd *cobra.Command, args []string) error {
			a.CurrentSpaceID = nil
			return a.Print(map[string]any{"current_space_id": nil}, "space unset\n")
		},
	}
}

func NewListSpacesCommand(a *app.App) *cobra.Command {
	var includeArchived bool
	cmd := &cobra.Command{
		Use:   "spaces",
		Short: "List visible spaces through the daemon Client API",
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
			if err != nil {
				return err
			}
			defer conn.Close()
			res, err := clientv1.NewSpaceServiceClient(conn).ListSpaces(authCtx, &clientv1.ListSpacesRequest{IncludeArchived: includeArchived})
			if err != nil {
				return err
			}
			if a.Output == "json" {
				return a.Print(res.GetSpaces(), "")
			}
			app.RenderClientSpacesTable(res.GetSpaces())
			return nil
		},
	}
	cmd.Flags().BoolVar(&includeArchived, "include-archived", false, "include archived spaces")
	return cmd
}
