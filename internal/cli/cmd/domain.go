package cmd

import (
	"fmt"

	"github.com/myceldb/mycel/internal/cli/app"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func NewDomainCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "domain", Short: "Manage graph domains"}
	cmd.AddCommand(NewListDomainsCommand(a), NewAddDomainCommand(a), NewShowDomainCommand(a), NewUpdateDomainCommand(a), NewDeleteDomainCommand(a))
	return cmd
}

func NewListDomainsCommand(a *app.App) *cobra.Command {
	var spaceIDText string
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List domains in a space through the daemon Client API",
		RunE: func(cmd *cobra.Command, args []string) error {
			spaceID, err := a.ResolveSpaceID(spaceIDText)
			if err != nil {
				return err
			}
			conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
			if err != nil {
				return err
			}
			defer conn.Close()
			res, err := clientv1.NewDomainServiceClient(conn).ListDomains(authCtx, &clientv1.ListDomainsRequest{SpaceId: spaceID.String()})
			if err != nil {
				return err
			}
			if a.Output == "json" {
				return a.Print(res.GetDomains(), "")
			}
			app.RenderClientDomainsTable(res.GetDomains())
			return nil
		},
	}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID (defaults to current REPL space)")
	return cmd
}

func NewAddDomainCommand(a *app.App) *cobra.Command {
	var spaceIDText, name, description string
	cmd := &cobra.Command{
		Use:   "add KEY",
		Short: "Add a graph domain to a space through the daemon Client API",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			spaceID, err := a.ResolveSpaceID(spaceIDText)
			if err != nil {
				return err
			}
			conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
			if err != nil {
				return err
			}
			defer conn.Close()
			displayName := name
			if displayName == "" {
				displayName = args[0]
			}
			res, err := clientv1.NewDomainServiceClient(conn).CreateDomain(authCtx, &clientv1.CreateDomainRequest{SpaceId: spaceID.String(), Key: args[0], Name: displayName, Description: description})
			if err != nil {
				return err
			}
			return a.Print(res.GetDomain(), fmt.Sprintf("domain added: %s (%s)\n", res.GetDomain().GetKey(), res.GetDomain().GetDomainId()))
		},
	}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID (defaults to current REPL space)")
	cmd.Flags().StringVar(&name, "name", "", "domain display name")
	cmd.Flags().StringVar(&description, "description", "", "domain description")
	return cmd
}

func NewShowDomainCommand(a *app.App) *cobra.Command {
	var spaceIDText, domainIDText string
	cmd := &cobra.Command{
		Use:     "show KEY",
		Aliases: []string{"get"},
		Short:   "Show a graph domain by key or ID through the daemon Client API",
		Args:    cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			spaceID, err := a.ResolveSpaceID(spaceIDText)
			if err != nil {
				return err
			}
			conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
			if err != nil {
				return err
			}
			defer conn.Close()
			req := &clientv1.GetDomainRequest{SpaceId: spaceID.String()}
			if domainIDText != "" {
				req.DomainId = domainIDText
			} else if len(args) == 1 {
				req.Key = args[0]
			}
			res, err := clientv1.NewDomainServiceClient(conn).GetDomain(authCtx, req)
			if err != nil {
				return err
			}
			return a.Print(res.GetDomain(), fmt.Sprintf("domain: %s (%s)\n", res.GetDomain().GetKey(), res.GetDomain().GetDomainId()))
		},
	}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID (defaults to current REPL space)")
	cmd.Flags().StringVar(&domainIDText, "domain-id", "", "domain ID")
	return cmd
}

func NewUpdateDomainCommand(a *app.App) *cobra.Command {
	var spaceIDText, domainIDText, name, description string
	cmd := &cobra.Command{Use: "update", Short: "Update graph domain metadata through the daemon Client API", RunE: func(cmd *cobra.Command, args []string) error {
		if domainIDText == "" {
			return fmt.Errorf("--domain-id is required")
		}
		spaceID, err := a.ResolveSpaceID(spaceIDText)
		if err != nil {
			return err
		}
		paths := []string{}
		domain := &clientv1.Domain{DomainId: domainIDText}
		if cmd.Flags().Changed("name") {
			domain.Name = name
			paths = append(paths, "name")
		}
		if cmd.Flags().Changed("description") {
			domain.Description = description
			paths = append(paths, "description")
		}
		if len(paths) == 0 {
			return fmt.Errorf("at least one of --name or --description is required")
		}
		conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewDomainServiceClient(conn).UpdateDomain(authCtx, &clientv1.UpdateDomainRequest{SpaceId: spaceID.String(), DomainId: domainIDText, Domain: domain, UpdateMask: &fieldmaskpb.FieldMask{Paths: paths}})
		if err != nil {
			return err
		}
		return a.Print(res.GetDomain(), fmt.Sprintf("domain updated: %s (%s)\n", res.GetDomain().GetKey(), res.GetDomain().GetDomainId()))
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID (defaults to current REPL space)")
	cmd.Flags().StringVar(&domainIDText, "domain-id", "", "domain ID")
	cmd.Flags().StringVar(&name, "name", "", "domain display name")
	cmd.Flags().StringVar(&description, "description", "", "domain description")
	return cmd
}

func NewDeleteDomainCommand(a *app.App) *cobra.Command {
	var spaceIDText string
	cmd := &cobra.Command{Use: "delete DOMAIN_ID", Aliases: []string{"del", "remove", "rm"}, Short: "Delete a graph domain through the daemon Client API", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		spaceID, err := a.ResolveSpaceID(spaceIDText)
		if err != nil {
			return err
		}
		conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		if _, err := clientv1.NewDomainServiceClient(conn).DeleteDomain(authCtx, &clientv1.DeleteDomainRequest{SpaceId: spaceID.String(), DomainId: args[0]}); err != nil {
			return err
		}
		return a.Print(map[string]any{"deleted_domain_id": args[0], "space_id": spaceID}, fmt.Sprintf("domain deleted: %s\n", args[0]))
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID (defaults to current REPL space)")
	return cmd
}
