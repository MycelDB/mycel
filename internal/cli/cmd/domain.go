package cmd

import (
	"fmt"

	"github.com/myceldb/mycel/domain/graph"
	mycelengine "github.com/myceldb/mycel/engine"
	"github.com/myceldb/mycel/internal/cli/app"
	"github.com/spf13/cobra"
)

func NewDomainCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "domain", Short: "Manage graph domains"}
	cmd.AddCommand(NewListDomainsCommand(a), NewAddDomainCommand(a), NewShowDomainCommand(a))
	return cmd
}

func NewListDomainsCommand(a *app.App) *cobra.Command {
	var spaceIDText string
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List domains in a space",
		RunE: func(cmd *cobra.Command, args []string) error {
			spaceID, err := a.ResolveSpaceID(spaceIDText)
			if err != nil {
				return err
			}
			tok, err := a.AccessToken(cmd.Context())
			if err != nil {
				return err
			}
			domains, err := a.Engine.ListDomains(cmd.Context(), mycelengine.ListDomainsInput{AccessToken: tok, SpaceID: spaceID})
			if err != nil {
				return err
			}
			if a.Output == "json" {
				return a.Print(domains, "")
			}
			app.RenderDomainsTable(domains)
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
		Short: "Add a graph domain to a space",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			spaceID, err := a.ResolveSpaceID(spaceIDText)
			if err != nil {
				return err
			}
			tok, err := a.AccessToken(cmd.Context())
			if err != nil {
				return err
			}
			domain, err := a.Engine.CreateDomain(cmd.Context(), mycelengine.CreateDomainInput{AccessToken: tok, SpaceID: spaceID, Key: args[0], Name: name, Description: description})
			if err != nil {
				return err
			}
			return a.Print(domain, fmt.Sprintf("domain added: %s (%s)\n", domain.Key, domain.ID))
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
		Use:   "show KEY",
		Short: "Show a graph domain by key or ID",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			spaceID, err := a.ResolveSpaceID(spaceIDText)
			if err != nil {
				return err
			}
			tok, err := a.AccessToken(cmd.Context())
			if err != nil {
				return err
			}
			in := mycelengine.GetDomainInput{AccessToken: tok, SpaceID: spaceID}
			if domainIDText != "" {
				id, err := app.ParseUUID[graph.DomainID](domainIDText)
				if err != nil {
					return err
				}
				in.DomainID = id
			} else if len(args) == 1 {
				in.Key = args[0]
			} else {
				in.Key = graph.DefaultDomainKey
			}
			domain, err := a.Engine.GetDomain(cmd.Context(), in)
			if err != nil {
				return err
			}
			return a.Print(domain, fmt.Sprintf("domain: %s (%s)\n", domain.Key, domain.ID))
		},
	}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID (defaults to current REPL space)")
	cmd.Flags().StringVar(&domainIDText, "domain-id", "", "domain ID")
	return cmd
}
