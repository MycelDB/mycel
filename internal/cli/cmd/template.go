package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	knotengine "martinbeauvais.com/mbgit/knotbase/knotdb/engine"
	domainsession "martinbeauvais.com/mbgit/knotbase/knotdb/session"
	"martinbeauvais.com/mbgit/knotbase/knotdb/internal/cli/app"
)

func NewAddTemplateCommand(a *app.App) *cobra.Command {
	var spaceIDText, filePath string
	cmd := &cobra.Command{
		Use:   "template",
		Short: "Import templates from a JSON file or stdin",
		RunE: func(cmd *cobra.Command, args []string) error {
			if filePath == "" {
				return fmt.Errorf("--file is required; use --file - to read from stdin")
			}
			spaceID, err := a.ResolveSpaceID(spaceIDText)
			if err != nil {
				return err
			}
			doc, err := app.ReadTemplateDocument(filePath)
			if err != nil {
				return err
			}
			tok, err := a.AccessToken(cmd.Context())
			if err != nil {
				return err
			}
			sess, err := a.Engine.OpenSession(cmd.Context(), knotengine.OpenSessionInput{AccessToken: tok, SpaceID: spaceID})
			if err != nil {
				return err
			}
			defer sess.Close()
			templates, err := sess.ImportTemplates(cmd.Context(), domainsession.ImportTemplatesInput{Document: doc})
			if err != nil {
				return err
			}
			if a.Output == "json" {
				return a.Print(templates, "")
			}
			app.RenderTemplatesTable(templates)
			return nil
		},
	}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "target space ID")
	cmd.Flags().StringVar(&filePath, "file", "", "template JSON file path, or - for stdin")
	return cmd
}

func NewListTemplatesCommand(a *app.App) *cobra.Command {
	var spaceIDText string
	cmd := &cobra.Command{
		Use:   "templates",
		Short: "List templates for a space",
		RunE: func(cmd *cobra.Command, args []string) error {
			spaceID, err := a.ResolveSpaceID(spaceIDText)
			if err != nil {
				return err
			}
			tok, err := a.AccessToken(cmd.Context())
			if err != nil {
				return err
			}
			sess, err := a.Engine.OpenSession(cmd.Context(), knotengine.OpenSessionInput{AccessToken: tok, SpaceID: spaceID})
			if err != nil {
				return err
			}
			defer sess.Close()
			templates, err := sess.ListTemplates(cmd.Context())
			if err != nil {
				return err
			}
			if a.Output == "json" {
				return a.Print(templates, "")
			}
			app.RenderTemplatesTable(templates)
			return nil
		},
	}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "target space ID")
	return cmd
}
