package cmd

import (
	"fmt"

	domainspace "github.com/myceldb/mycel/domain/space"
	mycelengine "github.com/myceldb/mycel/engine"
	"github.com/myceldb/mycel/internal/cli/app"
	"github.com/spf13/cobra"
)

func NewAddSpaceCommand(a *app.App) *cobra.Command {
	var name string
	cmd := &cobra.Command{
		Use:   "space [NAME]",
		Short: "Add a space",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 1 {
				name = args[0]
			}
			if name == "" {
				return fmt.Errorf("space name is required")
			}
			tok, err := a.AccessToken(cmd.Context())
			if err != nil {
				return err
			}
			sp, err := a.Engine.CreateSpace(cmd.Context(), mycelengine.CreateSpaceInput{AccessToken: tok, Name: name})
			if err != nil {
				return err
			}
			return a.Print(sp, fmt.Sprintf("space added: %s (%s)\n", sp.Name, sp.SpaceID))
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "space name")
	return cmd
}

func NewDeleteSpaceCommand(a *app.App) *cobra.Command {
	return &cobra.Command{
		Use:   "space [SPACE_ID]",
		Short: "Hard-delete a space and all associated constructs",
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
			tok, err := a.AccessToken(cmd.Context())
			if err != nil {
				return err
			}
			if err := a.Engine.DeleteSpace(cmd.Context(), mycelengine.DeleteSpaceInput{AccessToken: tok, SpaceID: id}); err != nil {
				return err
			}
			return a.Print(map[string]any{"deleted_space_id": id}, fmt.Sprintf("space deleted: %s\n", id))
		},
	}
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
			if err := a.SetCurrentSpace(cmd.Context(), id); err != nil {
				return err
			}
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
	return &cobra.Command{
		Use:   "spaces",
		Short: "List existing spaces",
		RunE: func(cmd *cobra.Command, args []string) error {
			tok, err := a.AccessToken(cmd.Context())
			if err != nil {
				return err
			}
			spaces, err := a.Engine.ListSpaces(cmd.Context(), mycelengine.ListSpacesInput{AccessToken: tok})
			if err != nil {
				return err
			}
			if a.Output == "json" {
				return a.Print(spaces, "")
			}
			app.RenderSpacesTable(spaces)
			return nil
		},
	}
}
