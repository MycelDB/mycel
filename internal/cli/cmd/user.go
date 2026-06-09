package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"martinbeauvais.com/mbgit/knotbase/knotdb/domain/identity"
	knotengine "martinbeauvais.com/mbgit/knotbase/knotdb/engine"
	"martinbeauvais.com/mbgit/knotbase/knotdb/internal/cli/app"
)

func NewAddUserCommand(a *app.App) *cobra.Command {
	var ref, email, username, status, newPassword string
	cmd := &cobra.Command{
		Use:   "user",
		Short: "Add a user",
		RunE: func(cmd *cobra.Command, args []string) error {
			if ref == "" || newPassword == "" {
				return fmt.Errorf("--ref and --new-password are required")
			}
			tok, err := a.AccessToken(cmd.Context())
			if err != nil {
				return err
			}
			in := identity.UserInput{Ref: identity.UserRef(ref), Status: identity.UserStatus(status)}
			if email != "" {
				in.Email = &email
			}
			if username != "" {
				in.Username = &username
			}
			u, err := a.Engine.CreateUser(cmd.Context(), knotengine.CreateUserInput{AccessToken: tok, User: in, Password: newPassword})
			if err != nil {
				return err
			}
			return a.Print(u, fmt.Sprintf("user added: %s (%s)\n", u.Ref, u.ID))
		},
	}
	cmd.Flags().StringVar(&ref, "ref", "", "new user's user_ref")
	cmd.Flags().StringVar(&email, "email", "", "new user's email")
	cmd.Flags().StringVar(&username, "user-name", "", "new user's display username")
	cmd.Flags().StringVar(&status, "status", string(identity.UserStatusActive), "new user's status")
	cmd.Flags().StringVar(&newPassword, "new-password", "", "new user's password")
	return cmd
}

func NewDeleteUserCommand(a *app.App) *cobra.Command {
	return &cobra.Command{
		Use:   "user USER_ID",
		Short: "Hard-delete a user and all owned spaces",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := app.ParseUUID[identity.UserID](args[0])
			if err != nil {
				return err
			}
			tok, err := a.AccessToken(cmd.Context())
			if err != nil {
				return err
			}
			if err := a.Engine.DeleteUser(cmd.Context(), knotengine.DeleteUserInput{AccessToken: tok, UserID: id}); err != nil {
				return err
			}
			return a.Print(map[string]any{"deleted_user_id": id}, fmt.Sprintf("user deleted: %s\n", id))
		},
	}
}

func NewListUsersCommand(a *app.App) *cobra.Command {
	return &cobra.Command{
		Use:   "users",
		Short: "List existing users",
		RunE: func(cmd *cobra.Command, args []string) error {
			tok, err := a.AccessToken(cmd.Context())
			if err != nil {
				return err
			}
			users, err := a.Engine.ListUsers(cmd.Context(), knotengine.ListUsersInput{AccessToken: tok})
			if err != nil {
				return err
			}
			if a.Output == "json" {
				return a.Print(users, "")
			}
			app.RenderUsersTable(users)
			return nil
		},
	}
}
