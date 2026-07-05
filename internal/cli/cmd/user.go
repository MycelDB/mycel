package cmd

import (
	"fmt"
	"strings"

	adminv1 "github.com/myceldb/mycel/gen/go/mycel/admin/v1"
	"github.com/myceldb/mycel/internal/cli/app"
	"github.com/spf13/cobra"
)

func NewAddUserCommand(a *app.App) *cobra.Command {
	var username, refAlias, newPassword string
	var disabled bool
	cmd := &cobra.Command{
		Use:   "user",
		Short: "Add a user",
		RunE: func(cmd *cobra.Command, args []string) error {
			if username == "" {
				username = refAlias
			}
			if username == "" || newPassword == "" {
				return fmt.Errorf("--user-username/--ref and --new-password are required")
			}
			conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
			if err != nil {
				return err
			}
			defer conn.Close()
			res, err := adminv1.NewAdminUserServiceClient(conn).CreateUser(authCtx, &adminv1.CreateUserRequest{Username: username, Password: &newPassword, Disabled: disabled})
			if err != nil {
				return fmt.Errorf("add user via daemon: %w", err)
			}
			return printUser(a, res.GetUser(), fmt.Sprintf("user added: %s (%s)\n", res.GetUser().GetUsername(), res.GetUser().GetUserId()))
		},
	}
	cmd.Flags().StringVar(&username, "user-username", "", "new user's unique username")
	cmd.Flags().StringVar(&refAlias, "ref", "", "deprecated alias for --user-username")
	cmd.Flags().BoolVar(&disabled, "disabled", false, "create disabled")
	cmd.Flags().StringVar(&newPassword, "new-password", "", "new user's password")
	return cmd
}

func NewGetUserCommand(a *app.App) *cobra.Command {
	var id string
	cmd := &cobra.Command{Use: "get", Short: "Get a user", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" {
			return fmt.Errorf("--user-id is required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminUserServiceClient(conn).GetUser(authCtx, &adminv1.GetUserRequest{UserId: id})
		if err != nil {
			return err
		}
		return printUser(a, res.GetUser(), "user: "+res.GetUser().GetUsername()+"\n")
	}}
	cmd.Flags().StringVar(&id, "user-id", "", "user id")
	return cmd
}

func NewFindUserCommand(a *app.App) *cobra.Command {
	var username, refAlias string
	cmd := &cobra.Command{Use: "find", Short: "Find a user", RunE: func(cmd *cobra.Command, args []string) error {
		if username == "" {
			username = refAlias
		}
		if username == "" {
			return fmt.Errorf("--user-username/--ref is required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminUserServiceClient(conn).FindUser(authCtx, &adminv1.FindUserRequest{Username: username})
		if err != nil {
			return err
		}
		return printUser(a, res.GetUser(), "user: "+res.GetUser().GetUsername()+"\n")
	}}
	cmd.Flags().StringVar(&username, "user-username", "", "user username")
	cmd.Flags().StringVar(&refAlias, "ref", "", "deprecated alias for --user-username")
	return cmd
}

func NewDeleteUserCommand(a *app.App) *cobra.Command {
	var revokeSessions bool
	cmd := &cobra.Command{
		Use:   "user USER_ID",
		Short: "Soft-delete a user",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
			if err != nil {
				return err
			}
			defer conn.Close()
			res, err := adminv1.NewAdminUserServiceClient(conn).DeleteUser(authCtx, &adminv1.DeleteUserRequest{UserId: args[0], RevokeSessions: revokeSessions})
			if err != nil {
				return err
			}
			return printUser(a, res.GetUser(), fmt.Sprintf("user deleted: %s\n", res.GetUser().GetUserId()))
		},
	}
	cmd.Flags().BoolVar(&revokeSessions, "revoke-sessions", false, "revoke active user sessions")
	return cmd
}

func NewDisableUserCommand(a *app.App) *cobra.Command {
	var id, reason string
	var revokeSessions bool
	cmd := &cobra.Command{Use: "disable", Short: "Disable a user", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" {
			return fmt.Errorf("--user-id is required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminUserServiceClient(conn).DisableUser(authCtx, &adminv1.DisableUserRequest{UserId: id, Reason: reason, RevokeSessions: revokeSessions})
		if err != nil {
			return err
		}
		return printUser(a, res.GetUser(), "user disabled: "+res.GetUser().GetUsername()+"\n")
	}}
	cmd.Flags().StringVar(&id, "user-id", "", "user id")
	cmd.Flags().StringVar(&reason, "reason", "", "reason")
	cmd.Flags().BoolVar(&revokeSessions, "revoke-sessions", false, "revoke active user sessions")
	return cmd
}

func NewEnableUserCommand(a *app.App) *cobra.Command {
	var id string
	cmd := &cobra.Command{Use: "enable", Short: "Enable a user", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" {
			return fmt.Errorf("--user-id is required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminUserServiceClient(conn).EnableUser(authCtx, &adminv1.EnableUserRequest{UserId: id})
		if err != nil {
			return err
		}
		return printUser(a, res.GetUser(), "user enabled: "+res.GetUser().GetUsername()+"\n")
	}}
	cmd.Flags().StringVar(&id, "user-id", "", "user id")
	return cmd
}

func NewSetUserPasswordCommand(a *app.App) *cobra.Command {
	var id, newPassword string
	var revokeSessions bool
	cmd := &cobra.Command{Use: "set", Short: "Set a user password", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" || strings.TrimSpace(newPassword) == "" {
			return fmt.Errorf("--user-id and --new-password are required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminUserServiceClient(conn).SetUserPassword(authCtx, &adminv1.SetUserPasswordRequest{UserId: id, Password: newPassword, RevokeSessions: revokeSessions})
		if err != nil {
			return err
		}
		return printUser(a, res.GetUser(), "user password changed: "+res.GetUser().GetUsername()+"\n")
	}}
	cmd.Flags().StringVar(&id, "user-id", "", "user id")
	cmd.Flags().StringVar(&newPassword, "new-password", "", "new password")
	cmd.Flags().BoolVar(&revokeSessions, "revoke-sessions", false, "revoke active user sessions")
	return cmd
}

func NewUserPasswordCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "password", Short: "Manage user passwords"}
	cmd.AddCommand(NewSetUserPasswordCommand(a))
	return cmd
}

func NewListUsersCommand(a *app.App) *cobra.Command {
	var includeDisabled, includeDeleted bool
	cmd := &cobra.Command{
		Use:   "users",
		Short: "List existing users",
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
			if err != nil {
				return err
			}
			defer conn.Close()
			res, err := adminv1.NewAdminUserServiceClient(conn).ListUsers(authCtx, &adminv1.ListUsersRequest{IncludeDisabled: includeDisabled, IncludeDeleted: includeDeleted})
			if err != nil {
				return err
			}
			if a.Output == "json" {
				return a.Print(res.GetUsers(), "")
			}
			app.RenderDaemonUsersTable(res.GetUsers())
			return nil
		},
	}
	cmd.Flags().BoolVar(&includeDisabled, "include-disabled", false, "include disabled users")
	cmd.Flags().BoolVar(&includeDeleted, "include-deleted", false, "include deleted users")
	return cmd
}

func NewListUserSessionsCommand(a *app.App) *cobra.Command {
	var id string
	var includeInactive bool
	cmd := &cobra.Command{Use: "list", Short: "List user sessions", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" {
			return fmt.Errorf("--user-id is required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminUserServiceClient(conn).ListUserSessions(authCtx, &adminv1.ListUserSessionsRequest{UserId: id, IncludeInactive: includeInactive})
		if err != nil {
			return err
		}
		sessions := res.GetSessions()
		if sessions == nil {
			sessions = []*adminv1.AdminAuthSessionSummary{}
		}
		return a.Print(sessions, "")
	}}
	cmd.Flags().StringVar(&id, "user-id", "", "user id")
	cmd.Flags().BoolVar(&includeInactive, "include-inactive", false, "include revoked/expired sessions")
	return cmd
}

func NewRevokeUserSessionCommand(a *app.App) *cobra.Command {
	var id, sessionID string
	cmd := &cobra.Command{Use: "revoke", Short: "Revoke a user session", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" || sessionID == "" {
			return fmt.Errorf("--user-id and --session-id are required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		_, err = adminv1.NewAdminUserServiceClient(conn).RevokeUserSession(authCtx, &adminv1.RevokeUserSessionRequest{UserId: id, AuthSessionId: sessionID})
		if err != nil {
			return err
		}
		fmt.Println("session revoked")
		return nil
	}}
	cmd.Flags().StringVar(&id, "user-id", "", "user id")
	cmd.Flags().StringVar(&sessionID, "session-id", "", "session id")
	return cmd
}

func NewRevokeUserSessionsCommand(a *app.App) *cobra.Command {
	var id string
	cmd := &cobra.Command{Use: "revoke-all", Short: "Revoke all user sessions", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" {
			return fmt.Errorf("--user-id is required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminUserServiceClient(conn).RevokeUserSessions(authCtx, &adminv1.RevokeUserSessionsRequest{UserId: id})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("sessions revoked: %d\n", res.GetRevokedCount()))
	}}
	cmd.Flags().StringVar(&id, "user-id", "", "user id")
	return cmd
}

func NewUserSessionCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "session", Short: "Manage user sessions"}
	cmd.AddCommand(NewListUserSessionsCommand(a), NewRevokeUserSessionCommand(a), NewRevokeUserSessionsCommand(a))
	return cmd
}

func printUser(a *app.App, user *adminv1.User, text string) error {
	if a.Output == "json" {
		return a.Print(user, "")
	}
	return a.Print(user, text)
}
