package cmd

import (
	"fmt"
	"strings"

	"github.com/myceldb/mycel/internal/cli/app"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
	"github.com/spf13/cobra"
)

func NewAddUserCommand(a *app.App) *cobra.Command {
	var username, refAlias, newPassword string
	var disabled bool
	cmd := &cobra.Command{
		Use:   "user",
		Short: "Add a human principal",
		RunE: func(cmd *cobra.Command, args []string) error {
			if username == "" {
				username = refAlias
			}
			if username == "" || newPassword == "" {
				return fmt.Errorf("--user-username/--principal-username/--ref and --new-password are required")
			}
			conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
			if err != nil {
				return err
			}
			defer conn.Close()
			res, err := adminv1.NewAdminPrincipalServiceClient(conn).CreatePrincipal(authCtx, &adminv1.CreatePrincipalRequest{Username: username, Password: &newPassword, Type: commonv1.PrincipalType_PRINCIPAL_TYPE_HUMAN, LoginEnabled: true, Disabled: disabled})
			if err != nil {
				return fmt.Errorf("add principal via daemon: %w", err)
			}
			return printUserPrincipal(a, res.GetPrincipal(), fmt.Sprintf("principal added: %s (%s)\n", res.GetPrincipal().GetUsername(), res.GetPrincipal().GetPrincipalId()))
		},
	}
	cmd.Flags().StringVar(&username, "principal-username", "", "new principal's unique username")
	cmd.Flags().StringVar(&username, "user-username", "", "deprecated; use --principal-username")
	cmd.Flags().StringVar(&refAlias, "ref", "", "deprecated alias for --principal-username")
	cmd.Flags().BoolVar(&disabled, "disabled", false, "create disabled")
	cmd.Flags().StringVar(&newPassword, "new-password", "", "new principal's password")
	return cmd
}

func NewGetUserCommand(a *app.App) *cobra.Command {
	var id string
	cmd := &cobra.Command{Use: "get", Short: "Get a principal", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" {
			return fmt.Errorf("--user-id/--principal-id is required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminPrincipalServiceClient(conn).GetPrincipal(authCtx, &adminv1.GetPrincipalRequest{PrincipalId: id})
		if err != nil {
			return err
		}
		return printUserPrincipal(a, res.GetPrincipal(), "principal: "+res.GetPrincipal().GetUsername()+"\n")
	}}
	cmd.Flags().StringVar(&id, "principal-id", "", "principal id")
	cmd.Flags().StringVar(&id, "user-id", "", "deprecated; use --principal-id")
	return cmd
}

func NewFindUserCommand(a *app.App) *cobra.Command {
	var username, refAlias string
	cmd := &cobra.Command{Use: "find", Short: "Find a principal", RunE: func(cmd *cobra.Command, args []string) error {
		if username == "" {
			username = refAlias
		}
		if username == "" {
			return fmt.Errorf("--user-username/--principal-username/--ref is required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminPrincipalServiceClient(conn).FindPrincipal(authCtx, &adminv1.FindPrincipalRequest{Lookup: &adminv1.FindPrincipalRequest_Username{Username: username}})
		if err != nil {
			return err
		}
		return printUserPrincipal(a, res.GetPrincipal(), "principal: "+res.GetPrincipal().GetUsername()+"\n")
	}}
	cmd.Flags().StringVar(&username, "principal-username", "", "principal username")
	cmd.Flags().StringVar(&username, "user-username", "", "deprecated; use --principal-username")
	cmd.Flags().StringVar(&refAlias, "ref", "", "deprecated alias for --principal-username")
	return cmd
}

func NewDeleteUserCommand(a *app.App) *cobra.Command {
	var revokeSessions bool
	cmd := &cobra.Command{
		Use:   "user PRINCIPAL_ID",
		Short: "Soft-delete a principal",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
			if err != nil {
				return err
			}
			defer conn.Close()
			res, err := adminv1.NewAdminPrincipalServiceClient(conn).DeletePrincipal(authCtx, &adminv1.DeletePrincipalRequest{PrincipalId: args[0], RevokeSessions: revokeSessions})
			if err != nil {
				return err
			}
			return printUserPrincipal(a, res.GetPrincipal(), fmt.Sprintf("principal deleted: %s\n", res.GetPrincipal().GetPrincipalId()))
		},
	}
	cmd.Flags().BoolVar(&revokeSessions, "revoke-sessions", false, "revoke active principal sessions")
	return cmd
}

func NewDisableUserCommand(a *app.App) *cobra.Command {
	var id, reason string
	var revokeSessions bool
	cmd := &cobra.Command{Use: "disable", Short: "Disable a principal", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" {
			return fmt.Errorf("--user-id/--principal-id is required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminPrincipalServiceClient(conn).DisablePrincipal(authCtx, &adminv1.DisablePrincipalRequest{PrincipalId: id, Reason: reason, RevokeSessions: revokeSessions})
		if err != nil {
			return err
		}
		return printUserPrincipal(a, res.GetPrincipal(), "principal disabled: "+res.GetPrincipal().GetUsername()+"\n")
	}}
	cmd.Flags().StringVar(&id, "principal-id", "", "principal id")
	cmd.Flags().StringVar(&id, "user-id", "", "deprecated; use --principal-id")
	cmd.Flags().StringVar(&reason, "reason", "", "reason")
	cmd.Flags().BoolVar(&revokeSessions, "revoke-sessions", false, "revoke active principal sessions")
	return cmd
}

func NewEnableUserCommand(a *app.App) *cobra.Command {
	var id string
	cmd := &cobra.Command{Use: "enable", Short: "Enable a principal", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" {
			return fmt.Errorf("--user-id/--principal-id is required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminPrincipalServiceClient(conn).EnablePrincipal(authCtx, &adminv1.EnablePrincipalRequest{PrincipalId: id})
		if err != nil {
			return err
		}
		return printUserPrincipal(a, res.GetPrincipal(), "principal enabled: "+res.GetPrincipal().GetUsername()+"\n")
	}}
	cmd.Flags().StringVar(&id, "principal-id", "", "principal id")
	cmd.Flags().StringVar(&id, "user-id", "", "deprecated; use --principal-id")
	return cmd
}

func NewSetUserPasswordCommand(a *app.App) *cobra.Command {
	var id, newPassword string
	var revokeSessions bool
	cmd := &cobra.Command{Use: "set", Short: "Set a principal password", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" || strings.TrimSpace(newPassword) == "" {
			return fmt.Errorf("--user-id/--principal-id and --new-password are required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminPrincipalServiceClient(conn).SetPrincipalPassword(authCtx, &adminv1.SetPrincipalPasswordRequest{PrincipalId: id, Password: newPassword, RevokeSessions: revokeSessions})
		if err != nil {
			return err
		}
		return printUserPrincipal(a, res.GetPrincipal(), "principal password changed: "+res.GetPrincipal().GetUsername()+"\n")
	}}
	cmd.Flags().StringVar(&id, "principal-id", "", "principal id")
	cmd.Flags().StringVar(&id, "user-id", "", "deprecated; use --principal-id")
	cmd.Flags().StringVar(&newPassword, "new-password", "", "new password")
	cmd.Flags().BoolVar(&revokeSessions, "revoke-sessions", false, "revoke active principal sessions")
	return cmd
}

func NewUserPasswordCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "password", Short: "Manage principal passwords"}
	cmd.AddCommand(NewSetUserPasswordCommand(a))
	return cmd
}

func NewListUsersCommand(a *app.App) *cobra.Command {
	var includeDisabled, includeDeleted bool
	cmd := &cobra.Command{
		Use:   "users",
		Short: "List existing principals",
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
			if err != nil {
				return err
			}
			defer conn.Close()
			res, err := adminv1.NewAdminPrincipalServiceClient(conn).ListPrincipals(authCtx, &adminv1.ListPrincipalsRequest{IncludeDisabled: includeDisabled, IncludeDeleted: includeDeleted})
			if err != nil {
				return err
			}
			return a.Print(res.GetPrincipals(), fmt.Sprintf("principals: %d\n", len(res.GetPrincipals())))
		},
	}
	cmd.Flags().BoolVar(&includeDisabled, "include-disabled", false, "include disabled principals")
	cmd.Flags().BoolVar(&includeDeleted, "include-deleted", false, "include deleted principals")
	return cmd
}

func NewListUserSessionsCommand(a *app.App) *cobra.Command {
	var id string
	var includeInactive bool
	cmd := &cobra.Command{Use: "list", Short: "List principal sessions", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" {
			return fmt.Errorf("--user-id/--principal-id is required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminPrincipalServiceClient(conn).ListPrincipalSessions(authCtx, &adminv1.ListPrincipalSessionsRequest{PrincipalId: id, IncludeInactive: includeInactive})
		if err != nil {
			return err
		}
		sessions := res.GetSessions()
		if sessions == nil {
			sessions = []*commonv1.AuthSessionSummary{}
		}
		return a.Print(sessions, "")
	}}
	cmd.Flags().StringVar(&id, "principal-id", "", "principal id")
	cmd.Flags().StringVar(&id, "user-id", "", "deprecated; use --principal-id")
	cmd.Flags().BoolVar(&includeInactive, "include-inactive", false, "include revoked/expired sessions")
	return cmd
}

func NewRevokeUserSessionCommand(a *app.App) *cobra.Command {
	var id, sessionID string
	cmd := &cobra.Command{Use: "revoke", Short: "Revoke a principal session", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" || sessionID == "" {
			return fmt.Errorf("--user-id/--principal-id and --session-id are required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		_, err = adminv1.NewAdminPrincipalServiceClient(conn).RevokePrincipalSession(authCtx, &adminv1.RevokePrincipalSessionRequest{PrincipalId: id, AuthSessionId: sessionID})
		if err != nil {
			return err
		}
		fmt.Println("session revoked")
		return nil
	}}
	cmd.Flags().StringVar(&id, "principal-id", "", "principal id")
	cmd.Flags().StringVar(&id, "user-id", "", "deprecated; use --principal-id")
	cmd.Flags().StringVar(&sessionID, "session-id", "", "session id")
	return cmd
}

func NewRevokeUserSessionsCommand(a *app.App) *cobra.Command {
	var id string
	cmd := &cobra.Command{Use: "revoke-all", Short: "Revoke all principal sessions", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" {
			return fmt.Errorf("--user-id/--principal-id is required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminPrincipalServiceClient(conn).RevokePrincipalSessions(authCtx, &adminv1.RevokePrincipalSessionsRequest{PrincipalId: id})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("sessions revoked: %d\n", res.GetRevokedCount()))
	}}
	cmd.Flags().StringVar(&id, "principal-id", "", "principal id")
	cmd.Flags().StringVar(&id, "user-id", "", "deprecated; use --principal-id")
	return cmd
}

func NewUserSessionCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "session", Short: "Manage principal sessions"}
	cmd.AddCommand(NewListUserSessionsCommand(a), NewRevokeUserSessionCommand(a), NewRevokeUserSessionsCommand(a))
	return cmd
}

func printUserPrincipal(a *app.App, principal *adminv1.Principal, text string) error {
	if a.Output == "json" {
		return a.Print(principal, "")
	}
	return a.Print(principal, text)
}
