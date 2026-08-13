package cmd

import (
	"fmt"
	"strings"

	"github.com/myceldb/mycel/internal/cli/app"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func NewPrincipalCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "principal", Short: "Manage mycel principals"}
	cmd.AddCommand(
		NewPrincipalListCommand(a),
		NewPrincipalGetCommand(a),
		NewPrincipalFindCommand(a),
		NewPrincipalCreateCommand(a),
		NewPrincipalUpdateCommand(a),
		NewPrincipalDisableCommand(a),
		NewPrincipalEnableCommand(a),
		NewPrincipalDeleteCommand(a),
		NewPrincipalPasswordCommand(a),
		NewPrincipalSessionCommand(a),
		NewPrincipalRoleCommand(a),
		NewPrincipalCapabilityCommand(a),
	)
	return cmd
}

func NewPrincipalListCommand(a *app.App) *cobra.Command {
	var includeDisabled, includeDeleted bool
	cmd := &cobra.Command{Use: "list", Short: "List principals", RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminPrincipalServiceClient(conn).ListPrincipals(authCtx, &adminv1.ListPrincipalsRequest{IncludeDisabled: includeDisabled, IncludeDeleted: includeDeleted})
		if err != nil {
			return err
		}
		if a.Output == "json" {
			return a.Print(res.GetPrincipals(), "")
		}
		app.RenderPrincipalsTable(res.GetPrincipals())
		return nil
	}}
	cmd.Flags().BoolVar(&includeDisabled, "include-disabled", false, "include disabled principals")
	cmd.Flags().BoolVar(&includeDeleted, "include-deleted", false, "include deleted principals")
	return cmd
}

func NewPrincipalGetCommand(a *app.App) *cobra.Command {
	var principalID string
	cmd := &cobra.Command{Use: "get", Short: "Get a principal", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(principalID) == "" {
			return fmt.Errorf("--principal-id is required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminPrincipalServiceClient(conn).GetPrincipal(authCtx, &adminv1.GetPrincipalRequest{PrincipalId: principalID})
		if err != nil {
			return err
		}
		return a.Print(res.GetPrincipal(), fmt.Sprintf("principal: %s\n", res.GetPrincipal().GetPrincipalId()))
	}}
	cmd.Flags().StringVar(&principalID, "principal-id", "", "principal ID")
	return cmd
}

func NewPrincipalFindCommand(a *app.App) *cobra.Command {
	var username, email string
	cmd := &cobra.Command{Use: "find", Short: "Find a principal by username or email", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(username) == "" && strings.TrimSpace(email) == "" {
			return fmt.Errorf("--principal-username or --email is required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		req := &adminv1.FindPrincipalRequest{}
		if strings.TrimSpace(username) != "" {
			req.Lookup = &adminv1.FindPrincipalRequest_Username{Username: username}
		} else {
			req.Lookup = &adminv1.FindPrincipalRequest_Email{Email: email}
		}
		res, err := adminv1.NewAdminPrincipalServiceClient(conn).FindPrincipal(authCtx, req)
		if err != nil {
			return err
		}
		return a.Print(res.GetPrincipal(), fmt.Sprintf("principal: %s\n", res.GetPrincipal().GetPrincipalId()))
	}}
	cmd.Flags().StringVar(&username, "principal-username", "", "principal username")
	cmd.Flags().StringVar(&email, "email", "", "principal email")
	return cmd
}

func NewPrincipalCreateCommand(a *app.App) *cobra.Command {
	var username, displayName, email, newPassword, kind string
	var loginEnabled, disabled bool
	var roles []string
	cmd := &cobra.Command{Use: "create", Short: "Create a principal", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(username) == "" && principalTypeFromFlag(kind) != commonv1.PrincipalType_PRINCIPAL_TYPE_SYSTEM {
			return fmt.Errorf("--principal-username is required")
		}
		if loginEnabled && newPassword == "" {
			return fmt.Errorf("--new-password is required when --login-enabled is set")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminPrincipalServiceClient(conn).CreatePrincipal(authCtx, &adminv1.CreatePrincipalRequest{Username: username, DisplayName: displayName, Email: email, Password: optionalStringPtr(newPassword), Type: principalTypeFromFlag(kind), LoginEnabled: loginEnabled, Disabled: disabled, Roles: roles})
		if err != nil {
			return err
		}
		return a.Print(res.GetPrincipal(), fmt.Sprintf("principal created: %s\n", res.GetPrincipal().GetPrincipalId()))
	}}
	cmd.Flags().StringVar(&username, "principal-username", "", "principal username")
	cmd.Flags().StringVar(&displayName, "display-name", "", "principal display name")
	cmd.Flags().StringVar(&email, "email", "", "principal email")
	cmd.Flags().StringVar(&newPassword, "new-password", "", "principal password")
	cmd.Flags().StringVar(&kind, "type", "human", "principal type: human, service, or system")
	cmd.Flags().BoolVar(&loginEnabled, "login-enabled", true, "allow password login for this principal")
	cmd.Flags().BoolVar(&disabled, "disabled", false, "create disabled principal")
	cmd.Flags().StringSliceVar(&roles, "role", nil, "role binding to add; repeatable")
	return cmd
}

func NewPrincipalUpdateCommand(a *app.App) *cobra.Command {
	var principalID, username, displayName, email, kind string
	var loginEnabled bool
	cmd := &cobra.Command{Use: "update", Short: "Update a principal", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(principalID) == "" {
			return fmt.Errorf("--principal-id is required")
		}
		principal := &adminv1.Principal{PrincipalId: principalID}
		paths := []string{}
		if cmd.Flags().Changed("principal-username") {
			principal.Username = username
			paths = append(paths, "username")
		}
		if cmd.Flags().Changed("display-name") {
			principal.DisplayName = displayName
			paths = append(paths, "display_name")
		}
		if cmd.Flags().Changed("email") {
			principal.Email = email
			paths = append(paths, "email")
		}
		if cmd.Flags().Changed("type") {
			principal.Type = principalTypeFromFlag(kind)
			paths = append(paths, "type")
		}
		if cmd.Flags().Changed("login-enabled") {
			principal.LoginEnabled = loginEnabled
			paths = append(paths, "login_enabled")
		}
		if len(paths) == 0 {
			return fmt.Errorf("at least one field flag is required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminPrincipalServiceClient(conn).UpdatePrincipal(authCtx, &adminv1.UpdatePrincipalRequest{Principal: principal, UpdateMask: &fieldmaskpb.FieldMask{Paths: paths}})
		if err != nil {
			return err
		}
		return a.Print(res.GetPrincipal(), fmt.Sprintf("principal updated: %s\n", res.GetPrincipal().GetPrincipalId()))
	}}
	cmd.Flags().StringVar(&principalID, "principal-id", "", "principal ID")
	cmd.Flags().StringVar(&username, "principal-username", "", "principal username")
	cmd.Flags().StringVar(&displayName, "display-name", "", "principal display name")
	cmd.Flags().StringVar(&email, "email", "", "principal email")
	cmd.Flags().StringVar(&kind, "type", "human", "principal type: human, service, or system")
	cmd.Flags().BoolVar(&loginEnabled, "login-enabled", true, "principal login enabled")
	return cmd
}

func NewPrincipalDisableCommand(a *app.App) *cobra.Command {
	var principalID, reason string
	var revokeSessions bool
	cmd := &cobra.Command{Use: "disable", Short: "Disable a principal", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(principalID) == "" {
			return fmt.Errorf("--principal-id is required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminPrincipalServiceClient(conn).DisablePrincipal(authCtx, &adminv1.DisablePrincipalRequest{PrincipalId: principalID, Reason: reason, RevokeSessions: revokeSessions})
		if err != nil {
			return err
		}
		return a.Print(res.GetPrincipal(), fmt.Sprintf("principal disabled: %s\n", principalID))
	}}
	cmd.Flags().StringVar(&principalID, "principal-id", "", "principal ID")
	cmd.Flags().StringVar(&reason, "reason", "", "audit reason")
	cmd.Flags().BoolVar(&revokeSessions, "revoke-sessions", false, "revoke principal sessions")
	return cmd
}

func NewPrincipalEnableCommand(a *app.App) *cobra.Command {
	var principalID string
	cmd := &cobra.Command{Use: "enable", Short: "Enable a principal", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(principalID) == "" {
			return fmt.Errorf("--principal-id is required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminPrincipalServiceClient(conn).EnablePrincipal(authCtx, &adminv1.EnablePrincipalRequest{PrincipalId: principalID})
		if err != nil {
			return err
		}
		return a.Print(res.GetPrincipal(), fmt.Sprintf("principal enabled: %s\n", principalID))
	}}
	cmd.Flags().StringVar(&principalID, "principal-id", "", "principal ID")
	return cmd
}

func NewPrincipalDeleteCommand(a *app.App) *cobra.Command {
	var principalID string
	var revokeSessions bool
	cmd := &cobra.Command{Use: "delete", Short: "Delete a principal", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(principalID) == "" {
			return fmt.Errorf("--principal-id is required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminPrincipalServiceClient(conn).DeletePrincipal(authCtx, &adminv1.DeletePrincipalRequest{PrincipalId: principalID, RevokeSessions: revokeSessions})
		if err != nil {
			return err
		}
		return a.Print(res.GetPrincipal(), fmt.Sprintf("principal deleted: %s\n", principalID))
	}}
	cmd.Flags().StringVar(&principalID, "principal-id", "", "principal ID")
	cmd.Flags().BoolVar(&revokeSessions, "revoke-sessions", false, "revoke principal sessions")
	return cmd
}

func NewPrincipalPasswordCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "password", Short: "Manage principal passwords"}
	var principalID, newPassword string
	var revokeSessions bool
	set := &cobra.Command{Use: "set", Short: "Set a principal password", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(principalID) == "" || newPassword == "" {
			return fmt.Errorf("--principal-id and --new-password are required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminPrincipalServiceClient(conn).SetPrincipalPassword(authCtx, &adminv1.SetPrincipalPasswordRequest{PrincipalId: principalID, Password: newPassword, RevokeSessions: revokeSessions})
		if err != nil {
			return err
		}
		return a.Print(res.GetPrincipal(), fmt.Sprintf("principal password changed: %s\n", principalID))
	}}
	set.Flags().StringVar(&principalID, "principal-id", "", "principal ID")
	set.Flags().StringVar(&newPassword, "new-password", "", "new password")
	set.Flags().BoolVar(&revokeSessions, "revoke-sessions", false, "revoke principal sessions")
	cmd.AddCommand(set)
	return cmd
}

func NewPrincipalSessionCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "session", Short: "Manage principal sessions"}
	cmd.AddCommand(NewPrincipalSessionListCommand(a), NewPrincipalSessionCreateCommand(a), NewPrincipalSessionRevokeCommand(a), NewPrincipalSessionRevokeAllCommand(a))
	return cmd
}

func NewPrincipalSessionListCommand(a *app.App) *cobra.Command {
	var principalID string
	var includeInactive bool
	cmd := &cobra.Command{Use: "list", Short: "List principal sessions", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(principalID) == "" {
			return fmt.Errorf("--principal-id is required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminPrincipalServiceClient(conn).ListPrincipalSessions(authCtx, &adminv1.ListPrincipalSessionsRequest{PrincipalId: principalID, IncludeInactive: includeInactive})
		if err != nil {
			return err
		}
		return a.Print(res.GetSessions(), fmt.Sprintf("principal sessions: %d\n", len(res.GetSessions())))
	}}
	cmd.Flags().StringVar(&principalID, "principal-id", "", "principal ID")
	cmd.Flags().BoolVar(&includeInactive, "include-inactive", false, "include revoked/expired sessions")
	return cmd
}

func NewPrincipalSessionCreateCommand(a *app.App) *cobra.Command {
	var principalID string
	cmd := &cobra.Command{Use: "create", Short: "Create a delegated principal session", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(principalID) == "" {
			return fmt.Errorf("--principal-id is required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminPrincipalServiceClient(conn).CreatePrincipalSession(authCtx, &adminv1.CreatePrincipalSessionRequest{PrincipalId: principalID, Client: &commonv1.ClientInfo{Name: "mycel-cli"}})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("principal session created: %s\nrefresh_token: %s\n", res.GetAuthSessionId(), res.GetRefreshToken()))
	}}
	cmd.Flags().StringVar(&principalID, "principal-id", "", "principal ID")
	return cmd
}

func NewPrincipalSessionRevokeCommand(a *app.App) *cobra.Command {
	var principalID, sessionID string
	cmd := &cobra.Command{Use: "revoke", Short: "Revoke a principal session", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(principalID) == "" || strings.TrimSpace(sessionID) == "" {
			return fmt.Errorf("--principal-id and --session-id are required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminPrincipalServiceClient(conn).RevokePrincipalSession(authCtx, &adminv1.RevokePrincipalSessionRequest{PrincipalId: principalID, AuthSessionId: sessionID})
		if err != nil {
			return err
		}
		return a.Print(res, "principal session revoked\n")
	}}
	cmd.Flags().StringVar(&principalID, "principal-id", "", "principal ID")
	cmd.Flags().StringVar(&sessionID, "session-id", "", "auth session ID")
	return cmd
}

func NewPrincipalSessionRevokeAllCommand(a *app.App) *cobra.Command {
	var principalID string
	cmd := &cobra.Command{Use: "revoke-all", Short: "Revoke all principal sessions", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(principalID) == "" {
			return fmt.Errorf("--principal-id is required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminPrincipalServiceClient(conn).RevokePrincipalSessions(authCtx, &adminv1.RevokePrincipalSessionsRequest{PrincipalId: principalID})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("principal sessions revoked: %d\n", res.GetRevokedCount()))
	}}
	cmd.Flags().StringVar(&principalID, "principal-id", "", "principal ID")
	return cmd
}

func NewPrincipalRoleCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "role", Short: "Manage principal roles"}
	cmd.AddCommand(NewPrincipalRoleListCommand(a), NewPrincipalRoleGrantCommand(a), NewPrincipalRoleRevokeCommand(a))
	return cmd
}

func NewPrincipalRoleListCommand(a *app.App) *cobra.Command {
	var principalID string
	cmd := &cobra.Command{Use: "list", Short: "List principal roles", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(principalID) == "" {
			return fmt.Errorf("--principal-id is required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminPrincipalServiceClient(conn).ListPrincipalRoles(authCtx, &adminv1.ListPrincipalRolesRequest{PrincipalId: principalID})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("principal role grants: %d\n", len(res.GetGrants())))
	}}
	cmd.Flags().StringVar(&principalID, "principal-id", "", "principal ID")
	return cmd
}

func NewPrincipalRoleGrantCommand(a *app.App) *cobra.Command {
	var principalID, role, reason string
	cmd := &cobra.Command{Use: "grant", Short: "Grant a principal role", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(principalID) == "" || strings.TrimSpace(role) == "" {
			return fmt.Errorf("--principal-id and --role are required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminPrincipalServiceClient(conn).GrantPrincipalRole(authCtx, &adminv1.GrantPrincipalRoleRequest{PrincipalId: principalID, Role: role, Reason: reason})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("principal role granted: %s\n", res.GetGrant().GetRoleGrantId()))
	}}
	cmd.Flags().StringVar(&principalID, "principal-id", "", "principal ID")
	cmd.Flags().StringVar(&role, "role", "", "role name")
	cmd.Flags().StringVar(&reason, "reason", "", "audit reason")
	return cmd
}

func NewPrincipalRoleRevokeCommand(a *app.App) *cobra.Command {
	var principalID, grantID, reason string
	cmd := &cobra.Command{Use: "revoke", Short: "Revoke a principal role", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(principalID) == "" || strings.TrimSpace(grantID) == "" {
			return fmt.Errorf("--principal-id and --grant-id are required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminPrincipalServiceClient(conn).RevokePrincipalRole(authCtx, &adminv1.RevokePrincipalRoleRequest{PrincipalId: principalID, RoleGrantId: grantID, Reason: reason})
		if err != nil {
			return err
		}
		return a.Print(res, "principal role revoked\n")
	}}
	cmd.Flags().StringVar(&principalID, "principal-id", "", "principal ID")
	cmd.Flags().StringVar(&grantID, "grant-id", "", "role grant ID")
	cmd.Flags().StringVar(&reason, "reason", "", "audit reason")
	return cmd
}

func NewPrincipalCapabilityCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "capability", Short: "Manage principal capabilities"}
	cmd.AddCommand(NewPrincipalCapabilityListCommand(a), NewPrincipalCapabilityGrantCommand(a), NewPrincipalCapabilityRevokeCommand(a))
	return cmd
}

func NewPrincipalCapabilityListCommand(a *app.App) *cobra.Command {
	var principalID string
	cmd := &cobra.Command{Use: "list", Short: "List principal capabilities", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(principalID) == "" {
			return fmt.Errorf("--principal-id is required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminPrincipalServiceClient(conn).ListPrincipalCapabilities(authCtx, &adminv1.ListPrincipalCapabilitiesRequest{PrincipalId: principalID})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("principal capability grants: %d\n", len(res.GetGrants())))
	}}
	cmd.Flags().StringVar(&principalID, "principal-id", "", "principal ID")
	return cmd
}

func NewPrincipalCapabilityGrantCommand(a *app.App) *cobra.Command {
	var principalID, capability, reason string
	cmd := &cobra.Command{Use: "grant", Short: "Grant a principal capability", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(principalID) == "" || strings.TrimSpace(capability) == "" {
			return fmt.Errorf("--principal-id and --capability are required")
		}
		parsed, err := parseCapability(capability)
		if err != nil {
			return err
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminPrincipalServiceClient(conn).GrantPrincipalCapability(authCtx, &adminv1.GrantPrincipalCapabilityRequest{PrincipalId: principalID, Capability: parsed, Reason: reason})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("principal capability granted: %s\n", res.GetGrant().GetCapabilityGrantId()))
	}}
	cmd.Flags().StringVar(&principalID, "principal-id", "", "principal ID")
	cmd.Flags().StringVar(&capability, "capability", "", "capability, e.g. identity-principal-update")
	cmd.Flags().StringVar(&reason, "reason", "", "audit reason")
	return cmd
}

func NewPrincipalCapabilityRevokeCommand(a *app.App) *cobra.Command {
	var principalID, grantID, reason string
	cmd := &cobra.Command{Use: "revoke", Short: "Revoke a principal capability", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(principalID) == "" || strings.TrimSpace(grantID) == "" {
			return fmt.Errorf("--principal-id and --grant-id are required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminPrincipalServiceClient(conn).RevokePrincipalCapability(authCtx, &adminv1.RevokePrincipalCapabilityRequest{PrincipalId: principalID, CapabilityGrantId: grantID, Reason: reason})
		if err != nil {
			return err
		}
		return a.Print(res, "principal capability revoked\n")
	}}
	cmd.Flags().StringVar(&principalID, "principal-id", "", "principal ID")
	cmd.Flags().StringVar(&grantID, "grant-id", "", "capability grant ID")
	cmd.Flags().StringVar(&reason, "reason", "", "audit reason")
	return cmd
}

func principalTypeFromFlag(raw string) commonv1.PrincipalType {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "system":
		return commonv1.PrincipalType_PRINCIPAL_TYPE_SYSTEM
	case "service":
		return commonv1.PrincipalType_PRINCIPAL_TYPE_SERVICE
	default:
		return commonv1.PrincipalType_PRINCIPAL_TYPE_HUMAN
	}
}

func optionalStringPtr(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}
