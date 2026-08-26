package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/myceldb/mycel/internal/cli/app"
	daemonconfig "github.com/myceldb/mycel/internal/daemon/config"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func NewAdminCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "admin", Aliases: []string{"admins"}, Short: "Manage daemon admins"}
	list := NewListAdminsCommand(a)
	list.Use = "list"
	list.Aliases = []string{"ls"}
	list.Short = "List daemon admins"
	cmd.AddCommand(list, NewGetAdminCommand(a), NewFindAdminCommand(a), NewCreateAdminCommand(a), NewUpdateAdminCommand(a), NewDisableAdminCommand(a), NewEnableAdminCommand(a), NewDeleteAdminCommand(a), NewAdminPasswordCommand(a), NewAdminRoleCommand(a), NewAdminCapabilityCommand(a), NewAdminSessionCommand(a), NewAdminDomainCommand(a), NewAdminActivityCommand(a), NewAdminBackupCommand(a), NewAdminUserBackupCommand(a))
	return cmd
}

func NewListAdminsCommand(a *app.App) *cobra.Command {
	return &cobra.Command{Use: "admins", Short: "List daemon admin principals", RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminPrincipalServiceClient(conn).ListPrincipals(authCtx, &adminv1.ListPrincipalsRequest{})
		if err != nil {
			return fmt.Errorf("list daemon admin principals: %w", err)
		}
		return a.Print(res.GetPrincipals(), fmt.Sprintf("principals: %d\n", len(res.GetPrincipals())))
	}}
}

func NewGetAdminCommand(a *app.App) *cobra.Command {
	var id string
	cmd := &cobra.Command{Use: "get", Short: "Get a daemon admin principal", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" {
			return fmt.Errorf("--operator-id/--principal-id is required")
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
		return printPrincipalAlias(a, res.GetPrincipal(), "admin principal: "+res.GetPrincipal().GetUsername()+"\n")
	}}
	cmd.Flags().StringVar(&id, "principal-id", "", "principal id")
	cmd.Flags().StringVar(&id, "operator-id", "", "deprecated; use --principal-id")
	return cmd
}

func NewFindAdminCommand(a *app.App) *cobra.Command {
	var principalUsername, email string
	cmd := &cobra.Command{Use: "find", Short: "Find a daemon admin principal", RunE: func(cmd *cobra.Command, args []string) error {
		if principalUsername == "" && email == "" {
			return fmt.Errorf("--operator-username/--principal-username or --email is required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		req := &adminv1.FindPrincipalRequest{}
		if principalUsername != "" {
			req.Lookup = &adminv1.FindPrincipalRequest_Username{Username: principalUsername}
		} else {
			req.Lookup = &adminv1.FindPrincipalRequest_Email{Email: email}
		}
		res, err := adminv1.NewAdminPrincipalServiceClient(conn).FindPrincipal(authCtx, req)
		if err != nil {
			return err
		}
		return printPrincipalAlias(a, res.GetPrincipal(), "admin principal: "+res.GetPrincipal().GetUsername()+"\n")
	}}
	cmd.Flags().StringVar(&principalUsername, "principal-username", "", "principal username")
	cmd.Flags().StringVar(&principalUsername, "operator-username", "", "deprecated; use --principal-username")
	cmd.Flags().StringVar(&email, "email", "", "principal email")
	return cmd
}

func NewCreateAdminCommand(a *app.App) *cobra.Command {
	var principalUsername, newPassword, email string
	var roles []string
	var disabled bool
	cmd := &cobra.Command{Use: "create", Short: "Create a daemon admin principal", RunE: func(cmd *cobra.Command, args []string) error {
		if principalUsername == "" || newPassword == "" {
			return fmt.Errorf("--operator-username/--principal-username and --new-password are required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		req := &adminv1.CreatePrincipalRequest{Username: principalUsername, Email: email, Password: &newPassword, Type: commonv1.PrincipalType_PRINCIPAL_TYPE_HUMAN, LoginEnabled: true, Disabled: disabled}
		for _, raw := range roles {
			req.Roles = append(req.Roles, principalRoleFromAdminFlag(raw))
		}
		res, err := adminv1.NewAdminPrincipalServiceClient(conn).CreatePrincipal(authCtx, req)
		if err != nil {
			return err
		}
		return printPrincipalAlias(a, res.GetPrincipal(), "admin principal created: "+res.GetPrincipal().GetUsername()+"\n")
	}}
	cmd.Flags().StringVar(&principalUsername, "principal-username", "", "new principal username")
	cmd.Flags().StringVar(&principalUsername, "operator-username", "", "deprecated; use --principal-username")
	cmd.Flags().StringVar(&newPassword, "new-password", "", "new principal password")
	cmd.Flags().StringVar(&email, "email", "", "new principal email")
	cmd.Flags().StringArrayVar(&roles, "role", nil, "initial role, e.g. system.admin, identity.admin")
	cmd.Flags().BoolVar(&disabled, "disabled", false, "create disabled")
	return cmd
}

func NewUpdateAdminCommand(a *app.App) *cobra.Command {
	var id, email string
	cmd := &cobra.Command{Use: "update", Short: "Update a daemon admin principal", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" {
			return fmt.Errorf("--operator-id/--principal-id is required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminPrincipalServiceClient(conn).UpdatePrincipal(authCtx, &adminv1.UpdatePrincipalRequest{Principal: &adminv1.Principal{PrincipalId: id, Email: email}, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"email"}}})
		if err != nil {
			return err
		}
		return printPrincipalAlias(a, res.GetPrincipal(), "admin principal updated: "+res.GetPrincipal().GetUsername()+"\n")
	}}
	cmd.Flags().StringVar(&id, "principal-id", "", "principal id")
	cmd.Flags().StringVar(&id, "operator-id", "", "deprecated; use --principal-id")
	cmd.Flags().StringVar(&email, "email", "", "principal email")
	return cmd
}

func NewDisableAdminCommand(a *app.App) *cobra.Command {
	var id, reason string
	cmd := &cobra.Command{Use: "disable", Short: "Disable a daemon admin principal", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" {
			return fmt.Errorf("--operator-id/--principal-id is required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminPrincipalServiceClient(conn).DisablePrincipal(authCtx, &adminv1.DisablePrincipalRequest{PrincipalId: id, Reason: reason})
		if err != nil {
			return err
		}
		return printPrincipalAlias(a, res.GetPrincipal(), "admin principal disabled: "+res.GetPrincipal().GetUsername()+"\n")
	}}
	cmd.Flags().StringVar(&id, "principal-id", "", "principal id")
	cmd.Flags().StringVar(&id, "operator-id", "", "deprecated; use --principal-id")
	cmd.Flags().StringVar(&reason, "reason", "", "reason")
	return cmd
}

func NewEnableAdminCommand(a *app.App) *cobra.Command {
	var id string
	cmd := &cobra.Command{Use: "enable", Short: "Enable a daemon admin principal", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" {
			return fmt.Errorf("--operator-id/--principal-id is required")
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
		return printPrincipalAlias(a, res.GetPrincipal(), "admin principal enabled: "+res.GetPrincipal().GetUsername()+"\n")
	}}
	cmd.Flags().StringVar(&id, "principal-id", "", "principal id")
	cmd.Flags().StringVar(&id, "operator-id", "", "deprecated; use --principal-id")
	return cmd
}

func NewDeleteAdminCommand(a *app.App) *cobra.Command {
	var id string
	cmd := &cobra.Command{Use: "delete", Aliases: []string{"del", "rm"}, Short: "Soft-delete a daemon admin principal", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" {
			return fmt.Errorf("--operator-id/--principal-id is required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminPrincipalServiceClient(conn).DeletePrincipal(authCtx, &adminv1.DeletePrincipalRequest{PrincipalId: id})
		if err != nil {
			return err
		}
		return printPrincipalAlias(a, res.GetPrincipal(), "admin principal deleted: "+res.GetPrincipal().GetUsername()+"\n")
	}}
	cmd.Flags().StringVar(&id, "principal-id", "", "principal id")
	cmd.Flags().StringVar(&id, "operator-id", "", "deprecated; use --principal-id")
	return cmd
}

func NewAdminPasswordCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "password", Short: "Manage daemon admin principal passwords"}
	set := NewSetAdminPasswordCommand(a)
	set.Use = "set"
	set.Short = "Change a daemon admin principal password"
	cmd.AddCommand(set)
	return cmd
}

func NewSetAdminPasswordCommand(a *app.App) *cobra.Command {
	var newPassword, principalID string
	cmd := &cobra.Command{Use: "set", Short: "Change a daemon admin principal password", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(newPassword) == "" {
			return fmt.Errorf("--new-password is required")
		}
		conn, authCtx, principal, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		if principalID == "" {
			principalID = principal.GetPrincipalId()
		}
		res, err := adminv1.NewAdminPrincipalServiceClient(conn).SetPrincipalPassword(authCtx, &adminv1.SetPrincipalPasswordRequest{PrincipalId: principalID, Password: newPassword})
		if err != nil {
			return fmt.Errorf("set daemon admin principal password: %w", err)
		}
		return printPrincipalAlias(a, res.GetPrincipal(), "admin password changed: "+res.GetPrincipal().GetUsername()+"\n")
	}}
	cmd.Flags().StringVar(&principalID, "principal-id", "", "principal id (defaults to authenticated principal)")
	cmd.Flags().StringVar(&principalID, "operator-id", "", "deprecated; use --principal-id")
	cmd.Flags().StringVar(&newPassword, "new-password", "", "new daemon admin password")
	return cmd
}

func NewAdminRoleCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "role", Short: "Manage daemon admin principal roles"}
	cmd.AddCommand(NewListAdminRolesCommand(a), NewGrantAdminRoleCommand(a), NewRevokeAdminRoleCommand(a))
	return cmd
}

func NewListAdminRolesCommand(a *app.App) *cobra.Command {
	var id string
	cmd := &cobra.Command{Use: "list", Short: "List daemon admin principal roles", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" {
			return fmt.Errorf("--operator-id/--principal-id is required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminPrincipalServiceClient(conn).ListPrincipalRoles(authCtx, &adminv1.ListPrincipalRolesRequest{PrincipalId: id})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("principal role grants: %d\n", len(res.GetGrants())))
	}}
	cmd.Flags().StringVar(&id, "principal-id", "", "principal id")
	cmd.Flags().StringVar(&id, "operator-id", "", "deprecated; use --principal-id")
	return cmd
}

func NewGrantAdminRoleCommand(a *app.App) *cobra.Command {
	var id, role, reason string
	cmd := &cobra.Command{Use: "grant", Short: "Grant a daemon admin principal role", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" || role == "" {
			return fmt.Errorf("--operator-id/--principal-id and --role are required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminPrincipalServiceClient(conn).GrantPrincipalRole(authCtx, &adminv1.GrantPrincipalRoleRequest{PrincipalId: id, Role: principalRoleFromAdminFlag(role), Reason: reason})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("principal role granted: %s\n", res.GetGrant().GetRoleGrantId()))
	}}
	cmd.Flags().StringVar(&id, "principal-id", "", "principal id")
	cmd.Flags().StringVar(&id, "operator-id", "", "deprecated; use --principal-id")
	cmd.Flags().StringVar(&role, "role", "", "role")
	cmd.Flags().StringVar(&reason, "reason", "", "reason")
	return cmd
}

func NewRevokeAdminRoleCommand(a *app.App) *cobra.Command {
	var id, grantID string
	cmd := &cobra.Command{Use: "revoke", Short: "Revoke a daemon admin principal role", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" || grantID == "" {
			return fmt.Errorf("--operator-id/--principal-id and --grant-id are required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminPrincipalServiceClient(conn).RevokePrincipalRole(authCtx, &adminv1.RevokePrincipalRoleRequest{PrincipalId: id, RoleGrantId: grantID})
		if err != nil {
			return err
		}
		return a.Print(res, "principal role revoked\n")
	}}
	cmd.Flags().StringVar(&id, "principal-id", "", "principal id")
	cmd.Flags().StringVar(&id, "operator-id", "", "deprecated; use --principal-id")
	cmd.Flags().StringVar(&grantID, "grant-id", "", "grant id")
	return cmd
}

func NewAdminCapabilityCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "capability", Short: "Manage daemon admin principal capabilities"}
	cmd.AddCommand(NewListAdminCapabilitiesCommand(a), NewGrantAdminCapabilityCommand(a), NewRevokeAdminCapabilityCommand(a))
	return cmd
}

func NewListAdminCapabilitiesCommand(a *app.App) *cobra.Command {
	var id string
	cmd := &cobra.Command{Use: "list", Short: "List daemon admin principal capabilities", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" {
			return fmt.Errorf("--operator-id/--principal-id is required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminPrincipalServiceClient(conn).ListPrincipalCapabilities(authCtx, &adminv1.ListPrincipalCapabilitiesRequest{PrincipalId: id})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("principal capability grants: %d\n", len(res.GetGrants())))
	}}
	cmd.Flags().StringVar(&id, "principal-id", "", "principal id")
	cmd.Flags().StringVar(&id, "operator-id", "", "deprecated; use --principal-id")
	return cmd
}

func NewGrantAdminCapabilityCommand(a *app.App) *cobra.Command {
	var id, capability, reason string
	cmd := &cobra.Command{Use: "grant", Short: "Grant a daemon admin principal capability", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" || capability == "" {
			return fmt.Errorf("--operator-id/--principal-id and --capability are required")
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
		res, err := adminv1.NewAdminPrincipalServiceClient(conn).GrantPrincipalCapability(authCtx, &adminv1.GrantPrincipalCapabilityRequest{PrincipalId: id, Capability: parsed, Reason: reason})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("principal capability granted: %s\n", res.GetGrant().GetCapabilityGrantId()))
	}}
	cmd.Flags().StringVar(&id, "principal-id", "", "principal id")
	cmd.Flags().StringVar(&id, "operator-id", "", "deprecated; use --principal-id")
	cmd.Flags().StringVar(&capability, "capability", "", "capability, e.g. identity-principal-update")
	cmd.Flags().StringVar(&reason, "reason", "", "reason")
	return cmd
}

func NewRevokeAdminCapabilityCommand(a *app.App) *cobra.Command {
	var id, grantID string
	cmd := &cobra.Command{Use: "revoke", Short: "Revoke a daemon admin principal capability", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" || grantID == "" {
			return fmt.Errorf("--operator-id/--principal-id and --grant-id are required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminPrincipalServiceClient(conn).RevokePrincipalCapability(authCtx, &adminv1.RevokePrincipalCapabilityRequest{PrincipalId: id, CapabilityGrantId: grantID})
		if err != nil {
			return err
		}
		return a.Print(res, "principal capability revoked\n")
	}}
	cmd.Flags().StringVar(&id, "principal-id", "", "principal id")
	cmd.Flags().StringVar(&id, "operator-id", "", "deprecated; use --principal-id")
	cmd.Flags().StringVar(&grantID, "grant-id", "", "grant id")
	return cmd
}

func NewAdminSessionCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "session", Short: "Manage daemon admin principal sessions"}
	cmd.AddCommand(NewListAdminSessionsCommand(a), NewRevokeAdminSessionCommand(a), NewRevokeAdminSessionsCommand(a))
	return cmd
}

func NewListAdminSessionsCommand(a *app.App) *cobra.Command {
	var id string
	cmd := &cobra.Command{Use: "list", Short: "List daemon admin principal sessions", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" {
			return fmt.Errorf("--operator-id/--principal-id is required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminPrincipalServiceClient(conn).ListPrincipalSessions(authCtx, &adminv1.ListPrincipalSessionsRequest{PrincipalId: id})
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
	cmd.Flags().StringVar(&id, "operator-id", "", "deprecated; use --principal-id")
	return cmd
}

func NewRevokeAdminSessionCommand(a *app.App) *cobra.Command {
	var id, sessionID string
	cmd := &cobra.Command{Use: "revoke", Short: "Revoke daemon admin principal session", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" || sessionID == "" {
			return fmt.Errorf("--operator-id/--principal-id and --session-id are required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		_, err = adminv1.NewAdminPrincipalServiceClient(conn).RevokePrincipalSession(authCtx, &adminv1.RevokePrincipalSessionRequest{PrincipalId: id, AuthSessionId: sessionID})
		if err != nil && status.Code(err) != codes.NotFound {
			return err
		}
		fmt.Println("session revoked")
		return nil
	}}
	cmd.Flags().StringVar(&id, "principal-id", "", "principal id")
	cmd.Flags().StringVar(&id, "operator-id", "", "deprecated; use --principal-id")
	cmd.Flags().StringVar(&sessionID, "session-id", "", "session id")
	return cmd
}

func NewRevokeAdminSessionsCommand(a *app.App) *cobra.Command {
	var id string
	cmd := &cobra.Command{Use: "revoke-all", Short: "Revoke all daemon admin principal sessions", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" {
			return fmt.Errorf("--operator-id/--principal-id is required")
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
	cmd.Flags().StringVar(&id, "operator-id", "", "deprecated; use --principal-id")
	return cmd
}

func printPrincipalAlias(a *app.App, principal *adminv1.Principal, text string) error {
	if a.Output == "json" {
		return a.Print(principal, "")
	}
	return a.Print(principal, text)
}

func principalRoleFromAdminFlag(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "system-admin", "system_admin", "system.admin":
		return "system.admin"
	case "user-admin", "user_admin", "operator-admin", "operator_admin", "identity-admin", "identity_admin", "identity.admin":
		return "identity.admin"
	case "space-admin", "space_admin", "space.admin":
		return "space.admin"
	case "semantic-admin", "semantic_admin", "semantic.admin":
		return "semantic.admin"
	case "storage-admin", "storage_admin", "backup-operator", "backup_operator", "backup.operator":
		return "backup.operator"
	case "mesh-admin", "mesh_admin", "cluster-operator", "cluster_operator", "cluster.operator":
		return "cluster.operator"
	case "audit-reader", "audit_reader", "audit.reader":
		return "audit.reader"
	default:
		return strings.TrimSpace(raw)
	}
}

func loginDaemonOperator(ctx context.Context, a *app.App) (*grpc.ClientConn, context.Context, *commonv1.AuthPrincipal, error) {
	if strings.TrimSpace(a.UserRef) == "" || strings.TrimSpace(a.Password) == "" {
		return nil, nil, nil, fmt.Errorf("--username/-u and --password/-p are required for admin commands")
	}
	conn, addr, err := dialDaemon(ctx, a)
	if err != nil {
		return nil, nil, nil, err
	}
	authClient := commonv1.NewAuthServiceClient(conn)
	login, err := authClient.Login(ctx, &commonv1.LoginRequest{Username: a.UserRef, Password: a.Password, Client: &commonv1.ClientInfo{Name: "mycel-cli", Platform: "cli"}})
	if err != nil {
		_ = conn.Close()
		return nil, nil, nil, fmt.Errorf("login daemon principal via %s: %w", addr, err)
	}
	authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+login.GetAccessToken())
	who, err := authClient.WhoAmI(authCtx, &commonv1.WhoAmIRequest{})
	if err != nil {
		_ = conn.Close()
		return nil, nil, nil, fmt.Errorf("resolve daemon principal identity via %s: %w", addr, err)
	}
	return conn, authCtx, who.GetPrincipal(), nil
}

func NewAdminDomainCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "domain", Short: "Admin domain lookup helpers"}
	cmd.AddCommand(NewAdminDomainListCommand(a), NewAdminDomainGetCommand(a))
	return cmd
}

func NewAdminDomainListCommand(a *app.App) *cobra.Command {
	var spaceID, pageToken string
	var pageSize int32
	cmd := &cobra.Command{Use: "list", Short: "List domains in a space via admin gRPC", RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminDomainServiceClient(conn).ListDomains(authCtx, &adminv1.AdminDomainServiceListDomainsRequest{SpaceId: spaceID, PageSize: pageSize, PageToken: pageToken})
		if err != nil {
			return err
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		for _, domain := range res.GetDomains() {
			fmt.Printf("%s\t%s\t%s\n", domain.GetDomainId(), domain.GetKey(), domain.GetName())
		}
		if res.GetNextPageToken() != "" {
			fmt.Printf("next page token: %s\n", res.GetNextPageToken())
		}
		return nil
	}}
	cmd.Flags().StringVar(&spaceID, "space-id", "", "space ID")
	cmd.Flags().Int32Var(&pageSize, "page-size", 100, "page size")
	cmd.Flags().StringVar(&pageToken, "page-token", "", "page token")
	_ = cmd.MarkFlagRequired("space-id")
	return cmd
}

func NewAdminDomainGetCommand(a *app.App) *cobra.Command {
	var spaceID string
	cmd := &cobra.Command{Use: "get DOMAIN", Short: "Get a domain by UUID or key via admin gRPC", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminDomainServiceClient(conn).GetDomain(authCtx, &adminv1.AdminDomainServiceGetDomainRequest{SpaceId: spaceID, DomainRef: args[0]})
		if err != nil {
			return err
		}
		return a.Print(res.GetDomain(), fmt.Sprintf("domain: %s\n", res.GetDomain().GetDomainId()))
	}}
	cmd.Flags().StringVar(&spaceID, "space-id", "", "space ID")
	_ = cmd.MarkFlagRequired("space-id")
	return cmd
}

func resolveDaemonAddr(a *app.App) (string, error) {
	if strings.TrimSpace(a.DaemonAddr) != "" {
		return strings.TrimSpace(a.DaemonAddr), nil
	}
	cfg, err := daemonconfig.LoadFromEnv()
	if err != nil {
		return "", err
	}
	return cfg.GRPCAddr, nil
}

func parseCapability(raw string) (commonv1.Capability, error) {
	key := strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(raw), "-", "_"))
	if !strings.HasPrefix(key, "CAPABILITY_") {
		key = "CAPABILITY_" + key
	}
	if value, ok := commonv1.Capability_value[key]; ok && value != int32(commonv1.Capability_CAPABILITY_UNSPECIFIED) {
		return commonv1.Capability(value), nil
	}
	return commonv1.Capability_CAPABILITY_UNSPECIFIED, fmt.Errorf("unknown capability %q", raw)
}
