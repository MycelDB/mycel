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
	"google.golang.org/grpc/metadata"
)

func NewAdminCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "admin", Aliases: []string{"admins"}, Short: "Manage daemon admins"}
	list := NewListAdminsCommand(a)
	list.Use = "list"
	list.Aliases = []string{"ls"}
	list.Short = "List daemon admins"
	cmd.AddCommand(list, NewGetAdminCommand(a), NewFindAdminCommand(a), NewCreateAdminCommand(a), NewUpdateAdminCommand(a), NewDisableAdminCommand(a), NewEnableAdminCommand(a), NewDeleteAdminCommand(a), NewAdminPasswordCommand(a), NewAdminRoleCommand(a), NewAdminCapabilityCommand(a), NewAdminSessionCommand(a), NewAdminDomainCommand(a), NewAdminBackupCommand(a))
	return cmd
}

func NewListAdminsCommand(a *app.App) *cobra.Command {
	return &cobra.Command{Use: "admins", Short: "List daemon admins", RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		client := adminv1.NewAdminOperatorServiceClient(conn)
		res, err := client.ListOperators(authCtx, &adminv1.ListOperatorsRequest{})
		if err != nil {
			return fmt.Errorf("list daemon admins: %w", err)
		}
		if a.Output == "json" {
			return a.Print(res.GetOperators(), "")
		}
		app.RenderDaemonOperatorsTable(res.GetOperators())
		return nil
	}}
}

func NewGetAdminCommand(a *app.App) *cobra.Command {
	var id string
	cmd := &cobra.Command{Use: "get", Short: "Get a daemon admin", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" {
			return fmt.Errorf("--operator-id is required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminOperatorServiceClient(conn).GetOperator(authCtx, &adminv1.GetOperatorRequest{OperatorId: id})
		if err != nil {
			return err
		}
		return printOperator(a, res.GetOperator(), "admin: "+res.GetOperator().GetUsername()+"\n")
	}}
	cmd.Flags().StringVar(&id, "operator-id", "", "operator id")
	return cmd
}

func NewFindAdminCommand(a *app.App) *cobra.Command {
	var operatorUsername, email string
	cmd := &cobra.Command{Use: "find", Short: "Find a daemon admin", RunE: func(cmd *cobra.Command, args []string) error {
		if operatorUsername == "" && email == "" {
			return fmt.Errorf("--operator-username or --email is required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		req := &adminv1.FindOperatorRequest{}
		if operatorUsername != "" {
			req.Lookup = &adminv1.FindOperatorRequest_Username{Username: operatorUsername}
		} else {
			req.Lookup = &adminv1.FindOperatorRequest_Email{Email: email}
		}
		res, err := adminv1.NewAdminOperatorServiceClient(conn).FindOperator(authCtx, req)
		if err != nil {
			return err
		}
		return printOperator(a, res.GetOperator(), "admin: "+res.GetOperator().GetUsername()+"\n")
	}}
	cmd.Flags().StringVar(&operatorUsername, "operator-username", "", "operator username")
	cmd.Flags().StringVar(&email, "email", "", "operator email")
	return cmd
}

func NewCreateAdminCommand(a *app.App) *cobra.Command {
	var operatorUsername, newPassword, email string
	var roles []string
	var disabled bool
	cmd := &cobra.Command{Use: "create", Short: "Create a daemon admin", RunE: func(cmd *cobra.Command, args []string) error {
		if operatorUsername == "" || newPassword == "" {
			return fmt.Errorf("--operator-username and --new-password are required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		req := &adminv1.CreateOperatorRequest{Username: operatorUsername, Email: email, Password: &newPassword, Disabled: disabled}
		for _, raw := range roles {
			role, err := parseOperatorRole(raw)
			if err != nil {
				return err
			}
			req.Roles = append(req.Roles, role)
		}
		res, err := adminv1.NewAdminOperatorServiceClient(conn).CreateOperator(authCtx, req)
		if err != nil {
			return err
		}
		return printOperator(a, res.GetOperator(), "admin created: "+res.GetOperator().GetUsername()+"\n")
	}}
	cmd.Flags().StringVar(&operatorUsername, "operator-username", "", "new operator username")
	cmd.Flags().StringVar(&newPassword, "new-password", "", "new operator password")
	cmd.Flags().StringVar(&email, "email", "", "new operator email")
	cmd.Flags().StringArrayVar(&roles, "role", nil, "initial role, e.g. system-admin, user-admin")
	cmd.Flags().BoolVar(&disabled, "disabled", false, "create disabled")
	return cmd
}

func NewUpdateAdminCommand(a *app.App) *cobra.Command {
	var id, email string
	cmd := &cobra.Command{Use: "update", Short: "Update a daemon admin", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" {
			return fmt.Errorf("--operator-id is required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminOperatorServiceClient(conn).UpdateOperator(authCtx, &adminv1.UpdateOperatorRequest{Operator: &adminv1.Operator{OperatorId: id, Email: email}})
		if err != nil {
			return err
		}
		return printOperator(a, res.GetOperator(), "admin updated: "+res.GetOperator().GetUsername()+"\n")
	}}
	cmd.Flags().StringVar(&id, "operator-id", "", "operator id")
	cmd.Flags().StringVar(&email, "email", "", "operator email")
	return cmd
}
func NewDisableAdminCommand(a *app.App) *cobra.Command {
	var id, reason string
	cmd := &cobra.Command{Use: "disable", Short: "Disable a daemon admin", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" {
			return fmt.Errorf("--operator-id is required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminOperatorServiceClient(conn).DisableOperator(authCtx, &adminv1.DisableOperatorRequest{OperatorId: id, Reason: reason})
		if err != nil {
			return err
		}
		return printOperator(a, res.GetOperator(), "admin disabled: "+res.GetOperator().GetUsername()+"\n")
	}}
	cmd.Flags().StringVar(&id, "operator-id", "", "operator id")
	cmd.Flags().StringVar(&reason, "reason", "", "reason")
	return cmd
}
func NewEnableAdminCommand(a *app.App) *cobra.Command {
	var id string
	cmd := &cobra.Command{Use: "enable", Short: "Enable a daemon admin", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" {
			return fmt.Errorf("--operator-id is required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminOperatorServiceClient(conn).EnableOperator(authCtx, &adminv1.EnableOperatorRequest{OperatorId: id})
		if err != nil {
			return err
		}
		return printOperator(a, res.GetOperator(), "admin enabled: "+res.GetOperator().GetUsername()+"\n")
	}}
	cmd.Flags().StringVar(&id, "operator-id", "", "operator id")
	return cmd
}
func NewDeleteAdminCommand(a *app.App) *cobra.Command {
	var id string
	cmd := &cobra.Command{Use: "delete", Aliases: []string{"del", "rm"}, Short: "Soft-delete a daemon admin", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" {
			return fmt.Errorf("--operator-id is required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminOperatorServiceClient(conn).DeleteOperator(authCtx, &adminv1.DeleteOperatorRequest{OperatorId: id})
		if err != nil {
			return err
		}
		return printOperator(a, res.GetOperator(), "admin deleted: "+res.GetOperator().GetUsername()+"\n")
	}}
	cmd.Flags().StringVar(&id, "operator-id", "", "operator id")
	return cmd
}

func NewAdminPasswordCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "password", Short: "Manage daemon admin passwords"}
	set := NewSetAdminPasswordCommand(a)
	set.Use = "set"
	set.Short = "Change a daemon admin password"
	cmd.AddCommand(set)
	return cmd
}
func NewSetAdminPasswordCommand(a *app.App) *cobra.Command {
	var newPassword, operatorID string
	cmd := &cobra.Command{Use: "set", Short: "Change a daemon admin password", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(newPassword) == "" {
			return fmt.Errorf("--new-password is required")
		}
		conn, authCtx, operator, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		if operatorID == "" {
			operatorID = operator.GetOperatorId()
		}
		res, err := adminv1.NewAdminOperatorServiceClient(conn).SetOperatorPassword(authCtx, &adminv1.SetOperatorPasswordRequest{OperatorId: operatorID, Password: newPassword})
		if err != nil {
			return fmt.Errorf("set daemon admin password: %w", err)
		}
		return printOperator(a, res.GetOperator(), "admin password changed: "+res.GetOperator().GetUsername()+"\n")
	}}
	cmd.Flags().StringVar(&operatorID, "operator-id", "", "operator id (defaults to authenticated operator)")
	cmd.Flags().StringVar(&newPassword, "new-password", "", "new daemon admin password")
	return cmd
}

func NewAdminRoleCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "role", Short: "Manage daemon admin roles"}
	cmd.AddCommand(NewListAdminRolesCommand(a), NewGrantAdminRoleCommand(a), NewRevokeAdminRoleCommand(a))
	return cmd
}
func NewListAdminRolesCommand(a *app.App) *cobra.Command {
	var id string
	cmd := &cobra.Command{Use: "list", Short: "List daemon admin roles", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" {
			return fmt.Errorf("--operator-id is required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminOperatorServiceClient(conn).ListOperatorRoles(authCtx, &adminv1.ListOperatorRolesRequest{OperatorId: id})
		if err != nil {
			return err
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		for _, grant := range res.GetGrants() {
			fmt.Printf("%s\t%s\n", grant.GetRoleGrantId(), grant.GetRole().String())
		}
		return nil
	}}
	cmd.Flags().StringVar(&id, "operator-id", "", "operator id")
	return cmd
}
func NewGrantAdminRoleCommand(a *app.App) *cobra.Command {
	var id, role, reason string
	cmd := &cobra.Command{Use: "grant", Short: "Grant a daemon admin role", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" || role == "" {
			return fmt.Errorf("--operator-id and --role are required")
		}
		parsed, err := parseOperatorRole(role)
		if err != nil {
			return err
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminOperatorServiceClient(conn).GrantOperatorRole(authCtx, &adminv1.GrantOperatorRoleRequest{OperatorId: id, Role: parsed, Reason: reason})
		if err != nil {
			return err
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		fmt.Printf("role granted: %s\n", res.GetGrant().GetRoleGrantId())
		return nil
	}}
	cmd.Flags().StringVar(&id, "operator-id", "", "operator id")
	cmd.Flags().StringVar(&role, "role", "", "role")
	cmd.Flags().StringVar(&reason, "reason", "", "reason")
	return cmd
}
func NewRevokeAdminRoleCommand(a *app.App) *cobra.Command {
	var id, grantID string
	cmd := &cobra.Command{Use: "revoke", Short: "Revoke a daemon admin role", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" || grantID == "" {
			return fmt.Errorf("--operator-id and --grant-id are required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminOperatorServiceClient(conn).RevokeOperatorRole(authCtx, &adminv1.RevokeOperatorRoleRequest{OperatorId: id, RoleGrantId: grantID})
		if err != nil {
			return err
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		fmt.Println("role revoked")
		return nil
	}}
	cmd.Flags().StringVar(&id, "operator-id", "", "operator id")
	cmd.Flags().StringVar(&grantID, "grant-id", "", "grant id")
	return cmd
}

func NewAdminCapabilityCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "capability", Short: "Manage daemon admin capabilities"}
	cmd.AddCommand(NewListAdminCapabilitiesCommand(a), NewGrantAdminCapabilityCommand(a), NewRevokeAdminCapabilityCommand(a))
	return cmd
}
func NewListAdminCapabilitiesCommand(a *app.App) *cobra.Command {
	var id string
	cmd := &cobra.Command{Use: "list", Short: "List daemon admin capabilities", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" {
			return fmt.Errorf("--operator-id is required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminOperatorServiceClient(conn).ListOperatorCapabilities(authCtx, &adminv1.ListOperatorCapabilitiesRequest{OperatorId: id})
		if err != nil {
			return err
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		for _, cap := range res.GetEffectiveCapabilities() {
			fmt.Println(cap.String())
		}
		return nil
	}}
	cmd.Flags().StringVar(&id, "operator-id", "", "operator id")
	return cmd
}
func NewGrantAdminCapabilityCommand(a *app.App) *cobra.Command {
	var id, capability, reason string
	cmd := &cobra.Command{Use: "grant", Short: "Grant a daemon admin capability", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" || capability == "" {
			return fmt.Errorf("--operator-id and --capability are required")
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
		res, err := adminv1.NewAdminOperatorServiceClient(conn).GrantOperatorCapability(authCtx, &adminv1.GrantOperatorCapabilityRequest{OperatorId: id, Capability: parsed, Reason: reason})
		if err != nil {
			return err
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		fmt.Printf("capability granted: %s\n", res.GetGrant().GetCapabilityGrantId())
		return nil
	}}
	cmd.Flags().StringVar(&id, "operator-id", "", "operator id")
	cmd.Flags().StringVar(&capability, "capability", "", "capability, e.g. operator-manage")
	cmd.Flags().StringVar(&reason, "reason", "", "reason")
	return cmd
}
func NewRevokeAdminCapabilityCommand(a *app.App) *cobra.Command {
	var id, grantID string
	cmd := &cobra.Command{Use: "revoke", Short: "Revoke a daemon admin capability", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" || grantID == "" {
			return fmt.Errorf("--operator-id and --grant-id are required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminOperatorServiceClient(conn).RevokeOperatorCapability(authCtx, &adminv1.RevokeOperatorCapabilityRequest{OperatorId: id, CapabilityGrantId: grantID})
		if err != nil {
			return err
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		fmt.Println("capability revoked")
		return nil
	}}
	cmd.Flags().StringVar(&id, "operator-id", "", "operator id")
	cmd.Flags().StringVar(&grantID, "grant-id", "", "grant id")
	return cmd
}

func NewAdminSessionCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "session", Short: "Manage daemon admin sessions"}
	cmd.AddCommand(NewListAdminSessionsCommand(a), NewRevokeAdminSessionCommand(a), NewRevokeAdminSessionsCommand(a))
	return cmd
}
func NewListAdminSessionsCommand(a *app.App) *cobra.Command {
	var id string
	cmd := &cobra.Command{Use: "list", Short: "List daemon admin sessions", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" {
			return fmt.Errorf("--operator-id is required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminOperatorServiceClient(conn).ListOperatorSessions(authCtx, &adminv1.ListOperatorSessionsRequest{OperatorId: id})
		if err != nil {
			return err
		}
		sessions := res.GetSessions()
		if sessions == nil {
			sessions = []*adminv1.OperatorAuthSessionSummary{}
		}
		return a.Print(sessions, "")
	}}
	cmd.Flags().StringVar(&id, "operator-id", "", "operator id")
	return cmd
}
func NewRevokeAdminSessionCommand(a *app.App) *cobra.Command {
	var id, sessionID string
	cmd := &cobra.Command{Use: "revoke", Short: "Revoke daemon admin session", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" || sessionID == "" {
			return fmt.Errorf("--operator-id and --session-id are required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		_, err = adminv1.NewAdminOperatorServiceClient(conn).RevokeOperatorSession(authCtx, &adminv1.RevokeOperatorSessionRequest{OperatorId: id, AuthSessionId: sessionID})
		if err != nil {
			return err
		}
		fmt.Println("session revoked")
		return nil
	}}
	cmd.Flags().StringVar(&id, "operator-id", "", "operator id")
	cmd.Flags().StringVar(&sessionID, "session-id", "", "session id")
	return cmd
}
func NewRevokeAdminSessionsCommand(a *app.App) *cobra.Command {
	var id string
	cmd := &cobra.Command{Use: "revoke-all", Short: "Revoke all daemon admin sessions", RunE: func(cmd *cobra.Command, args []string) error {
		if id == "" {
			return fmt.Errorf("--operator-id is required")
		}
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := adminv1.NewAdminOperatorServiceClient(conn).RevokeOperatorSessions(authCtx, &adminv1.RevokeOperatorSessionsRequest{OperatorId: id})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("sessions revoked: %d\n", res.GetRevokedCount()))
	}}
	cmd.Flags().StringVar(&id, "operator-id", "", "operator id")
	return cmd
}

func printOperator(a *app.App, operator *adminv1.Operator, text string) error {
	if a.Output == "json" {
		return a.Print(operator, "")
	}
	return a.Print(operator, text)
}

func loginDaemonOperator(ctx context.Context, a *app.App) (*grpc.ClientConn, context.Context, *adminv1.Operator, error) {
	if strings.TrimSpace(a.UserRef) == "" || strings.TrimSpace(a.Password) == "" {
		return nil, nil, nil, fmt.Errorf("--username/-u and --password/-p are required for admin commands")
	}
	conn, addr, err := dialDaemon(ctx, a)
	if err != nil {
		return nil, nil, nil, err
	}
	authClient := adminv1.NewAdminAuthServiceClient(conn)
	login, err := authClient.LoginOperator(ctx, &adminv1.LoginOperatorRequest{Username: a.UserRef, Password: a.Password, Client: &adminv1.OperatorClientInfo{Name: "mycel-cli", Platform: "cli"}})
	if err != nil {
		_ = conn.Close()
		return nil, nil, nil, fmt.Errorf("login daemon operator via %s: %w", addr, err)
	}
	authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+login.GetAccessToken())
	who, err := authClient.WhoAmI(authCtx, &adminv1.WhoAmIRequest{})
	if err != nil {
		_ = conn.Close()
		return nil, nil, nil, fmt.Errorf("resolve daemon operator identity via %s: %w", addr, err)
	}
	return conn, authCtx, who.GetOperator(), nil
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

func parseOperatorRole(raw string) (adminv1.OperatorRole, error) {
	key := "OPERATOR_ROLE_" + strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(raw), "-", "_"))
	if value, ok := adminv1.OperatorRole_value[key]; ok && value != int32(adminv1.OperatorRole_OPERATOR_ROLE_UNSPECIFIED) {
		return adminv1.OperatorRole(value), nil
	}
	return adminv1.OperatorRole_OPERATOR_ROLE_UNSPECIFIED, fmt.Errorf("unknown role %q", raw)
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
