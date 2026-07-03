package cmd

import (
	"context"
	"fmt"
	"strings"

	adminv1 "github.com/myceldb/mycel/gen/go/mycel/admin/v1"
	"github.com/myceldb/mycel/internal/cli/app"
	daemonconfig "github.com/myceldb/mycel/internal/daemon/config"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func NewAdminCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "admin", Aliases: []string{"admins"}, Short: "Manage daemon admins"}
	list := NewListAdminsCommand(a)
	list.Use = "list"
	list.Aliases = []string{"ls"}
	list.Short = "List daemon admins"
	cmd.AddCommand(list, NewAdminPasswordCommand(a))
	return cmd
}

func NewAdminPasswordCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "password", Short: "Manage daemon admin passwords"}
	set := NewSetAdminPasswordCommand(a)
	set.Use = "set"
	set.Short = "Change the authenticated daemon admin password"
	cmd.AddCommand(set)
	return cmd
}

func NewSetAdminPasswordCommand(a *app.App) *cobra.Command {
	var newPassword string
	cmd := &cobra.Command{
		Use:   "set",
		Short: "Change the authenticated daemon admin password",
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(newPassword) == "" {
				return fmt.Errorf("--new-password is required")
			}
			conn, authCtx, operator, err := loginDaemonOperator(cmd.Context(), a)
			if err != nil {
				return err
			}
			defer conn.Close()
			client := adminv1.NewAdminOperatorServiceClient(conn)
			res, err := client.SetOperatorPassword(authCtx, &adminv1.SetOperatorPasswordRequest{OperatorId: operator.GetOperatorId(), Password: newPassword})
			if err != nil {
				return fmt.Errorf("set daemon admin password: %w", err)
			}
			if a.Output == "json" {
				return a.Print(res.GetOperator(), "")
			}
			return a.Print(res.GetOperator(), fmt.Sprintf("admin password changed: %s\n", res.GetOperator().GetUsername()))
		},
	}
	cmd.Flags().StringVar(&newPassword, "new-password", "", "new daemon admin password")
	return cmd
}

func NewListAdminsCommand(a *app.App) *cobra.Command {
	return &cobra.Command{
		Use:   "admins",
		Short: "List daemon admins",
		RunE: func(cmd *cobra.Command, args []string) error {
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
		},
	}
}

func loginDaemonOperator(ctx context.Context, a *app.App) (*grpc.ClientConn, context.Context, *adminv1.Operator, error) {
	if strings.TrimSpace(a.UserRef) == "" || strings.TrimSpace(a.Password) == "" {
		return nil, nil, nil, fmt.Errorf("--username/-u and --password/-p are required for admin commands")
	}
	addr, err := resolveDaemonAddr(a)
	if err != nil {
		return nil, nil, nil, err
	}
	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("dial myceld gRPC at %s: %w", addr, err)
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
