package cmd

import (
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
	cmd.AddCommand(list)
	return cmd
}

func NewListAdminsCommand(a *app.App) *cobra.Command {
	return &cobra.Command{
		Use:   "admins",
		Short: "List daemon admins",
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(a.UserRef) == "" || strings.TrimSpace(a.Password) == "" {
				return fmt.Errorf("--username/-u and --password/-p are required for admin commands")
			}
			addr, err := resolveDaemonAddr(a)
			if err != nil {
				return err
			}
			conn, err := grpc.DialContext(cmd.Context(), addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return fmt.Errorf("dial myceld gRPC at %s: %w", addr, err)
			}
			defer conn.Close()
			authClient := adminv1.NewAdminAuthServiceClient(conn)
			login, err := authClient.LoginOperator(cmd.Context(), &adminv1.LoginOperatorRequest{Username: a.UserRef, Password: a.Password, Client: &adminv1.OperatorClientInfo{Name: "mycel-cli", Platform: "cli"}})
			if err != nil {
				return fmt.Errorf("login daemon operator via %s: %w", addr, err)
			}
			client := adminv1.NewAdminOperatorServiceClient(conn)
			authCtx := metadata.AppendToOutgoingContext(cmd.Context(), "authorization", "Bearer "+login.GetAccessToken())
			res, err := client.ListOperators(authCtx, &adminv1.ListOperatorsRequest{})
			if err != nil {
				return fmt.Errorf("list daemon admins via %s: %w", addr, err)
			}
			if a.Output == "json" {
				return a.Print(res.GetOperators(), "")
			}
			app.RenderDaemonOperatorsTable(res.GetOperators())
			return nil
		},
	}
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
