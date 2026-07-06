package cmd

import (
	"context"
	"fmt"
	"strings"

	clientv1 "github.com/myceldb/mycel-api/gen/go/mycel/client/v1"
	"github.com/myceldb/mycel/internal/cli/app"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func NewAuthCommand(a *app.App) *cobra.Command {
	auth := &cobra.Command{Use: "auth", Short: "Manage client authentication"}
	session := &cobra.Command{Use: "session", Short: "Manage durable auth sessions"}
	session.AddCommand(NewAuthSessionListCommand(a), NewAuthSessionRevokeCommand(a), NewAuthSessionRevokeOtherCommand(a), NewAuthSessionCleanupCommand(a))
	auth.AddCommand(NewAuthLoginCommand(a), NewAuthRefreshCommand(a), NewAuthLogoutCommand(a), NewAuthWhoAmICommand(a), session)
	return auth
}

func NewAuthLoginCommand(a *app.App) *cobra.Command {
	return &cobra.Command{Use: "login", Short: "Login as a standard user", RunE: func(cmd *cobra.Command, args []string) error {
		conn, _, login, err := loginDaemonUser(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		return a.Print(login, fmt.Sprintf("logged in: %s\nrefresh_token: %s\n", login.GetPrincipal().GetUsername(), login.GetRefreshToken()))
	}}
}

func NewAuthRefreshCommand(a *app.App) *cobra.Command {
	var refreshToken string
	cmd := &cobra.Command{Use: "refresh", Short: "Refresh a standard-user auth session", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(refreshToken) == "" {
			return fmt.Errorf("--refresh-token is required")
		}
		conn, _, err := dialDaemon(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewAuthServiceClient(conn).Refresh(cmd.Context(), &clientv1.RefreshRequest{RefreshToken: &refreshToken, Client: &clientv1.ClientInfo{Name: "mycel-cli", Platform: "cli"}})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("refreshed: %s\nrefresh_token: %s\n", res.GetPrincipal().GetUsername(), res.GetRefreshToken()))
	}}
	cmd.Flags().StringVar(&refreshToken, "refresh-token", "", "refresh token")
	return cmd
}

func NewAuthWhoAmICommand(a *app.App) *cobra.Command {
	return &cobra.Command{Use: "whoami", Short: "Show the authenticated standard user", RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonUser(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewAuthServiceClient(conn).WhoAmI(authCtx, &clientv1.WhoAmIRequest{})
		if err != nil {
			return err
		}
		return a.Print(res.GetPrincipal(), "user: "+res.GetPrincipal().GetUsername()+"\n")
	}}
}

func NewAuthLogoutCommand(a *app.App) *cobra.Command {
	var sessionID string
	cmd := &cobra.Command{Use: "logout", Short: "Logout/revoke a standard-user auth session", RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonUser(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		req := &clientv1.LogoutRequest{}
		if sessionID != "" {
			req.AuthSessionId = &sessionID
		}
		_, err = clientv1.NewAuthServiceClient(conn).Logout(authCtx, req)
		if err != nil {
			return err
		}
		return a.Print(map[string]any{"logged_out": true}, "logged out\n")
	}}
	cmd.Flags().StringVar(&sessionID, "session-id", "", "session id to revoke; defaults to current login session")
	return cmd
}

func NewAuthSessionListCommand(a *app.App) *cobra.Command {
	var includeInactive bool
	cmd := &cobra.Command{Use: "list", Short: "List refresh sessions for the authenticated user", RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonUser(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewAuthServiceClient(conn).ListAuthSessions(authCtx, &clientv1.ListAuthSessionsRequest{IncludeInactive: includeInactive})
		if err != nil {
			return err
		}
		if a.Output == "json" {
			return a.Print(res.GetSessions(), "")
		}
		app.RenderClientAuthSessionsTable(res.GetSessions())
		return nil
	}}
	cmd.Flags().BoolVar(&includeInactive, "include-inactive", false, "include inactive sessions")
	return cmd
}

func NewAuthSessionRevokeCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "revoke SESSION_ID", Short: "Revoke one refresh session owned by the authenticated user", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonUser(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		_, err = clientv1.NewAuthServiceClient(conn).RevokeAuthSession(authCtx, &clientv1.RevokeAuthSessionRequest{AuthSessionId: args[0]})
		if err != nil {
			return err
		}
		return a.Print(map[string]any{"revoked": true, "session_id": args[0]}, fmt.Sprintf("revoked refresh session %s\n", args[0]))
	}}
	cmd.Flags().String("reason", "", "deprecated; ignored by daemon AuthService")
	return cmd
}

func NewAuthSessionRevokeOtherCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "revoke-other", Short: "Revoke all other refresh sessions owned by the authenticated user", RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonUser(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewAuthServiceClient(conn).RevokeOtherAuthSessions(authCtx, &clientv1.RevokeOtherAuthSessionsRequest{})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("revoked %d other refresh sessions\n", res.GetRevokedCount()))
	}}
	cmd.Flags().String("current-session-id", "", "deprecated; daemon uses the current bearer token session")
	cmd.Flags().String("reason", "", "deprecated; ignored by daemon AuthService")
	return cmd
}

func NewAuthSessionCleanupCommand(a *app.App) *cobra.Command {
	return &cobra.Command{Use: "cleanup", Short: "Clean up expired/revoked refresh-session token hashes (not available over daemon gRPC yet)", RunE: func(cmd *cobra.Command, args []string) error {
		return fmt.Errorf("auth session cleanup is not available over daemon gRPC yet")
	}}
}

func loginDaemonUser(ctx context.Context, a *app.App) (*grpc.ClientConn, context.Context, *clientv1.LoginResponse, error) {
	if strings.TrimSpace(a.UserRef) == "" || strings.TrimSpace(a.Password) == "" {
		return nil, nil, nil, fmt.Errorf("--username/-u and --password/-p are required for auth commands")
	}
	conn, addr, err := dialDaemon(ctx, a)
	if err != nil {
		return nil, nil, nil, err
	}
	client := clientv1.NewAuthServiceClient(conn)
	login, err := client.Login(ctx, &clientv1.LoginRequest{Username: a.UserRef, Password: a.Password, Client: &clientv1.ClientInfo{Name: "mycel-cli", Platform: "cli"}})
	if err != nil {
		_ = conn.Close()
		return nil, nil, nil, fmt.Errorf("login user via %s: %w", addr, err)
	}
	authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+login.GetAccessToken())
	return conn, authCtx, login, nil
}
