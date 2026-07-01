package cmd

import (
	"fmt"
	"strings"

	mycelengine "github.com/myceldb/mycel/engine"
	"github.com/myceldb/mycel/internal/cli/app"
	"github.com/spf13/cobra"
)

func NewAuthCommand(a *app.App) *cobra.Command {
	auth := &cobra.Command{Use: "auth", Short: "Manage authentication resources"}
	session := &cobra.Command{Use: "session", Short: "Manage durable refresh sessions"}
	session.AddCommand(NewAuthSessionListCommand(a), NewAuthSessionRevokeCommand(a), NewAuthSessionRevokeOtherCommand(a), NewAuthSessionCleanupCommand(a))
	auth.AddCommand(session)
	return auth
}

func NewAuthSessionListCommand(a *app.App) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List refresh sessions for the authenticated user",
		RunE: func(cmd *cobra.Command, args []string) error {
			tok, err := a.AccessToken(cmd.Context())
			if err != nil {
				return err
			}
			sessions, err := a.Engine.ListRefreshSessions(cmd.Context(), mycelengine.ListRefreshSessionsInput{AccessToken: tok})
			if err != nil {
				return err
			}
			if a.Output == "json" {
				return a.Print(sessions, "")
			}
			app.RenderRefreshSessionsTable(sessions)
			return nil
		},
	}
}

func NewAuthSessionRevokeCommand(a *app.App) *cobra.Command {
	var reason string
	cmd := &cobra.Command{
		Use:   "revoke SESSION_ID",
		Short: "Revoke one refresh session owned by the authenticated user",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			sessionID, err := app.ParseUUID[mycelengine.RefreshSessionID](args[0])
			if err != nil {
				return fmt.Errorf("invalid session id: %w", err)
			}
			tok, err := a.AccessToken(cmd.Context())
			if err != nil {
				return err
			}
			if err := a.Engine.RevokeRefreshSession(cmd.Context(), mycelengine.RevokeRefreshSessionInput{AccessToken: tok, SessionID: sessionID, Reason: reason}); err != nil {
				return err
			}
			return a.Print(map[string]any{"revoked": true, "session_id": sessionID}, fmt.Sprintf("revoked refresh session %s\n", sessionID))
		},
	}
	cmd.Flags().StringVar(&reason, "reason", "", "revocation reason")
	return cmd
}

func NewAuthSessionRevokeOtherCommand(a *app.App) *cobra.Command {
	var currentSessionIDText string
	var reason string
	cmd := &cobra.Command{
		Use:   "revoke-other",
		Short: "Revoke all other refresh sessions owned by the authenticated user",
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(currentSessionIDText) == "" {
				return fmt.Errorf("--current-session-id is required")
			}
			currentSessionID, err := app.ParseUUID[mycelengine.RefreshSessionID](currentSessionIDText)
			if err != nil {
				return fmt.Errorf("invalid current session id: %w", err)
			}
			tok, err := a.AccessToken(cmd.Context())
			if err != nil {
				return err
			}
			count, err := a.Engine.RevokeOtherRefreshSessions(cmd.Context(), mycelengine.RevokeOtherRefreshSessionsInput{AccessToken: tok, CurrentSessionID: currentSessionID, Reason: reason})
			if err != nil {
				return err
			}
			return a.Print(map[string]any{"revoked_count": count, "current_session_id": currentSessionID}, fmt.Sprintf("revoked %d other refresh sessions\n", count))
		},
	}
	cmd.Flags().StringVar(&currentSessionIDText, "current-session-id", "", "session id to keep active")
	cmd.Flags().StringVar(&reason, "reason", "", "revocation reason")
	return cmd
}

func NewAuthSessionCleanupCommand(a *app.App) *cobra.Command {
	return &cobra.Command{
		Use:   "cleanup",
		Short: "Clean up expired/revoked refresh-session token hashes",
		RunE: func(cmd *cobra.Command, args []string) error {
			tok, err := a.AccessToken(cmd.Context())
			if err != nil {
				return err
			}
			res, err := a.Engine.CleanupRefreshSessions(cmd.Context(), mycelengine.CleanupRefreshSessionsInput{AccessToken: tok})
			if err != nil {
				return err
			}
			return a.Print(res, fmt.Sprintf("cleaned %d refresh sessions\n", res.ChangedCount))
		},
	}
}
