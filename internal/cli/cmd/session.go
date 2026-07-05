package cmd

import (
	"context"
	"fmt"
	"time"

	domainspace "github.com/myceldb/mycel/domain/space"
	clientv1 "github.com/myceldb/mycel/gen/go/mycel/client/v1"
	"github.com/myceldb/mycel/internal/cli/app"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/durationpb"
)

func NewSessionCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "session", Short: "Manage daemon graph sessions"}
	cmd.AddCommand(NewSessionOpenCommand(a), NewSessionGetCommand(a), NewSessionHeartbeatCommand(a), NewSessionCloseCommand(a))
	return cmd
}

func NewSessionOpenCommand(a *app.App) *cobra.Command {
	var spaceIDText, domainID, domainKey string
	var idleTimeout time.Duration
	cmd := &cobra.Command{Use: "open", Short: "Open a daemon graph session", RunE: func(cmd *cobra.Command, args []string) error {
		spaceID, err := app.ParseUUID[domainspace.SpaceID](spaceIDText)
		if err != nil {
			return err
		}
		conn, authCtx, _, err := loginDaemonUser(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		resolvedDomainID, err := resolveDaemonDomainID(clientv1.NewDomainServiceClient(conn), authCtx, spaceID.String(), domainID, domainKey)
		if err != nil {
			return err
		}
		req := &clientv1.OpenSessionRequest{SpaceId: spaceID.String(), DomainId: resolvedDomainID}
		if idleTimeout > 0 {
			req.RequestedIdleTimeout = durationpb.New(idleTimeout)
		}
		res, err := clientv1.NewSessionServiceClient(conn).OpenSession(authCtx, req)
		if err != nil {
			return err
		}
		return a.Print(res.GetSession(), fmt.Sprintf("session opened: %s\n", res.GetSession().GetSessionId()))
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID")
	cmd.Flags().StringVar(&domainID, "domain-id", "", "domain ID")
	cmd.Flags().StringVar(&domainKey, "domain", "", "domain key (defaults to the space default domain)")
	cmd.Flags().DurationVar(&idleTimeout, "idle-timeout", 0, "requested idle timeout, e.g. 30m")
	_ = cmd.MarkFlagRequired("space-id")
	return cmd
}

func NewSessionGetCommand(a *app.App) *cobra.Command {
	return &cobra.Command{Use: "get SESSION_ID", Short: "Get a daemon graph session", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonUser(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewSessionServiceClient(conn).GetSession(authCtx, &clientv1.GetSessionRequest{SessionId: args[0]})
		if err != nil {
			return err
		}
		return a.Print(res.GetSession(), fmt.Sprintf("session: %s %s\n", res.GetSession().GetSessionId(), res.GetSession().GetState()))
	}}
}

func NewSessionHeartbeatCommand(a *app.App) *cobra.Command {
	var extension time.Duration
	cmd := &cobra.Command{Use: "heartbeat SESSION_ID", Short: "Heartbeat a daemon graph session", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonUser(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		req := &clientv1.HeartbeatSessionRequest{SessionId: args[0]}
		if extension > 0 {
			req.RequestedExtension = durationpb.New(extension)
		}
		res, err := clientv1.NewSessionServiceClient(conn).HeartbeatSession(authCtx, req)
		if err != nil {
			return err
		}
		return a.Print(res.GetSession(), fmt.Sprintf("session heartbeat: %s\n", res.GetSession().GetSessionId()))
	}}
	cmd.Flags().DurationVar(&extension, "extension", 0, "requested extension, e.g. 30m")
	return cmd
}

func NewSessionCloseCommand(a *app.App) *cobra.Command {
	return &cobra.Command{Use: "close SESSION_ID", Short: "Close a daemon graph session", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonUser(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewSessionServiceClient(conn).CloseSession(authCtx, &clientv1.CloseSessionRequest{SessionId: args[0]})
		if err != nil {
			return err
		}
		return a.Print(res.GetSession(), fmt.Sprintf("session closed: %s\n", res.GetSession().GetSessionId()))
	}}
}

func NewTransactionCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "transaction", Aliases: []string{"tx"}, Short: "Manage daemon graph transactions"}
	cmd.AddCommand(NewTransactionBeginCommand(a), NewTransactionGetCommand(a), NewTransactionCommitCommand(a), NewTransactionRollbackCommand(a), NewTransactionCloseCommand(a))
	return cmd
}

func NewTransactionBeginCommand(a *app.App) *cobra.Command {
	var mode string
	cmd := &cobra.Command{Use: "begin SESSION_ID", Short: "Begin a daemon graph transaction", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonUser(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewTransactionServiceClient(conn).BeginTransaction(authCtx, &clientv1.BeginTransactionRequest{SessionId: args[0], Mode: parseTransactionMode(mode)})
		if err != nil {
			return err
		}
		return a.Print(res.GetTransaction(), fmt.Sprintf("transaction begun: %s\n", res.GetTransaction().GetTransactionId()))
	}}
	cmd.Flags().StringVar(&mode, "mode", "read-write", "transaction mode: read-only or read-write")
	return cmd
}

func NewTransactionGetCommand(a *app.App) *cobra.Command {
	return &cobra.Command{Use: "get TRANSACTION_ID", Short: "Get a daemon graph transaction", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonUser(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewTransactionServiceClient(conn).GetTransaction(authCtx, &clientv1.GetTransactionRequest{TransactionId: args[0]})
		if err != nil {
			return err
		}
		return a.Print(res.GetTransaction(), fmt.Sprintf("transaction: %s %s\n", res.GetTransaction().GetTransactionId(), res.GetTransaction().GetState()))
	}}
}

func NewTransactionCommitCommand(a *app.App) *cobra.Command {
	return &cobra.Command{Use: "commit TRANSACTION_ID", Short: "Commit a daemon graph transaction", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonUser(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewTransactionServiceClient(conn).CommitTransaction(authCtx, &clientv1.CommitTransactionRequest{TransactionId: args[0]})
		if err != nil {
			return err
		}
		return a.Print(res.GetCommit(), fmt.Sprintf("transaction committed: %s revision=%d\n", res.GetCommit().GetTransactionId(), res.GetCommit().GetCommittedRevision()))
	}}
}

func NewTransactionRollbackCommand(a *app.App) *cobra.Command {
	return &cobra.Command{Use: "rollback TRANSACTION_ID", Short: "Rollback a daemon graph transaction", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonUser(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewTransactionServiceClient(conn).RollbackTransaction(authCtx, &clientv1.RollbackTransactionRequest{TransactionId: args[0]})
		if err != nil {
			return err
		}
		return a.Print(res.GetTransaction(), fmt.Sprintf("transaction rolled back: %s\n", res.GetTransaction().GetTransactionId()))
	}}
}

func NewTransactionCloseCommand(a *app.App) *cobra.Command {
	return &cobra.Command{Use: "close TRANSACTION_ID", Short: "Close a daemon graph transaction", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonUser(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewTransactionServiceClient(conn).CloseTransaction(authCtx, &clientv1.CloseTransactionRequest{TransactionId: args[0]})
		if err != nil {
			return err
		}
		return a.Print(res.GetTransaction(), fmt.Sprintf("transaction closed: %s\n", res.GetTransaction().GetTransactionId()))
	}}
}

func resolveDaemonDomainID(client clientv1.DomainServiceClient, authCtx context.Context, spaceID string, domainID string, domainKey string) (string, error) {
	if domainID != "" {
		return domainID, nil
	}
	res, err := client.GetDomain(authCtx, &clientv1.GetDomainRequest{SpaceId: spaceID, Key: domainKey})
	if err != nil {
		return "", err
	}
	return res.GetDomain().GetDomainId(), nil
}

func parseTransactionMode(mode string) clientv1.TransactionMode {
	switch mode {
	case "read-only", "readonly", "ro":
		return clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY
	default:
		return clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE
	}
}
