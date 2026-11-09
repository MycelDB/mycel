package cmd

import (
	"fmt"
	"strings"

	"github.com/myceldb/mycel/internal/cli/app"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	"github.com/spf13/cobra"
)

func newAutomationReconciliationCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "reconciliation",
		Short: "Reconcile existing graph automation targets",
	}
	cmd.AddCommand(newAutomationReconciliationStartCommand(a), newAutomationReconciliationGetCommand(a), newAutomationReconciliationCancelCommand(a))
	return cmd
}

func newAutomationReconciliationStartCommand(a *app.App) *cobra.Command {
	var flags automationDomainFlags
	var bindingID, idempotencyKey string
	var batchSize, maxConcurrency int32
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start a durable graph automation reconciliation",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if strings.TrimSpace(bindingID) == "" {
				return fmt.Errorf("--binding-id is required")
			}
			if strings.TrimSpace(idempotencyKey) == "" {
				return fmt.Errorf("--idempotency-key is required")
			}
			conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
			if err != nil {
				return err
			}
			defer conn.Close()
			domainID, err := resolveAutomationDomainID(cmd, a, conn, authCtx, flags)
			if err != nil {
				return err
			}
			spaceID, err := a.ResolveSpaceID(strings.TrimSpace(flags.SpaceID))
			if err != nil {
				return err
			}
			res, err := clientv1.NewAutomationServiceClient(conn).StartGraphAutomationReconciliation(authCtx, &clientv1.StartGraphAutomationReconciliationRequest{
				SpaceId: spaceID.String(), DomainId: domainID, BindingId: bindingID, IdempotencyKey: idempotencyKey,
				BatchSize: batchSize, MaxConcurrency: maxConcurrency,
			})
			if err != nil {
				return err
			}
			return printReconciliationOperation(cmd, a, res.GetOperation())
		},
	}
	bindAutomationDomainFlags(cmd, &flags)
	cmd.Flags().StringVar(&bindingID, "binding-id", "", "graph automation binding ID")
	cmd.Flags().StringVar(&idempotencyKey, "idempotency-key", "", "stable reconciliation idempotency key")
	cmd.Flags().Int32Var(&batchSize, "batch-size", 0, "discovery batch size (daemon default when zero)")
	cmd.Flags().Int32Var(&maxConcurrency, "max-concurrency", 0, "maximum active reconciliation invocations (unbounded when zero)")
	return cmd
}

func newAutomationReconciliationGetCommand(a *app.App) *cobra.Command {
	var spaceID string
	cmd := &cobra.Command{
		Use:   "get <operation-id>",
		Short: "Get durable graph automation reconciliation progress",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
			if err != nil {
				return err
			}
			defer conn.Close()
			resolvedSpaceID, err := a.ResolveSpaceID(strings.TrimSpace(spaceID))
			if err != nil {
				return err
			}
			res, err := clientv1.NewAutomationServiceClient(conn).GetGraphAutomationReconciliation(authCtx, &clientv1.GetGraphAutomationReconciliationRequest{SpaceId: resolvedSpaceID.String(), OperationId: args[0]})
			if err != nil {
				return err
			}
			return printReconciliationOperation(cmd, a, res.GetOperation())
		},
	}
	cmd.Flags().StringVar(&spaceID, "space-id", "", "space ID")
	return cmd
}

func newAutomationReconciliationCancelCommand(a *app.App) *cobra.Command {
	var spaceID string
	cmd := &cobra.Command{
		Use:   "cancel <operation-id>",
		Short: "Cancel a graph automation reconciliation",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
			if err != nil {
				return err
			}
			defer conn.Close()
			resolvedSpaceID, err := a.ResolveSpaceID(strings.TrimSpace(spaceID))
			if err != nil {
				return err
			}
			res, err := clientv1.NewAutomationServiceClient(conn).CancelGraphAutomationReconciliation(authCtx, &clientv1.CancelGraphAutomationReconciliationRequest{SpaceId: resolvedSpaceID.String(), OperationId: args[0]})
			if err != nil {
				return err
			}
			return printReconciliationOperation(cmd, a, res.GetOperation())
		},
	}
	cmd.Flags().StringVar(&spaceID, "space-id", "", "space ID")
	return cmd
}

func printReconciliationOperation(cmd *cobra.Command, a *app.App, operation *clientv1.GraphAutomationReconciliation) error {
	if a.Output == "json" {
		return a.Print(operation, "")
	}
	if operation == nil {
		return fmt.Errorf("daemon returned an empty reconciliation operation")
	}
	fmt.Fprintf(cmd.OutOrStdout(), "%s\t%s\tscanned=%d scheduled=%d completed=%d fresh_skipped=%d failed=%d\n", operation.GetOperationId(), operation.GetStatus(), operation.GetScannedCount(), operation.GetScheduledCount(), operation.GetCompletedCount(), operation.GetFreshSkippedCount(), operation.GetFailedCount())
	return nil
}
