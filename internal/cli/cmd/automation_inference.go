package cmd

import (
	"fmt"
	"strings"

	"github.com/myceldb/mycel/internal/cli/app"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
	"github.com/spf13/cobra"
)

func newProcedureInferenceCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "inference-attempt", Aliases: []string{"inference-attempts"}, Short: "Inspect sanitized graph-procedure inference attempts"}
	cmd.AddCommand(newProcedureInferenceGetCommand(a), newProcedureInferenceListCommand(a), newProcedureInferenceChangesCommand(a))
	return cmd
}

func newProcedureInferenceGetCommand(a *app.App) *cobra.Command {
	var spaceID string
	cmd := &cobra.Command{Use: "get <attempt-id>", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		resolvedSpaceID, err := resolveProcedureInferenceSpaceID(a, spaceID)
		if err != nil {
			return err
		}
		conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewAutomationServiceClient(conn).GetProcedureInferenceAttempt(authCtx, &clientv1.GetProcedureInferenceAttemptRequest{SpaceId: resolvedSpaceID, AttemptId: strings.TrimSpace(args[0])})
		if err != nil {
			return err
		}
		return a.Print(res, "")
	}}
	cmd.Flags().StringVar(&spaceID, "space-id", "", "space UUID (defaults to the configured current space)")
	return cmd
}

func newProcedureInferenceListCommand(a *app.App) *cobra.Command {
	var spaceID, pageToken string
	var pageSize int32
	var filter procedureInferenceFilterFlags
	cmd := &cobra.Command{Use: "list", RunE: func(cmd *cobra.Command, _ []string) error {
		resolvedSpaceID, err := resolveProcedureInferenceSpaceID(a, spaceID)
		if err != nil {
			return err
		}
		conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewAutomationServiceClient(conn).ListProcedureInferenceAttempts(authCtx, &clientv1.ListProcedureInferenceAttemptsRequest{SpaceId: resolvedSpaceID, Filter: filter.proto(), PageSize: pageSize, PageToken: pageToken})
		if err != nil {
			return err
		}
		return a.Print(res, "")
	}}
	bindProcedureInferenceFlags(cmd, &spaceID, &filter)
	cmd.Flags().Int32Var(&pageSize, "page-size", 100, "page size (1-500)")
	cmd.Flags().StringVar(&pageToken, "page-token", "", "opaque next-page token")
	return cmd
}

func newProcedureInferenceChangesCommand(a *app.App) *cobra.Command {
	var spaceID, afterCursor string
	var pageSize int32
	var filter procedureInferenceFilterFlags
	cmd := &cobra.Command{Use: "changes", Short: "Poll immutable attempt changes after an opaque cursor", RunE: func(cmd *cobra.Command, _ []string) error {
		resolvedSpaceID, err := resolveProcedureInferenceSpaceID(a, spaceID)
		if err != nil {
			return err
		}
		conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewAutomationServiceClient(conn).ListProcedureInferenceAttemptChanges(authCtx, &clientv1.ListProcedureInferenceAttemptChangesRequest{SpaceId: resolvedSpaceID, Filter: filter.proto(), PageSize: pageSize, AfterCursor: afterCursor})
		if err != nil {
			return err
		}
		if res.GetHistoryGap() {
			return fmt.Errorf("procedure inference history gap: earliest retained sequence is %d and earliest retained state sequence is %d; stop cursor advancement, preserve the old cursor, alert, and record an explicit reconciliation decision before resuming (a fresh snapshot contains only retained state and cannot prove the missed interval complete)", res.GetEarliestRetainedSequence(), res.GetEarliestRetainedStateSequence())
		}
		return a.Print(res, "")
	}}
	bindProcedureInferenceFlags(cmd, &spaceID, &filter)
	cmd.Flags().Int32Var(&pageSize, "page-size", 100, "page size (1-500)")
	cmd.Flags().StringVar(&afterCursor, "after-cursor", "", "opaque filter-bound cursor or list snapshot cursor")
	return cmd
}

type procedureInferenceFilterFlags struct {
	domainID, procedureID, bindingID, invocationID, runID string
}

func (f procedureInferenceFilterFlags) proto() *commonv1.ProcedureInferenceAttemptFilter {
	return &commonv1.ProcedureInferenceAttemptFilter{DomainId: strings.TrimSpace(f.domainID), ProcedureId: strings.TrimSpace(f.procedureID), BindingId: strings.TrimSpace(f.bindingID), InvocationId: strings.TrimSpace(f.invocationID), RunId: strings.TrimSpace(f.runID)}
}

func bindProcedureInferenceFlags(cmd *cobra.Command, spaceID *string, filter *procedureInferenceFilterFlags) {
	cmd.Flags().StringVar(spaceID, "space-id", "", "space UUID (defaults to the configured current space)")
	cmd.Flags().StringVar(&filter.domainID, "domain-id", "", "filter by domain UUID")
	cmd.Flags().StringVar(&filter.procedureID, "procedure-id", "", "filter by procedure ID")
	cmd.Flags().StringVar(&filter.bindingID, "binding-id", "", "filter by binding ID")
	cmd.Flags().StringVar(&filter.invocationID, "invocation-id", "", "filter by invocation ID")
	cmd.Flags().StringVar(&filter.runID, "run-id", "", "filter by run ID")
}

func resolveProcedureInferenceSpaceID(a *app.App, value string) (string, error) {
	spaceID, err := a.ResolveSpaceID(strings.TrimSpace(value))
	if err != nil {
		return "", err
	}
	return spaceID.String(), nil
}
