package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/cli/app"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	model "github.com/myceldb/mycel/internal/graph/model"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v3"
)

func NewSemanticCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "semantic", Short: "Manage semantic generation rules and search"}
	rule := &cobra.Command{Use: "rule", Short: "Manage semantic generation rules"}
	rule.AddCommand(
		newSemanticRuleListCommand(a),
		newSemanticRuleGetCommand(a),
		newSemanticRuleValidateCommand(a),
		newSemanticRuleCreateCommand(a),
		newSemanticRuleUpdateCommand(a),
		newSemanticRuleSetEnabledCommand(a, true),
		newSemanticRuleSetEnabledCommand(a, false),
		newSemanticRuleDeleteCommand(a),
		newSemanticRuleBackfillCommand(a),
	)
	cmd.AddCommand(rule, newSemanticSearchCommand(a), newSemanticMaintenanceCommand(a), newSemanticMigrateCommand(a))
	return cmd
}

func newSemanticSearchCommand(a *app.App) *cobra.Command {
	var spaceIDText, domainRef, text, bindingKey string
	var ruleRefs []string
	var limit int
	var minScore float64
	cmd := &cobra.Command{Use: "search", Short: "Search semantic generation rule bindings", RunE: func(cmd *cobra.Command, args []string) error {
		return runDaemonSemanticSearch(cmd, a, spaceIDText, domainRef, text, ruleRefs, bindingKey, limit, minScore)
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID")
	cmd.Flags().StringVar(&domainRef, "domain", "", "domain key or ID")
	cmd.Flags().StringVar(&text, "text", "", "query text")
	cmd.Flags().StringSliceVar(&ruleRefs, "rule", nil, "semantic rule key or ID to search")
	cmd.Flags().StringVar(&bindingKey, "binding", "", "embedding binding key to search")
	cmd.Flags().IntVar(&limit, "limit", 10, "maximum merged results")
	cmd.Flags().Float64Var(&minScore, "min-score", 0, "minimum cosine score")
	_ = cmd.MarkFlagRequired("space-id")
	_ = cmd.MarkFlagRequired("text")
	return cmd
}

func runDaemonSemanticSearch(cmd *cobra.Command, a *app.App, spaceIDText string, domainRef string, text string, ruleRefs []string, bindingKey string, limit int, minScore float64) error {
	conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
	if err != nil {
		return err
	}
	defer conn.Close()
	spaceID, err := app.ParseUUID[domainspace.SpaceID](spaceIDText)
	if err != nil {
		return fmt.Errorf("--space-id must be a UUID: %w", err)
	}
	domainID, err := daemonResolveDomainID(cmd.Context(), conn, authCtx, spaceID.String(), domainRef)
	if err != nil {
		return err
	}
	semanticClient := clientv1.NewSemanticServiceClient(conn)
	var ruleID *string
	if len(ruleRefs) > 1 {
		return fmt.Errorf("daemon SemanticService supports at most one --rule in v1")
	}
	if len(ruleRefs) == 1 && strings.TrimSpace(ruleRefs[0]) != "" {
		resolved, err := daemonResolveClientSemanticRuleID(cmd.Context(), semanticClient, authCtx, spaceID.String(), domainID, ruleRefs[0])
		if err != nil {
			return err
		}
		ruleID = &resolved
	}
	req := &clientv1.SemanticSearchRequest{SpaceId: spaceID.String(), DomainId: domainID, Query: text, Limit: int32(limit)}
	if ruleID != nil {
		req.SemanticRuleId = ruleID
	}
	if trimmed := strings.TrimSpace(bindingKey); trimmed != "" {
		req.EmbeddingBindingKey = &trimmed
	}
	if minScore > 0 {
		req.MinScore = &minScore
	}
	res, err := semanticClient.SemanticSearch(authCtx, req)
	if err != nil {
		return err
	}
	if a.Output == "json" {
		return a.Print(res, "")
	}
	for _, warning := range res.GetWarnings() {
		fmt.Printf("warning\t%s\n", warning)
	}
	for _, result := range res.GetResults() {
		fmt.Printf("%.4f\trule=%s\tbinding=%s\tnode=%s\t%s\n", result.GetScore(), result.GetSemanticRuleId(), result.GetEmbeddingBindingKey(), result.GetNodeId(), result.GetSnippet())
	}
	return nil
}

func newSemanticRuleListCommand(a *app.App) *cobra.Command {
	var spaceIDText, domainRef, pageToken string
	var pageSize int32
	var includeDisabled bool
	cmd := &cobra.Command{Use: "list", Short: "List semantic generation rules", RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		spaceID, err := app.ParseUUID[domainspace.SpaceID](spaceIDText)
		if err != nil {
			return fmt.Errorf("--space-id must be a UUID: %w", err)
		}
		domainID, err := daemonResolveDomainID(cmd.Context(), conn, authCtx, spaceID.String(), domainRef)
		if err != nil {
			return err
		}
		res, err := clientv1.NewSemanticServiceClient(conn).ListSemanticRules(authCtx, &clientv1.ListSemanticRulesRequest{SpaceId: spaceID.String(), DomainId: domainID, PageSize: pageSize, PageToken: pageToken, IncludeDisabled: includeDisabled})
		if err != nil {
			return err
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		for _, rule := range res.GetRules() {
			fmt.Printf("%s\t%s\t%s\tbindings=%s\n", rule.GetSemanticRuleId(), rule.GetKey(), rule.GetState().String(), strings.Join(bindingKeys(rule.GetBindings()), ","))
		}
		if res.GetNextPageToken() != "" {
			fmt.Printf("next page token: %s\n", res.GetNextPageToken())
		}
		return nil
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID")
	cmd.Flags().StringVar(&domainRef, "domain", model.DefaultDomainKey, "domain key or ID")
	cmd.Flags().Int32Var(&pageSize, "page-size", 100, "page size")
	cmd.Flags().StringVar(&pageToken, "page-token", "", "page token")
	cmd.Flags().BoolVar(&includeDisabled, "include-disabled", false, "include disabled rules")
	_ = cmd.MarkFlagRequired("space-id")
	return cmd
}

func newSemanticRuleGetCommand(a *app.App) *cobra.Command {
	var spaceIDText, domainRef string
	cmd := &cobra.Command{Use: "get RULE", Short: "Get a semantic generation rule", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		spaceID, err := a.ResolveSpaceID(spaceIDText)
		if err != nil {
			return err
		}
		ruleID, err := daemonResolveAdminSemanticRuleID(cmd.Context(), conn, authCtx, spaceID.String(), domainRef, args[0])
		if err != nil {
			return err
		}
		res, err := adminv1.NewAdminSemanticServiceClient(conn).GetSemanticRule(authCtx, &adminv1.GetSemanticRuleRequest{SpaceId: spaceID.String(), SemanticRuleId: ruleID})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("semantic rule: %s\n", res.GetRule().GetSemanticRuleId()))
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID")
	cmd.Flags().StringVar(&domainRef, "domain", model.DefaultDomainKey, "domain key or ID for resolving rule keys")
	_ = cmd.MarkFlagRequired("space-id")
	return cmd
}

func newSemanticRuleValidateCommand(a *app.App) *cobra.Command {
	f := semanticRuleCLIFlags{Enabled: true, Searchable: true, SourceMode: string(domainsemantic.SemanticSourceSelf), SelectorMode: string(domainsemantic.SemanticTargetSelectorNodeType), BindingKey: "search", Purpose: string(domainsemantic.SemanticIndexPurposeSearch), PhysicalIndex: domainsemantic.SemanticPhysicalIndexExact}
	cmd := &cobra.Command{Use: "validate [KEY]", Short: "Validate a semantic generation rule without persisting it", Args: cobra.MaximumNArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		rule, conn, authCtx, err := buildSemanticRuleFromCLI(cmd, a, f, args)
		if conn != nil {
			defer conn.Close()
		}
		if err != nil {
			return err
		}
		res, err := adminv1.NewAdminSemanticServiceClient(conn).ValidateSemanticRule(authCtx, &adminv1.ValidateSemanticRuleRequest{Rule: rule})
		if err != nil {
			return err
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		for _, d := range res.GetDiagnostics() {
			fmt.Printf("%s\t%s\t%s\n", d.GetSeverity(), d.GetPath(), d.GetMessage())
		}
		if res.GetValid() {
			fmt.Println("semantic rule valid")
		}
		return nil
	}}
	bindSemanticRuleCreateFlags(cmd, &f, true)
	return cmd
}

func newSemanticRuleCreateCommand(a *app.App) *cobra.Command {
	f := semanticRuleCLIFlags{Enabled: true, Searchable: true, SourceMode: string(domainsemantic.SemanticSourceSelf), SelectorMode: string(domainsemantic.SemanticTargetSelectorNodeType), BindingKey: "search", Purpose: string(domainsemantic.SemanticIndexPurposeSearch), PhysicalIndex: domainsemantic.SemanticPhysicalIndexExact}
	cmd := &cobra.Command{Use: "create [KEY]", Short: "Create a semantic generation rule", Args: cobra.MaximumNArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		rule, conn, authCtx, err := buildSemanticRuleFromCLI(cmd, a, f, args)
		if conn != nil {
			defer conn.Close()
		}
		if err != nil {
			return err
		}
		res, err := adminv1.NewAdminSemanticServiceClient(conn).CreateSemanticRule(authCtx, &adminv1.CreateSemanticRuleRequest{Rule: rule})
		if err != nil {
			return err
		}
		return a.Print(res.GetSummary(), fmt.Sprintf("semantic rule created: %s\n", res.GetRule().GetSemanticRuleId()))
	}}
	bindSemanticRuleCreateFlags(cmd, &f, true)
	return cmd
}

func newSemanticRuleUpdateCommand(a *app.App) *cobra.Command {
	var spaceIDText, domainRef, file string
	cmd := &cobra.Command{Use: "update RULE --file FILE", Short: "Update a semantic generation rule from a JSON/YAML file", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		spaceID, err := a.ResolveSpaceID(spaceIDText)
		if err != nil {
			return err
		}
		ruleID, err := daemonResolveAdminSemanticRuleID(cmd.Context(), conn, authCtx, spaceID.String(), domainRef, args[0])
		if err != nil {
			return err
		}
		rule, err := readSemanticRuleFile(file)
		if err != nil {
			return err
		}
		rule.SpaceId = firstNonEmpty(rule.GetSpaceId(), spaceID.String())
		if rule.GetDomainId() != "" {
			if _, err := uuid.Parse(rule.GetDomainId()); err != nil {
				domainID, err := daemonResolveAdminDomainID(cmd.Context(), conn, authCtx, spaceID.String(), rule.GetDomainId())
				if err != nil {
					return err
				}
				rule.DomainId = domainID
			}
		} else if strings.TrimSpace(domainRef) != "" {
			domainID, err := daemonResolveAdminDomainID(cmd.Context(), conn, authCtx, spaceID.String(), domainRef)
			if err != nil {
				return err
			}
			rule.DomainId = domainID
		}
		res, err := adminv1.NewAdminSemanticServiceClient(conn).UpdateSemanticRule(authCtx, &adminv1.UpdateSemanticRuleRequest{SpaceId: spaceID.String(), SemanticRuleId: ruleID, Rule: rule})
		if err != nil {
			return err
		}
		return a.Print(res.GetSummary(), fmt.Sprintf("semantic rule updated: %s\n", res.GetRule().GetSemanticRuleId()))
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID")
	cmd.Flags().StringVar(&domainRef, "domain", model.DefaultDomainKey, "domain key or ID for resolving rule keys")
	cmd.Flags().StringVar(&file, "file", "", "JSON/YAML semantic rule file, or - for stdin")
	_ = cmd.MarkFlagRequired("space-id")
	_ = cmd.MarkFlagRequired("file")
	return cmd
}

func newSemanticRuleSetEnabledCommand(a *app.App, enabled bool) *cobra.Command {
	use := "disable RULE"
	short := "Disable a semantic generation rule"
	if enabled {
		use = "enable RULE"
		short = "Enable a semantic generation rule"
	}
	var spaceIDText, domainRef string
	cmd := &cobra.Command{Use: use, Short: short, Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		spaceID, err := a.ResolveSpaceID(spaceIDText)
		if err != nil {
			return err
		}
		ruleID, err := daemonResolveAdminSemanticRuleID(cmd.Context(), conn, authCtx, spaceID.String(), domainRef, args[0])
		if err != nil {
			return err
		}
		res, err := adminv1.NewAdminSemanticServiceClient(conn).SetSemanticRuleEnabled(authCtx, &adminv1.SetSemanticRuleEnabledRequest{SpaceId: spaceID.String(), SemanticRuleId: ruleID, Enabled: enabled})
		if err != nil {
			return err
		}
		state := "disabled"
		if enabled {
			state = "enabled"
		}
		return a.Print(res.GetSummary(), fmt.Sprintf("semantic rule %s: %s\n", state, res.GetRule().GetSemanticRuleId()))
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID")
	cmd.Flags().StringVar(&domainRef, "domain", model.DefaultDomainKey, "domain key or ID for resolving rule keys")
	_ = cmd.MarkFlagRequired("space-id")
	return cmd
}

func newSemanticRuleDeleteCommand(a *app.App) *cobra.Command {
	var spaceIDText, domainRef string
	var purgeVectors bool
	cmd := &cobra.Command{Use: "delete RULE", Aliases: []string{"rm", "remove"}, Short: "Delete a semantic generation rule", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		spaceID, err := a.ResolveSpaceID(spaceIDText)
		if err != nil {
			return err
		}
		ruleID, err := daemonResolveAdminSemanticRuleID(cmd.Context(), conn, authCtx, spaceID.String(), domainRef, args[0])
		if err != nil {
			return err
		}
		res, err := adminv1.NewAdminSemanticServiceClient(conn).DeleteSemanticRule(authCtx, &adminv1.DeleteSemanticRuleRequest{SpaceId: spaceID.String(), SemanticRuleId: ruleID, PurgeVectors: purgeVectors})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("semantic rule deleted: %s (work_items=%d decisions=%d vectors_purged=%t)\n", res.GetSemanticRuleId(), res.GetWorkItemsDeleted(), res.GetPolicyDecisionsDeleted(), res.GetVectorsPurged()))
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID")
	cmd.Flags().StringVar(&domainRef, "domain", model.DefaultDomainKey, "domain key or ID for resolving rule keys")
	cmd.Flags().BoolVar(&purgeVectors, "purge-vectors", false, "delete local vector records for the rule")
	_ = cmd.MarkFlagRequired("space-id")
	return cmd
}

func newSemanticRuleBackfillCommand(a *app.App) *cobra.Command {
	var spaceIDText, domainRef, bindingKey string
	var nodeTexts []string
	var force, continueOnError bool
	var limit int
	cmd := &cobra.Command{Use: "backfill RULE", Short: "Backfill a semantic generation rule binding", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return runDaemonSemanticRuleBackfill(cmd, a, args[0], spaceIDText, domainRef, bindingKey, nodeTexts, force, limit, continueOnError)
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID")
	cmd.Flags().StringVar(&domainRef, "domain", model.DefaultDomainKey, "domain key or ID")
	cmd.Flags().StringVar(&bindingKey, "binding", "", "embedding binding key to backfill")
	cmd.Flags().StringSliceVar(&nodeTexts, "node", nil, "explicit root node ID to backfill")
	cmd.Flags().IntVar(&limit, "limit", 100, "maximum selected roots to backfill (0 for all selected)")
	cmd.Flags().BoolVar(&force, "force", false, "regenerate even if source hash is current")
	cmd.Flags().BoolVar(&continueOnError, "continue-on-error", false, "continue after per-node failures")
	_ = cmd.MarkFlagRequired("space-id")
	_ = cmd.MarkFlagRequired("binding")
	return cmd
}

func runDaemonSemanticRuleBackfill(cmd *cobra.Command, a *app.App, ruleRef, spaceIDText, domainRef, bindingKey string, nodeTexts []string, force bool, limit int, continueOnError bool) error {
	conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
	if err != nil {
		return err
	}
	defer conn.Close()
	spaceID, err := a.ResolveSpaceID(spaceIDText)
	if err != nil {
		return err
	}
	ruleID, err := daemonResolveAdminSemanticRuleID(cmd.Context(), conn, authCtx, spaceID.String(), domainRef, ruleRef)
	if err != nil {
		return err
	}
	res, err := adminv1.NewAdminSemanticMaintenanceServiceClient(conn).BackfillSemanticRule(authCtx, &adminv1.BackfillSemanticRuleRequest{SpaceId: spaceID.String(), SemanticRuleId: ruleID, EmbeddingBindingKey: strings.TrimSpace(bindingKey), NodeIds: append([]string(nil), nodeTexts...), Force: force, Limit: int32(limit), ContinueOnError: continueOnError})
	if err != nil {
		return err
	}
	var b strings.Builder
	fmt.Fprintf(&b, "semantic_rule=%s binding=%s selected=%d generated=%d skipped=%d failed=%d\n", res.GetSemanticRuleId(), res.GetEmbeddingBindingKey(), res.GetSelectedCount(), res.GetGeneratedCount(), res.GetSkippedCount(), res.GetFailedCount())
	for _, skipped := range res.GetSkipped() {
		fmt.Fprintf(&b, "skipped\tnode=%s\treason=%s\n", skipped.GetNodeId(), skipped.GetReason())
	}
	for _, failure := range res.GetFailures() {
		fmt.Fprintf(&b, "failed\tnode=%s\terror=%s\n", failure.GetNodeId(), failure.GetError())
	}
	return a.Print(res, b.String())
}

func newSemanticMaintenanceCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "maintenance", Short: "Inspect and run semantic rule maintenance"}
	cmd.AddCommand(newSemanticMaintenanceStatusCommand(a), newSemanticMaintenanceListCommand(a), newSemanticMaintenanceRetryCommand(a), newSemanticMaintenanceCancelCommand(a), newSemanticMaintenanceAnalyzeCommand(a), newSemanticMaintenanceProcessCommand(a))
	return cmd
}

func newSemanticMaintenanceStatusCommand(a *app.App) *cobra.Command {
	var spaceIDText string
	cmd := &cobra.Command{Use: "status", Short: "Show semantic rule maintenance queue status", RunE: func(cmd *cobra.Command, args []string) error {
		return runDaemonSemanticMaintenanceStatus(cmd, a, spaceIDText)
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID")
	return cmd
}

func newSemanticMaintenanceListCommand(a *app.App) *cobra.Command {
	var spaceIDText, statusText, domainRef, ruleRef, bindingKey string
	var limit int
	cmd := &cobra.Command{Use: "list", Short: "List safe semantic rule maintenance work item metadata", RunE: func(cmd *cobra.Command, args []string) error {
		return runDaemonSemanticMaintenanceList(cmd, a, spaceIDText, statusText, domainRef, ruleRef, bindingKey, limit)
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID")
	cmd.Flags().StringVar(&domainRef, "domain", model.DefaultDomainKey, "domain key or ID for resolving rule keys")
	cmd.Flags().StringVar(&ruleRef, "rule", "", "optional semantic rule key or ID")
	cmd.Flags().StringVar(&bindingKey, "binding", "", "optional embedding binding key")
	cmd.Flags().StringVar(&statusText, "status", "", "optional work status filter")
	cmd.Flags().IntVar(&limit, "limit", 100, "maximum work items to list")
	return cmd
}

func newSemanticMaintenanceRetryCommand(a *app.App) *cobra.Command {
	var spaceIDText string
	cmd := &cobra.Command{Use: "retry WORK_ITEM_ID", Short: "Retry a failed or delayed semantic rule maintenance work item", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return runDaemonSemanticMaintenanceRetry(cmd, a, spaceIDText, args[0])
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID")
	return cmd
}

func newSemanticMaintenanceCancelCommand(a *app.App) *cobra.Command {
	var spaceIDText string
	cmd := &cobra.Command{Use: "cancel WORK_ITEM_ID", Short: "Cancel a semantic rule maintenance work item", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return runDaemonSemanticMaintenanceCancel(cmd, a, spaceIDText, args[0])
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID")
	return cmd
}

func newSemanticMaintenanceAnalyzeCommand(a *app.App) *cobra.Command {
	var spaceIDText, domainRef, ruleRef, bindingKey string
	var limit int
	cmd := &cobra.Command{Use: "analyze", Short: "Analyze graph dirty events into semantic rule dirty work", RunE: func(cmd *cobra.Command, args []string) error {
		return runDaemonSemanticMaintenanceAnalyze(cmd, a, spaceIDText, domainRef, ruleRef, bindingKey, limit)
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID")
	cmd.Flags().StringVar(&domainRef, "domain", model.DefaultDomainKey, "domain key or ID for resolving rule keys")
	cmd.Flags().StringVar(&ruleRef, "rule", "", "optional semantic rule key or ID")
	cmd.Flags().StringVar(&bindingKey, "binding", "", "optional embedding binding key")
	cmd.Flags().IntVar(&limit, "limit", 0, "maximum events to process")
	return cmd
}

func newSemanticMaintenanceProcessCommand(a *app.App) *cobra.Command {
	var spaceIDText string
	var limit int
	cmd := &cobra.Command{Use: "process", Short: "Process pending semantic rule dirty work", RunE: func(cmd *cobra.Command, args []string) error {
		return runDaemonSemanticMaintenanceProcess(cmd, a, spaceIDText, limit)
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID")
	cmd.Flags().IntVar(&limit, "limit", 1, "maximum work items to process")
	return cmd
}

func runDaemonSemanticMaintenanceStatus(cmd *cobra.Command, a *app.App, spaceIDText string) error {
	conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
	if err != nil {
		return err
	}
	defer conn.Close()
	spaceID, err := a.ResolveSpaceID(spaceIDText)
	if err != nil {
		return err
	}
	res, err := adminv1.NewAdminSemanticMaintenanceServiceClient(conn).GetSemanticMaintenanceStatus(authCtx, &adminv1.GetSemanticMaintenanceStatusRequest{SpaceId: spaceID.String()})
	if err != nil {
		return err
	}
	return a.Print(res, fmt.Sprintf("enabled=%t degraded=%t pending=%d running=%d failed_retryable=%d failed_permanent=%d\n", res.GetEnabled(), res.GetDegraded(), res.GetQueueDepthPending(), res.GetQueueDepthRunning(), res.GetQueueDepthFailedRetryable(), res.GetQueueDepthFailedPermanent()))
}

func runDaemonSemanticMaintenanceList(cmd *cobra.Command, a *app.App, spaceIDText, statusText, domainRef, ruleRef, bindingKey string, limit int) error {
	conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
	if err != nil {
		return err
	}
	defer conn.Close()
	spaceID, err := a.ResolveSpaceID(spaceIDText)
	if err != nil {
		return err
	}
	ruleID := ""
	if strings.TrimSpace(ruleRef) != "" {
		ruleID, err = daemonResolveAdminSemanticRuleID(cmd.Context(), conn, authCtx, spaceID.String(), domainRef, ruleRef)
		if err != nil {
			return err
		}
	}
	res, err := adminv1.NewAdminSemanticMaintenanceServiceClient(conn).ListSemanticMaintenanceWork(authCtx, &adminv1.ListSemanticMaintenanceWorkRequest{SpaceId: spaceID.String(), Status: strings.TrimSpace(statusText), Limit: int32(limit), SemanticRuleId: ruleID, EmbeddingBindingKey: strings.TrimSpace(bindingKey)})
	if err != nil {
		return err
	}
	return a.Print(res, fmt.Sprintf("items=%d\n", len(res.GetItems())))
}

func runDaemonSemanticMaintenanceRetry(cmd *cobra.Command, a *app.App, spaceIDText, workID string) error {
	conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
	if err != nil {
		return err
	}
	defer conn.Close()
	spaceID, err := a.ResolveSpaceID(spaceIDText)
	if err != nil {
		return err
	}
	res, err := adminv1.NewAdminSemanticMaintenanceServiceClient(conn).RetrySemanticMaintenanceWork(authCtx, &adminv1.RetrySemanticMaintenanceWorkRequest{SpaceId: spaceID.String(), WorkItemId: strings.TrimSpace(workID)})
	if err != nil {
		return err
	}
	return a.Print(res, fmt.Sprintf("retried=%s status=%s\n", res.GetItem().GetWorkItemId(), res.GetItem().GetStatus()))
}

func runDaemonSemanticMaintenanceCancel(cmd *cobra.Command, a *app.App, spaceIDText, workID string) error {
	conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
	if err != nil {
		return err
	}
	defer conn.Close()
	spaceID, err := a.ResolveSpaceID(spaceIDText)
	if err != nil {
		return err
	}
	res, err := adminv1.NewAdminSemanticMaintenanceServiceClient(conn).CancelSemanticMaintenanceWork(authCtx, &adminv1.CancelSemanticMaintenanceWorkRequest{SpaceId: spaceID.String(), WorkItemId: strings.TrimSpace(workID)})
	if err != nil {
		return err
	}
	return a.Print(res, fmt.Sprintf("cancelled=%s status=%s\n", res.GetItem().GetWorkItemId(), res.GetItem().GetStatus()))
}

func runDaemonSemanticMaintenanceAnalyze(cmd *cobra.Command, a *app.App, spaceIDText, domainRef, ruleRef, bindingKey string, limit int) error {
	conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
	if err != nil {
		return err
	}
	defer conn.Close()
	spaceID, err := a.ResolveSpaceID(spaceIDText)
	if err != nil {
		return err
	}
	ruleID := ""
	if strings.TrimSpace(ruleRef) != "" {
		ruleID, err = daemonResolveAdminSemanticRuleID(cmd.Context(), conn, authCtx, spaceID.String(), domainRef, ruleRef)
		if err != nil {
			return err
		}
	}
	res, err := adminv1.NewAdminSemanticMaintenanceServiceClient(conn).AnalyzeSemanticDirtyWork(authCtx, &adminv1.AnalyzeSemanticDirtyWorkRequest{SpaceId: spaceID.String(), SemanticRuleId: ruleID, EmbeddingBindingKey: strings.TrimSpace(bindingKey), Limit: int32(limit)})
	if err != nil {
		return err
	}
	return a.Print(res, fmt.Sprintf("processed_events=%d enqueued_items=%d\n", res.GetProcessedEvents(), res.GetEnqueuedItems()))
}

func runDaemonSemanticMaintenanceProcess(cmd *cobra.Command, a *app.App, spaceIDText string, limit int) error {
	conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
	if err != nil {
		return err
	}
	defer conn.Close()
	spaceID, err := a.ResolveSpaceID(spaceIDText)
	if err != nil {
		return err
	}
	res, err := adminv1.NewAdminSemanticMaintenanceServiceClient(conn).ProcessSemanticDirtyWork(authCtx, &adminv1.ProcessSemanticDirtyWorkRequest{SpaceId: spaceID.String(), Limit: int32(limit)})
	if err != nil {
		return err
	}
	return a.Print(res, fmt.Sprintf("processed=%d completed=%d failed=%d\n", res.GetProcessedItems(), res.GetCompletedItems(), res.GetFailedItems()))
}

func daemonResolveDomainID(ctx context.Context, conn grpc.ClientConnInterface, authCtx context.Context, spaceID string, domainRef string) (string, error) {
	client := clientv1.NewDomainServiceClient(conn)
	ref := strings.TrimSpace(domainRef)
	if ref == "" {
		ref = model.DefaultDomainKey
	}
	if _, err := uuid.Parse(ref); err == nil {
		res, err := client.GetDomain(authCtx, &clientv1.GetDomainRequest{SpaceId: spaceID, DomainId: ref})
		if err != nil {
			return "", err
		}
		return res.GetDomain().GetDomainId(), nil
	}
	res, err := client.GetDomain(authCtx, &clientv1.GetDomainRequest{SpaceId: spaceID, Key: ref})
	if err != nil {
		return "", err
	}
	return res.GetDomain().GetDomainId(), nil
}

func daemonResolveClientSemanticRuleID(ctx context.Context, semanticClient clientv1.SemanticServiceClient, authCtx context.Context, spaceID string, domainID string, raw string) (string, error) {
	ref := strings.TrimSpace(raw)
	if _, err := uuid.Parse(ref); err == nil {
		return ref, nil
	}
	res, err := semanticClient.ListSemanticRules(authCtx, &clientv1.ListSemanticRulesRequest{SpaceId: spaceID, DomainId: domainID, PageSize: 500, IncludeDisabled: true})
	if err != nil {
		return "", err
	}
	key := normalizeCLIKey(ref)
	for _, rule := range res.GetRules() {
		if normalizeCLIKey(rule.GetKey()) == key {
			return rule.GetSemanticRuleId(), nil
		}
	}
	return "", fmt.Errorf("semantic rule %q not found", raw)
}

func daemonResolveAdminSemanticRuleID(ctx context.Context, conn *grpc.ClientConn, authCtx context.Context, spaceID string, domainRef string, raw string) (string, error) {
	ref := strings.TrimSpace(raw)
	if _, err := uuid.Parse(ref); err == nil {
		return ref, nil
	}
	domainID := ""
	if strings.TrimSpace(domainRef) != "" {
		resolved, err := daemonResolveAdminDomainID(ctx, conn, authCtx, spaceID, domainRef)
		if err != nil {
			return "", err
		}
		domainID = resolved
	}
	listed, err := adminv1.NewAdminSemanticServiceClient(conn).ListSemanticRules(authCtx, &adminv1.ListSemanticRulesRequest{SpaceId: spaceID, DomainId: domainID, PageSize: 500, IncludeDisabled: true})
	if err != nil {
		return "", err
	}
	key := normalizeCLIKey(ref)
	for _, rule := range listed.GetRules() {
		if normalizeCLIKey(rule.GetKey()) == key {
			return rule.GetSemanticRuleId(), nil
		}
	}
	return "", fmt.Errorf("semantic rule %q not found", raw)
}

type semanticRuleCLIFlags struct {
	File             string
	SpaceID          string
	DomainRef        string
	DisplayName      string
	Description      string
	Enabled          bool
	Labels           []string
	SelectorMode     string
	SourceMode       string
	IncludeProps     []string
	ExcludeProps     []string
	Profile          string
	ProfileID        string
	VectorStore      string
	VectorStoreID    string
	BindingKey       string
	Purpose          string
	Searchable       bool
	PhysicalIndex    string
	DirtyCooldown    string
	OwnerPrincipalID string
}

func bindSemanticRuleCreateFlags(cmd *cobra.Command, f *semanticRuleCLIFlags, includeSpace bool) {
	cmd.Flags().StringVar(&f.File, "file", "", "JSON/YAML semantic rule file, or - for stdin")
	if includeSpace {
		cmd.Flags().StringVar(&f.SpaceID, "space-id", "", "space ID")
		cmd.Flags().StringVar(&f.DomainRef, "domain", model.DefaultDomainKey, "domain key or ID")
	}
	cmd.Flags().StringVar(&f.DisplayName, "name", "", "semantic rule display name")
	cmd.Flags().StringVar(&f.Description, "description", "", "semantic rule description")
	cmd.Flags().BoolVar(&f.Enabled, "enabled", true, "enable rule")
	cmd.Flags().StringSliceVar(&f.Labels, "label", nil, "target node label; repeatable or comma-separated")
	cmd.Flags().StringVar(&f.SelectorMode, "selector", string(domainsemantic.SemanticTargetSelectorNodeType), "target selector mode")
	cmd.Flags().StringVar(&f.SourceMode, "source", string(domainsemantic.SemanticSourceSelf), "source assembly mode: self, subtree, or context_query")
	cmd.Flags().StringArrayVar(&f.IncludeProps, "include-prop", nil, "node prop to include in source text")
	cmd.Flags().StringArrayVar(&f.ExcludeProps, "exclude-prop", nil, "node prop to exclude from source text")
	cmd.Flags().StringVar(&f.Profile, "profile", "", "Intelligence Access profile key or ID")
	cmd.Flags().StringVar(&f.ProfileID, "profile-id", "", "Intelligence Access profile UUID")
	cmd.Flags().StringVar(&f.VectorStore, "vector-store", "mycel-file", "vector store key or ID")
	cmd.Flags().StringVar(&f.VectorStoreID, "vector-store-id", "", "vector store UUID")
	cmd.Flags().StringVar(&f.BindingKey, "binding", "search", "embedding binding key")
	cmd.Flags().StringVar(&f.Purpose, "purpose", string(domainsemantic.SemanticIndexPurposeSearch), "embedding binding purpose")
	cmd.Flags().BoolVar(&f.Searchable, "searchable", true, "maintain a searchable physical index")
	cmd.Flags().StringVar(&f.PhysicalIndex, "physical-index", domainsemantic.SemanticPhysicalIndexExact, "physical search index type")
	cmd.Flags().StringVar(&f.DirtyCooldown, "dirty-cooldown", "", "dirty work debounce duration, e.g. 30s")
	cmd.Flags().StringVar(&f.OwnerPrincipalID, "owner-principal-id", "", "owner principal ID")
}

func buildSemanticRuleFromCLI(cmd *cobra.Command, a *app.App, f semanticRuleCLIFlags, args []string) (*adminv1.SemanticGenerationRule, *grpc.ClientConn, context.Context, error) {
	conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
	if err != nil {
		return nil, nil, nil, err
	}
	if strings.TrimSpace(f.File) != "" {
		rule, err := readSemanticRuleFile(f.File)
		if err != nil {
			_ = conn.Close()
			return nil, nil, nil, err
		}
		if strings.TrimSpace(rule.GetSpaceId()) != "" {
			spaceID, err := a.ResolveSpaceID(rule.GetSpaceId())
			if err != nil {
				_ = conn.Close()
				return nil, nil, nil, err
			}
			rule.SpaceId = spaceID.String()
		} else if strings.TrimSpace(f.SpaceID) != "" {
			spaceID, err := a.ResolveSpaceID(f.SpaceID)
			if err != nil {
				_ = conn.Close()
				return nil, nil, nil, err
			}
			rule.SpaceId = spaceID.String()
		}
		if strings.TrimSpace(rule.GetDomainId()) != "" && rule.GetSpaceId() != "" {
			if _, err := uuid.Parse(rule.GetDomainId()); err != nil {
				domainID, err := daemonResolveAdminDomainID(cmd.Context(), conn, authCtx, rule.GetSpaceId(), rule.GetDomainId())
				if err != nil {
					_ = conn.Close()
					return nil, nil, nil, err
				}
				rule.DomainId = domainID
			}
		} else if strings.TrimSpace(f.DomainRef) != "" && rule.GetSpaceId() != "" {
			domainID, err := daemonResolveAdminDomainID(cmd.Context(), conn, authCtx, rule.GetSpaceId(), f.DomainRef)
			if err != nil {
				_ = conn.Close()
				return nil, nil, nil, err
			}
			rule.DomainId = domainID
		}
		return rule, conn, authCtx, nil
	}
	if len(args) == 0 || strings.TrimSpace(args[0]) == "" {
		_ = conn.Close()
		return nil, nil, nil, fmt.Errorf("KEY argument or --file is required")
	}
	spaceID, err := a.ResolveSpaceID(f.SpaceID)
	if err != nil {
		_ = conn.Close()
		return nil, nil, nil, err
	}
	domainID, err := daemonResolveAdminDomainID(cmd.Context(), conn, authCtx, spaceID.String(), f.DomainRef)
	if err != nil {
		_ = conn.Close()
		return nil, nil, nil, err
	}
	rule := semanticRuleFromFlags(args[0], spaceID.String(), domainID, f)
	return rule, conn, authCtx, nil
}

func semanticRuleFromFlags(key, spaceID, domainID string, f semanticRuleCLIFlags) *adminv1.SemanticGenerationRule {
	labels := append([]string(nil), f.Labels...)
	if len(labels) == 0 {
		labels = []string{"Note"}
	}
	binding := &adminv1.SemanticEmbeddingBinding{Key: firstNonEmpty(f.BindingKey, "search"), Purpose: firstNonEmpty(f.Purpose, string(domainsemantic.SemanticIndexPurposeSearch)), Enabled: true}
	if profile := strings.TrimSpace(f.Profile); profile != "" {
		if _, err := uuid.Parse(profile); err == nil {
			binding.IntelligenceProfileId = profile
		} else {
			binding.IntelligenceProfile = profile
		}
	}
	if strings.TrimSpace(f.ProfileID) != "" {
		binding.IntelligenceProfileId = strings.TrimSpace(f.ProfileID)
	}
	if store := strings.TrimSpace(f.VectorStore); store != "" {
		if _, err := uuid.Parse(store); err == nil {
			binding.VectorStoreId = store
		} else {
			binding.VectorStore = store
		}
	}
	if strings.TrimSpace(f.VectorStoreID) != "" {
		binding.VectorStoreId = strings.TrimSpace(f.VectorStoreID)
	}
	return &adminv1.SemanticGenerationRule{SpaceId: spaceID, DomainId: domainID, Key: strings.TrimSpace(key), DisplayName: firstNonEmpty(f.DisplayName, strings.TrimSpace(key)), Description: strings.TrimSpace(f.Description), Enabled: f.Enabled, Trigger: &adminv1.SemanticTriggerPolicy{Events: []string{domainsemantic.DefaultSemanticTriggerEventChanged}, Labels: labels, Debounce: strings.TrimSpace(f.DirtyCooldown)}, Selector: &adminv1.SemanticTargetSelector{Mode: firstNonEmpty(f.SelectorMode, string(domainsemantic.SemanticTargetSelectorNodeType)), Labels: labels}, Source: &adminv1.SemanticSourceAssemblyPolicy{Mode: firstNonEmpty(f.SourceMode, string(domainsemantic.SemanticSourceSelf)), IncludeProperties: append([]string(nil), f.IncludeProps...), ExcludeProperties: append([]string(nil), f.ExcludeProps...)}, Embeddings: []*adminv1.SemanticEmbeddingBinding{binding}, Maintenance: &adminv1.SemanticMaintenancePolicy{DirtyCooldown: strings.TrimSpace(f.DirtyCooldown)}, Storage: &adminv1.SemanticStoragePolicy{Searchable: f.Searchable, PhysicalIndex: firstNonEmpty(f.PhysicalIndex, domainsemantic.SemanticPhysicalIndexExact)}, OwnerPrincipalId: strings.TrimSpace(f.OwnerPrincipalID)}
}

type semanticRuleFile struct {
	Rule                 *semanticRuleDoc            `json:"rule" yaml:"rule"`
	SemanticRuleID       string                      `json:"semantic_rule_id" yaml:"semantic_rule_id"`
	SpaceID              string                      `json:"space_id" yaml:"space_id"`
	DomainID             string                      `json:"domain_id" yaml:"domain_id"`
	Key                  string                      `json:"key" yaml:"key"`
	DisplayName          string                      `json:"display_name" yaml:"display_name"`
	Description          string                      `json:"description" yaml:"description"`
	Enabled              *bool                       `json:"enabled" yaml:"enabled"`
	Trigger              *semanticRuleTriggerDoc     `json:"trigger" yaml:"trigger"`
	Selector             *semanticRuleSelectorDoc    `json:"selector" yaml:"selector"`
	Source               *semanticRuleSourceDoc      `json:"source" yaml:"source"`
	Embeddings           []semanticRuleEmbeddingDoc  `json:"embeddings" yaml:"embeddings"`
	Maintenance          *semanticRuleMaintenanceDoc `json:"maintenance" yaml:"maintenance"`
	Storage              *semanticRuleStorageDoc     `json:"storage" yaml:"storage"`
	OwnerPrincipalID     string                      `json:"owner_principal_id" yaml:"owner_principal_id"`
	CreatedByPrincipalID string                      `json:"created_by_principal_id" yaml:"created_by_principal_id"`
}

type semanticRuleDoc semanticRuleFile

type semanticRuleTriggerDoc struct {
	Events   []string `json:"events" yaml:"events"`
	Labels   []string `json:"labels" yaml:"labels"`
	Debounce string   `json:"debounce" yaml:"debounce"`
}

type semanticRuleSelectorDoc struct {
	Mode        string   `json:"mode" yaml:"mode"`
	Labels      []string `json:"labels" yaml:"labels"`
	GQL         string   `json:"gql" yaml:"gql"`
	TargetAlias string   `json:"target_alias" yaml:"target_alias"`
	MaxResults  int32    `json:"max_results" yaml:"max_results"`
	NodeIDs     []string `json:"node_ids" yaml:"node_ids"`
}

type semanticRuleSourceDoc struct {
	Mode              string   `json:"mode" yaml:"mode"`
	IncludeProperties []string `json:"include_properties" yaml:"include_properties"`
	ExcludeProperties []string `json:"exclude_properties" yaml:"exclude_properties"`
	MaxDepth          *int32   `json:"max_depth" yaml:"max_depth"`
	MinimumTextLength int32    `json:"minimum_text_length" yaml:"minimum_text_length"`
	ContextGQL        string   `json:"context_gql" yaml:"context_gql"`
}

type semanticRuleEmbeddingDoc struct {
	Key                   string                 `json:"key" yaml:"key"`
	Purpose               string                 `json:"purpose" yaml:"purpose"`
	IntelligenceProfile   string                 `json:"intelligence_profile" yaml:"intelligence_profile"`
	IntelligenceProfileID string                 `json:"intelligence_profile_id" yaml:"intelligence_profile_id"`
	VectorStore           string                 `json:"vector_store" yaml:"vector_store"`
	VectorStoreID         string                 `json:"vector_store_id" yaml:"vector_store_id"`
	Enabled               *bool                  `json:"enabled" yaml:"enabled"`
	Metadata              map[string]interface{} `json:"metadata" yaml:"metadata"`
}

type semanticRuleMaintenanceDoc struct {
	DirtyCooldown     string `json:"dirty_cooldown" yaml:"dirty_cooldown"`
	MaxBatchSize      int32  `json:"max_batch_size" yaml:"max_batch_size"`
	WorkerConcurrency int32  `json:"worker_concurrency" yaml:"worker_concurrency"`
}

type semanticRuleStorageDoc struct {
	Searchable    *bool  `json:"searchable" yaml:"searchable"`
	PhysicalIndex string `json:"physical_index" yaml:"physical_index"`
}

func readSemanticRuleFile(path string) (*adminv1.SemanticGenerationRule, error) {
	var data []byte
	var err error
	if strings.TrimSpace(path) == "-" {
		data, err = io.ReadAll(os.Stdin)
	} else {
		data, err = os.ReadFile(path)
	}
	if err != nil {
		return nil, err
	}
	var file semanticRuleFile
	if err := yaml.Unmarshal(data, &file); err != nil {
		return nil, fmt.Errorf("parse semantic rule file: %w", err)
	}
	doc := semanticRuleDoc(file)
	if file.Rule != nil {
		doc = *file.Rule
	}
	return semanticRuleDocToProto(doc), nil
}

func semanticRuleDocToProto(doc semanticRuleDoc) *adminv1.SemanticGenerationRule {
	enabled := true
	if doc.Enabled != nil {
		enabled = *doc.Enabled
	}
	rule := &adminv1.SemanticGenerationRule{SemanticRuleId: strings.TrimSpace(doc.SemanticRuleID), SpaceId: strings.TrimSpace(doc.SpaceID), DomainId: strings.TrimSpace(doc.DomainID), Key: strings.TrimSpace(doc.Key), DisplayName: strings.TrimSpace(doc.DisplayName), Description: strings.TrimSpace(doc.Description), Enabled: enabled, OwnerPrincipalId: strings.TrimSpace(doc.OwnerPrincipalID), CreatedByPrincipalId: strings.TrimSpace(doc.CreatedByPrincipalID)}
	if doc.Trigger != nil {
		rule.Trigger = &adminv1.SemanticTriggerPolicy{Events: append([]string(nil), doc.Trigger.Events...), Labels: append([]string(nil), doc.Trigger.Labels...), Debounce: strings.TrimSpace(doc.Trigger.Debounce)}
	}
	if doc.Selector != nil {
		rule.Selector = &adminv1.SemanticTargetSelector{Mode: strings.TrimSpace(doc.Selector.Mode), Labels: append([]string(nil), doc.Selector.Labels...), Gql: doc.Selector.GQL, TargetAlias: strings.TrimSpace(doc.Selector.TargetAlias), MaxResults: doc.Selector.MaxResults, NodeIds: append([]string(nil), doc.Selector.NodeIDs...)}
	}
	if doc.Source != nil {
		rule.Source = &adminv1.SemanticSourceAssemblyPolicy{Mode: strings.TrimSpace(doc.Source.Mode), IncludeProperties: append([]string(nil), doc.Source.IncludeProperties...), ExcludeProperties: append([]string(nil), doc.Source.ExcludeProperties...), MaxDepth: doc.Source.MaxDepth, MinimumTextLength: doc.Source.MinimumTextLength, ContextGql: doc.Source.ContextGQL}
	}
	for _, item := range doc.Embeddings {
		enabled := true
		if item.Enabled != nil {
			enabled = *item.Enabled
		}
		rule.Embeddings = append(rule.Embeddings, &adminv1.SemanticEmbeddingBinding{Key: strings.TrimSpace(item.Key), Purpose: strings.TrimSpace(item.Purpose), IntelligenceProfile: strings.TrimSpace(item.IntelligenceProfile), IntelligenceProfileId: strings.TrimSpace(item.IntelligenceProfileID), VectorStore: strings.TrimSpace(item.VectorStore), VectorStoreId: strings.TrimSpace(item.VectorStoreID), Enabled: enabled})
	}
	if doc.Maintenance != nil {
		rule.Maintenance = &adminv1.SemanticMaintenancePolicy{DirtyCooldown: strings.TrimSpace(doc.Maintenance.DirtyCooldown), MaxBatchSize: doc.Maintenance.MaxBatchSize, WorkerConcurrency: doc.Maintenance.WorkerConcurrency}
	}
	if doc.Storage != nil {
		searchable := true
		if doc.Storage.Searchable != nil {
			searchable = *doc.Storage.Searchable
		}
		rule.Storage = &adminv1.SemanticStoragePolicy{Searchable: searchable, PhysicalIndex: strings.TrimSpace(doc.Storage.PhysicalIndex)}
	}
	return rule
}

func bindingKeys(bindings []*clientv1.SemanticEmbeddingBindingSummary) []string {
	out := make([]string, 0, len(bindings))
	for _, binding := range bindings {
		out = append(out, binding.GetKey())
	}
	return out
}
