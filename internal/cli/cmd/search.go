package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/myceldb/mycel/internal/cli/app"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	model "github.com/myceldb/mycel/internal/graph/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

func NewSearchCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "search", Short: "Search graph content"}
	lexical := &cobra.Command{Use: "lexical [QUERY]", Short: "Run BM25 lexical search", Args: cobra.MaximumNArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		query, _ := cmd.Flags().GetString("query")
		if len(args) == 1 && strings.TrimSpace(query) == "" {
			query = args[0]
		}
		return runLexicalSearch(cmd, a, query)
	}}
	bindLexicalSearchFlags(lexical)
	lexical.AddCommand(newLexicalStatusCommand(a), newLexicalRebuildCommand(a))
	hybrid := &cobra.Command{Use: "hybrid [QUERY]", Short: "Run hybrid lexical + semantic search", Args: cobra.MaximumNArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		query, _ := cmd.Flags().GetString("query")
		if len(args) == 1 && strings.TrimSpace(query) == "" {
			query = args[0]
		}
		return runHybridSearch(cmd, a, query)
	}}
	bindHybridSearchFlags(hybrid)
	cmd.AddCommand(lexical, hybrid)
	return cmd
}

func bindLexicalSearchFlags(cmd *cobra.Command) {
	cmd.Flags().String("space-id", "", "space ID")
	cmd.Flags().String("domain", model.DefaultDomainKey, "domain key or ID")
	cmd.Flags().String("query", "", "query text")
	cmd.Flags().Int32("page-size", 20, "page size")
	cmd.Flags().String("page-token", "", "page token")
	cmd.Flags().Bool("allow-stale", false, "allow stale lexical index results")
	cmd.Flags().Int64("max-revision-lag", 0, "maximum acceptable lexical index revision lag")
	cmd.Flags().Bool("diagnostics", false, "include search diagnostics")
	_ = cmd.MarkFlagRequired("space-id")
}

func bindHybridSearchFlags(cmd *cobra.Command) {
	bindLexicalSearchFlags(cmd)
	cmd.Flags().Float64("lexical-weight", 0.5, "hybrid lexical weight")
	cmd.Flags().Float64("semantic-weight", 0.5, "hybrid semantic weight")
	cmd.Flags().Bool("require-both", false, "require both lexical and semantic matches")
	cmd.Flags().Int32("lexical-candidates", 0, "lexical candidates to retrieve before fusion")
	cmd.Flags().Int32("semantic-candidates", 0, "semantic candidates to retrieve before fusion")
	cmd.Flags().String("semantic-rule-id", "", "semantic rule ID for hybrid semantic retrieval")
	cmd.Flags().String("embedding-binding-key", "", "semantic embedding binding key; requires --semantic-rule-id")
	cmd.Flags().Float64("semantic-min-score", 0, "minimum semantic score threshold")
	cmd.Flags().StringArray("label", nil, "required node label filter; repeatable")
	cmd.Flags().StringArray("node-id", nil, "candidate node ID allow-list filter; repeatable")
	cmd.Flags().StringArray("property-filter", nil, "property filter path:operator:value[,value]; operators: equals, not-equals, in, contains, exists")
}

func runLexicalSearch(cmd *cobra.Command, a *app.App, query string) error {
	if strings.TrimSpace(query) == "" {
		return fmt.Errorf("query is required")
	}
	conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
	if err != nil {
		return err
	}
	defer conn.Close()
	spaceIDText, _ := cmd.Flags().GetString("space-id")
	domainRef, _ := cmd.Flags().GetString("domain")
	pageSize, _ := cmd.Flags().GetInt32("page-size")
	pageToken, _ := cmd.Flags().GetString("page-token")
	allowStale, _ := cmd.Flags().GetBool("allow-stale")
	maxLag, _ := cmd.Flags().GetInt64("max-revision-lag")
	diagnostics, _ := cmd.Flags().GetBool("diagnostics")
	spaceID, err := app.ParseUUID[domainspace.SpaceID](spaceIDText)
	if err != nil {
		return fmt.Errorf("--space-id must be a UUID: %w", err)
	}
	domainID, err := daemonResolveDomainID(cmd.Context(), conn, authCtx, spaceID.String(), domainRef)
	if err != nil {
		return err
	}
	res, err := clientv1.NewSearchServiceClient(conn).Search(authCtx, &clientv1.SearchRequest{SpaceId: spaceID.String(), DomainId: domainID, Mode: clientv1.SearchMode_SEARCH_MODE_LEXICAL, Query: query, PageSize: pageSize, PageToken: pageToken, AllowStale: allowStale, MaxRevisionLag: maxLag, IncludeDiagnostics: diagnostics})
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
		fmt.Printf("%.4f\tnode=%s\trevision=%d\n", result.GetScore(), result.GetNodeId(), result.GetIndexedGraphRevision())
	}
	if res.GetNextPageToken() != "" {
		fmt.Printf("next page token: %s\n", res.GetNextPageToken())
	}
	if freshness := res.GetFreshness(); freshness != nil {
		fmt.Printf("freshness\t%s\tindexed=%d\tlatest=%d\tlag=%d\n", freshness.GetState(), freshness.GetIndexedGraphRevision(), freshness.GetLatestKnownGraphRevision(), freshness.GetRevisionLag())
	}
	return nil
}

func runHybridSearch(cmd *cobra.Command, a *app.App, query string) error {
	if strings.TrimSpace(query) == "" {
		return fmt.Errorf("query is required")
	}
	conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
	if err != nil {
		return err
	}
	defer conn.Close()
	spaceIDText, _ := cmd.Flags().GetString("space-id")
	domainRef, _ := cmd.Flags().GetString("domain")
	pageSize, _ := cmd.Flags().GetInt32("page-size")
	allowStale, _ := cmd.Flags().GetBool("allow-stale")
	maxLag, _ := cmd.Flags().GetInt64("max-revision-lag")
	diagnostics, _ := cmd.Flags().GetBool("diagnostics")
	lexicalWeight, _ := cmd.Flags().GetFloat64("lexical-weight")
	semanticWeight, _ := cmd.Flags().GetFloat64("semantic-weight")
	requireBoth, _ := cmd.Flags().GetBool("require-both")
	lexicalCandidates, _ := cmd.Flags().GetInt32("lexical-candidates")
	semanticCandidates, _ := cmd.Flags().GetInt32("semantic-candidates")
	semanticRuleID, _ := cmd.Flags().GetString("semantic-rule-id")
	embeddingBindingKey, _ := cmd.Flags().GetString("embedding-binding-key")
	semanticMinScore, _ := cmd.Flags().GetFloat64("semantic-min-score")
	spaceID, err := app.ParseUUID[domainspace.SpaceID](spaceIDText)
	if err != nil {
		return fmt.Errorf("--space-id must be a UUID: %w", err)
	}
	domainID, err := daemonResolveDomainID(cmd.Context(), conn, authCtx, spaceID.String(), domainRef)
	if err != nil {
		return err
	}
	filters, err := searchFiltersFromFlags(cmd)
	if err != nil {
		return err
	}
	semantic := &clientv1.SemanticSearchOptions{CandidateCount: semanticCandidates}
	if strings.TrimSpace(semanticRuleID) != "" {
		semantic.SemanticRuleId = stringPtr(strings.TrimSpace(semanticRuleID))
	}
	if strings.TrimSpace(embeddingBindingKey) != "" {
		semantic.EmbeddingBindingKey = stringPtr(strings.TrimSpace(embeddingBindingKey))
	}
	if cmd.Flags().Changed("semantic-min-score") {
		semantic.MinScore = float64Ptr(semanticMinScore)
	}
	res, err := clientv1.NewSearchServiceClient(conn).Search(authCtx, &clientv1.SearchRequest{SpaceId: spaceID.String(), DomainId: domainID, Mode: clientv1.SearchMode_SEARCH_MODE_HYBRID, Query: query, PageSize: pageSize, AllowStale: allowStale, MaxRevisionLag: maxLag, IncludeDiagnostics: diagnostics, Filters: filters, Hybrid: &clientv1.HybridSearchOptions{LexicalWeight: lexicalWeight, SemanticWeight: semanticWeight, FusionStrategy: clientv1.HybridFusionStrategy_HYBRID_FUSION_STRATEGY_WEIGHTED_RECIPROCAL_RANK, RequireBoth: requireBoth}, Lexical: &clientv1.LexicalSearchOptions{CandidateCount: lexicalCandidates}, Semantic: semantic})
	if err != nil {
		return err
	}
	return printSearchResponse(a, res, diagnostics)
}

func printSearchResponse(a *app.App, res *clientv1.SearchResponse, diagnostics bool) error {
	if a.Output == "json" {
		return a.Print(res, "")
	}
	for _, warning := range res.GetWarnings() {
		fmt.Printf("warning\t%s\n", warning)
	}
	for _, result := range res.GetResults() {
		parts := []string{fmt.Sprintf("%.4f", result.GetScore()), fmt.Sprintf("node=%s", result.GetNodeId()), fmt.Sprintf("revision=%d", result.GetIndexedGraphRevision())}
		if diagnostics {
			for _, source := range result.GetSources() {
				parts = append(parts, fmt.Sprintf("%s_rank=%d", strings.ToLower(strings.TrimPrefix(source.GetKind().String(), "SEARCH_RESULT_SOURCE_KIND_")), source.GetRank()))
			}
		}
		fmt.Println(strings.Join(parts, "\t"))
	}
	if res.GetNextPageToken() != "" {
		fmt.Printf("next page token: %s\n", res.GetNextPageToken())
	}
	if freshness := res.GetFreshness(); freshness != nil {
		fmt.Printf("freshness\t%s\tindexed=%d\tlatest=%d\tlag=%d\n", freshness.GetState(), freshness.GetIndexedGraphRevision(), freshness.GetLatestKnownGraphRevision(), freshness.GetRevisionLag())
	}
	return nil
}

func searchFiltersFromFlags(cmd *cobra.Command) (*clientv1.SearchFilters, error) {
	labels, _ := cmd.Flags().GetStringArray("label")
	nodeIDs, _ := cmd.Flags().GetStringArray("node-id")
	propertySpecs, _ := cmd.Flags().GetStringArray("property-filter")
	filters := &clientv1.SearchFilters{NodeLabels: trimNonEmpty(labels), NodeIds: trimNonEmpty(nodeIDs)}
	for _, spec := range propertySpecs {
		filter, err := parsePropertyFilterSpec(spec)
		if err != nil {
			return nil, err
		}
		filters.Properties = append(filters.Properties, filter)
	}
	if len(filters.NodeLabels) == 0 && len(filters.NodeIds) == 0 && len(filters.Properties) == 0 {
		return nil, nil
	}
	return filters, nil
}

func parsePropertyFilterSpec(spec string) (*clientv1.PropertyFilter, error) {
	parts := strings.SplitN(spec, ":", 3)
	if len(parts) < 2 {
		return nil, fmt.Errorf("--property-filter must use path:operator:value[,value]")
	}
	path := strings.TrimSpace(parts[0])
	operator, err := cliFilterOperator(strings.TrimSpace(parts[1]))
	if err != nil {
		return nil, err
	}
	values := []string{}
	if len(parts) == 3 {
		values = trimNonEmpty(strings.Split(parts[2], ","))
	}
	if path == "" {
		return nil, fmt.Errorf("--property-filter path is required")
	}
	return &clientv1.PropertyFilter{Path: path, Operator: operator, Values: values}, nil
}

func cliFilterOperator(raw string) (clientv1.FilterOperator, error) {
	switch strings.ToLower(strings.ReplaceAll(strings.TrimSpace(raw), "_", "-")) {
	case "equals", "eq", "=":
		return clientv1.FilterOperator_FILTER_OPERATOR_EQUALS, nil
	case "not-equals", "ne", "!=":
		return clientv1.FilterOperator_FILTER_OPERATOR_NOT_EQUALS, nil
	case "in":
		return clientv1.FilterOperator_FILTER_OPERATOR_IN, nil
	case "contains":
		return clientv1.FilterOperator_FILTER_OPERATOR_CONTAINS, nil
	case "exists":
		return clientv1.FilterOperator_FILTER_OPERATOR_EXISTS, nil
	default:
		return clientv1.FilterOperator_FILTER_OPERATOR_UNSPECIFIED, fmt.Errorf("unsupported --property-filter operator %q", raw)
	}
}

func trimNonEmpty(values []string) []string {
	out := []string{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}

func stringPtr(value string) *string { return &value }

func float64Ptr(value float64) *float64 { return &value }

func newLexicalStatusCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "status", Short: "Show lexical index status", RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		spaceID, domainID, err := lexicalScopeFromFlags(cmd, conn, authCtx)
		if err != nil {
			return err
		}
		res, err := clientv1.NewSearchServiceClient(conn).GetLexicalIndexStatus(authCtx, &clientv1.GetLexicalIndexStatusRequest{SpaceId: spaceID, DomainId: domainID})
		if err != nil {
			return err
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		st := res.GetStatus()
		fmt.Printf("state\t%s\nspace\t%s\ndomain\t%s\nindexed_revision\t%d\nlatest_revision\t%d\nrevision_lag\t%d\nsegments\t%d\nlive_documents\t%d\ndeleted_documents\t%d\n", st.GetState(), st.GetSpaceId(), st.GetDomainId(), st.GetIndexedGraphRevision(), st.GetLatestKnownGraphRevision(), st.GetRevisionLag(), st.GetSegmentCount(), st.GetLiveDocumentCount(), st.GetDeletedDocumentCount())
		return nil
	}}
	cmd.Flags().String("space-id", "", "space ID")
	cmd.Flags().String("domain", model.DefaultDomainKey, "domain key or ID")
	_ = cmd.MarkFlagRequired("space-id")
	return cmd
}

func newLexicalRebuildCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "rebuild", Short: "Rebuild a lexical index", RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		spaceID, domainID, err := lexicalScopeFromFlags(cmd, conn, authCtx)
		if err != nil {
			return err
		}
		force, _ := cmd.Flags().GetBool("force")
		dryRun, _ := cmd.Flags().GetBool("dry-run")
		res, err := adminv1.NewAdminLexicalMaintenanceServiceClient(conn).RebuildLexicalIndex(authCtx, &adminv1.RebuildLexicalIndexRequest{SpaceId: spaceID, DomainId: domainID, Force: force, DryRun: dryRun})
		if err != nil {
			return err
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		fmt.Printf("lexical rebuild %s: accepted=%v dry_run=%v rebuild_id=%s\n", res.GetState(), res.GetAccepted(), res.GetDryRun(), res.GetRebuildId())
		for _, warning := range res.GetWarnings() {
			fmt.Printf("warning\t%s\n", warning)
		}
		return nil
	}}
	cmd.Flags().String("space-id", "", "space ID")
	cmd.Flags().String("domain", model.DefaultDomainKey, "domain key or ID")
	cmd.Flags().Bool("force", false, "force rebuild even if fresh")
	cmd.Flags().Bool("dry-run", false, "validate rebuild without starting work")
	_ = cmd.MarkFlagRequired("space-id")
	return cmd
}

func lexicalScopeFromFlags(cmd *cobra.Command, conn grpc.ClientConnInterface, authCtx context.Context) (string, string, error) {
	spaceIDText, _ := cmd.Flags().GetString("space-id")
	domainRef, _ := cmd.Flags().GetString("domain")
	spaceID, err := app.ParseUUID[domainspace.SpaceID](spaceIDText)
	if err != nil {
		return "", "", fmt.Errorf("--space-id must be a UUID: %w", err)
	}
	domainID, err := daemonResolveDomainID(cmd.Context(), conn, authCtx, spaceID.String(), domainRef)
	if err != nil {
		return "", "", err
	}
	return spaceID.String(), domainID, nil
}
