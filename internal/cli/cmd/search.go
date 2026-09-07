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
	cmd.AddCommand(lexical)
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
