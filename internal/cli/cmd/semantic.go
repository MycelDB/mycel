package cmd

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/domain/graph"
	domainsemantic "github.com/myceldb/mycel/domain/semantic"
	domainspace "github.com/myceldb/mycel/domain/space"
	mycelengine "github.com/myceldb/mycel/engine"
	adminv1 "github.com/myceldb/mycel/gen/go/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel/gen/go/mycel/client/v1"
	"github.com/myceldb/mycel/internal/cli/app"
	"github.com/myceldb/mycel/internal/semantic/backfill"
	"github.com/myceldb/mycel/internal/semantic/connectors"
	"github.com/myceldb/mycel/internal/semantic/maintenance"
	semanticsearch "github.com/myceldb/mycel/internal/semantic/search"
	"github.com/myceldb/mycel/internal/semantic/vectorstore"
	storeaccounting "github.com/myceldb/mycel/store/accounting"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

func NewSemanticCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "semantic", Short: "Manage semantic indexes and search"}
	index := &cobra.Command{Use: "index", Short: "Manage semantic indexes"}
	index.AddCommand(newSemanticIndexListCommand(a), newSemanticIndexAddCommand(a), newSemanticIndexBackfillCommand(a), newSemanticIndexDeleteCommand(a))
	cmd.AddCommand(index, newSemanticSearchCommand(a), newSemanticMaintenanceCommand(a), newSemanticMigrateCommand(a))
	return cmd
}

func newSemanticSearchCommand(a *app.App) *cobra.Command {
	var spaceIDText, domainRef, text string
	var indexRefs []string
	var limit int
	var minScore float64
	cmd := &cobra.Command{Use: "search", Short: "Search semantic indexes", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(a.DaemonAddr) != "" {
			return runDaemonSemanticSearch(cmd, a, spaceIDText, domainRef, text, indexRefs, limit, minScore)
		}
		spaceID, err := a.ResolveSpaceID(spaceIDText)
		if err != nil {
			return err
		}
		tok, err := a.AccessToken(cmd.Context())
		if err != nil {
			return err
		}
		currentUser, err := a.Engine.CurrentUser(cmd.Context(), mycelengine.CurrentUserInput{AccessToken: tok})
		if err != nil {
			return err
		}
		domainID, err := resolveDomainID(cmd.Context(), a, tok, spaceID, domainRef)
		if err != nil {
			return err
		}
		globalMgr, err := authenticatedSemanticGlobalManager(cmd.Context(), a)
		if err != nil {
			return err
		}
		spaceMgr, err := authenticatedSemanticSpaceManager(cmd.Context(), a, spaceID)
		if err != nil {
			return err
		}
		indexIDs := []domainsemantic.SemanticIndexID{}
		for _, ref := range indexRefs {
			index, err := resolveSemanticIndexForDomain(cmd.Context(), spaceMgr, ref, domainID)
			if err != nil {
				return err
			}
			indexIDs = append(indexIDs, index.ID)
		}
		acct := storeaccounting.NewManager()
		if err := acct.Init(cmd.Context(), filepath.Join(a.DataDir, "meta", "accounting")); err != nil {
			return err
		}
		planner := semanticsearch.Planner{GlobalManager: globalMgr, SpaceManager: spaceMgr, Connector: connectors.Service{GlobalManager: globalMgr, Accounting: acct, SecretKeyB64: a.UserStoreEncryptionKeyB64, ActorPrincipalID: currentUser.ID}, VectorBackend: vectorstore.MycelFileBackend{GraphsDir: filepath.Join(a.DataDir, "graphs")}}
		result, err := planner.Search(cmd.Context(), semanticsearch.Input{SpaceID: spaceID, DomainID: domainID, SemanticIndexIDs: indexIDs, Text: text, Limit: limit, MinScore: minScore, ActorPrincipalID: currentUser.ID})
		if err != nil {
			return err
		}
		var b strings.Builder
		for _, warning := range result.Warnings {
			fmt.Fprintf(&b, "warning\t%s\n", warning)
		}
		for _, r := range result.Results {
			fmt.Fprintf(&b, "%.4f\tnode=%s\tindex=%s\tmodel=%s\n", r.Score, r.NodeID, r.SemanticIndexID, r.ModelID)
		}
		return a.Print(result, b.String())
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID")
	cmd.Flags().StringVar(&domainRef, "domain", "", "domain key or ID")
	cmd.Flags().StringVar(&text, "text", "", "query text")
	cmd.Flags().StringSliceVar(&indexRefs, "index", nil, "semantic index key or ID to search")
	cmd.Flags().IntVar(&limit, "limit", 10, "maximum merged results")
	cmd.Flags().Float64Var(&minScore, "min-score", 0, "minimum cosine score")
	_ = cmd.MarkFlagRequired("text")
	return cmd
}

func newSemanticIndexListCommand(a *app.App) *cobra.Command {
	var spaceIDText, domainRef, pageToken string
	var pageSize int32
	cmd := &cobra.Command{Use: "list", Short: "List semantic indexes via daemon gRPC", RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonUser(cmd.Context(), a)
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
		res, err := clientv1.NewSemanticServiceClient(conn).ListSemanticIndexes(authCtx, &clientv1.ListSemanticIndexesRequest{SpaceId: spaceID.String(), DomainId: domainID, PageSize: pageSize, PageToken: pageToken})
		if err != nil {
			return err
		}
		if a.Output == "json" {
			return a.Print(res, "")
		}
		for _, index := range res.GetIndexes() {
			fmt.Printf("%s\t%s\t%s\t%s\n", index.GetSemanticIndexId(), index.GetKey(), index.GetState().String(), index.GetModelLabel())
		}
		if res.GetNextPageToken() != "" {
			fmt.Printf("next page token: %s\n", res.GetNextPageToken())
		}
		return nil
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID")
	cmd.Flags().StringVar(&domainRef, "domain", graph.DefaultDomainKey, "domain key or ID")
	cmd.Flags().Int32Var(&pageSize, "page-size", 100, "page size")
	cmd.Flags().StringVar(&pageToken, "page-token", "", "page token")
	_ = cmd.MarkFlagRequired("space-id")
	return cmd
}

func runDaemonSemanticSearch(cmd *cobra.Command, a *app.App, spaceIDText string, domainRef string, text string, indexRefs []string, limit int, minScore float64) error {
	conn, authCtx, _, err := loginDaemonUser(cmd.Context(), a)
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
	var indexID *string
	if len(indexRefs) > 1 {
		return fmt.Errorf("daemon SemanticService supports at most one --index in v1")
	}
	if len(indexRefs) == 1 && strings.TrimSpace(indexRefs[0]) != "" {
		resolved, err := daemonResolveSemanticIndexID(cmd.Context(), semanticClient, authCtx, spaceID.String(), domainID, indexRefs[0])
		if err != nil {
			return err
		}
		indexID = &resolved
	}
	req := &clientv1.SemanticSearchRequest{SpaceId: spaceID.String(), DomainId: domainID, Query: text, Limit: int32(limit)}
	if indexID != nil {
		req.SemanticIndexId = indexID
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
		fmt.Printf("%.4f\tnode=%s\t%s\n", result.GetScore(), result.GetNodeId(), result.GetSnippet())
	}
	return nil
}

func daemonResolveDomainID(ctx context.Context, conn grpc.ClientConnInterface, authCtx context.Context, spaceID string, domainRef string) (string, error) {
	client := clientv1.NewDomainServiceClient(conn)
	ref := strings.TrimSpace(domainRef)
	if ref == "" {
		ref = graph.DefaultDomainKey
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

func daemonResolveSemanticIndexID(ctx context.Context, semanticClient clientv1.SemanticServiceClient, authCtx context.Context, spaceID string, domainID string, raw string) (string, error) {
	ref := strings.TrimSpace(raw)
	if _, err := uuid.Parse(ref); err == nil {
		return ref, nil
	}
	res, err := semanticClient.ListSemanticIndexes(authCtx, &clientv1.ListSemanticIndexesRequest{SpaceId: spaceID, DomainId: domainID, PageSize: 500})
	if err != nil {
		return "", err
	}
	key := normalizeCLIKey(ref)
	for _, index := range res.GetIndexes() {
		if normalizeCLIKey(index.GetKey()) == key {
			return index.GetSemanticIndexId(), nil
		}
	}
	return "", fmt.Errorf("semantic index %q not found", raw)
}

func runDaemonSemanticIndexAdd(cmd *cobra.Command, a *app.App, key string, spaceIDText string, domainRef string, purpose string, source string, endpointRef string, modelRef string, vectorStoreRef string, name string, templateKeys []string, includeProps []string, enabled bool) error {
	conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
	if err != nil {
		return err
	}
	defer conn.Close()
	spaceID, err := app.ParseUUID[domainspace.SpaceID](spaceIDText)
	if err != nil {
		return fmt.Errorf("--space-id must be a UUID: %w", err)
	}
	domainID, err := daemonResolveAdminDomainID(cmd.Context(), conn, authCtx, spaceID.String(), domainRef)
	if err != nil {
		return err
	}
	inferenceClient := adminv1.NewAdminInferenceServiceClient(conn)
	endpointID, err := daemonResolveAdminModelEndpointID(cmd.Context(), inferenceClient, authCtx, endpointRef)
	if err != nil {
		return err
	}
	modelID, err := daemonResolveAdminModelID(cmd.Context(), inferenceClient, authCtx, modelRef)
	if err != nil {
		return err
	}
	vectorStoreID, err := daemonResolveAdminVectorStoreID(cmd.Context(), inferenceClient, authCtx, vectorStoreRef)
	if err != nil {
		return err
	}
	res, err := adminv1.NewAdminSemanticServiceClient(conn).UpsertSemanticIndex(authCtx, &adminv1.UpsertSemanticIndexRequest{SpaceId: spaceID.String(), DomainId: domainID, Key: key, DisplayName: firstNonEmpty(name, key), Purpose: firstNonEmpty(purpose, string(domainsemantic.SemanticIndexPurposeSearch)), SourcePolicy: &adminv1.SemanticSourcePolicy{Extraction: firstNonEmpty(source, string(domainsemantic.SourceExtractionSubtree)), TemplateKeys: templateKeys, IncludeProps: includeProps}, ModelEndpointId: endpointID, ModelId: modelID, VectorStoreId: vectorStoreID, Enabled: enabled})
	if err != nil {
		return err
	}
	return a.Print(res.GetIndex(), fmt.Sprintf("semantic index added: %s\n", res.GetIndex().GetSemanticIndexId()))
}

func daemonResolveAdminModelEndpointID(ctx context.Context, client adminv1.AdminInferenceServiceClient, authCtx context.Context, raw string) (string, error) {
	ref := strings.TrimSpace(raw)
	if id, err := uuid.Parse(ref); err == nil && id != uuid.Nil {
		return id.String(), nil
	}
	res, err := client.ListModelEndpoints(authCtx, &adminv1.AdminInferenceServiceListModelEndpointsRequest{PageSize: 500})
	if err != nil {
		return "", err
	}
	key := normalizeCLIKey(ref)
	for _, endpoint := range res.GetModelEndpoints() {
		if normalizeCLIKey(endpoint.GetKey()) == key {
			return endpoint.GetModelEndpointId(), nil
		}
	}
	return "", fmt.Errorf("model endpoint %q not found", raw)
}

func daemonResolveAdminModelID(ctx context.Context, client adminv1.AdminInferenceServiceClient, authCtx context.Context, raw string) (string, error) {
	ref := strings.TrimSpace(raw)
	if id, err := uuid.Parse(ref); err == nil && id != uuid.Nil {
		return id.String(), nil
	}
	res, err := client.ListModels(authCtx, &adminv1.AdminInferenceServiceListModelsRequest{PageSize: 500})
	if err != nil {
		return "", err
	}
	key := normalizeCLIKey(ref)
	for _, model := range res.GetModels() {
		if normalizeCLIKey(model.GetKey()) == key {
			return model.GetModelId(), nil
		}
	}
	return "", fmt.Errorf("model %q not found", raw)
}

func daemonResolveAdminVectorStoreID(ctx context.Context, client adminv1.AdminInferenceServiceClient, authCtx context.Context, raw string) (string, error) {
	ref := strings.TrimSpace(raw)
	if id, err := uuid.Parse(ref); err == nil && id != uuid.Nil {
		return id.String(), nil
	}
	res, err := client.ListVectorStores(authCtx, &adminv1.AdminInferenceServiceListVectorStoresRequest{PageSize: 500})
	if err != nil {
		return "", err
	}
	key := normalizeCLIKey(ref)
	for _, store := range res.GetVectorStores() {
		if normalizeCLIKey(store.GetKey()) == key {
			return store.GetVectorStoreId(), nil
		}
	}
	return "", fmt.Errorf("vector store %q not found", raw)
}

func newSemanticMaintenanceCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "maintenance", Short: "Run semantic maintenance primitives"}
	cmd.AddCommand(newSemanticMaintenanceAnalyzeCommand(a), newSemanticMaintenanceProcessCommand(a))
	return cmd
}

func newSemanticMaintenanceAnalyzeCommand(a *app.App) *cobra.Command {
	var spaceIDText, indexRef string
	var limit int
	cmd := &cobra.Command{Use: "analyze", Short: "Analyze graph dirty events into semantic dirty work", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(a.DaemonAddr) != "" {
			return runDaemonSemanticMaintenanceAnalyze(cmd, a, spaceIDText, indexRef, limit)
		}
		spaceID, err := a.ResolveSpaceID(spaceIDText)
		if err != nil {
			return err
		}
		spaceMgr, err := authenticatedSemanticSpaceManager(cmd.Context(), a, spaceID)
		if err != nil {
			return err
		}
		var indexID domainsemantic.SemanticIndexID
		if strings.TrimSpace(indexRef) != "" {
			indexes, err := spaceMgr.ListSemanticIndexes(cmd.Context())
			if err != nil {
				return err
			}
			for _, index := range indexes {
				if index.ID.String() == indexRef || normalizeCLIKey(index.Key) == normalizeCLIKey(indexRef) {
					indexID = index.ID
					break
				}
			}
			if indexID == uuid.Nil {
				return fmt.Errorf("semantic index %q not found", indexRef)
			}
		}
		result, err := (maintenance.Analyzer{SpaceManager: spaceMgr}).AnalyzeOnce(cmd.Context(), maintenance.AnalyzeInput{SemanticIndexID: indexID, Limit: limit})
		if err != nil {
			return err
		}
		return a.Print(result, fmt.Sprintf("processed_events=%d enqueued_items=%d\n", result.ProcessedEvents, result.EnqueuedItems))
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID")
	cmd.Flags().StringVar(&indexRef, "index", "", "optional semantic index key or ID")
	cmd.Flags().IntVar(&limit, "limit", 0, "maximum events to process")
	return cmd
}

func newSemanticMaintenanceProcessCommand(a *app.App) *cobra.Command {
	var spaceIDText string
	var limit int
	cmd := &cobra.Command{Use: "process", Short: "Process pending semantic dirty work", RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(a.DaemonAddr) != "" {
			return runDaemonSemanticMaintenanceProcess(cmd, a, spaceIDText, limit)
		}
		spaceID, err := a.ResolveSpaceID(spaceIDText)
		if err != nil {
			return err
		}
		tok, err := a.AccessToken(cmd.Context())
		if err != nil {
			return err
		}
		spaceMgr, err := authenticatedSemanticSpaceManager(cmd.Context(), a, spaceID)
		if err != nil {
			return err
		}
		globalMgr, err := authenticatedSemanticGlobalManager(cmd.Context(), a)
		if err != nil {
			return err
		}
		sess, err := a.Engine.OpenSession(cmd.Context(), mycelengine.OpenSessionInput{AccessToken: tok, SpaceID: spaceID})
		if err != nil {
			return err
		}
		defer sess.Close()
		acct := storeaccounting.NewManager()
		if err := acct.Init(cmd.Context(), filepath.Join(a.DataDir, "meta", "accounting")); err != nil {
			return err
		}
		runner := backfill.Runner{Session: sess, GlobalManager: globalMgr, SpaceManager: spaceMgr, Connector: connectors.Service{GlobalManager: globalMgr, Accounting: acct, SecretKeyB64: a.UserStoreEncryptionKeyB64}, VectorBackend: vectorstore.MycelFileBackend{GraphsDir: filepath.Join(a.DataDir, "graphs")}}
		result, err := (maintenance.Worker{SpaceManager: spaceMgr, Backfill: runner}).ProcessOnce(cmd.Context(), limit)
		if err != nil {
			return err
		}
		return a.Print(result, fmt.Sprintf("processed=%d completed=%d failed=%d\n", result.Processed, result.Completed, result.Failed))
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID")
	cmd.Flags().IntVar(&limit, "limit", 1, "maximum work items to process")
	return cmd
}

func newSemanticIndexDeleteCommand(a *app.App) *cobra.Command {
	var spaceIDText, domainRef string
	var purgeVectors, purgeReferences bool
	cmd := &cobra.Command{Use: "delete INDEX", Aliases: []string{"rm", "remove"}, Short: "Hard-delete a semantic index via daemon Admin API", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		spaceID, err := a.ResolveSpaceID(spaceIDText)
		if err != nil {
			return err
		}
		indexID := strings.TrimSpace(args[0])
		if _, err := uuid.Parse(indexID); err != nil {
			domainID, err := daemonResolveAdminDomainID(cmd.Context(), conn, authCtx, spaceID.String(), domainRef)
			if err != nil {
				return err
			}
			listed, err := adminv1.NewAdminSemanticServiceClient(conn).ListSemanticIndexes(authCtx, &adminv1.AdminSemanticServiceListSemanticIndexesRequest{SpaceId: spaceID.String(), DomainId: domainID, PageSize: 500, IncludeDisabled: true})
			if err != nil {
				return err
			}
			for _, index := range listed.GetIndexes() {
				if normalizeCLIKey(index.GetKey()) == normalizeCLIKey(args[0]) {
					indexID = index.GetSemanticIndexId()
					break
				}
			}
			if _, err := uuid.Parse(indexID); err != nil {
				return fmt.Errorf("semantic index %q not found", args[0])
			}
		}
		res, err := adminv1.NewAdminSemanticServiceClient(conn).DeleteSemanticIndex(authCtx, &adminv1.DeleteSemanticIndexRequest{SpaceId: spaceID.String(), SemanticIndexId: indexID, PurgeVectors: purgeVectors, PurgeReferences: purgeReferences})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("semantic index deleted: %s (grants=%d policies=%d decisions=%d vectors_purged=%t)\n", res.GetSemanticIndexId(), res.GetCredentialGrantsDeleted(), res.GetInferencePoliciesDeleted(), res.GetPolicyDecisionsDeleted(), res.GetVectorsPurged()))
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID")
	cmd.Flags().StringVar(&domainRef, "domain", graph.DefaultDomainKey, "domain key or ID for resolving INDEX keys")
	cmd.Flags().BoolVar(&purgeVectors, "purge-vectors", false, "delete local vector records for the index")
	cmd.Flags().BoolVar(&purgeReferences, "purge-references", false, "delete credential grants and policies scoped to the index")
	_ = cmd.MarkFlagRequired("space-id")
	return cmd
}

func newSemanticIndexBackfillCommand(a *app.App) *cobra.Command {
	var spaceIDText, domainRef string
	var nodeTexts []string
	var force, continueOnError bool
	var limit int
	cmd := &cobra.Command{Use: "backfill INDEX", Short: "Backfill a semantic index", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(a.DaemonAddr) != "" {
			return runDaemonSemanticIndexBackfill(cmd, a, args[0], spaceIDText, domainRef, nodeTexts, force, limit, continueOnError)
		}
		spaceID, err := a.ResolveSpaceID(spaceIDText)
		if err != nil {
			return err
		}
		tok, err := a.AccessToken(cmd.Context())
		if err != nil {
			return err
		}
		domainID, err := resolveDomainID(cmd.Context(), a, tok, spaceID, domainRef)
		if err != nil {
			return err
		}
		globalMgr, err := authenticatedSemanticGlobalManager(cmd.Context(), a)
		if err != nil {
			return err
		}
		spaceMgr, err := authenticatedSemanticSpaceManager(cmd.Context(), a, spaceID)
		if err != nil {
			return err
		}
		selectedIndex, err := resolveSemanticIndexForDomain(cmd.Context(), spaceMgr, args[0], domainID)
		if err != nil {
			return err
		}
		indexID := selectedIndex.ID
		stores, err := globalMgr.ListVectorStores(cmd.Context())
		if err != nil {
			return err
		}
		if !isMycelFileVectorStore(stores, selectedIndex.VectorStoreID) {
			return fmt.Errorf("semantic index %s uses unsupported vector store for phase 5; only mycel-file is implemented", selectedIndex.ID)
		}
		nodeIDs := []graph.NodeID{}
		for _, raw := range nodeTexts {
			id, err := app.ParseUUID[graph.NodeID](raw)
			if err != nil {
				return err
			}
			nodeIDs = append(nodeIDs, id)
		}
		sess, err := a.Engine.OpenSession(cmd.Context(), mycelengine.OpenSessionInput{AccessToken: tok, SpaceID: spaceID, DomainID: &domainID})
		if err != nil {
			return err
		}
		defer sess.Close()
		acct := storeaccounting.NewManager()
		if err := acct.Init(cmd.Context(), filepath.Join(a.DataDir, "meta", "accounting")); err != nil {
			return err
		}
		runner := backfill.Runner{Session: sess, GlobalManager: globalMgr, SpaceManager: spaceMgr, Connector: connectors.Service{GlobalManager: globalMgr, Accounting: acct, SecretKeyB64: a.UserStoreEncryptionKeyB64}, VectorBackend: vectorstore.MycelFileBackend{GraphsDir: filepath.Join(a.DataDir, "graphs")}}
		result, err := runner.Run(cmd.Context(), backfill.Input{SpaceID: spaceID, SemanticIndexID: indexID, NodeIDs: nodeIDs, Force: force, Limit: limit, ContinueOnError: continueOnError})
		if err != nil {
			return err
		}
		var b strings.Builder
		fmt.Fprintf(&b, "selected=%d generated=%d skipped=%d failed=%d\n", result.SelectedCount, result.GeneratedCount, result.SkippedCount, result.FailedCount)
		for _, rec := range result.Records {
			fmt.Fprintf(&b, "%s\tnode=%s\tsource_hash=%s\n", rec.ID, rec.NodeID, rec.SourceHash)
		}
		for _, skipped := range result.Skipped {
			fmt.Fprintf(&b, "skipped\tnode=%s\treason=%s\n", skipped.NodeID, skipped.Reason)
		}
		for _, failure := range result.Failures {
			fmt.Fprintf(&b, "failed\tnode=%s\terror=%s\n", failure.NodeID, failure.Error)
		}
		return a.Print(result, b.String())
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID")
	cmd.Flags().StringVar(&domainRef, "domain", "", "domain key or ID")
	cmd.Flags().StringSliceVar(&nodeTexts, "node", nil, "explicit root node ID to backfill")
	cmd.Flags().IntVar(&limit, "limit", 100, "maximum selected roots to backfill (0 for all selected)")
	cmd.Flags().BoolVar(&force, "force", false, "regenerate even if source hash is current")
	cmd.Flags().BoolVar(&continueOnError, "continue-on-error", false, "continue after per-node failures")
	return cmd
}

func runDaemonSemanticMaintenanceAnalyze(cmd *cobra.Command, a *app.App, spaceIDText, indexRef string, limit int) error {
	conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
	if err != nil {
		return err
	}
	defer conn.Close()
	spaceID, err := a.ResolveSpaceID(spaceIDText)
	if err != nil {
		return err
	}
	indexID := ""
	if strings.TrimSpace(indexRef) != "" {
		if _, err := uuid.Parse(strings.TrimSpace(indexRef)); err != nil {
			return fmt.Errorf("daemon semantic maintenance analyze requires --index to be a semantic index UUID")
		}
		indexID = strings.TrimSpace(indexRef)
	}
	res, err := adminv1.NewAdminSemanticMaintenanceServiceClient(conn).AnalyzeSemanticDirtyWork(authCtx, &adminv1.AnalyzeSemanticDirtyWorkRequest{SpaceId: spaceID.String(), SemanticIndexId: indexID, Limit: int32(limit)})
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

func runDaemonSemanticIndexBackfill(cmd *cobra.Command, a *app.App, indexRef, spaceIDText, domainRef string, nodeTexts []string, force bool, limit int, continueOnError bool) error {
	conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
	if err != nil {
		return err
	}
	defer conn.Close()
	spaceID, err := a.ResolveSpaceID(spaceIDText)
	if err != nil {
		return err
	}
	if _, err := uuid.Parse(strings.TrimSpace(indexRef)); err != nil {
		domainID, err := daemonResolveAdminDomainID(cmd.Context(), conn, authCtx, spaceID.String(), domainRef)
		if err != nil {
			return err
		}
		listed, err := adminv1.NewAdminSemanticServiceClient(conn).ListSemanticIndexes(authCtx, &adminv1.AdminSemanticServiceListSemanticIndexesRequest{SpaceId: spaceID.String(), DomainId: domainID, PageSize: 500, IncludeDisabled: true})
		if err != nil {
			return err
		}
		for _, index := range listed.GetIndexes() {
			if normalizeCLIKey(index.GetKey()) == normalizeCLIKey(indexRef) {
				indexRef = index.GetSemanticIndexId()
				break
			}
		}
		if _, err := uuid.Parse(strings.TrimSpace(indexRef)); err != nil {
			return fmt.Errorf("semantic index %q not found", indexRef)
		}
	}
	nodeIDs := append([]string(nil), nodeTexts...)
	res, err := adminv1.NewAdminSemanticMaintenanceServiceClient(conn).BackfillSemanticIndex(authCtx, &adminv1.BackfillSemanticIndexRequest{SpaceId: spaceID.String(), SemanticIndexId: strings.TrimSpace(indexRef), NodeIds: nodeIDs, Force: force, Limit: int32(limit), ContinueOnError: continueOnError})
	if err != nil {
		return err
	}
	var b strings.Builder
	fmt.Fprintf(&b, "selected=%d generated=%d skipped=%d failed=%d\n", res.GetSelectedCount(), res.GetGeneratedCount(), res.GetSkippedCount(), res.GetFailedCount())
	for _, skipped := range res.GetSkipped() {
		fmt.Fprintf(&b, "skipped\tnode=%s\treason=%s\n", skipped.GetNodeId(), skipped.GetReason())
	}
	for _, failure := range res.GetFailures() {
		fmt.Fprintf(&b, "failed\tnode=%s\terror=%s\n", failure.GetNodeId(), failure.GetError())
	}
	return a.Print(res, b.String())
}

func resolveSemanticIndexForDomain(ctx context.Context, mgr interface {
	ListSemanticIndexes(context.Context) ([]domainsemantic.SemanticIndex, error)
}, raw string, domainID graph.DomainID) (domainsemantic.SemanticIndex, error) {
	indexes, err := mgr.ListSemanticIndexes(ctx)
	if err != nil {
		return domainsemantic.SemanticIndex{}, err
	}
	if id, err := app.ParseUUID[domainsemantic.SemanticIndexID](raw); err == nil {
		for _, index := range indexes {
			if index.ID == id {
				if index.DomainID != domainID {
					return domainsemantic.SemanticIndex{}, fmt.Errorf("semantic index %s belongs to a different domain", index.ID)
				}
				return index, nil
			}
		}
		return domainsemantic.SemanticIndex{}, fmt.Errorf("semantic index %q not found", raw)
	}
	key := normalizeCLIKey(raw)
	for _, index := range indexes {
		if index.DomainID == domainID && normalizeCLIKey(index.Key) == key {
			return index, nil
		}
	}
	return domainsemantic.SemanticIndex{}, fmt.Errorf("semantic index %q not found in domain %s", raw, domainID)
}

func isMycelFileVectorStore(stores []domainsemantic.VectorStoreBackend, id domainsemantic.VectorStoreID) bool {
	for _, store := range stores {
		if store.ID == id && store.Enabled && store.Type == domainsemantic.VectorStoreMycelFile {
			return true
		}
	}
	return false
}

func newSemanticIndexAddCommand(a *app.App) *cobra.Command {
	var spaceIDText, domainRef, purpose, source, endpointRef, modelRef, vectorStoreRef, name string
	var templateKeys, includeProps []string
	var enabled bool
	cmd := &cobra.Command{Use: "add KEY", Short: "Add or update a semantic index", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(a.DaemonAddr) != "" {
			return runDaemonSemanticIndexAdd(cmd, a, args[0], spaceIDText, domainRef, purpose, source, endpointRef, modelRef, vectorStoreRef, name, templateKeys, includeProps, enabled)
		}
		spaceID, err := a.ResolveSpaceID(spaceIDText)
		if err != nil {
			return err
		}
		tok, err := a.AccessToken(cmd.Context())
		if err != nil {
			return err
		}
		domainID, err := resolveDomainID(cmd.Context(), a, tok, spaceID, domainRef)
		if err != nil {
			return err
		}
		globalMgr, err := authenticatedSemanticGlobalManager(cmd.Context(), a)
		if err != nil {
			return err
		}
		endpointID, err := resolveModelEndpointID(cmd.Context(), globalMgr, endpointRef)
		if err != nil {
			return err
		}
		modelID, err := resolveModelID(cmd.Context(), globalMgr, modelRef)
		if err != nil {
			return err
		}
		vectorStoreID, err := resolveVectorStoreID(cmd.Context(), globalMgr, vectorStoreRef)
		if err != nil {
			return err
		}
		capabilityID, err := capabilityFor(cmd.Context(), globalMgr, endpointID, modelID, domainsemantic.OperationEmbeddings)
		if err != nil {
			return err
		}
		spaceMgr, err := authenticatedSemanticSpaceManager(cmd.Context(), a, spaceID)
		if err != nil {
			return err
		}
		index, err := spaceMgr.UpsertSemanticIndex(cmd.Context(), domainsemantic.SemanticIndex{
			SpaceID:                   spaceID,
			DomainID:                  domainID,
			Key:                       args[0],
			Name:                      firstNonEmpty(name, args[0]),
			Purpose:                   domainsemantic.SemanticIndexPurpose(purpose),
			SourcePolicy:              domainsemantic.SemanticSourcePolicy{TemplateKeys: templateKeys, Extraction: domainsemantic.SourceExtraction(source), IncludeProps: includeProps},
			ModelEndpointID:           endpointID,
			ModelID:                   modelID,
			ModelEndpointCapabilityID: capabilityID,
			VectorStoreID:             vectorStoreID,
			Enabled:                   enabled,
		})
		if err != nil {
			return err
		}
		if err := appendSemanticConfigEvent(a.DataDir, "semantic_index_changed", &spaceID, map[string]any{"semantic_index_id": index.ID.String()}); err != nil {
			return err
		}
		return a.Print(index, fmt.Sprintf("semantic index added: %s\n", index.ID))
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID")
	cmd.Flags().StringVar(&domainRef, "domain", "", "domain key or ID")
	cmd.Flags().StringVar(&purpose, "purpose", string(domainsemantic.SemanticIndexPurposeSearch), "semantic index purpose")
	cmd.Flags().StringArrayVar(&templateKeys, "template-key", nil, "template key selected by the source policy")
	cmd.Flags().StringVar(&source, "source", string(domainsemantic.SourceExtractionSubtree), "source extraction: self or subtree")
	cmd.Flags().StringArrayVar(&includeProps, "include-prop", nil, "node prop to include in source text")
	cmd.Flags().StringVar(&endpointRef, "model-endpoint", "", "model endpoint key or ID")
	cmd.Flags().StringVar(&modelRef, "model", "", "model key or ID")
	cmd.Flags().StringVar(&vectorStoreRef, "vector-store", "mycel-file", "vector store key or ID")
	cmd.Flags().StringVar(&name, "name", "", "semantic index display name")
	cmd.Flags().BoolVar(&enabled, "enabled", true, "enable index")
	_ = cmd.MarkFlagRequired("model-endpoint")
	_ = cmd.MarkFlagRequired("model")
	return cmd
}
