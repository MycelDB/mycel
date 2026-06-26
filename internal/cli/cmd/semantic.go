package cmd

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/myceldb/mycel/domain/graph"
	domainsemantic "github.com/myceldb/mycel/domain/semantic"
	mycelengine "github.com/myceldb/mycel/engine"
	"github.com/myceldb/mycel/internal/cli/app"
	"github.com/myceldb/mycel/internal/semantic/backfill"
	"github.com/myceldb/mycel/internal/semantic/connectors"
	semanticsearch "github.com/myceldb/mycel/internal/semantic/search"
	"github.com/myceldb/mycel/internal/semantic/vectorstore"
	storeaccounting "github.com/myceldb/mycel/store/accounting"
	"github.com/spf13/cobra"
)

func NewSemanticCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "semantic", Short: "Manage semantic indexes and search"}
	index := &cobra.Command{Use: "index", Short: "Manage semantic indexes"}
	index.AddCommand(newSemanticIndexAddCommand(a), newSemanticIndexBackfillCommand(a))
	cmd.AddCommand(index, newSemanticSearchCommand(a))
	return cmd
}

func newSemanticSearchCommand(a *app.App) *cobra.Command {
	var spaceIDText, domainRef, text string
	var indexRefs []string
	var limit int
	var minScore float64
	cmd := &cobra.Command{Use: "search", Short: "Search semantic indexes", RunE: func(cmd *cobra.Command, args []string) error {
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

func newSemanticIndexBackfillCommand(a *app.App) *cobra.Command {
	var spaceIDText, domainRef string
	var nodeTexts []string
	var force, continueOnError bool
	var limit int
	cmd := &cobra.Command{Use: "backfill INDEX", Short: "Backfill a semantic index", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
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
