package cmd

import (
	"fmt"

	domainsemantic "github.com/myceldb/mycel/domain/semantic"
	"github.com/myceldb/mycel/internal/cli/app"
	"github.com/spf13/cobra"
)

func NewSemanticCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "semantic", Short: "Manage semantic indexes and search"}
	index := &cobra.Command{Use: "index", Short: "Manage semantic indexes"}
	index.AddCommand(newSemanticIndexAddCommand(a))
	cmd.AddCommand(index)
	return cmd
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
