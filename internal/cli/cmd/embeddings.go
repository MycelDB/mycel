package cmd

import (
	"fmt"
	"os"
	"strings"

	domainembedding "github.com/myceldb/mycel/domain/embedding"
	"github.com/myceldb/mycel/domain/graph"
	mycelengine "github.com/myceldb/mycel/engine"
	"github.com/myceldb/mycel/internal/cli/app"
	domainsession "github.com/myceldb/mycel/session"
	"github.com/spf13/cobra"
)

func NewEmbeddingsCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "embeddings", Short: "Manage and use MycelDB embeddings"}
	cmd.AddCommand(newEmbeddingsCatalogCommand(a), newEmbeddingKeysCommand(a), newEmbeddingProfilesCommand(a), newEmbeddingGenerateCommand(a), newEmbeddingSearchCommand(a))
	return cmd
}

func newEmbeddingsCatalogCommand(a *app.App) *cobra.Command {
	return &cobra.Command{Use: "catalog", Short: "List built-in embedding providers and models", RunE: func(cmd *cobra.Command, args []string) error {
		tok, err := a.AccessToken(cmd.Context())
		if err != nil {
			return err
		}
		cat, err := a.Engine.EmbeddingCatalog(cmd.Context(), mycelengine.EmbeddingCatalogInput{AccessToken: tok})
		if err != nil {
			return err
		}
		var b strings.Builder
		for _, p := range cat.Providers {
			fmt.Fprintf(&b, "%s\t%s\t%s\n", p.ID, p.DisplayName, p.Protocol)
			for _, m := range p.Models {
				fmt.Fprintf(&b, "  %s\t%s\tdim=%d\n", m.ID, m.Model, m.Dimensions)
			}
		}
		return a.Print(cat, b.String())
	}}
}

func newEmbeddingKeysCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "keys", Short: "Manage legacy embedding provider API keys", Deprecated: "use inference credential commands for semantic indexes"}
	cmd.AddCommand(newEmbeddingKeysListCommand(a), newEmbeddingKeysAddCommand(a), newEmbeddingKeysDeleteCommand(a))
	return cmd
}

func newEmbeddingKeysListCommand(a *app.App) *cobra.Command {
	return &cobra.Command{Use: "list", Short: "List embedding provider keys", RunE: func(cmd *cobra.Command, args []string) error {
		tok, err := a.AccessToken(cmd.Context())
		if err != nil {
			return err
		}
		keys, err := a.Engine.ListEmbeddingKeys(cmd.Context(), mycelengine.ListEmbeddingKeysInput{AccessToken: tok})
		if err != nil {
			return err
		}
		var b strings.Builder
		for _, k := range keys {
			fmt.Fprintf(&b, "%s\t%s\t%s\tdefault=%v\tdisabled=%v\thas_key=%v\n", k.ID, k.ProviderID, k.Name, k.IsDefault, k.Disabled, k.HasAPIKey)
		}
		return a.Print(keys, b.String())
	}}
}

func newEmbeddingKeysAddCommand(a *app.App) *cobra.Command {
	var providerID, name, apiKey, apiKeyEnv string
	var isDefault, disabled bool
	cmd := &cobra.Command{Use: "add", Short: "Add an embedding provider API key", RunE: func(cmd *cobra.Command, args []string) error {
		if apiKey == "" && apiKeyEnv != "" {
			apiKey = os.Getenv(apiKeyEnv)
		}
		tok, err := a.AccessToken(cmd.Context())
		if err != nil {
			return err
		}
		key, err := a.Engine.AddEmbeddingKey(cmd.Context(), mycelengine.AddEmbeddingKeyInput{AccessToken: tok, ProviderID: providerID, Name: name, APIKey: apiKey, IsDefault: isDefault, Disabled: disabled})
		if err != nil {
			return err
		}
		return a.Print(key, fmt.Sprintf("embedding key added: %s\n", key.ID))
	}}
	cmd.Flags().StringVar(&providerID, "provider", "", "embedding provider ID")
	cmd.Flags().StringVar(&name, "name", "", "key display name")
	cmd.Flags().StringVar(&apiKey, "api-key", "", "API key value (prefer --api-key-env)")
	cmd.Flags().StringVar(&apiKeyEnv, "api-key-env", "", "environment variable containing the API key")
	cmd.Flags().BoolVar(&isDefault, "default", false, "make this the default key for the provider")
	cmd.Flags().BoolVar(&disabled, "disabled", false, "create the key disabled")
	_ = cmd.MarkFlagRequired("provider")
	_ = cmd.MarkFlagRequired("name")
	return cmd
}

func newEmbeddingKeysDeleteCommand(a *app.App) *cobra.Command {
	return &cobra.Command{Use: "delete KEY_ID", Short: "Delete an embedding provider key", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		id, err := app.ParseUUID[domainembedding.ProviderKeyID](args[0])
		if err != nil {
			return err
		}
		tok, err := a.AccessToken(cmd.Context())
		if err != nil {
			return err
		}
		if err := a.Engine.DeleteEmbeddingKey(cmd.Context(), mycelengine.DeleteEmbeddingKeyInput{AccessToken: tok, ID: id}); err != nil {
			return err
		}
		return a.Print(map[string]any{"deleted_key_id": id}, fmt.Sprintf("embedding key deleted: %s\n", id))
	}}
}

func newEmbeddingProfilesCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "profiles", Short: "Manage legacy embedding profiles", Deprecated: "use semantic indexes; migrate with `semantic migrate legacy-embeddings`"}
	cmd.AddCommand(newEmbeddingProfilesListCommand(a), newEmbeddingProfilesAddCommand(a), newEmbeddingProfilesDeleteCommand(a))
	return cmd
}

func newEmbeddingProfilesListCommand(a *app.App) *cobra.Command {
	return &cobra.Command{Use: "list", Short: "List embedding profiles", RunE: func(cmd *cobra.Command, args []string) error {
		tok, err := a.AccessToken(cmd.Context())
		if err != nil {
			return err
		}
		profiles, err := a.Engine.ListEmbeddingProfiles(cmd.Context(), mycelengine.ListEmbeddingProfilesInput{AccessToken: tok})
		if err != nil {
			return err
		}
		var b strings.Builder
		for _, p := range profiles {
			fmt.Fprintf(&b, "%s\t%s\t%s\t%s\t%s\n", p.ID, p.Name, p.ProviderID, p.ModelID, p.SourceMode)
		}
		return a.Print(profiles, b.String())
	}}
}

func newEmbeddingProfilesAddCommand(a *app.App) *cobra.Command {
	var name, providerID, modelID, source string
	var includeProps []string
	var maxDepth int
	var minimumTextLength int
	cmd := &cobra.Command{Use: "add", Short: "Add an embedding profile", RunE: func(cmd *cobra.Command, args []string) error {
		var maxDepthPtr *int
		if cmd.Flags().Changed("max-depth") {
			maxDepthPtr = &maxDepth
		}
		tok, err := a.AccessToken(cmd.Context())
		if err != nil {
			return err
		}
		p, err := a.Engine.AddEmbeddingProfile(cmd.Context(), mycelengine.AddEmbeddingProfileInput{AccessToken: tok, Name: name, ProviderID: providerID, ModelID: modelID, SourceMode: domainembedding.SourceMode(source), IncludeProps: includeProps, MaxDepth: maxDepthPtr, MinimumTextLength: minimumTextLength})
		if err != nil {
			return err
		}
		return a.Print(p, fmt.Sprintf("embedding profile added: %s\n", p.ID))
	}}
	cmd.Flags().StringVar(&name, "name", "", "profile name")
	cmd.Flags().StringVar(&providerID, "provider", "", "embedding provider ID")
	cmd.Flags().StringVar(&modelID, "model", "", "embedding model ID")
	cmd.Flags().StringVar(&source, "source", string(domainembedding.SourceModeSubtree), "source mode: self or subtree")
	cmd.Flags().StringSliceVar(&includeProps, "include-prop", nil, "node prop to include in source text (repeatable or comma-separated)")
	cmd.Flags().IntVar(&maxDepth, "max-depth", 0, "maximum subtree depth")
	cmd.Flags().IntVar(&minimumTextLength, "minimum-text-length", 0, "minimum source text length")
	_ = cmd.MarkFlagRequired("name")
	_ = cmd.MarkFlagRequired("provider")
	_ = cmd.MarkFlagRequired("model")
	return cmd
}

func newEmbeddingProfilesDeleteCommand(a *app.App) *cobra.Command {
	return &cobra.Command{Use: "delete PROFILE_ID", Short: "Delete an embedding profile", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		id, err := app.ParseUUID[domainembedding.ProfileID](args[0])
		if err != nil {
			return err
		}
		tok, err := a.AccessToken(cmd.Context())
		if err != nil {
			return err
		}
		if err := a.Engine.DeleteEmbeddingProfile(cmd.Context(), mycelengine.DeleteEmbeddingProfileInput{AccessToken: tok, ID: id}); err != nil {
			return err
		}
		return a.Print(map[string]any{"deleted_profile_id": id}, fmt.Sprintf("embedding profile deleted: %s\n", id))
	}}
}

func newEmbeddingGenerateCommand(a *app.App) *cobra.Command {
	var spaceIDText, domainKey, profileIDText, providerID, modelID, source, keyIDText, contains string
	var nodeIDTexts, includeProps, templateKeys []string
	var force, continueOnError bool
	var maxDepth, minimumTextLength, limit int
	cmd := &cobra.Command{Use: "generate", Short: "Generate legacy embedding-profile records for selected nodes", Deprecated: "use semantic index backfill or semantic maintenance process", RunE: func(cmd *cobra.Command, args []string) error {
		spaceID, err := a.ResolveSpaceID(spaceIDText)
		if err != nil {
			return err
		}
		var profileID *domainembedding.ProfileID
		if profileIDText != "" {
			id, err := app.ParseUUID[domainembedding.ProfileID](profileIDText)
			if err != nil {
				return err
			}
			profileID = &id
		}
		var keyID *domainembedding.ProviderKeyID
		if keyIDText != "" {
			id, err := app.ParseUUID[domainembedding.ProviderKeyID](keyIDText)
			if err != nil {
				return err
			}
			keyID = &id
		}
		var maxDepthPtr *int
		if cmd.Flags().Changed("max-depth") {
			maxDepthPtr = &maxDepth
		}
		nodeIDs := []graph.NodeID{}
		for _, text := range nodeIDTexts {
			id, err := app.ParseUUID[graph.NodeID](text)
			if err != nil {
				return err
			}
			nodeIDs = append(nodeIDs, id)
		}
		tok, err := a.AccessToken(cmd.Context())
		if err != nil {
			return err
		}
		sess, err := a.Engine.OpenSession(cmd.Context(), mycelengine.OpenSessionInput{AccessToken: tok, SpaceID: spaceID, DomainKey: domainKey})
		if err != nil {
			return err
		}
		defer sess.Close()
		result, err := sess.GenerateNodeEmbeddingBatch(cmd.Context(), domainsession.GenerateNodeEmbeddingBatchInput{NodeIDs: nodeIDs, TemplateKeys: templateKeys, Contains: contains, Limit: limit, ProfileID: profileID, ProviderID: providerID, ModelID: modelID, ProviderKeyID: keyID, SourceMode: domainembedding.SourceMode(source), IncludeProps: includeProps, MaxDepth: maxDepthPtr, MinimumTextLength: minimumTextLength, Force: force, ContinueOnError: continueOnError})
		if err != nil {
			return err
		}
		var b strings.Builder
		fmt.Fprintf(&b, "selected=%d generated=%d skipped=%d failed=%d\n", result.SelectedCount, result.GeneratedCount, result.SkippedCount, result.FailedCount)
		for _, r := range result.Records {
			fmt.Fprintf(&b, "%s\tnode=%s\tmodel=%s\tsource=%s\n", r.ID, r.NodeID, r.ModelID, r.SourceMode)
		}
		for _, failure := range result.Failures {
			fmt.Fprintf(&b, "failed\tnode=%s\terror=%s\n", failure.NodeID, failure.Error)
		}
		return a.Print(result, b.String())
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID")
	cmd.Flags().StringVar(&domainKey, "domain", "", "domain key (defaults to the space default domain)")
	cmd.Flags().StringSliceVar(&nodeIDTexts, "node", nil, "node ID to embed (repeatable or comma-separated)")
	cmd.Flags().StringSliceVar(&templateKeys, "template-key", nil, "template key selector for batch/backfill generation")
	cmd.Flags().StringVar(&contains, "contains", "", "case-insensitive content substring selector for batch/backfill generation")
	cmd.Flags().IntVar(&limit, "limit", 100, "maximum selected nodes for batch/backfill generation (0 for all selected)")
	cmd.Flags().StringVar(&profileIDText, "profile", "", "embedding profile ID")
	cmd.Flags().StringVar(&providerID, "provider", "", "embedding provider ID override")
	cmd.Flags().StringVar(&modelID, "model", "", "embedding model ID override")
	cmd.Flags().StringVar(&keyIDText, "key", "", "provider key ID override")
	cmd.Flags().StringVar(&source, "source", "", "source mode override: self or subtree")
	cmd.Flags().StringSliceVar(&includeProps, "include-prop", nil, "node prop to include in source text")
	cmd.Flags().IntVar(&maxDepth, "max-depth", 0, "maximum subtree depth")
	cmd.Flags().IntVar(&minimumTextLength, "minimum-text-length", 0, "minimum source text length")
	cmd.Flags().BoolVar(&force, "force", false, "regenerate even if source hash already exists")
	cmd.Flags().BoolVar(&continueOnError, "continue-on-error", false, "continue batch/backfill generation when one node fails")
	return cmd
}

func newEmbeddingSearchCommand(a *app.App) *cobra.Command {
	var spaceIDText, domainKey, profileIDText, providerID, modelID, keyIDText, text string
	var limit int
	var minScore float64
	cmd := &cobra.Command{Use: "search", Short: "Search legacy embedding-profile records", Deprecated: "use semantic search", RunE: func(cmd *cobra.Command, args []string) error {
		spaceID, err := a.ResolveSpaceID(spaceIDText)
		if err != nil {
			return err
		}
		var profileID *domainembedding.ProfileID
		if profileIDText != "" {
			id, err := app.ParseUUID[domainembedding.ProfileID](profileIDText)
			if err != nil {
				return err
			}
			profileID = &id
		}
		var keyID *domainembedding.ProviderKeyID
		if keyIDText != "" {
			id, err := app.ParseUUID[domainembedding.ProviderKeyID](keyIDText)
			if err != nil {
				return err
			}
			keyID = &id
		}
		tok, err := a.AccessToken(cmd.Context())
		if err != nil {
			return err
		}
		sess, err := a.Engine.OpenSession(cmd.Context(), mycelengine.OpenSessionInput{AccessToken: tok, SpaceID: spaceID, DomainKey: domainKey})
		if err != nil {
			return err
		}
		defer sess.Close()
		results, err := sess.SemanticSearch(cmd.Context(), domainsession.SemanticSearchInput{Text: text, ProfileID: profileID, ProviderID: providerID, ModelID: modelID, ProviderKeyID: keyID, Limit: limit, MinScore: minScore})
		if err != nil {
			return err
		}
		var b strings.Builder
		for _, r := range results {
			fmt.Fprintf(&b, "%.4f\t%s\t%s\n", r.Score, r.NodeID, r.ModelID)
		}
		return a.Print(results, b.String())
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID")
	cmd.Flags().StringVar(&domainKey, "domain", "", "domain key (defaults to the space default domain)")
	cmd.Flags().StringVar(&text, "text", "", "query text")
	cmd.Flags().StringVar(&profileIDText, "profile", "", "embedding profile ID")
	cmd.Flags().StringVar(&providerID, "provider", "", "embedding provider ID override")
	cmd.Flags().StringVar(&modelID, "model", "", "embedding model ID override")
	cmd.Flags().StringVar(&keyIDText, "key", "", "provider key ID override")
	cmd.Flags().IntVar(&limit, "limit", 10, "maximum number of results")
	cmd.Flags().Float64Var(&minScore, "min-score", 0, "minimum cosine score")
	_ = cmd.MarkFlagRequired("text")
	return cmd
}
