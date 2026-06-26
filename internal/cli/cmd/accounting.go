package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/domain/graph"
	"github.com/myceldb/mycel/domain/identity"
	domainsemantic "github.com/myceldb/mycel/domain/semantic"
	domainspace "github.com/myceldb/mycel/domain/space"
	mycelengine "github.com/myceldb/mycel/engine"
	"github.com/myceldb/mycel/internal/cli/app"
	storeaccounting "github.com/myceldb/mycel/store/accounting"
	storesemantic "github.com/myceldb/mycel/store/semantic"
	"github.com/spf13/cobra"
)

type accountingUsageFlags struct {
	from, to, user, space, domain, node, semanticIndex, operation, modelEndpoint, model, credentialGrant, status string
	limit                                                                                                        int
}

func NewAccountingCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "accounting", Short: "Inspect inference accounting usage"}
	usage := &cobra.Command{Use: "usage", Short: "Inspect inference usage accounting"}
	usage.AddCommand(newAccountingUsageSummarizeCommand(a), newAccountingUsageEventsCommand(a), newAccountingUsageExportCommand(a), newAccountingUsageRebuildIndexesCommand(a), newAccountingUsageRebuildRollupsCommand(a))
	cmd.AddCommand(usage)
	return cmd
}

func newAccountingUsageSummarizeCommand(a *app.App) *cobra.Command {
	var flags accountingUsageFlags
	var groupBy string
	cmd := &cobra.Command{Use: "summarize", Short: "Summarize inference usage", RunE: func(cmd *cobra.Command, args []string) error {
		mgr, filter, err := accountingManagerAndFilter(cmd.Context(), a, flags)
		if err != nil {
			return err
		}
		groups := splitCSV(groupBy)
		rows, err := mgr.Summarize(cmd.Context(), filter, groups)
		if err != nil {
			return err
		}
		if a.Output == "json" {
			return a.Print(rows, "")
		}
		var b strings.Builder
		for _, row := range rows {
			fmt.Fprintf(&b, "group=%v calls=%d success=%d failed=%d input=%d output=%d total=%d provider_reported=%d estimated=%d unavailable=%d\n", row.Group, row.CallCount, row.SuccessCount, row.FailedCount, row.InputTokens, row.OutputTokens, row.TotalTokens, row.ProviderReportedTokens, row.EstimatedTokens, row.UnavailableTokenCount)
		}
		return a.Print(rows, b.String())
	}}
	addAccountingUsageFlags(cmd, &flags)
	cmd.Flags().StringVar(&groupBy, "group-by", "", "comma-separated grouping keys")
	return cmd
}

func newAccountingUsageEventsCommand(a *app.App) *cobra.Command {
	var flags accountingUsageFlags
	cmd := &cobra.Command{Use: "events", Short: "List inference usage events", RunE: func(cmd *cobra.Command, args []string) error {
		mgr, filter, err := accountingManagerAndFilter(cmd.Context(), a, flags)
		if err != nil {
			return err
		}
		events, err := mgr.List(cmd.Context(), filter)
		if err != nil {
			return err
		}
		if a.Output == "json" {
			return a.Print(events, "")
		}
		var b strings.Builder
		for _, event := range events {
			fmt.Fprintf(&b, "%s\t%s\t%s\t%s\ttokens=%d\tspace=%s\tnode=%s\n", event.ID, event.CreatedAt.Format(time.RFC3339), event.Status, event.Operation, event.TotalTokens, event.SpaceID, event.TargetNodeID)
		}
		return a.Print(events, b.String())
	}}
	addAccountingUsageFlags(cmd, &flags)
	cmd.Flags().IntVar(&flags.limit, "limit", 0, "maximum events to return")
	return cmd
}

func newAccountingUsageExportCommand(a *app.App) *cobra.Command {
	var flags accountingUsageFlags
	var format, output string
	cmd := &cobra.Command{Use: "export", Short: "Export inference usage events", RunE: func(cmd *cobra.Command, args []string) error {
		mgr, filter, err := accountingManagerAndFilter(cmd.Context(), a, flags)
		if err != nil {
			return err
		}
		events, err := mgr.List(cmd.Context(), filter)
		if err != nil {
			return err
		}
		format = strings.ToLower(format)
		if format != "" && format != "json" && format != "jsonl" && format != "csv" {
			return fmt.Errorf("unsupported export format %q", format)
		}
		var out *os.File
		if output == "" || output == "-" {
			out = os.Stdout
		} else {
			f, err := os.Create(output)
			if err != nil {
				return err
			}
			defer f.Close()
			out = f
		}
		switch format {
		case "csv":
			return storeaccounting.WriteCSV(out, events)
		case "jsonl":
			enc := json.NewEncoder(out)
			for _, event := range events {
				if err := enc.Encode(event); err != nil {
					return err
				}
			}
			return nil
		case "json", "":
			enc := json.NewEncoder(out)
			enc.SetIndent("", "  ")
			return enc.Encode(events)
		default:
			return fmt.Errorf("unsupported export format %q", format)
		}
	}}
	addAccountingUsageFlags(cmd, &flags)
	cmd.Flags().StringVar(&format, "format", "json", "export format: json, jsonl, csv")
	cmd.Flags().StringVar(&output, "output", "", "output path, or stdout when empty")
	return cmd
}

func newAccountingUsageRebuildIndexesCommand(a *app.App) *cobra.Command {
	return &cobra.Command{Use: "rebuild-indexes", Short: "Rebuild derived accounting indexes", RunE: func(cmd *cobra.Command, args []string) error {
		mgr, err := authorizedAccountingManager(cmd.Context(), a)
		if err != nil {
			return err
		}
		if err := mgr.RebuildIndexes(cmd.Context()); err != nil {
			return err
		}
		return a.Print(map[string]any{"rebuilt": "indexes"}, "accounting indexes rebuilt\n")
	}}
}

func newAccountingUsageRebuildRollupsCommand(a *app.App) *cobra.Command {
	return &cobra.Command{Use: "rebuild-rollups", Short: "Rebuild derived accounting rollups", RunE: func(cmd *cobra.Command, args []string) error {
		mgr, err := authorizedAccountingManager(cmd.Context(), a)
		if err != nil {
			return err
		}
		if err := mgr.RebuildRollups(cmd.Context()); err != nil {
			return err
		}
		return a.Print(map[string]any{"rebuilt": "rollups"}, "accounting rollups rebuilt\n")
	}}
}

func addAccountingUsageFlags(cmd *cobra.Command, flags *accountingUsageFlags) {
	cmd.Flags().StringVar(&flags.from, "from", "", "inclusive start date/time")
	cmd.Flags().StringVar(&flags.to, "to", "", "exclusive end date/time")
	cmd.Flags().StringVar(&flags.user, "user", "", "principal user ID or ref")
	cmd.Flags().StringVar(&flags.space, "space", "", "space ID or name")
	cmd.Flags().StringVar(&flags.domain, "domain", "", "domain ID or key (requires --space for key lookup)")
	cmd.Flags().StringVar(&flags.node, "node", "", "node ID")
	cmd.Flags().StringVar(&flags.semanticIndex, "semantic-index", "", "semantic index ID")
	cmd.Flags().StringVar(&flags.operation, "operation", "", "operation filter")
	cmd.Flags().StringVar(&flags.modelEndpoint, "model-endpoint", "", "model endpoint ID or key")
	cmd.Flags().StringVar(&flags.model, "model", "", "model ID or key")
	cmd.Flags().StringVar(&flags.credentialGrant, "credential-grant", "", "credential grant ID")
	cmd.Flags().StringVar(&flags.status, "status", "", "status filter")
}

func accountingManagerAndFilter(ctx context.Context, a *app.App, flags accountingUsageFlags) (storeaccounting.Manager, storeaccounting.Filter, error) {
	mgr, err := authorizedAccountingManager(ctx, a)
	if err != nil {
		return nil, storeaccounting.Filter{}, err
	}
	filter, err := accountingFilter(ctx, a, flags)
	if err != nil {
		return nil, storeaccounting.Filter{}, err
	}
	return mgr, filter, nil
}

func authorizedAccountingManager(ctx context.Context, a *app.App) (storeaccounting.Manager, error) {
	tok, err := a.AccessToken(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := a.Engine.ListSystemAccess(ctx, mycelengine.ListSystemAccessInput{AccessToken: tok}); err != nil {
		return nil, err
	}
	mgr := storeaccounting.NewManager()
	if err := mgr.Init(ctx, filepath.Join(a.DataDir, "meta", "accounting")); err != nil {
		return nil, err
	}
	return mgr, nil
}

func accountingFilter(ctx context.Context, a *app.App, flags accountingUsageFlags) (storeaccounting.Filter, error) {
	var filter storeaccounting.Filter
	if flags.from != "" {
		t, err := parseAccountingTime(flags.from)
		if err != nil {
			return filter, err
		}
		filter.From = &t
	}
	if flags.to != "" {
		t, err := parseAccountingTime(flags.to)
		if err != nil {
			return filter, err
		}
		filter.To = &t
	}
	if flags.user != "" {
		id, err := resolveAccountingUserID(a, flags.user)
		if err != nil {
			return filter, err
		}
		filter.PrincipalID = id
	}
	if flags.space != "" {
		id, err := resolveAccountingSpaceID(ctx, a, flags.space)
		if err != nil {
			return filter, err
		}
		filter.SpaceID = id
	}
	if flags.domain != "" {
		id, err := resolveAccountingDomainID(ctx, a, filter.SpaceID, flags.domain)
		if err != nil {
			return filter, err
		}
		filter.DomainID = id
	}
	if flags.node != "" {
		id, err := app.ParseUUID[graph.NodeID](flags.node)
		if err != nil {
			return filter, err
		}
		filter.NodeID = id
	}
	if flags.semanticIndex != "" {
		id, err := app.ParseUUID[domainsemantic.SemanticIndexID](flags.semanticIndex)
		if err != nil {
			return filter, err
		}
		filter.SemanticIndexID = id
	}
	if flags.modelEndpoint != "" {
		id, err := resolveAccountingModelEndpointID(ctx, a, flags.modelEndpoint)
		if err != nil {
			return filter, err
		}
		filter.ModelEndpointID = id
	}
	if flags.model != "" {
		id, err := resolveAccountingModelID(ctx, a, flags.model)
		if err != nil {
			return filter, err
		}
		filter.ModelID = id
	}
	if flags.credentialGrant != "" {
		id, err := app.ParseUUID[domainsemantic.CredentialGrantID](flags.credentialGrant)
		if err != nil {
			return filter, err
		}
		filter.CredentialGrantID = id
	}
	filter.Operation = strings.TrimSpace(flags.operation)
	filter.Status = strings.TrimSpace(flags.status)
	filter.Limit = flags.limit
	return filter, nil
}

func resolveAccountingUserID(a *app.App, raw string) (identity.UserID, error) {
	if id, err := uuid.Parse(strings.TrimSpace(raw)); err == nil {
		return identity.UserID(id), nil
	}
	tok, err := a.AccessToken(context.Background())
	if err != nil {
		return uuid.Nil, err
	}
	users, err := a.Engine.ListUsers(context.Background(), mycelengine.ListUsersInput{AccessToken: tok})
	if err != nil {
		return uuid.Nil, err
	}
	for _, user := range users {
		if string(user.Ref) == raw || (user.Username != nil && *user.Username == raw) {
			return user.ID, nil
		}
	}
	return uuid.Nil, fmt.Errorf("user %q not found", raw)
}

func resolveAccountingSpaceID(ctx context.Context, a *app.App, raw string) (domainspace.SpaceID, error) {
	if id, err := uuid.Parse(strings.TrimSpace(raw)); err == nil {
		return domainspace.SpaceID(id), nil
	}
	tok, err := a.AccessToken(ctx)
	if err != nil {
		return uuid.Nil, err
	}
	spaces, err := a.Engine.ListSpaces(ctx, mycelengine.ListSpacesInput{AccessToken: tok})
	if err != nil {
		return uuid.Nil, err
	}
	for _, sp := range spaces {
		if sp.Name == raw {
			return sp.SpaceID, nil
		}
	}
	return uuid.Nil, fmt.Errorf("space %q not found", raw)
}

func resolveAccountingDomainID(ctx context.Context, a *app.App, spaceID domainspace.SpaceID, raw string) (graph.DomainID, error) {
	if id, err := uuid.Parse(strings.TrimSpace(raw)); err == nil {
		return graph.DomainID(id), nil
	}
	if spaceID == uuid.Nil {
		return uuid.Nil, fmt.Errorf("--space is required when resolving domain key %q", raw)
	}
	tok, err := a.AccessToken(ctx)
	if err != nil {
		return uuid.Nil, err
	}
	domain, err := a.Engine.GetDomain(ctx, mycelengine.GetDomainInput{AccessToken: tok, SpaceID: spaceID, Key: raw})
	if err != nil {
		return uuid.Nil, err
	}
	return domain.ID, nil
}

func resolveAccountingModelEndpointID(ctx context.Context, a *app.App, raw string) (domainsemantic.ModelEndpointID, error) {
	if id, err := uuid.Parse(strings.TrimSpace(raw)); err == nil {
		return domainsemantic.ModelEndpointID(id), nil
	}
	mgr := storesemantic.NewGlobalManager()
	if err := mgr.Init(ctx, filepath.Join(a.DataDir, "meta")); err != nil {
		return uuid.Nil, err
	}
	return resolveModelEndpointID(ctx, mgr, raw)
}

func resolveAccountingModelID(ctx context.Context, a *app.App, raw string) (domainsemantic.InferenceModelID, error) {
	if id, err := uuid.Parse(strings.TrimSpace(raw)); err == nil {
		return domainsemantic.InferenceModelID(id), nil
	}
	mgr := storesemantic.NewGlobalManager()
	if err := mgr.Init(ctx, filepath.Join(a.DataDir, "meta")); err != nil {
		return uuid.Nil, err
	}
	return resolveModelID(ctx, mgr, raw)
}

func parseAccountingTime(raw string) (time.Time, error) {
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02"} {
		if t, err := time.Parse(layout, raw); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid time %q", raw)
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := []string{}
	for _, part := range parts {
		if strings.TrimSpace(part) != "" {
			out = append(out, strings.TrimSpace(part))
		}
	}
	return out
}
