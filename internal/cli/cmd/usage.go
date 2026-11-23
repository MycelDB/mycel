package cmd

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/myceldb/mycel/internal/cli/app"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	"github.com/spf13/cobra"
)

type adminUsageFlags struct {
	SpaceID                   string
	DomainID                  string
	DomainInclusion           string
	IncludeDiagnosticPhysical bool
	IncludeDiagnosticHealth   bool
	RequireQuotaSafe          bool
	MaxStaleness              string
	MinimumRevision           string
	Metrics                   []string
	LimitID                   string
	MetricName                string
	MetricVersion             uint32
	HardLimit                 uint64
	LimitEnabled              bool
	PolicyRef                 string
	Description               string
	Mode                      string
	PageSize                  int32
	PageToken                 string
	IncludeReconciliation     bool
}

func NewAdminUsageCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "usage", Short: "Inspect quota-safe usage accounting"}
	cmd.AddCommand(NewAdminUsageGetCommand(a), NewAdminUsageListCommand(a), NewAdminUsageHealthCommand(a), NewAdminUsageLimitsCommand(a))
	return cmd
}

func NewAdminUsageGetCommand(a *app.App) *cobra.Command {
	var f adminUsageFlags
	cmd := &cobra.Command{Use: "get", Short: "Get a usage snapshot", RunE: func(cmd *cobra.Command, args []string) error {
		req, err := usageGetRequestFromFlags(f)
		if err != nil {
			return err
		}
		client, authCtx, closeConn, err := adminUsageClient(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer closeConn()
		res, err := client.GetUsage(authCtx, req)
		if err != nil {
			return fmt.Errorf("get usage: %w", err)
		}
		return printUsageSnapshot(a, res.GetSnapshot())
	}}
	addUsageScopeFlags(cmd, &f)
	addUsageSnapshotFlags(cmd, &f)
	return cmd
}

func NewAdminUsageListCommand(a *app.App) *cobra.Command {
	var f adminUsageFlags
	cmd := &cobra.Command{Use: "list", Short: "List materialized usage snapshots", RunE: func(cmd *cobra.Command, args []string) error {
		req, err := usageListRequestFromFlags(f)
		if err != nil {
			return err
		}
		client, authCtx, closeConn, err := adminUsageClient(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer closeConn()
		res, err := client.ListUsage(authCtx, req)
		if err != nil {
			return fmt.Errorf("list usage: %w", err)
		}
		return printUsageList(a, res)
	}}
	cmd.Flags().StringVar(&f.Mode, "mode", "spaces", "list mode: spaces, domains_in_space, or space_with_domain_breakdown")
	addUsageScopeFlags(cmd, &f)
	addUsageSnapshotFlags(cmd, &f)
	cmd.Flags().Int32Var(&f.PageSize, "page-size", 100, "maximum snapshots to return")
	cmd.Flags().StringVar(&f.PageToken, "page-token", "", "continuation token from a previous list")
	return cmd
}

func NewAdminUsageHealthCommand(a *app.App) *cobra.Command {
	var f adminUsageFlags
	cmd := &cobra.Command{Use: "health", Short: "Get usage accounting health", RunE: func(cmd *cobra.Command, args []string) error {
		req, err := usageHealthRequestFromFlags(f)
		if err != nil {
			return err
		}
		client, authCtx, closeConn, err := adminUsageClient(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer closeConn()
		res, err := client.GetUsageAccountingHealth(authCtx, req)
		if err != nil {
			return fmt.Errorf("get usage accounting health: %w", err)
		}
		return printUsageHealth(a, res.GetHealth())
	}}
	addUsageScopeFlags(cmd, &f)
	cmd.Flags().BoolVar(&f.IncludeReconciliation, "include-reconciliation", false, "include reconciliation health details when available")
	cmd.Flags().BoolVar(&f.IncludeDiagnosticHealth, "include-diagnostic-collectors", false, "include diagnostic collector health")
	return cmd
}

func NewAdminUsageLimitsCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "limits", Short: "Manage hard usage admission limits"}
	cmd.AddCommand(NewAdminUsageLimitsUpsertCommand(a), NewAdminUsageLimitsGetCommand(a), NewAdminUsageLimitsListCommand(a), NewAdminUsageLimitsDeleteCommand(a))
	return cmd
}

func NewAdminUsageLimitsUpsertCommand(a *app.App) *cobra.Command {
	var f adminUsageFlags
	f.LimitEnabled = true
	cmd := &cobra.Command{Use: "upsert", Short: "Create or update a hard usage limit", RunE: func(cmd *cobra.Command, args []string) error {
		req, err := usageLimitUpsertRequestFromFlags(f)
		if err != nil {
			return err
		}
		client, authCtx, closeConn, err := adminUsageAdmissionClient(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer closeConn()
		res, err := client.UpsertUsageLimit(authCtx, req)
		if err != nil {
			return fmt.Errorf("upsert usage limit: %w", err)
		}
		return printUsageLimit(a, res.GetLimit())
	}}
	addUsageScopeFlags(cmd, &f)
	addUsageLimitKeyFlags(cmd, &f)
	cmd.Flags().Uint64Var(&f.HardLimit, "hard-limit", 0, "hard limit value in the metric unit")
	cmd.Flags().BoolVar(&f.LimitEnabled, "enabled", true, "enable the limit")
	cmd.Flags().StringVar(&f.PolicyRef, "policy-ref", "", "non-secret external policy reference")
	cmd.Flags().StringVar(&f.Description, "description", "", "operator description")
	return cmd
}

func NewAdminUsageLimitsGetCommand(a *app.App) *cobra.Command {
	var f adminUsageFlags
	cmd := &cobra.Command{Use: "get", Short: "Get a hard usage limit", RunE: func(cmd *cobra.Command, args []string) error {
		req, err := usageLimitGetRequestFromFlags(f)
		if err != nil {
			return err
		}
		client, authCtx, closeConn, err := adminUsageAdmissionClient(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer closeConn()
		res, err := client.GetUsageLimit(authCtx, req)
		if err != nil {
			return fmt.Errorf("get usage limit: %w", err)
		}
		return printUsageLimit(a, res.GetLimit())
	}}
	addUsageScopeFlags(cmd, &f)
	addUsageLimitLookupFlags(cmd, &f)
	return cmd
}

func NewAdminUsageLimitsListCommand(a *app.App) *cobra.Command {
	var f adminUsageFlags
	cmd := &cobra.Command{Use: "list", Short: "List hard usage limits", RunE: func(cmd *cobra.Command, args []string) error {
		req, err := usageLimitListRequestFromFlags(f)
		if err != nil {
			return err
		}
		client, authCtx, closeConn, err := adminUsageAdmissionClient(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer closeConn()
		res, err := client.ListUsageLimits(authCtx, req)
		if err != nil {
			return fmt.Errorf("list usage limits: %w", err)
		}
		return printUsageLimits(a, res)
	}}
	addUsageScopeFlags(cmd, &f)
	cmd.Flags().StringVar(&f.MetricName, "metric", "", "metric name filter")
	cmd.Flags().Int32Var(&f.PageSize, "page-size", 100, "maximum limits to return")
	cmd.Flags().StringVar(&f.PageToken, "page-token", "", "continuation token from a previous list")
	return cmd
}

func NewAdminUsageLimitsDeleteCommand(a *app.App) *cobra.Command {
	var f adminUsageFlags
	cmd := &cobra.Command{Use: "delete", Short: "Delete a hard usage limit", RunE: func(cmd *cobra.Command, args []string) error {
		req, err := usageLimitDeleteRequestFromFlags(f)
		if err != nil {
			return err
		}
		client, authCtx, closeConn, err := adminUsageAdmissionClient(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer closeConn()
		res, err := client.DeleteUsageLimit(authCtx, req)
		if err != nil {
			return fmt.Errorf("delete usage limit: %w", err)
		}
		return printUsageLimit(a, res.GetLimit())
	}}
	addUsageScopeFlags(cmd, &f)
	addUsageLimitLookupFlags(cmd, &f)
	return cmd
}

func addUsageScopeFlags(cmd *cobra.Command, f *adminUsageFlags) {
	cmd.Flags().StringVar(&f.SpaceID, "space-id", "", "Mycel space id")
	cmd.Flags().StringVar(&f.DomainID, "domain-id", "", "optional Mycel domain id")
}

func addUsageSnapshotFlags(cmd *cobra.Command, f *adminUsageFlags) {
	cmd.Flags().StringVar(&f.DomainInclusion, "domain-inclusion", "", "space aggregate domain inclusion: retained_non_system, retained_all, or system_only")
	cmd.Flags().BoolVar(&f.IncludeDiagnosticPhysical, "include-diagnostic-physical", false, "include diagnostic physical metrics when available")
	cmd.Flags().BoolVar(&f.RequireQuotaSafe, "require-quota-safe", false, "fail if a complete quota-safe snapshot is unavailable")
	cmd.Flags().StringVar(&f.MaxStaleness, "max-staleness", "", "maximum counter staleness duration, e.g. 30s or 5m")
	cmd.Flags().StringVar(&f.MinimumRevision, "minimum-revision", "", "minimum usage revision token")
	cmd.Flags().StringArrayVar(&f.Metrics, "metric", nil, "metric name to request; repeat for multiple metrics")
}

func addUsageLimitKeyFlags(cmd *cobra.Command, f *adminUsageFlags) {
	cmd.Flags().StringVar(&f.DomainInclusion, "domain-inclusion", "", "space aggregate domain inclusion: retained_non_system, retained_all, or system_only")
	cmd.Flags().StringVar(&f.MetricName, "metric", "", "hard-limit metric name")
	cmd.Flags().Uint32Var(&f.MetricVersion, "metric-version", 1, "metric definition version")
}

func addUsageLimitLookupFlags(cmd *cobra.Command, f *adminUsageFlags) {
	cmd.Flags().StringVar(&f.LimitID, "limit-id", "", "usage limit id")
	addUsageLimitKeyFlags(cmd, f)
}

func adminUsageClient(ctx context.Context, a *app.App) (adminv1.AdminUsageAccountingServiceClient, context.Context, func(), error) {
	conn, authCtx, _, err := loginDaemonOperator(ctx, a)
	if err != nil {
		return nil, nil, nil, err
	}
	return adminv1.NewAdminUsageAccountingServiceClient(conn), authCtx, func() { _ = conn.Close() }, nil
}

func adminUsageAdmissionClient(ctx context.Context, a *app.App) (adminv1.AdminUsageAdmissionServiceClient, context.Context, func(), error) {
	conn, authCtx, _, err := loginDaemonOperator(ctx, a)
	if err != nil {
		return nil, nil, nil, err
	}
	return adminv1.NewAdminUsageAdmissionServiceClient(conn), authCtx, func() { _ = conn.Close() }, nil
}

func usageGetRequestFromFlags(f adminUsageFlags) (*adminv1.GetUsageRequest, error) {
	scope, err := usageScopeFromFlags(f.SpaceID, f.DomainID, true)
	if err != nil {
		return nil, err
	}
	domainInclusion, err := usageDomainInclusionFlag(f.DomainInclusion)
	if err != nil {
		return nil, err
	}
	maxStaleness, err := usageMaxStalenessSeconds(f.MaxStaleness)
	if err != nil {
		return nil, err
	}
	return &adminv1.GetUsageRequest{Scope: scope, DomainInclusion: domainInclusion, IncludeDiagnosticPhysical: f.IncludeDiagnosticPhysical, RequireQuotaSafe: f.RequireQuotaSafe, MaxStalenessSeconds: maxStaleness, MinimumRevision: usageRevisionFlag(f.MinimumRevision), MetricNames: trimStringFlags(f.Metrics)}, nil
}

func usageListRequestFromFlags(f adminUsageFlags) (*adminv1.ListUsageRequest, error) {
	mode, err := usageListModeFlag(f.Mode)
	if err != nil {
		return nil, err
	}
	requireSpace := mode != adminv1.UsageListMode_USAGE_LIST_MODE_SPACES
	scope, err := usageScopeFromFlags(f.SpaceID, f.DomainID, requireSpace)
	if err != nil {
		return nil, err
	}
	domainInclusion, err := usageDomainInclusionFlag(f.DomainInclusion)
	if err != nil {
		return nil, err
	}
	maxStaleness, err := usageMaxStalenessSeconds(f.MaxStaleness)
	if err != nil {
		return nil, err
	}
	return &adminv1.ListUsageRequest{Parent: scope, Mode: mode, DomainInclusion: domainInclusion, IncludeDiagnosticPhysical: f.IncludeDiagnosticPhysical, RequireQuotaSafe: f.RequireQuotaSafe, MaxStalenessSeconds: maxStaleness, PageSize: f.PageSize, PageToken: f.PageToken, MetricNames: trimStringFlags(f.Metrics), MinimumRevision: usageRevisionFlag(f.MinimumRevision)}, nil
}

func usageHealthRequestFromFlags(f adminUsageFlags) (*adminv1.GetUsageAccountingHealthRequest, error) {
	scope, err := usageScopeFromFlags(f.SpaceID, f.DomainID, true)
	if err != nil {
		return nil, err
	}
	return &adminv1.GetUsageAccountingHealthRequest{Scope: scope, IncludeReconciliation: f.IncludeReconciliation, IncludeDiagnosticCollectors: f.IncludeDiagnosticHealth}, nil
}

func usageLimitUpsertRequestFromFlags(f adminUsageFlags) (*adminv1.UpsertUsageLimitRequest, error) {
	limit, err := usageLimitFromFlags(f, true)
	if err != nil {
		return nil, err
	}
	limit.HardLimit = f.HardLimit
	limit.Enabled = f.LimitEnabled
	limit.PolicyRef = strings.TrimSpace(f.PolicyRef)
	limit.Description = strings.TrimSpace(f.Description)
	return &adminv1.UpsertUsageLimitRequest{Limit: limit}, nil
}

func usageLimitGetRequestFromFlags(f adminUsageFlags) (*adminv1.GetUsageLimitRequest, error) {
	scope, domainInclusion, err := usageLimitLookupKeyFromFlags(f)
	if err != nil {
		return nil, err
	}
	return &adminv1.GetUsageLimitRequest{LimitId: strings.TrimSpace(f.LimitID), Scope: scope, DomainInclusion: domainInclusion, MetricName: strings.TrimSpace(f.MetricName), MetricVersion: f.MetricVersion}, nil
}

func usageLimitDeleteRequestFromFlags(f adminUsageFlags) (*adminv1.DeleteUsageLimitRequest, error) {
	scope, domainInclusion, err := usageLimitLookupKeyFromFlags(f)
	if err != nil {
		return nil, err
	}
	return &adminv1.DeleteUsageLimitRequest{LimitId: strings.TrimSpace(f.LimitID), Scope: scope, DomainInclusion: domainInclusion, MetricName: strings.TrimSpace(f.MetricName), MetricVersion: f.MetricVersion}, nil
}

func usageLimitListRequestFromFlags(f adminUsageFlags) (*adminv1.ListUsageLimitsRequest, error) {
	scope, err := usageScopeFromFlags(f.SpaceID, f.DomainID, false)
	if err != nil {
		return nil, err
	}
	return &adminv1.ListUsageLimitsRequest{Parent: scope, PageSize: f.PageSize, PageToken: f.PageToken, MetricName: strings.TrimSpace(f.MetricName)}, nil
}

func usageLimitFromFlags(f adminUsageFlags, requireScope bool) (*adminv1.UsageLimit, error) {
	scope, err := usageScopeFromFlags(f.SpaceID, f.DomainID, requireScope)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(f.MetricName) == "" {
		return nil, fmt.Errorf("--metric is required")
	}
	domainInclusion, err := usageDomainInclusionFlag(f.DomainInclusion)
	if err != nil {
		return nil, err
	}
	return &adminv1.UsageLimit{LimitId: strings.TrimSpace(f.LimitID), Scope: scope, DomainInclusion: domainInclusion, MetricName: strings.TrimSpace(f.MetricName), MetricVersion: f.MetricVersion}, nil
}

func usageLimitLookupKeyFromFlags(f adminUsageFlags) (*adminv1.UsageScope, adminv1.DomainInclusionMode, error) {
	if strings.TrimSpace(f.LimitID) != "" {
		return nil, adminv1.DomainInclusionMode_DOMAIN_INCLUSION_MODE_UNSPECIFIED, nil
	}
	limit, err := usageLimitFromFlags(f, true)
	if err != nil {
		return nil, adminv1.DomainInclusionMode_DOMAIN_INCLUSION_MODE_UNSPECIFIED, err
	}
	return limit.GetScope(), limit.GetDomainInclusion(), nil
}

func usageScopeFromFlags(spaceID, domainID string, requireSpace bool) (*adminv1.UsageScope, error) {
	spaceID = strings.TrimSpace(spaceID)
	domainID = strings.TrimSpace(domainID)
	if spaceID == "" {
		if requireSpace {
			return nil, fmt.Errorf("--space-id is required")
		}
		return nil, nil
	}
	if domainID != "" {
		return &adminv1.UsageScope{Scope: &adminv1.UsageScope_Domain{Domain: &adminv1.DomainUsageScope{SpaceId: spaceID, DomainId: domainID}}}, nil
	}
	return &adminv1.UsageScope{Scope: &adminv1.UsageScope_Space{Space: &adminv1.SpaceUsageScope{SpaceId: spaceID}}}, nil
}

func usageDomainInclusionFlag(value string) (adminv1.DomainInclusionMode, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "":
		return adminv1.DomainInclusionMode_DOMAIN_INCLUSION_MODE_UNSPECIFIED, nil
	case "retained_non_system", "non-system", "non_system":
		return adminv1.DomainInclusionMode_DOMAIN_INCLUSION_MODE_RETAINED_NON_SYSTEM, nil
	case "retained_all", "all":
		return adminv1.DomainInclusionMode_DOMAIN_INCLUSION_MODE_RETAINED_ALL, nil
	case "system_only", "system":
		return adminv1.DomainInclusionMode_DOMAIN_INCLUSION_MODE_SYSTEM_ONLY, nil
	default:
		return adminv1.DomainInclusionMode_DOMAIN_INCLUSION_MODE_UNSPECIFIED, fmt.Errorf("unsupported --domain-inclusion %q", value)
	}
}

func usageListModeFlag(value string) (adminv1.UsageListMode, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "spaces", "space":
		return adminv1.UsageListMode_USAGE_LIST_MODE_SPACES, nil
	case "domains_in_space", "domains", "domain":
		return adminv1.UsageListMode_USAGE_LIST_MODE_DOMAINS_IN_SPACE, nil
	case "space_with_domain_breakdown", "breakdown":
		return adminv1.UsageListMode_USAGE_LIST_MODE_SPACE_WITH_DOMAIN_BREAKDOWN, nil
	default:
		return adminv1.UsageListMode_USAGE_LIST_MODE_UNSPECIFIED, fmt.Errorf("unsupported --mode %q", value)
	}
}

func usageMaxStalenessSeconds(value string) (uint64, error) {
	if strings.TrimSpace(value) == "" {
		return 0, nil
	}
	dur, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("invalid --max-staleness: %w", err)
	}
	if dur < 0 {
		return 0, fmt.Errorf("invalid --max-staleness: must be non-negative")
	}
	return uint64(dur / time.Second), nil
}

func usageRevisionFlag(token string) *adminv1.UsageRevision {
	if strings.TrimSpace(token) == "" {
		return nil
	}
	return &adminv1.UsageRevision{RevisionToken: strings.TrimSpace(token)}
}

func trimStringFlags(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func printUsageSnapshot(a *app.App, snapshot *adminv1.UsageSnapshot) error {
	if a.Output == "json" {
		return a.Print(snapshot, "")
	}
	if snapshot == nil {
		return a.Print(snapshot, "usage snapshot: <nil>\n")
	}
	var b strings.Builder
	fmt.Fprintf(&b, "usage snapshot: %s\n", formatUsageScope(snapshot.GetScope()))
	fmt.Fprintf(&b, "  state: %s\n", snapshot.GetAccountingState())
	fmt.Fprintf(&b, "  quota_safe_complete: %t\n", snapshot.GetQuotaSafeComplete())
	fmt.Fprintf(&b, "  domain_inclusion: %s\n", snapshot.GetDomainInclusion())
	fmt.Fprintf(&b, "  revision: %s\n", snapshot.GetAsOfRevision().GetRevisionToken())
	if len(snapshot.GetMetrics()) == 0 {
		b.WriteString("  metrics: <none>\n")
	} else {
		b.WriteString("  metrics:\n")
		for _, metric := range snapshot.GetMetrics() {
			fmt.Fprintf(&b, "    - %s: %d %s (authority=%s quota_safe=%t hard_limit=%t)\n", metric.GetName(), metric.GetValue(), metric.GetUnit(), metric.GetAuthority(), metric.GetQuotaSafe(), metric.GetHardLimitEligible())
		}
	}
	writeUsageStatuses(&b, "blockers", snapshot.GetBlockers())
	writeUsageStatuses(&b, "warnings", snapshot.GetWarnings())
	return a.Print(snapshot, b.String())
}

func printUsageList(a *app.App, res *adminv1.ListUsageResponse) error {
	if a.Output == "json" {
		return a.Print(res, "")
	}
	if res == nil {
		return a.Print(res, "usage snapshots: 0\n")
	}
	var b strings.Builder
	fmt.Fprintf(&b, "usage snapshots: %d\n", len(res.GetSnapshots()))
	for _, snapshot := range res.GetSnapshots() {
		fmt.Fprintf(&b, "- %s state=%s quota_safe_complete=%t metrics=%d\n", formatUsageScope(snapshot.GetScope()), snapshot.GetAccountingState(), snapshot.GetQuotaSafeComplete(), len(snapshot.GetMetrics()))
		for _, metric := range snapshot.GetMetrics() {
			fmt.Fprintf(&b, "    %s=%d %s\n", metric.GetName(), metric.GetValue(), metric.GetUnit())
		}
	}
	if res.GetNextPageToken() != "" {
		fmt.Fprintf(&b, "next_page_token: %s\n", res.GetNextPageToken())
	}
	return a.Print(res, b.String())
}

func printUsageHealth(a *app.App, health *adminv1.UsageAccountingHealth) error {
	if a.Output == "json" {
		return a.Print(health, "")
	}
	if health == nil {
		return a.Print(health, "usage accounting health: <nil>\n")
	}
	var b strings.Builder
	fmt.Fprintf(&b, "usage accounting health: %s\n", formatUsageScope(health.GetScope()))
	fmt.Fprintf(&b, "  state: %s\n", health.GetState())
	fmt.Fprintf(&b, "  quota_safe_ready: %t\n", health.GetQuotaSafeReady())
	fmt.Fprintf(&b, "  revision: %s\n", health.GetAsOfRevision().GetRevisionToken())
	fmt.Fprintf(&b, "  rebuilding_scope_count: %d\n", health.GetRebuildingScopeCount())
	fmt.Fprintf(&b, "  stale_scope_count: %d\n", health.GetStaleScopeCount())
	fmt.Fprintf(&b, "  drift_scope_count: %d\n", health.GetDriftScopeCount())
	fmt.Fprintf(&b, "  diagnostic_collector_unavailable_count: %d\n", health.GetDiagnosticCollectorUnavailableCount())
	writeUsageStatuses(&b, "blockers", health.GetBlockers())
	writeUsageStatuses(&b, "warnings", health.GetWarnings())
	return a.Print(health, b.String())
}

func printUsageLimit(a *app.App, limit *adminv1.UsageLimit) error {
	if a.Output == "json" {
		return a.Print(limit, "")
	}
	if limit == nil {
		return a.Print(limit, "usage limit: <nil>\n")
	}
	var b strings.Builder
	fmt.Fprintf(&b, "usage limit: %s\n", limit.GetLimitId())
	fmt.Fprintf(&b, "  scope: %s\n", formatUsageScope(limit.GetScope()))
	fmt.Fprintf(&b, "  metric: %s@%d\n", limit.GetMetricName(), limit.GetMetricVersion())
	fmt.Fprintf(&b, "  hard_limit: %d\n", limit.GetHardLimit())
	fmt.Fprintf(&b, "  enabled: %t\n", limit.GetEnabled())
	fmt.Fprintf(&b, "  domain_inclusion: %s\n", limit.GetDomainInclusion())
	if limit.GetPolicyRef() != "" {
		fmt.Fprintf(&b, "  policy_ref: %s\n", limit.GetPolicyRef())
	}
	return a.Print(limit, b.String())
}

func printUsageLimits(a *app.App, res *adminv1.ListUsageLimitsResponse) error {
	if a.Output == "json" {
		return a.Print(res, "")
	}
	if res == nil {
		return a.Print(res, "usage limits: 0\n")
	}
	var b strings.Builder
	fmt.Fprintf(&b, "usage limits: %d\n", len(res.GetLimits()))
	for _, limit := range res.GetLimits() {
		fmt.Fprintf(&b, "- %s %s %s@%d hard_limit=%d enabled=%t\n", limit.GetLimitId(), formatUsageScope(limit.GetScope()), limit.GetMetricName(), limit.GetMetricVersion(), limit.GetHardLimit(), limit.GetEnabled())
	}
	if res.GetNextPageToken() != "" {
		fmt.Fprintf(&b, "next_page_token: %s\n", res.GetNextPageToken())
	}
	return a.Print(res, b.String())
}

func formatUsageScope(scope *adminv1.UsageScope) string {
	if scope == nil {
		return "<unspecified>"
	}
	if domain := scope.GetDomain(); domain != nil {
		return fmt.Sprintf("domain:%s/%s", domain.GetSpaceId(), domain.GetDomainId())
	}
	if space := scope.GetSpace(); space != nil {
		return "space:" + space.GetSpaceId()
	}
	return "<unspecified>"
}

func writeUsageStatuses(b *strings.Builder, label string, statuses []*adminv1.UsageStatus) {
	if len(statuses) == 0 {
		return
	}
	fmt.Fprintf(b, "  %s:\n", label)
	for _, item := range statuses {
		fmt.Fprintf(b, "    - %s %s: %s\n", item.GetSeverity(), item.GetReason(), item.GetMessage())
	}
}
