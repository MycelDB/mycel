package admin

import (
	"context"
	"testing"

	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	usageservice "github.com/myceldb/mycel/internal/usage/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type usageCapabilityAuthorizer map[string]bool

func (a usageCapabilityAuthorizer) HasCapability(_ context.Context, _ string, capability string) (bool, error) {
	return a[capability], nil
}

func TestAdminUsageGetUsageMapsQuotaSafeMetrics(t *testing.T) {
	ctx := authenticatedContext()
	usage := usageservice.NewModule()
	spaceID := "space-usage"
	domainID := "domain-usage"
	if err := usage.ApplyDelta(ctx, usageservice.Delta{MutationID: "graph-1", Source: "graph", Revision: "1", Scope: usageservice.DomainScope(spaceID, domainID), Metrics: []usageservice.MetricDelta{{Name: usageservice.MetricGraphNodeCount, Delta: 2}, {Name: usageservice.MetricGraphEdgeCount, Delta: 1}}}); err != nil {
		t.Fatalf("ApplyDelta() error = %v", err)
	}
	svc := NewAdminUsageAccountingService(usage, usageCapabilityAuthorizer{usageReadCapability: true})
	res, err := svc.GetUsage(ctx, &adminv1.GetUsageRequest{Scope: &adminv1.UsageScope{Scope: &adminv1.UsageScope_Domain{Domain: &adminv1.DomainUsageScope{SpaceId: spaceID, DomainId: domainID}}}, RequireQuotaSafe: true})
	if err != nil {
		t.Fatalf("GetUsage() error = %v", err)
	}
	if got := res.GetSnapshot().GetScope().GetDomain().GetDomainId(); got != domainID {
		t.Fatalf("domain scope = %q, want %q", got, domainID)
	}
	assertAdminUsageMetric(t, res.GetSnapshot(), usageservice.MetricGraphNodeCount, 2)
	assertAdminUsageMetric(t, res.GetSnapshot(), usageservice.MetricGraphEdgeCount, 1)
}

func TestAdminUsageRequiresQuotaSafeFailsClosedWhileRebuilding(t *testing.T) {
	ctx := authenticatedContext()
	usage := usageservice.NewModule()
	if err := usage.MarkRebuilding(ctx, "test rebuild"); err != nil {
		t.Fatalf("MarkRebuilding() error = %v", err)
	}
	svc := NewAdminUsageAccountingService(usage, usageCapabilityAuthorizer{usageReadCapability: true})
	_, err := svc.GetUsage(ctx, &adminv1.GetUsageRequest{Scope: &adminv1.UsageScope{Scope: &adminv1.UsageScope_Space{Space: &adminv1.SpaceUsageScope{SpaceId: "space-usage"}}}, RequireQuotaSafe: true, MetricNames: []string{usageservice.MetricGraphNodeCount}})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("GetUsage() code = %v, want %v (err=%v)", status.Code(err), codes.FailedPrecondition, err)
	}
}

func TestAdminUsageReadRequiresUsageCapability(t *testing.T) {
	ctx := authenticatedContext()
	svc := NewAdminUsageAccountingService(usageservice.NewModule(), usageCapabilityAuthorizer{})
	_, err := svc.GetUsage(ctx, &adminv1.GetUsageRequest{Scope: &adminv1.UsageScope{Scope: &adminv1.UsageScope_Space{Space: &adminv1.SpaceUsageScope{SpaceId: "space-usage"}}}})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("GetUsage() code = %v, want %v (err=%v)", status.Code(err), codes.PermissionDenied, err)
	}
}

func TestAdminUsageDiagnosticReadRequiresDiagnosticCapability(t *testing.T) {
	ctx := authenticatedContext()
	svc := NewAdminUsageAccountingService(usageservice.NewModule(), usageCapabilityAuthorizer{usageReadCapability: true})
	_, err := svc.GetUsage(ctx, &adminv1.GetUsageRequest{Scope: &adminv1.UsageScope{Scope: &adminv1.UsageScope_Space{Space: &adminv1.SpaceUsageScope{SpaceId: "space-usage"}}}, IncludeDiagnosticPhysical: true})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("GetUsage() code = %v, want %v (err=%v)", status.Code(err), codes.PermissionDenied, err)
	}
}

func TestAdminUsageReconciliationRequiresReconcileCapability(t *testing.T) {
	ctx := authenticatedContext()
	svc := NewAdminUsageAccountingService(usageservice.NewModule(), usageCapabilityAuthorizer{usageReadCapability: true})
	_, err := svc.GetUsageAccountingHealth(ctx, &adminv1.GetUsageAccountingHealthRequest{Scope: &adminv1.UsageScope{Scope: &adminv1.UsageScope_Space{Space: &adminv1.SpaceUsageScope{SpaceId: "space-usage"}}}, IncludeReconciliation: true})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("GetUsageAccountingHealth() code = %v, want %v (err=%v)", status.Code(err), codes.PermissionDenied, err)
	}
}

func TestAdminUsageAdmissionLimitCRUDRequiresManageCapability(t *testing.T) {
	ctx := authenticatedContext()
	usage := usageservice.NewModule()
	svc := NewAdminUsageAdmissionService(usage, usageCapabilityAuthorizer{})
	_, err := svc.UpsertUsageLimit(ctx, &adminv1.UpsertUsageLimitRequest{Limit: &adminv1.UsageLimit{Scope: &adminv1.UsageScope{Scope: &adminv1.UsageScope_Domain{Domain: &adminv1.DomainUsageScope{SpaceId: "space-usage", DomainId: "domain-usage"}}}, MetricName: usageservice.MetricBlobActiveLogicalBytes, HardLimit: 1024, Enabled: true}})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("UpsertUsageLimit() code = %v, want %v (err=%v)", status.Code(err), codes.PermissionDenied, err)
	}
}

func TestAdminUsageAdmissionLimitCRUD(t *testing.T) {
	ctx := authenticatedContext()
	usage := usageservice.NewModule()
	svc := NewAdminUsageAdmissionService(usage, usageCapabilityAuthorizer{usageLimitManageCapability: true})
	scope := &adminv1.UsageScope{Scope: &adminv1.UsageScope_Domain{Domain: &adminv1.DomainUsageScope{SpaceId: "space-usage", DomainId: "domain-usage"}}}
	created, err := svc.UpsertUsageLimit(ctx, &adminv1.UpsertUsageLimitRequest{Limit: &adminv1.UsageLimit{Scope: scope, MetricName: usageservice.MetricBlobActiveLogicalBytes, HardLimit: 1024, Enabled: true, PolicyRef: "plan/test"}})
	if err != nil {
		t.Fatalf("UpsertUsageLimit() error = %v", err)
	}
	if created.GetLimit().GetLimitId() == "" || created.GetLimit().GetMetricName() != usageservice.MetricBlobActiveLogicalBytes || created.GetLimit().GetHardLimit() != 1024 {
		t.Fatalf("created limit = %#v", created.GetLimit())
	}
	got, err := svc.GetUsageLimit(ctx, &adminv1.GetUsageLimitRequest{LimitId: created.GetLimit().GetLimitId()})
	if err != nil {
		t.Fatalf("GetUsageLimit() error = %v", err)
	}
	if got.GetLimit().GetLimitId() != created.GetLimit().GetLimitId() {
		t.Fatalf("GetUsageLimit() id = %q, want %q", got.GetLimit().GetLimitId(), created.GetLimit().GetLimitId())
	}
	listed, err := svc.ListUsageLimits(ctx, &adminv1.ListUsageLimitsRequest{Parent: &adminv1.UsageScope{Scope: &adminv1.UsageScope_Space{Space: &adminv1.SpaceUsageScope{SpaceId: "space-usage"}}}})
	if err != nil {
		t.Fatalf("ListUsageLimits() error = %v", err)
	}
	if len(listed.GetLimits()) != 1 {
		t.Fatalf("ListUsageLimits() returned %d limits, want 1", len(listed.GetLimits()))
	}
	deleted, err := svc.DeleteUsageLimit(ctx, &adminv1.DeleteUsageLimitRequest{LimitId: created.GetLimit().GetLimitId()})
	if err != nil {
		t.Fatalf("DeleteUsageLimit() error = %v", err)
	}
	if deleted.GetLimit().GetLimitId() != created.GetLimit().GetLimitId() {
		t.Fatalf("DeleteUsageLimit() id = %q, want %q", deleted.GetLimit().GetLimitId(), created.GetLimit().GetLimitId())
	}
	_, err = svc.GetUsageLimit(ctx, &adminv1.GetUsageLimitRequest{LimitId: created.GetLimit().GetLimitId()})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("GetUsageLimit(deleted) code = %v, want %v (err=%v)", status.Code(err), codes.NotFound, err)
	}
}

func assertAdminUsageMetric(t *testing.T, snapshot *adminv1.UsageSnapshot, name string, want uint64) {
	t.Helper()
	for _, metric := range snapshot.GetMetrics() {
		if metric.GetName() == name {
			if got := metric.GetValue(); got != want {
				t.Fatalf("metric %s = %d, want %d", name, got, want)
			}
			return
		}
	}
	t.Fatalf("metric %s not found in %+v", name, snapshot.GetMetrics())
}
