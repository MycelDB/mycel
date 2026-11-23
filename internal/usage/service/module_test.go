package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/myceldb/mycel/internal/runtime"
)

type testHost struct {
	dataDir string
}

func (h testHost) Log() *slog.Logger { return slog.Default() }
func (h testHost) DataDir() string   { return h.dataDir }

func writeUsageTestFile(t *testing.T, path string, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestDefaultRegistryIncludesMVPQuotaSafeMetrics(t *testing.T) {
	registry := DefaultRegistry()
	for _, name := range []string{
		MetricGraphNodeCount,
		MetricGraphEdgeCount,
		MetricBlobActiveLogicalBytes,
		MetricBlobActiveCount,
		MetricBlobLocalActiveLogicalBytes,
		MetricBlobLocalActiveCount,
		MetricBlobObjectActiveLogicalBytes,
		MetricBlobObjectActiveCount,
		MetricBlobPendingDeleteLogicalBytes,
		MetricBlobPendingDeleteCount,
	} {
		def, ok := registry.Definition(name, 1)
		if !ok {
			t.Fatalf("metric %s is not registered", name)
		}
		if !def.QuotaSafe {
			t.Fatalf("metric %s is not quota-safe", name)
		}
	}
	if def, _ := registry.Definition(MetricBlobPendingDeleteCount, 1); def.HardLimitEligible {
		t.Fatalf("pending-delete count should not be hard-limit eligible in the MVP")
	}
	for _, name := range []string{MetricDiagnosticLocalScopePhysicalBytes, MetricDiagnosticLocalGraphStoreBytes, MetricDiagnosticLocalBlobStoreBytes, MetricDiagnosticLocalBlobMetadataBytes, MetricDiagnosticLocalLexicalStoreBytes, MetricDiagnosticLocalSemanticStoreBytes, MetricDiagnosticLocalProjectionStoreBytes} {
		def, ok := registry.Definition(name, 1)
		if !ok {
			t.Fatalf("diagnostic metric %s is not registered", name)
		}
		if def.QuotaSafe || def.HardLimitEligible || def.Authority != AuthorityObservedApproximate {
			t.Fatalf("diagnostic metric %s definition = %#v, want observed non-quota-safe", name, def)
		}
	}
}

func TestDiagnosticLocalPhysicalSnapshotAndHealth(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	writeUsageTestFile(t, filepath.Join(dataDir, "graphs", "space-1", "domains", "domain-1", "nodes.klog"), "nodes")
	writeUsageTestFile(t, filepath.Join(dataDir, "blobs", "space-1", "objects", "aa", "payload"), "payload-bytes")
	writeUsageTestFile(t, filepath.Join(dataDir, "blob_meta", "space-1.json"), `{"blob":"meta"}`)
	writeUsageTestFile(t, filepath.Join(dataDir, "search", "lexical", "space-1", "domain-1", "segments", "seg", "docs.bin"), "lexical")
	writeUsageTestFile(t, filepath.Join(dataDir, "graphs", "space-1", "semantic", "indexes", "idx", "records", "vec.bin"), "vectors")
	m := NewModule()
	if result := m.Init(ctx, testHost{dataDir: dataDir}); !result.OK {
		t.Fatalf("Init() = %#v", result)
	}
	if err := m.CompleteRebuild(ctx, nil, Revision{}); err != nil {
		t.Fatalf("CompleteRebuild() error = %v", err)
	}
	snapshot, err := m.Snapshot(ctx, SnapshotRequest{Scope: SpaceScope("space-1"), IncludeDiagnosticPhysical: true})
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	assertMetricValue(t, snapshot, MetricDiagnosticLocalGraphStoreBytes, uint64(len("nodes")))
	assertMetricValue(t, snapshot, MetricDiagnosticLocalBlobStoreBytes, uint64(len("payload-bytes")))
	assertMetricValue(t, snapshot, MetricDiagnosticLocalBlobMetadataBytes, uint64(len(`{"blob":"meta"}`)))
	assertMetricValue(t, snapshot, MetricDiagnosticLocalLexicalStoreBytes, uint64(len("lexical")))
	assertMetricValue(t, snapshot, MetricDiagnosticLocalSemanticStoreBytes, uint64(len("vectors")))
	for _, metric := range snapshot.Metrics {
		if strings.HasPrefix(metric.Name, "diagnostic.") && metric.QuotaSafe {
			t.Fatalf("diagnostic metric %s unexpectedly quota-safe", metric.Name)
		}
	}
	health := m.Health(ctx, HealthRequest{Scope: SpaceScope("space-1"), IncludeDiagnosticCollectors: true})
	if health.DiagnosticCollectorUnavailableCount != 0 || len(health.Warnings) != 0 {
		t.Fatalf("diagnostic health = %#v, want healthy collector", health)
	}
}

func TestDiagnosticLocalCollectorReportsBoundedScanWarning(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	writeUsageTestFile(t, filepath.Join(dataDir, "graphs", "space-1", "domains", "domain-1", "a"), "a")
	writeUsageTestFile(t, filepath.Join(dataDir, "graphs", "space-1", "domains", "domain-1", "b"), "b")
	collector := NewLocalDiagnosticCollector(dataDir)
	result, err := collector.CollectUsageDiagnostics(ctx, DiagnosticRequest{Scope: SpaceScope("space-1"), MaxEntries: 1})
	if err != nil {
		t.Fatalf("CollectUsageDiagnostics() error = %v", err)
	}
	if !result.Truncated || len(result.Statuses) == 0 || result.Statuses[0].Reason != StatusReasonDiagnosticCollectorUnavailable {
		t.Fatalf("diagnostic result = %#v, want bounded truncation warning", result)
	}
}

func TestUsageAdmissionLimitCRUDAndReservation(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if err := m.CompleteRebuild(ctx, []CounterValue{{Scope: DomainScope("space-1", "domain-1"), MetricName: MetricBlobActiveLogicalBytes, Value: 5}}, Revision{}); err != nil {
		t.Fatalf("CompleteRebuild() error = %v", err)
	}
	limit, err := m.UpsertUsageLimit(ctx, LimitMutationRequest{Limit: UsageLimit{Scope: DomainScope("space-1", "domain-1"), MetricName: MetricBlobActiveLogicalBytes, HardLimit: 10, Enabled: true, PolicyRef: "plan/test"}})
	if err != nil {
		t.Fatalf("UpsertUsageLimit() error = %v", err)
	}
	if limit.LimitID == "" || limit.DomainInclusion != DomainInclusionUnspecified {
		t.Fatalf("limit = %#v, want generated domain limit", limit)
	}
	if _, err := m.GetUsageLimit(ctx, LimitLookupRequest{LimitID: limit.LimitID}); err != nil {
		t.Fatalf("GetUsageLimit() error = %v", err)
	}
	listed, err := m.ListUsageLimits(ctx, LimitListRequest{Parent: SpaceScope("space-1")})
	if err != nil {
		t.Fatalf("ListUsageLimits() error = %v", err)
	}
	if len(listed.Limits) != 1 {
		t.Fatalf("ListUsageLimits() returned %d limits, want 1", len(listed.Limits))
	}
	delta := Delta{MutationID: "reservation-1", Scope: DomainScope("space-1", "domain-1"), Metrics: []MetricDelta{{Name: MetricBlobActiveLogicalBytes, Delta: 4}}}
	release, err := m.ReserveDeltaForAdmission(ctx, delta)
	if err != nil {
		t.Fatalf("ReserveDeltaForAdmission() error = %v", err)
	}
	if _, err := m.ReserveDeltaForAdmission(ctx, Delta{MutationID: "reservation-2", Scope: DomainScope("space-1", "domain-1"), Metrics: []MetricDelta{{Name: MetricBlobActiveLogicalBytes, Delta: 2}}}); err == nil {
		t.Fatalf("ReserveDeltaForAdmission() succeeded despite reserved capacity exceeding hard limit")
	}
	release()
	if release, err = m.ReserveDeltaForAdmission(ctx, Delta{MutationID: "reservation-3", Scope: DomainScope("space-1", "domain-1"), Metrics: []MetricDelta{{Name: MetricBlobActiveLogicalBytes, Delta: 5}}}); err != nil {
		t.Fatalf("ReserveDeltaForAdmission(after release) error = %v", err)
	} else {
		release()
	}
	if _, err := m.UpsertUsageLimit(ctx, LimitMutationRequest{Limit: UsageLimit{Scope: DomainScope("space-1", "domain-1"), MetricName: MetricDiagnosticLocalScopePhysicalBytes, HardLimit: 1, Enabled: true}}); err == nil {
		t.Fatalf("UpsertUsageLimit(diagnostic metric) succeeded, want hard-limit eligibility error")
	}
	if _, err := m.DeleteUsageLimit(ctx, LimitLookupRequest{LimitID: limit.LimitID}); err != nil {
		t.Fatalf("DeleteUsageLimit() error = %v", err)
	}
}

func TestUsageAdmissionConcurrentReservationsCannotExceedLimit(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if err := m.CompleteRebuild(ctx, nil, Revision{}); err != nil {
		t.Fatalf("CompleteRebuild() error = %v", err)
	}
	if _, err := m.UpsertUsageLimit(ctx, LimitMutationRequest{Limit: UsageLimit{Scope: DomainScope("space-1", "domain-1"), MetricName: MetricGraphNodeCount, HardLimit: 1, Enabled: true}}); err != nil {
		t.Fatalf("UpsertUsageLimit() error = %v", err)
	}
	start := make(chan struct{})
	releaseReservations := make(chan struct{})
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func(i int) {
			<-start
			release, err := m.ReserveDeltaForAdmission(ctx, Delta{MutationID: fmt.Sprintf("concurrent-%d", i), Scope: DomainScope("space-1", "domain-1"), Metrics: []MetricDelta{{Name: MetricGraphNodeCount, Delta: 1}}})
			results <- err
			if err == nil {
				<-releaseReservations
				release()
			}
		}(i)
	}
	close(start)
	errA := <-results
	errB := <-results
	close(releaseReservations)
	successes := 0
	var admissionErr *AdmissionError
	for _, err := range []error{errA, errB} {
		if err == nil {
			successes++
			continue
		}
		if !errors.As(err, &admissionErr) || admissionErr.Reason != ReasonUsageLimitExceeded {
			t.Fatalf("ReserveDeltaForAdmission() error = %v, want usage limit exceeded", err)
		}
	}
	if successes != 1 {
		t.Fatalf("successful reservations = %d, want exactly 1", successes)
	}
}

func TestUsageAdmissionWriteGateBlocksOnlyConfiguredLimits(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if err := m.MarkRebuilding(ctx, "test rebuilding"); err != nil {
		t.Fatalf("MarkRebuilding() error = %v", err)
	}
	if release, err := m.ReserveDeltaForAdmission(ctx, Delta{MutationID: "unlimited", Scope: DomainScope("space-1", "domain-1"), Metrics: []MetricDelta{{Name: MetricGraphNodeCount, Delta: 1}}}); err != nil {
		t.Fatalf("ReserveDeltaForAdmission(unlimited) error = %v", err)
	} else {
		release()
	}
	if _, err := m.UpsertUsageLimit(ctx, LimitMutationRequest{Limit: UsageLimit{Scope: DomainScope("space-1", "domain-1"), MetricName: MetricGraphNodeCount, HardLimit: 10, Enabled: true}}); err != nil {
		t.Fatalf("UpsertUsageLimit() error = %v", err)
	}
	_, err := m.ReserveDeltaForAdmission(ctx, Delta{MutationID: "blocked", Scope: DomainScope("space-1", "domain-1"), Metrics: []MetricDelta{{Name: MetricGraphNodeCount, Delta: 1}}})
	var admissionErr *AdmissionError
	if !errors.As(err, &admissionErr) || admissionErr.Reason != ReasonUsageWriteGateBlocked {
		t.Fatalf("ReserveDeltaForAdmission() error = %v, want usage write gate blocked", err)
	}
}

func TestApplyDeltaIdempotentAndAggregatesSpace(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	delta := Delta{
		MutationID:  "mutation-1",
		Source:      "partition-0",
		Revision:    "raft-index-1",
		Scope:       DomainScope("space-1", "domain-1"),
		DomainClass: DomainClassRetainedNonSystem,
		ObservedAt:  time.Now().UTC(),
		Metrics: []MetricDelta{
			{Name: MetricBlobActiveCount, Version: 1, Delta: 1},
			{Name: MetricBlobActiveLogicalBytes, Version: 1, Delta: 1024},
			{Name: MetricGraphNodeCount, Version: 1, Delta: 2},
		},
	}
	if err := m.ApplyDelta(ctx, delta); err != nil {
		t.Fatalf("ApplyDelta() error = %v", err)
	}
	if err := m.ApplyDelta(ctx, delta); err != nil {
		t.Fatalf("second ApplyDelta() error = %v", err)
	}
	domain := mustSnapshot(t, m, SnapshotRequest{Scope: DomainScope("space-1", "domain-1"), MetricNames: []string{MetricBlobActiveCount, MetricBlobActiveLogicalBytes, MetricGraphNodeCount}, RequireQuotaSafe: true})
	assertMetricValue(t, domain, MetricBlobActiveCount, 1)
	assertMetricValue(t, domain, MetricBlobActiveLogicalBytes, 1024)
	assertMetricValue(t, domain, MetricGraphNodeCount, 2)
	if !domain.QuotaSafeComplete || domain.State != AccountingStateReady {
		t.Fatalf("domain snapshot readiness = complete:%v state:%s blockers:%#v", domain.QuotaSafeComplete, domain.State, domain.Blockers)
	}
	space := mustSnapshot(t, m, SnapshotRequest{Scope: SpaceScope("space-1"), DomainInclusion: DomainInclusionRetainedNonSystem, MetricNames: []string{MetricBlobActiveCount, MetricBlobActiveLogicalBytes, MetricGraphNodeCount}, RequireQuotaSafe: true})
	assertMetricValue(t, space, MetricBlobActiveCount, 1)
	assertMetricValue(t, space, MetricBlobActiveLogicalBytes, 1024)
	assertMetricValue(t, space, MetricGraphNodeCount, 2)
	if got := space.Revision.Sources; len(got) != 1 || got[0].Source != "partition-0" || got[0].Revision != "raft-index-1" {
		t.Fatalf("source revisions = %#v, want partition source revision", got)
	}
}

func TestSystemDomainExcludedFromDefaultSpaceAggregate(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if err := m.ApplyDelta(ctx, Delta{MutationID: "system-1", Source: "partition-0", Revision: "10", Scope: DomainScope("space-1", "_system"), DomainClass: DomainClassSystem, Metrics: []MetricDelta{{Name: MetricBlobActiveCount, Delta: 3}}}); err != nil {
		t.Fatalf("ApplyDelta() error = %v", err)
	}
	defaultSpace := mustSnapshot(t, m, SnapshotRequest{Scope: SpaceScope("space-1"), DomainInclusion: DomainInclusionRetainedNonSystem, MetricNames: []string{MetricBlobActiveCount}, RequireQuotaSafe: true})
	assertMetricValue(t, defaultSpace, MetricBlobActiveCount, 0)
	systemOnly := mustSnapshot(t, m, SnapshotRequest{Scope: SpaceScope("space-1"), DomainInclusion: DomainInclusionSystemOnly, MetricNames: []string{MetricBlobActiveCount}, RequireQuotaSafe: true})
	assertMetricValue(t, systemOnly, MetricBlobActiveCount, 3)
	all := mustSnapshot(t, m, SnapshotRequest{Scope: SpaceScope("space-1"), DomainInclusion: DomainInclusionRetainedAll, MetricNames: []string{MetricBlobActiveCount}, RequireQuotaSafe: true})
	assertMetricValue(t, all, MetricBlobActiveCount, 3)
}

func TestListUsageScopes(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if err := m.ApplyDelta(ctx, Delta{MutationID: "domain-a", Scope: DomainScope("space-1", "domain-a"), Metrics: []MetricDelta{{Name: MetricGraphNodeCount, Delta: 2}}}); err != nil {
		t.Fatalf("ApplyDelta(domain-a) error = %v", err)
	}
	if err := m.ApplyDelta(ctx, Delta{MutationID: "domain-b", Scope: DomainScope("space-1", "domain-b"), Metrics: []MetricDelta{{Name: MetricGraphEdgeCount, Delta: 1}}}); err != nil {
		t.Fatalf("ApplyDelta(domain-b) error = %v", err)
	}
	spaces, err := m.List(ctx, ListRequest{Mode: ListModeSpaces, MetricNames: []string{MetricGraphNodeCount}})
	if err != nil {
		t.Fatalf("List(spaces) error = %v", err)
	}
	if len(spaces.Snapshots) != 1 || spaces.Snapshots[0].Scope != SpaceScope("space-1") {
		t.Fatalf("space snapshots = %#v, want space-1", spaces.Snapshots)
	}
	domains, err := m.List(ctx, ListRequest{Mode: ListModeDomainsInSpace, Parent: SpaceScope("space-1"), PageSize: 1})
	if err != nil {
		t.Fatalf("List(domains) error = %v", err)
	}
	if len(domains.Snapshots) != 1 || domains.Snapshots[0].Scope != DomainScope("space-1", "domain-a") || domains.NextPageToken == "" {
		t.Fatalf("first domain page = %#v, next=%q", domains.Snapshots, domains.NextPageToken)
	}
	second, err := m.List(ctx, ListRequest{Mode: ListModeDomainsInSpace, Parent: SpaceScope("space-1"), PageSize: 1, PageToken: domains.NextPageToken})
	if err != nil {
		t.Fatalf("List(domains second) error = %v", err)
	}
	if len(second.Snapshots) != 1 || second.Snapshots[0].Scope != DomainScope("space-1", "domain-b") || second.NextPageToken != "" {
		t.Fatalf("second domain page = %#v, next=%q", second.Snapshots, second.NextPageToken)
	}
}

func TestHealthReconciliationDetectsCounterDrift(t *testing.T) {
	ctx := context.Background()
	m := NewModule().WithReconciliationProviders(fakeReconciliationProvider{result: ReconciliationResult{Source: "test", Deltas: []Delta{{Scope: DomainScope("space-1", "domain-1"), DomainClass: DomainClassRetainedNonSystem, Metrics: []MetricDelta{{Name: MetricGraphNodeCount, Version: 1, Delta: 2}}}}}})
	if err := m.ApplyDelta(ctx, Delta{MutationID: "materialized", Scope: DomainScope("space-1", "domain-1"), Metrics: []MetricDelta{{Name: MetricGraphNodeCount, Version: 1, Delta: 1}}}); err != nil {
		t.Fatalf("ApplyDelta() error = %v", err)
	}
	health := m.Health(ctx, HealthRequest{Scope: DomainScope("space-1", "domain-1"), IncludeReconciliation: true})
	if health.State != AccountingStateDriftDetected || health.QuotaSafeReady {
		t.Fatalf("health = %#v, want drift_detected and not quota-safe", health)
	}
	if len(health.Blockers) != 1 || health.Blockers[0].Reason != StatusReasonReconciliationDrift || health.Blockers[0].Metric != MetricGraphNodeCount {
		t.Fatalf("health blockers = %#v, want graph node drift", health.Blockers)
	}
	if got := health.Blockers[0].Evidence["expected"]; got != "2" {
		t.Fatalf("expected evidence = %q, want 2", got)
	}
	if got := health.Blockers[0].Evidence["materialized"]; got != "1" {
		t.Fatalf("materialized evidence = %q, want 1", got)
	}
}

func TestHealthReconciliationHealthyCounters(t *testing.T) {
	ctx := context.Background()
	m := NewModule().WithReconciliationProviders(fakeReconciliationProvider{result: ReconciliationResult{Source: "test", Deltas: []Delta{{Scope: DomainScope("space-1", "domain-1"), DomainClass: DomainClassRetainedNonSystem, Metrics: []MetricDelta{{Name: MetricGraphNodeCount, Version: 1, Delta: 2}}}}}})
	if err := m.ApplyDelta(ctx, Delta{MutationID: "materialized", Scope: DomainScope("space-1", "domain-1"), Metrics: []MetricDelta{{Name: MetricGraphNodeCount, Version: 1, Delta: 2}}}); err != nil {
		t.Fatalf("ApplyDelta() error = %v", err)
	}
	health := m.Health(ctx, HealthRequest{Scope: DomainScope("space-1", "domain-1"), IncludeReconciliation: true})
	if health.State != AccountingStateReady || !health.QuotaSafeReady || len(health.Blockers) != 0 {
		t.Fatalf("health = %#v, want ready without blockers", health)
	}
}

func TestHealthReconciliationTruncatedScanBlocksQuotaSafeReadiness(t *testing.T) {
	ctx := context.Background()
	m := NewModule().WithReconciliationProviders(fakeReconciliationProvider{result: ReconciliationResult{Source: "test", Truncated: true, RecordsScanned: 101}})
	health := m.Health(ctx, HealthRequest{Scope: SpaceScope("space-1"), IncludeReconciliation: true})
	if health.State != AccountingStateUnavailable || health.QuotaSafeReady {
		t.Fatalf("health = %#v, want unavailable and not quota-safe", health)
	}
	if len(health.Blockers) != 1 || health.Blockers[0].Reason != StatusReasonMetricUnavailable || health.Blockers[0].Evidence["truncated"] != "true" {
		t.Fatalf("health blockers = %#v, want truncated reconciliation blocker", health.Blockers)
	}
}

type fakeReconciliationProvider struct {
	result ReconciliationResult
	err    error
}

func (p fakeReconciliationProvider) ReconcileUsage(context.Context, ReconciliationRequest) (ReconciliationResult, error) {
	return p.result, p.err
}

func TestRestoredUsageCounterStateMarksReconcilingAndReconciliationClears(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	original := NewModule()
	if result := original.Init(ctx, testHost{dataDir: dir}); !result.OK {
		t.Fatalf("original Init() = %#v", result)
	}
	if err := original.CompleteRebuild(ctx, nil, Revision{}); err != nil {
		t.Fatalf("CompleteRebuild() error = %v", err)
	}
	if err := original.ApplyDelta(ctx, Delta{MutationID: "mutation-1", Scope: DomainScope("space-1", "domain-1"), Metrics: []MetricDelta{{Name: MetricGraphNodeCount, Delta: 2}}}); err != nil {
		t.Fatalf("ApplyDelta() error = %v", err)
	}
	marker, err := BuildRestoredUsageState(ctx, dir, RestoredUsageStateOptions{BackupSetID: "backup-set-1", RestoreOperationID: "restore-1", RestoredAt: time.Now().UTC()})
	if err != nil {
		t.Fatalf("BuildRestoredUsageState() error = %v", err)
	}
	if !marker.CounterStateIncluded || marker.DesiredAccountingState != AccountingStateReconciling {
		t.Fatalf("marker = %#v, want included reconciling", marker)
	}
	if err := WriteRestoredUsageState(dir, marker); err != nil {
		t.Fatalf("WriteRestoredUsageState() error = %v", err)
	}
	restored := NewModule().WithReconciliationProviders(fakeReconciliationProvider{result: ReconciliationResult{Source: "test", Deltas: []Delta{{Scope: DomainScope("space-1", "domain-1"), Metrics: []MetricDelta{{Name: MetricGraphNodeCount, Version: 1, Delta: 2}}}}}})
	if result := restored.Init(ctx, testHost{dataDir: dir}); !result.OK {
		t.Fatalf("restored Init() = %#v", result)
	}
	if _, ok, err := ReadRestoredUsageState(dir); err != nil || ok {
		t.Fatalf("restored marker after Init ok=%t err=%v, want removed", ok, err)
	}
	snapshot, err := restored.Snapshot(ctx, SnapshotRequest{Scope: DomainScope("space-1", "domain-1"), MetricNames: []string{MetricGraphNodeCount}, RequireQuotaSafe: true})
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if snapshot.State != AccountingStateReconciling || snapshot.QuotaSafeComplete || len(snapshot.Blockers) == 0 {
		t.Fatalf("snapshot = %#v, want reconciling fail-closed", snapshot)
	}
	health := restored.Health(ctx, HealthRequest{Scope: DomainScope("space-1", "domain-1"), IncludeReconciliation: true})
	if health.State != AccountingStateReady || !health.QuotaSafeReady || len(health.Blockers) != 0 {
		t.Fatalf("health = %#v, want ready after matching reconciliation", health)
	}
	ready := mustSnapshot(t, restored, SnapshotRequest{Scope: DomainScope("space-1", "domain-1"), MetricNames: []string{MetricGraphNodeCount}, RequireQuotaSafe: true})
	assertMetricValue(t, ready, MetricGraphNodeCount, 2)
}

func TestRestoredUsageCounterMissingMarksRebuilding(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	marker, err := BuildRestoredUsageState(ctx, dir, RestoredUsageStateOptions{BackupSetID: "backup-set-1"})
	if err != nil {
		t.Fatalf("BuildRestoredUsageState() error = %v", err)
	}
	if marker.CounterStateIncluded || marker.DesiredAccountingState != AccountingStateRebuilding {
		t.Fatalf("marker = %#v, want missing rebuilding", marker)
	}
	if err := WriteRestoredUsageState(dir, marker); err != nil {
		t.Fatalf("WriteRestoredUsageState() error = %v", err)
	}
	restored := NewModule()
	if result := restored.Init(ctx, testHost{dataDir: dir}); !result.OK {
		t.Fatalf("restored Init() = %#v", result)
	}
	health := restored.Health(ctx, HealthRequest{Scope: SpaceScope("space-1")})
	if health.State != AccountingStateRebuilding || health.QuotaSafeReady || len(health.Blockers) == 0 {
		t.Fatalf("health = %#v, want rebuilding fail-closed", health)
	}
}

func TestRestoredUsageCounterCorruptOrStaleMarksRebuilding(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "meta", "usage"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(UsageCounterStatePath(dir), []byte(`not-json`), 0o600); err != nil {
		t.Fatal(err)
	}
	marker, err := BuildRestoredUsageState(ctx, dir, RestoredUsageStateOptions{BackupSetID: "backup-set-1"})
	if err != nil {
		t.Fatalf("BuildRestoredUsageState() error = %v", err)
	}
	if err := WriteRestoredUsageState(dir, marker); err != nil {
		t.Fatalf("WriteRestoredUsageState() error = %v", err)
	}
	restored := NewModule()
	if result := restored.Init(ctx, testHost{dataDir: dir}); !result.OK {
		t.Fatalf("restored corrupt Init() = %#v", result)
	}
	if health := restored.Health(ctx, HealthRequest{Scope: SpaceScope("space-1")}); health.State != AccountingStateRebuilding || health.QuotaSafeReady {
		t.Fatalf("corrupt health = %#v, want rebuilding", health)
	}

	dir = t.TempDir()
	valid := NewModule()
	if result := valid.Init(ctx, testHost{dataDir: dir}); !result.OK {
		t.Fatalf("valid Init() = %#v", result)
	}
	if err := valid.CompleteRebuild(ctx, nil, Revision{}); err != nil {
		t.Fatalf("CompleteRebuild() error = %v", err)
	}
	marker, err = BuildRestoredUsageState(ctx, dir, RestoredUsageStateOptions{BackupSetID: "backup-set-1"})
	if err != nil {
		t.Fatalf("BuildRestoredUsageState(valid) error = %v", err)
	}
	if err := os.WriteFile(UsageCounterStatePath(dir), []byte(`{"version":1,"accounting_state":"ready","updated_at":"2026-01-01T00:00:00Z","sequence":99}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := WriteRestoredUsageState(dir, marker); err != nil {
		t.Fatalf("WriteRestoredUsageState(valid) error = %v", err)
	}
	stale := NewModule()
	if result := stale.Init(ctx, testHost{dataDir: dir}); !result.OK {
		t.Fatalf("stale Init() = %#v", result)
	}
	if health := stale.Health(ctx, HealthRequest{Scope: SpaceScope("space-1")}); health.State != AccountingStateRebuilding || health.QuotaSafeReady {
		t.Fatalf("stale health = %#v, want rebuilding", health)
	}
}

func TestMissingPersistentStateReportsRebuilding(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, testHost{dataDir: t.TempDir()}); !result.OK {
		t.Fatalf("Init() = %#v", result)
	}
	health := m.Health(ctx, HealthRequest{Scope: SpaceScope("space-1")})
	if health.State != AccountingStateRebuilding || health.QuotaSafeReady {
		t.Fatalf("health = %#v, want rebuilding and not quota-safe", health)
	}
	if len(health.Blockers) != 1 || health.Blockers[0].Reason != StatusReasonCountersRebuilding {
		t.Fatalf("health blockers = %#v, want counters_rebuilding", health.Blockers)
	}
	snapshot, err := m.Snapshot(ctx, SnapshotRequest{Scope: SpaceScope("space-1"), MetricNames: []string{MetricBlobActiveCount}, RequireQuotaSafe: true})
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if snapshot.QuotaSafeComplete || len(snapshot.Blockers) == 0 {
		t.Fatalf("snapshot = %#v, want fail-closed blockers", snapshot)
	}
}

func TestPersistentStateRoundTripPreservesCountersAndIdempotency(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	m := NewModule()
	if result := m.Init(ctx, testHost{dataDir: dir}); !result.OK {
		t.Fatalf("Init() = %#v", result)
	}
	if err := m.CompleteRebuild(ctx, nil, Revision{Sources: []SourceRevision{{Source: "rebuild", Revision: "1"}}}); err != nil {
		t.Fatalf("CompleteRebuild() error = %v", err)
	}
	delta := Delta{MutationID: "mutation-1", Source: "partition-0", Revision: "2", Scope: DomainScope("space-1", "domain-1"), Metrics: []MetricDelta{{Name: MetricBlobActiveCount, Delta: 1}}}
	if err := m.ApplyDelta(ctx, delta); err != nil {
		t.Fatalf("ApplyDelta() error = %v", err)
	}

	restarted := NewModule()
	if result := restarted.Init(ctx, testHost{dataDir: dir}); !result.OK {
		t.Fatalf("restart Init() = %#v", result)
	}
	if err := restarted.ApplyDelta(ctx, delta); err != nil {
		t.Fatalf("restart duplicate ApplyDelta() error = %v", err)
	}
	snapshot := mustSnapshot(t, restarted, SnapshotRequest{Scope: SpaceScope("space-1"), DomainInclusion: DomainInclusionRetainedNonSystem, MetricNames: []string{MetricBlobActiveCount}, RequireQuotaSafe: true})
	assertMetricValue(t, snapshot, MetricBlobActiveCount, 1)
}

func TestModuleImplementsRuntimeService(t *testing.T) {
	var _ runtime.Service = (*Module)(nil)
}

func mustSnapshot(t *testing.T, m *Module, req SnapshotRequest) Snapshot {
	t.Helper()
	snapshot, err := m.Snapshot(context.Background(), req)
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	return snapshot
}

func assertMetricValue(t *testing.T, snapshot Snapshot, name string, want uint64) {
	t.Helper()
	for _, metric := range snapshot.Metrics {
		if metric.Name == name {
			if metric.Value != want {
				t.Fatalf("metric %s value = %d, want %d (snapshot %#v)", name, metric.Value, want, snapshot)
			}
			return
		}
	}
	t.Fatalf("metric %s not found in snapshot %#v", name, snapshot)
}
