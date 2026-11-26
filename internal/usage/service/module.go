package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/myceldb/mycel/internal/fsperm"
	"github.com/myceldb/mycel/internal/runtime"
)

const stateVersion = 1
const defaultReconciliationMaxRecords uint64 = 100000

var ErrCountersRebuilding = errors.New("usage counters are rebuilding")
var errUsageCounterUnderflow = errors.New("usage counter underflow")

type Module struct {
	mu                   sync.RWMutex
	logger               *slog.Logger
	dataDir              string
	hostDataDir          string
	statePath            string
	registry             Registry
	reconcilers          []ReconciliationProvider
	diagnosticCollectors []DiagnosticCollector
	reservationTotals    map[counterKey]uint64
	state                usageState
}

type usageState struct {
	AccountingState AccountingState
	UpdatedAt       time.Time
	Sequence        uint64
	Counters        map[counterKey]uint64
	Applied         map[string]appliedMutation
	SourceRevisions map[string]string
	Limits          map[string]UsageLimit
	RebuildReason   string
}

type counterKey struct {
	Scope           Scope
	DomainInclusion DomainInclusionMode
	MetricName      string
	MetricVersion   uint32
}

type appliedMutation struct {
	MutationID string    `json:"mutation_id"`
	Source     string    `json:"source,omitempty"`
	Revision   string    `json:"revision,omitempty"`
	AppliedAt  time.Time `json:"applied_at"`
}

type persistentState struct {
	Version         int                 `json:"version"`
	AccountingState AccountingState     `json:"accounting_state"`
	UpdatedAt       time.Time           `json:"updated_at"`
	Sequence        uint64              `json:"sequence"`
	Counters        []persistentCounter `json:"counters"`
	Applied         []appliedMutation   `json:"applied_mutations,omitempty"`
	SourceRevisions map[string]string   `json:"source_revisions,omitempty"`
	Limits          []persistentLimit   `json:"limits,omitempty"`
	RebuildReason   string              `json:"rebuild_reason,omitempty"`
}

// persistentLimit stores hard usage-admission limits next to the materialized
// counters so backup/restore and ordinary daemon restarts preserve configured
// write gates without a second secret-bearing metadata plane. Limit records only
// contain metric names, scopes, and non-secret operator references.
type persistentLimit struct {
	LimitID         string              `json:"limit_id"`
	Scope           Scope               `json:"scope"`
	DomainInclusion DomainInclusionMode `json:"domain_inclusion,omitempty"`
	MetricName      string              `json:"metric_name"`
	MetricVersion   uint32              `json:"metric_version"`
	HardLimit       uint64              `json:"hard_limit"`
	Enabled         bool                `json:"enabled"`
	PolicyRef       string              `json:"policy_ref,omitempty"`
	Description     string              `json:"description,omitempty"`
	CreatedAt       time.Time           `json:"created_at,omitempty"`
	UpdatedAt       time.Time           `json:"updated_at,omitempty"`
}

type persistentCounter struct {
	Scope           Scope               `json:"scope"`
	DomainInclusion DomainInclusionMode `json:"domain_inclusion,omitempty"`
	MetricName      string              `json:"metric_name"`
	MetricVersion   uint32              `json:"metric_version"`
	Value           uint64              `json:"value"`
}

func NewModule(registries ...Registry) *Module {
	registry := DefaultRegistry()
	if len(registries) > 0 {
		registry = registries[0]
	}
	now := time.Now().UTC()
	return &Module{registry: registry, reservationTotals: map[counterKey]uint64{}, state: usageState{AccountingState: AccountingStateReady, UpdatedAt: now, Counters: map[counterKey]uint64{}, Applied: map[string]appliedMutation{}, SourceRevisions: map[string]string{}, Limits: map[string]UsageLimit{}}}
}

func (m *Module) Name() string { return ModuleName }

func (m *Module) Init(ctx context.Context, host runtime.Host) runtime.InitResult {
	m.logger = host.Log()
	m.hostDataDir = host.DataDir()
	m.dataDir = filepath.Join(host.DataDir(), "meta", "usage")
	m.statePath = filepath.Join(m.dataDir, "counters.json")
	m.mu.Lock()
	if len(m.diagnosticCollectors) == 0 {
		m.diagnosticCollectors = []DiagnosticCollector{NewLocalDiagnosticCollector(m.hostDataDir)}
	}
	m.mu.Unlock()
	if err := os.MkdirAll(m.dataDir, fsperm.PrivateDir); err != nil {
		return runtime.Abort(ModuleName, "storage", "create usage metadata directory", err)
	}
	_, restoredUsageMarkerPresent, restoredUsageMarkerErr := ReadRestoredUsageState(host.DataDir())
	if restoredUsageMarkerPresent || restoredUsageMarkerErr != nil {
		if err := m.applyRestoredUsageStateMarker(ctx, host.DataDir()); err != nil {
			return runtime.Abort(ModuleName, "storage", "apply restored usage accounting marker", err)
		}
	} else if err := m.load(ctx); err != nil {
		return runtime.Abort(ModuleName, "storage", "load usage counters", err)
	}
	return runtime.OK(ModuleName)
}

func (m *Module) Registry() Registry { return m.registry }

func (m *Module) WithReconciliationProviders(providers ...ReconciliationProvider) *Module {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reconcilers = append([]ReconciliationProvider(nil), providers...)
	return m
}

func (m *Module) WithDiagnosticCollectors(collectors ...DiagnosticCollector) *Module {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.diagnosticCollectors = append([]DiagnosticCollector(nil), collectors...)
	return m
}

func (m *Module) ApplyDelta(ctx context.Context, delta Delta) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(delta.MutationID) == "" {
		return fmt.Errorf("usage delta mutation_id is required")
	}
	scope := delta.Scope.normalized()
	if err := scope.Validate(); err != nil {
		return err
	}
	if len(delta.Metrics) == 0 {
		return fmt.Errorf("usage delta must include at least one metric")
	}
	domainClass := delta.DomainClass
	if domainClass == "" {
		domainClass = DomainClassRetainedNonSystem
	}
	if domainClass != DomainClassRetainedNonSystem && domainClass != DomainClassSystem {
		return fmt.Errorf("unsupported usage domain class %q", domainClass)
	}
	observedAt := delta.ObservedAt.UTC()
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.state.Applied[delta.MutationID]; ok {
		return nil
	}
	next := m.cloneStateLocked()
	if next.Counters == nil {
		next.Counters = map[counterKey]uint64{}
	}
	for _, metricDelta := range delta.Metrics {
		if metricDelta.Version == 0 {
			metricDelta.Version = 1
		}
		if _, err := m.registry.MustDefinition(metricDelta.Name, metricDelta.Version); err != nil {
			return err
		}
		keys := aggregateKeys(scope, domainClass, metricDelta)
		for _, key := range keys {
			current := next.Counters[key]
			value, err := applySignedDelta(current, metricDelta.Delta)
			if err != nil {
				if next.AccountingState == AccountingStateRebuilding && errors.Is(err, errUsageCounterUnderflow) {
					value = 0
				} else {
					return fmt.Errorf("apply usage delta for %s: %w", metricDelta.Name, err)
				}
			}
			next.Counters[key] = value
		}
	}
	next.Sequence++
	next.UpdatedAt = observedAt
	if next.AccountingState == "" {
		next.AccountingState = AccountingStateReady
	}
	if next.Applied == nil {
		next.Applied = map[string]appliedMutation{}
	}
	next.Applied[delta.MutationID] = appliedMutation{MutationID: delta.MutationID, Source: delta.Source, Revision: delta.Revision, AppliedAt: observedAt}
	if next.SourceRevisions == nil {
		next.SourceRevisions = map[string]string{}
	}
	if strings.TrimSpace(delta.Source) != "" && strings.TrimSpace(delta.Revision) != "" {
		next.SourceRevisions[strings.TrimSpace(delta.Source)] = strings.TrimSpace(delta.Revision)
	}
	if err := m.persistLocked(next); err != nil {
		return err
	}
	m.state = next
	return nil
}

func (m *Module) CompleteRebuild(ctx context.Context, counters []CounterValue, revision Revision) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	now := time.Now().UTC()
	m.mu.Lock()
	defer m.mu.Unlock()
	limits := cloneLimits(m.state.Limits)
	next := usageState{AccountingState: AccountingStateReady, UpdatedAt: now, Sequence: 1, Counters: map[counterKey]uint64{}, Applied: map[string]appliedMutation{}, SourceRevisions: map[string]string{}, Limits: limits}
	for _, src := range revision.Sources {
		if strings.TrimSpace(src.Source) != "" && strings.TrimSpace(src.Revision) != "" {
			next.SourceRevisions[strings.TrimSpace(src.Source)] = strings.TrimSpace(src.Revision)
		}
	}
	for _, counter := range counters {
		if counter.MetricVersion == 0 {
			counter.MetricVersion = 1
		}
		if _, err := m.registry.MustDefinition(counter.MetricName, counter.MetricVersion); err != nil {
			return err
		}
		scope := counter.Scope.normalized()
		if err := scope.Validate(); err != nil {
			return err
		}
		key := counterKey{Scope: scope, DomainInclusion: normalizeDomainInclusion(scope, counter.DomainInclusion), MetricName: counter.MetricName, MetricVersion: counter.MetricVersion}
		next.Counters[key] = counter.Value
	}
	if err := m.persistLocked(next); err != nil {
		return err
	}
	m.state = next
	return nil
}

func (m *Module) MarkReconciling(ctx context.Context, reason string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	next := m.cloneStateLocked()
	next.AccountingState = AccountingStateReconciling
	next.RebuildReason = strings.TrimSpace(reason)
	next.UpdatedAt = time.Now().UTC()
	if err := m.persistLocked(next); err != nil {
		return err
	}
	m.state = next
	return nil
}

func (m *Module) MarkRebuilding(ctx context.Context, reason string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	next := m.cloneStateLocked()
	next.AccountingState = AccountingStateRebuilding
	next.RebuildReason = strings.TrimSpace(reason)
	next.UpdatedAt = time.Now().UTC()
	if err := m.persistLocked(next); err != nil {
		return err
	}
	m.state = next
	return nil
}

func (m *Module) Snapshot(ctx context.Context, req SnapshotRequest) (Snapshot, error) {
	scope := req.Scope.normalized()
	if err := scope.Validate(); err != nil {
		return Snapshot{}, err
	}
	inclusion := normalizeDomainInclusion(scope, req.DomainInclusion)
	m.mu.RLock()
	state := m.cloneStateLocked()
	diagnosticCollectors := append([]DiagnosticCollector(nil), m.diagnosticCollectors...)
	m.mu.RUnlock()
	rev := state.revision()
	snapshot := Snapshot{Scope: scope, DomainInclusion: inclusion, ObservedAt: state.UpdatedAt, Revision: rev, State: state.AccountingState, QuotaSafeComplete: state.AccountingState == AccountingStateReady}
	if state.AccountingState != AccountingStateReady {
		snapshot.Blockers = append(snapshot.Blockers, rebuildingStatus(scope, state))
		snapshot.QuotaSafeComplete = false
	}
	if req.MaxStaleness > 0 && !state.UpdatedAt.IsZero() && time.Since(state.UpdatedAt) > req.MaxStaleness {
		snapshot.Blockers = append(snapshot.Blockers, Status{Reason: StatusReasonCounterStale, Severity: StatusSeverityBlocker, Message: "usage counters are stale", Scope: scope, Evidence: map[string]string{"max_staleness": req.MaxStaleness.String()}})
		snapshot.QuotaSafeComplete = false
	}
	if req.MinimumRevision.Token != "" && req.MinimumRevision.Token != rev.Token {
		snapshot.Blockers = append(snapshot.Blockers, Status{Reason: StatusReasonUnsupportedRevision, Severity: StatusSeverityBlocker, Message: "requested usage revision is not available", Scope: scope, Evidence: map[string]string{"requested_revision": req.MinimumRevision.Token, "available_revision": rev.Token}})
		snapshot.QuotaSafeComplete = false
	}
	metricNames := req.MetricNames
	if len(metricNames) == 0 {
		metricNames = metricNamesForScope(state.Counters, scope, inclusion)
	}
	for _, name := range metricNames {
		def, ok := m.registry.Definition(name, 1)
		if !ok {
			status := Status{Reason: StatusReasonMetricUnavailable, Severity: StatusSeverityBlocker, Message: "usage metric is not registered", Scope: scope, Metric: name}
			snapshot.Blockers = append(snapshot.Blockers, status)
			snapshot.QuotaSafeComplete = false
			continue
		}
		if isDiagnosticMetricDefinition(def) {
			if req.RequireQuotaSafe {
				status := Status{Reason: StatusReasonMetricNotQuotaSafe, Severity: StatusSeverityBlocker, Message: "diagnostic physical usage metric is not quota-safe", Scope: scope, Metric: name}
				snapshot.Blockers = append(snapshot.Blockers, status)
				snapshot.QuotaSafeComplete = false
			}
			if !req.IncludeDiagnosticPhysical {
				status := Status{Reason: StatusReasonDiagnosticCollectorUnavailable, Severity: StatusSeverityWarning, Message: "diagnostic physical usage metric requires include_diagnostic_physical", Scope: scope, Metric: name}
				snapshot.Warnings = append(snapshot.Warnings, status)
			}
			continue
		}
		if req.RequireQuotaSafe && !def.QuotaSafe {
			status := Status{Reason: StatusReasonMetricNotQuotaSafe, Severity: StatusSeverityBlocker, Message: "usage metric is not quota-safe", Scope: scope, Metric: name}
			snapshot.Blockers = append(snapshot.Blockers, status)
			snapshot.QuotaSafeComplete = false
			continue
		}
		key := counterKey{Scope: scope, DomainInclusion: inclusion, MetricName: name, MetricVersion: def.Version}
		value := state.Counters[key]
		snapshot.Metrics = append(snapshot.Metrics, metricFromDefinition(def, value, state.UpdatedAt, rev))
	}
	if req.IncludeDiagnosticPhysical {
		m.appendDiagnosticSnapshot(ctx, req, diagnosticCollectors, &snapshot)
	}
	sort.Slice(snapshot.Metrics, func(i, j int) bool { return snapshot.Metrics[i].Name < snapshot.Metrics[j].Name })
	return snapshot, nil
}

func (m *Module) List(ctx context.Context, req ListRequest) (ListResponse, error) {
	if err := ctx.Err(); err != nil {
		return ListResponse{}, err
	}
	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = 100
	}
	if pageSize > 1000 {
		pageSize = 1000
	}
	offset := 0
	if strings.TrimSpace(req.PageToken) != "" {
		parsed, err := strconv.Atoi(strings.TrimSpace(req.PageToken))
		if err != nil || parsed < 0 {
			return ListResponse{}, fmt.Errorf("page_token must be a non-negative integer offset")
		}
		offset = parsed
	}
	mode := req.Mode
	if mode == "" {
		mode = ListModeSpaces
	}
	m.mu.RLock()
	state := m.cloneStateLocked()
	m.mu.RUnlock()
	if req.RequireQuotaSafe && state.AccountingState != AccountingStateReady {
		return ListResponse{}, ErrCountersRebuilding
	}
	scopes, err := listScopesForState(state, req.Parent.normalized(), mode)
	if err != nil {
		return ListResponse{}, err
	}
	if offset > len(scopes) {
		return ListResponse{}, fmt.Errorf("page_token offset is beyond the usage list")
	}
	end := offset + pageSize
	if end > len(scopes) {
		end = len(scopes)
	}
	response := ListResponse{Snapshots: make([]Snapshot, 0, end-offset)}
	for _, scope := range scopes[offset:end] {
		snapshot, err := m.Snapshot(ctx, SnapshotRequest{Scope: scope, DomainInclusion: req.DomainInclusion, MetricNames: req.MetricNames, RequireQuotaSafe: req.RequireQuotaSafe, IncludeDiagnosticPhysical: req.IncludeDiagnosticPhysical, MaxStaleness: req.MaxStaleness, MinimumRevision: req.MinimumRevision})
		if err != nil {
			return ListResponse{}, err
		}
		response.Snapshots = append(response.Snapshots, snapshot)
	}
	if end < len(scopes) {
		response.NextPageToken = strconv.Itoa(end)
	}
	return response, nil
}

func (m *Module) Health(ctx context.Context, req HealthRequest) Health {
	scope := req.Scope.normalized()
	m.mu.RLock()
	state := m.cloneStateLocked()
	reconcilers := append([]ReconciliationProvider(nil), m.reconcilers...)
	diagnosticCollectors := append([]DiagnosticCollector(nil), m.diagnosticCollectors...)
	m.mu.RUnlock()
	health := Health{Scope: scope, ObservedAt: state.UpdatedAt, State: state.AccountingState, Revision: state.revision(), QuotaSafeReady: state.AccountingState == AccountingStateReady}
	if state.AccountingState != AccountingStateReady {
		health.Blockers = append(health.Blockers, rebuildingStatus(scope, state))
	}
	if state.AccountingState == AccountingStateRebuilding || state.AccountingState == AccountingStateReconciling {
		health.RebuildingScopeCount = 1
	}
	if state.AccountingState == AccountingStateStale {
		health.StaleScopeCount = 1
	}
	if state.AccountingState == AccountingStateDriftDetected {
		health.DriftScopeCount = 1
	}
	if req.IncludeReconciliation {
		m.applyReconciliationHealth(ctx, req, state, reconcilers, &health)
	}
	if req.IncludeDiagnosticCollectors {
		m.applyDiagnosticHealth(ctx, req, diagnosticCollectors, &health)
	}
	return health
}

func (m *Module) appendDiagnosticSnapshot(ctx context.Context, req SnapshotRequest, collectors []DiagnosticCollector, snapshot *Snapshot) {
	if snapshot == nil {
		return
	}
	if len(collectors) == 0 {
		snapshot.Warnings = append(snapshot.Warnings, Status{Reason: StatusReasonDiagnosticCollectorUnavailable, Severity: StatusSeverityWarning, Message: "diagnostic physical collectors are not configured", Scope: req.Scope.normalized()})
		return
	}
	allowed := diagnosticMetricFilter(req.MetricNames)
	for _, collector := range collectors {
		if collector == nil {
			continue
		}
		result, err := collector.CollectUsageDiagnostics(ctx, DiagnosticRequest{Scope: req.Scope.normalized(), MaxEntries: defaultDiagnosticMaxEntries})
		if err != nil {
			snapshot.Warnings = append(snapshot.Warnings, Status{Reason: StatusReasonDiagnosticCollectorUnavailable, Severity: StatusSeverityWarning, Message: "diagnostic physical collector failed", Scope: req.Scope.normalized(), Evidence: map[string]string{"error": err.Error()}})
			continue
		}
		for _, status := range result.Statuses {
			snapshot.Warnings = append(snapshot.Warnings, status)
		}
		for _, metric := range result.Metrics {
			if len(allowed) > 0 && !allowed[metric.Name] {
				continue
			}
			if _, ok := m.registry.Definition(metric.Name, metric.Version); !ok {
				continue
			}
			snapshot.Metrics = append(snapshot.Metrics, metric)
		}
	}
}

func (m *Module) applyDiagnosticHealth(ctx context.Context, req HealthRequest, collectors []DiagnosticCollector, health *Health) {
	if health == nil {
		return
	}
	if len(collectors) == 0 {
		health.DiagnosticCollectorUnavailableCount++
		health.Warnings = append(health.Warnings, Status{Reason: StatusReasonDiagnosticCollectorUnavailable, Severity: StatusSeverityWarning, Message: "diagnostic physical collectors are not configured", Scope: req.Scope.normalized()})
		return
	}
	for _, collector := range collectors {
		if collector == nil {
			continue
		}
		result, err := collector.CollectUsageDiagnostics(ctx, DiagnosticRequest{Scope: req.Scope.normalized(), MaxEntries: defaultDiagnosticMaxEntries})
		if err != nil {
			health.DiagnosticCollectorUnavailableCount++
			health.Warnings = append(health.Warnings, Status{Reason: StatusReasonDiagnosticCollectorUnavailable, Severity: StatusSeverityWarning, Message: "diagnostic physical collector failed", Scope: req.Scope.normalized(), Evidence: map[string]string{"source": result.Source, "error": err.Error()}})
			continue
		}
		for _, status := range result.Statuses {
			health.DiagnosticCollectorUnavailableCount++
			health.Warnings = append(health.Warnings, status)
		}
		if result.Truncated && len(result.Statuses) == 0 {
			health.DiagnosticCollectorUnavailableCount++
			health.Warnings = append(health.Warnings, Status{Reason: StatusReasonDiagnosticCollectorUnavailable, Severity: StatusSeverityWarning, Message: "diagnostic physical collector was truncated", Scope: req.Scope.normalized(), Evidence: result.Evidence})
		}
	}
}

func diagnosticMetricFilter(names []string) map[string]bool {
	if len(names) == 0 {
		return nil
	}
	out := map[string]bool{}
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name != "" {
			out[name] = true
		}
	}
	return out
}

func (m *Module) applyReconciliationHealth(ctx context.Context, req HealthRequest, state usageState, reconcilers []ReconciliationProvider, health *Health) {
	if health == nil {
		return
	}
	if err := ctx.Err(); err != nil {
		health.Blockers = append(health.Blockers, Status{Reason: StatusReasonMetricUnavailable, Severity: StatusSeverityBlocker, Message: "usage reconciliation was canceled", Scope: req.Scope.normalized(), Evidence: map[string]string{"error": err.Error()}})
		health.QuotaSafeReady = false
		health.State = AccountingStateUnavailable
		return
	}
	if len(reconcilers) == 0 {
		health.Blockers = append(health.Blockers, Status{Reason: StatusReasonMetricUnavailable, Severity: StatusSeverityBlocker, Message: "usage reconciliation providers are not configured", Scope: req.Scope.normalized()})
		health.QuotaSafeReady = false
		health.State = AccountingStateUnavailable
		return
	}
	maxRecords := defaultReconciliationMaxRecords
	expected := map[counterKey]uint64{}
	reconciliationProblem := false
	for _, reconciler := range reconcilers {
		if reconciler == nil {
			continue
		}
		result, err := reconciler.ReconcileUsage(ctx, ReconciliationRequest{Scope: req.Scope.normalized(), MaxRecords: maxRecords})
		if err != nil {
			reconciliationProblem = true
			health.Blockers = append(health.Blockers, Status{Reason: StatusReasonMetricUnavailable, Severity: StatusSeverityBlocker, Message: "usage reconciliation provider failed", Scope: req.Scope.normalized(), Evidence: map[string]string{"source": result.Source, "error": err.Error()}})
			health.QuotaSafeReady = false
			health.State = AccountingStateUnavailable
			continue
		}
		if result.Truncated {
			reconciliationProblem = true
			health.Blockers = append(health.Blockers, Status{Reason: StatusReasonMetricUnavailable, Severity: StatusSeverityBlocker, Message: "usage reconciliation scan was truncated", Scope: req.Scope.normalized(), Evidence: reconciliationEvidence(result)})
			health.QuotaSafeReady = false
			health.State = AccountingStateUnavailable
			continue
		}
		for _, delta := range result.Deltas {
			domainClass := delta.DomainClass
			if domainClass == "" {
				domainClass = DomainClassRetainedNonSystem
			}
			for _, metric := range delta.Metrics {
				if metric.Version == 0 {
					metric.Version = 1
				}
				if _, ok := m.registry.Definition(metric.Name, metric.Version); !ok {
					continue
				}
				for _, key := range aggregateKeys(delta.Scope.normalized(), domainClass, metric) {
					current := expected[key]
					value, err := applySignedDelta(current, metric.Delta)
					if err != nil {
						reconciliationProblem = true
						health.Blockers = append(health.Blockers, Status{Reason: StatusReasonReconciliationDrift, Severity: StatusSeverityBlocker, Message: "usage reconciliation expected counter underflow", Scope: key.Scope, Metric: metric.Name, Evidence: map[string]string{"source": result.Source, "delta": fmt.Sprintf("%d", metric.Delta), "current": fmt.Sprintf("%d", current)}})
						health.QuotaSafeReady = false
						if health.State != AccountingStateUnavailable {
							health.State = AccountingStateDriftDetected
						}
						continue
					}
					expected[key] = value
				}
			}
		}
	}
	if !reconciliationProblem {
		updatedState, statuses, err := m.backfillReconciliationEligibleCounters(ctx, req.Scope.normalized(), state, expected)
		if err != nil {
			reconciliationProblem = true
			health.Warnings = append(health.Warnings, Status{Reason: StatusReasonMetricUnavailable, Severity: StatusSeverityWarning, Message: "usage reconciliation backfill could not be persisted", Scope: req.Scope.normalized(), Evidence: map[string]string{"error": err.Error()}})
		} else {
			state = updatedState
			health.ObservedAt = state.UpdatedAt
			health.Revision = state.revision()
			health.Warnings = append(health.Warnings, statuses...)
		}
	}
	for key, expectedValue := range expected {
		if !reconciliationKeyInScope(key, req.Scope.normalized()) {
			continue
		}
		actual := state.Counters[key]
		if actual == expectedValue {
			continue
		}
		reconciliationProblem = true
		health.DriftScopeCount++
		health.QuotaSafeReady = false
		if health.State != AccountingStateUnavailable {
			health.State = AccountingStateDriftDetected
		}
		health.Blockers = append(health.Blockers, Status{Reason: StatusReasonReconciliationDrift, Severity: StatusSeverityBlocker, Message: "usage counter drift detected", Scope: key.Scope, Metric: key.MetricName, Evidence: map[string]string{"domain_inclusion": string(key.DomainInclusion), "expected": fmt.Sprintf("%d", expectedValue), "materialized": fmt.Sprintf("%d", actual), "metric_version": fmt.Sprintf("%d", key.MetricVersion)}})
	}
	if !reconciliationProblem && (state.AccountingState == AccountingStateReconciling || state.AccountingState == AccountingStateRebuilding || state.AccountingState == AccountingStateDriftDetected) {
		if err := m.markReadyAfterReconciliation(ctx); err != nil {
			health.Warnings = append(health.Warnings, Status{Reason: StatusReasonMetricUnavailable, Severity: StatusSeverityWarning, Message: "usage reconciliation matched but readiness state could not be persisted", Scope: req.Scope.normalized(), Evidence: map[string]string{"error": err.Error()}})
			return
		}
		m.mu.RLock()
		readyState := m.cloneStateLocked()
		m.mu.RUnlock()
		health.State = AccountingStateReady
		health.ObservedAt = readyState.UpdatedAt
		health.Revision = readyState.revision()
		health.QuotaSafeReady = true
		health.RebuildingScopeCount = 0
		health.DriftScopeCount = 0
		health.Blockers = filterReconciliationClearedBlockers(health.Blockers)
	}
}

func filterReconciliationClearedBlockers(blockers []Status) []Status {
	out := blockers[:0]
	for _, blocker := range blockers {
		if blocker.Reason == StatusReasonCountersRebuilding {
			continue
		}
		out = append(out, blocker)
	}
	return out
}

func reconciliationKeyInScope(key counterKey, scope Scope) bool {
	scope = scope.normalized()
	if scope.Type == "" {
		return true
	}
	return key.Scope == scope
}

func backfillKeyInScope(key counterKey, scope Scope) bool {
	scope = scope.normalized()
	switch scope.Type {
	case ScopeTypeDomain:
		return key.Scope == scope
	case ScopeTypeSpace:
		if key.Scope == scope {
			return true
		}
		return key.Scope.Type == ScopeTypeDomain && strings.TrimSpace(key.Scope.SpaceID) == strings.TrimSpace(scope.SpaceID)
	default:
		return true
	}
}

func (m *Module) backfillReconciliationEligibleCounters(ctx context.Context, scope Scope, state usageState, expected map[counterKey]uint64) (usageState, []Status, error) {
	if err := ctx.Err(); err != nil {
		return state, nil, err
	}
	if state.AccountingState == AccountingStateRebuilding || len(state.Counters) == 0 || len(expected) == 0 {
		return state, nil, nil
	}
	keys := make([]counterKey, 0, len(expected))
	for key := range expected {
		if !backfillKeyInScope(key, scope) || !m.reconciliationBackfillEligible(key) {
			continue
		}
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		return state, nil, nil
	}
	sort.Slice(keys, func(i, j int) bool { return counterKeySortKey(keys[i]) < counterKeySortKey(keys[j]) })

	m.mu.Lock()
	defer m.mu.Unlock()
	current := m.cloneStateLocked()
	if current.AccountingState == AccountingStateRebuilding || len(current.Counters) == 0 {
		return current, nil, nil
	}
	statuses := make([]Status, 0, len(keys))
	now := time.Now().UTC()
	for _, key := range keys {
		mutationID := usageBackfillMutationID(key)
		if _, ok := current.Applied[mutationID]; ok {
			continue
		}
		expectedValue := expected[key]
		if _, hadBefore := current.Counters[key]; hadBefore {
			continue
		}
		before := "missing"
		current.Counters[key] = expectedValue
		if current.Applied == nil {
			current.Applied = map[string]appliedMutation{}
		}
		current.Applied[mutationID] = appliedMutation{MutationID: mutationID, Source: "usage:reconciliation_backfill", Revision: current.revision().Token, AppliedAt: now}
		statuses = append(statuses, Status{Reason: StatusReasonReconciliationDrift, Severity: StatusSeverityInfo, Message: "usage counter backfilled from reconciliation evidence", Scope: key.Scope, Metric: key.MetricName, Evidence: map[string]string{"action": "backfill", "domain_inclusion": string(key.DomainInclusion), "before": before, "after": fmt.Sprintf("%d", expectedValue), "metric_version": fmt.Sprintf("%d", key.MetricVersion), "mutation_id": mutationID}})
	}
	if len(statuses) == 0 {
		return current, nil, nil
	}
	current.Sequence++
	current.UpdatedAt = now
	if err := m.persistLocked(current); err != nil {
		return state, nil, err
	}
	m.state = current
	return current, statuses, nil
}

func (m *Module) reconciliationBackfillEligible(key counterKey) bool {
	def, ok := m.registry.Definition(key.MetricName, key.MetricVersion)
	return ok && def.QuotaSafe && def.ReconciliationBackfillEligible && !isDiagnosticMetricDefinition(def)
}

func usageBackfillMutationID(key counterKey) string {
	return "usage-backfill:v1:" + counterKeySortKey(key)
}

func counterKeySortKey(key counterKey) string {
	return fmt.Sprintf("%s|%s|%s|%s|%s|%d", key.Scope.Type, key.Scope.SpaceID, key.Scope.DomainID, key.DomainInclusion, key.MetricName, key.MetricVersion)
}

func reconciliationEvidence(result ReconciliationResult) map[string]string {
	evidence := map[string]string{"source": result.Source, "scopes_scanned": fmt.Sprintf("%d", result.ScopesScanned), "records_scanned": fmt.Sprintf("%d", result.RecordsScanned)}
	if result.Truncated {
		evidence["truncated"] = "true"
	}
	for key, value := range result.Evidence {
		evidence[key] = value
	}
	return evidence
}

func (m *Module) markReadyAfterReconciliation(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state.AccountingState == AccountingStateReady {
		return nil
	}
	next := m.cloneStateLocked()
	next.AccountingState = AccountingStateReady
	next.RebuildReason = ""
	next.UpdatedAt = time.Now().UTC()
	next.Sequence++
	if err := m.persistLocked(next); err != nil {
		return err
	}
	m.state = next
	return nil
}

func (m *Module) load(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	raw, err := os.ReadFile(m.statePath)
	if errors.Is(err, os.ErrNotExist) {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.state = usageState{AccountingState: AccountingStateRebuilding, UpdatedAt: time.Now().UTC(), Counters: map[counterKey]uint64{}, Applied: map[string]appliedMutation{}, SourceRevisions: map[string]string{}, Limits: map[string]UsageLimit{}, RebuildReason: "usage counter state is missing"}
		return nil
	}
	if err != nil {
		return err
	}
	state, err := m.parsePersistentState(raw)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = state
	return nil
}

func (m *Module) applyRestoredUsageStateMarker(ctx context.Context, hostDataDir string) error {
	marker, ok, markerErr := ReadRestoredUsageState(hostDataDir)
	if !ok {
		return nil
	}
	if markerErr != nil {
		return m.restoreUsageStateRebuilding(ctx, hostDataDir, "restored usage accounting marker is unreadable: "+markerErr.Error())
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !marker.CounterStateIncluded || marker.DesiredAccountingState == AccountingStateRebuilding {
		reason := strings.TrimSpace(marker.Reason)
		if reason == "" {
			reason = "restored usage counter state is marked rebuild-required"
		}
		return m.restoreUsageStateRebuilding(ctx, hostDataDir, reason)
	}
	raw, err := os.ReadFile(m.statePath)
	if err != nil {
		reason := "restored usage counter state is missing"
		if !errors.Is(err, os.ErrNotExist) {
			reason = "restored usage counter state is unreadable: " + err.Error()
		}
		return m.restoreUsageStateRebuilding(ctx, hostDataDir, reason)
	}
	if checksum := strings.TrimSpace(marker.CounterStateChecksumSHA256); checksum != "" {
		actual, present, err := UsageCounterStateChecksum(ctx, hostDataDir)
		if err != nil {
			return m.restoreUsageStateRebuilding(ctx, hostDataDir, "restored usage counter checksum could not be verified: "+err.Error())
		}
		if !present || actual != checksum {
			return m.restoreUsageStateRebuilding(ctx, hostDataDir, "restored usage counter checksum mismatch")
		}
	}
	state, err := m.parsePersistentState(raw)
	if err != nil {
		return m.restoreUsageStateRebuilding(ctx, hostDataDir, "restored usage counter state is invalid: "+err.Error())
	}
	state.AccountingState = AccountingStateReconciling
	state.RebuildReason = firstNonEmpty(strings.TrimSpace(marker.Reason), "restored usage counters require reconciliation before quota-safe readiness")
	state.UpdatedAt = time.Now().UTC()
	m.mu.Lock()
	persistErr := m.persistLocked(state)
	if persistErr == nil {
		m.state = state
	}
	m.mu.Unlock()
	if persistErr != nil {
		return persistErr
	}
	return removeRestoredUsageStateMarker(hostDataDir)
}

func (m *Module) restoreUsageStateRebuilding(ctx context.Context, hostDataDir string, reason string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	now := time.Now().UTC()
	state := usageState{AccountingState: AccountingStateRebuilding, UpdatedAt: now, Sequence: 1, Counters: map[counterKey]uint64{}, Applied: map[string]appliedMutation{}, SourceRevisions: map[string]string{}, Limits: map[string]UsageLimit{}, RebuildReason: strings.TrimSpace(reason)}
	if state.RebuildReason == "" {
		state.RebuildReason = "restored usage counters require rebuild"
	}
	m.mu.Lock()
	persistErr := m.persistLocked(state)
	if persistErr == nil {
		m.state = state
	}
	m.mu.Unlock()
	if persistErr != nil {
		return persistErr
	}
	return removeRestoredUsageStateMarker(hostDataDir)
}

func removeRestoredUsageStateMarker(hostDataDir string) error {
	path := RestoredUsageStatePath(hostDataDir)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (m *Module) parsePersistentState(raw []byte) (usageState, error) {
	var persisted persistentState
	if err := json.Unmarshal(raw, &persisted); err != nil {
		return usageState{}, err
	}
	if persisted.Version != stateVersion {
		return usageState{}, fmt.Errorf("unsupported usage counter state version %d", persisted.Version)
	}
	state := usageState{AccountingState: persisted.AccountingState, UpdatedAt: persisted.UpdatedAt, Sequence: persisted.Sequence, Counters: map[counterKey]uint64{}, Applied: map[string]appliedMutation{}, SourceRevisions: map[string]string{}, Limits: map[string]UsageLimit{}, RebuildReason: persisted.RebuildReason}
	if state.AccountingState == "" {
		state.AccountingState = AccountingStateReady
	}
	for _, counter := range persisted.Counters {
		key := counterKey{Scope: counter.Scope.normalized(), DomainInclusion: normalizeDomainInclusion(counter.Scope, counter.DomainInclusion), MetricName: counter.MetricName, MetricVersion: counter.MetricVersion}
		if key.MetricVersion == 0 {
			key.MetricVersion = 1
		}
		if _, err := m.registry.MustDefinition(key.MetricName, key.MetricVersion); err != nil {
			return usageState{}, err
		}
		state.Counters[key] = counter.Value
	}
	for _, applied := range persisted.Applied {
		if strings.TrimSpace(applied.MutationID) != "" {
			state.Applied[applied.MutationID] = applied
		}
	}
	for source, revision := range persisted.SourceRevisions {
		state.SourceRevisions[source] = revision
	}
	for _, persistedLimit := range persisted.Limits {
		limit := usageLimitFromPersistent(persistedLimit)
		if err := m.validateUsageLimit(limit); err != nil {
			return usageState{}, err
		}
		limit = normalizeUsageLimit(limit, time.Now().UTC())
		state.Limits[limit.LimitID] = limit
	}
	return state, nil
}

func (m *Module) persistLocked(state usageState) error {
	if m.statePath == "" {
		return nil
	}
	persisted := persistentState{Version: stateVersion, AccountingState: state.AccountingState, UpdatedAt: state.UpdatedAt, Sequence: state.Sequence, SourceRevisions: map[string]string{}, RebuildReason: state.RebuildReason}
	for key, value := range state.Counters {
		persisted.Counters = append(persisted.Counters, persistentCounter{Scope: key.Scope, DomainInclusion: key.DomainInclusion, MetricName: key.MetricName, MetricVersion: key.MetricVersion, Value: value})
	}
	sort.Slice(persisted.Counters, func(i, j int) bool {
		return persistentCounterKey(persisted.Counters[i]) < persistentCounterKey(persisted.Counters[j])
	})
	for _, applied := range state.Applied {
		persisted.Applied = append(persisted.Applied, applied)
	}
	sort.Slice(persisted.Applied, func(i, j int) bool { return persisted.Applied[i].MutationID < persisted.Applied[j].MutationID })
	for source, revision := range state.SourceRevisions {
		persisted.SourceRevisions[source] = revision
	}
	for _, limit := range state.Limits {
		persisted.Limits = append(persisted.Limits, persistentLimitFromUsageLimit(limit))
	}
	sort.Slice(persisted.Limits, func(i, j int) bool { return persisted.Limits[i].LimitID < persisted.Limits[j].LimitID })
	raw, err := json.MarshalIndent(persisted, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	if err := os.MkdirAll(filepath.Dir(m.statePath), fsperm.PrivateDir); err != nil {
		return err
	}
	tmp := m.statePath + ".tmp"
	if err := os.WriteFile(tmp, raw, fsperm.PrivateFile); err != nil {
		return err
	}
	return os.Rename(tmp, m.statePath)
}

func (m *Module) cloneStateLocked() usageState {
	state := usageState{AccountingState: m.state.AccountingState, UpdatedAt: m.state.UpdatedAt, Sequence: m.state.Sequence, Counters: map[counterKey]uint64{}, Applied: map[string]appliedMutation{}, SourceRevisions: map[string]string{}, Limits: map[string]UsageLimit{}, RebuildReason: m.state.RebuildReason}
	for key, value := range m.state.Counters {
		state.Counters[key] = value
	}
	for key, value := range m.state.Applied {
		state.Applied[key] = value
	}
	for key, value := range m.state.SourceRevisions {
		state.SourceRevisions[key] = value
	}
	for key, value := range m.state.Limits {
		state.Limits[key] = value
	}
	return state
}

func (s usageState) revision() Revision {
	rev := Revision{Token: fmt.Sprintf("usage-%d", s.Sequence)}
	for source, revision := range s.SourceRevisions {
		rev.Sources = append(rev.Sources, SourceRevision{Source: source, Revision: revision})
	}
	sort.Slice(rev.Sources, func(i, j int) bool { return rev.Sources[i].Source < rev.Sources[j].Source })
	return rev
}

func aggregateKeys(scope Scope, domainClass DomainClass, metric MetricDelta) []counterKey {
	version := metric.Version
	if version == 0 {
		version = 1
	}
	if scope.Type == ScopeTypeSpace {
		keys := []counterKey{{Scope: scope, DomainInclusion: DomainInclusionRetainedAll, MetricName: metric.Name, MetricVersion: version}}
		if domainClass == DomainClassSystem {
			keys = append(keys, counterKey{Scope: scope, DomainInclusion: DomainInclusionSystemOnly, MetricName: metric.Name, MetricVersion: version})
		} else {
			keys = append(keys, counterKey{Scope: scope, DomainInclusion: DomainInclusionRetainedNonSystem, MetricName: metric.Name, MetricVersion: version})
		}
		return keys
	}
	keys := []counterKey{{Scope: scope, MetricName: metric.Name, MetricVersion: version}}
	space := SpaceScope(scope.SpaceID)
	keys = append(keys, counterKey{Scope: space, DomainInclusion: DomainInclusionRetainedAll, MetricName: metric.Name, MetricVersion: version})
	if domainClass == DomainClassSystem {
		keys = append(keys, counterKey{Scope: space, DomainInclusion: DomainInclusionSystemOnly, MetricName: metric.Name, MetricVersion: version})
	} else {
		keys = append(keys, counterKey{Scope: space, DomainInclusion: DomainInclusionRetainedNonSystem, MetricName: metric.Name, MetricVersion: version})
	}
	return keys
}

func applySignedDelta(current uint64, delta int64) (uint64, error) {
	if delta >= 0 {
		amount := uint64(delta)
		if amount > ^uint64(0)-current {
			return 0, fmt.Errorf("usage counter overflow: current=%d delta=%d", current, delta)
		}
		return current + amount, nil
	}
	const minInt64 = -1 << 63
	if delta == minInt64 {
		return 0, fmt.Errorf("%w: current=%d delta=%d", errUsageCounterUnderflow, current, delta)
	}
	amount := uint64(-delta)
	if amount > current {
		return 0, fmt.Errorf("%w: current=%d delta=%d", errUsageCounterUnderflow, current, delta)
	}
	return current - amount, nil
}

func normalizeDomainInclusion(scope Scope, mode DomainInclusionMode) DomainInclusionMode {
	if scope.Type == ScopeTypeDomain {
		return DomainInclusionUnspecified
	}
	if mode == DomainInclusionUnspecified {
		return DomainInclusionRetainedNonSystem
	}
	return mode
}

func metricFromDefinition(def MetricDefinition, value uint64, observedAt time.Time, revision Revision) Metric {
	return Metric{Name: def.Name, Version: def.Version, Value: value, Unit: def.Unit, Authority: def.Authority, QuotaSafe: def.QuotaSafe, HardLimitEligible: def.HardLimitEligible, ObservedAt: observedAt, Revision: revision}
}

func isDiagnosticMetricDefinition(def MetricDefinition) bool {
	return def.Authority == AuthorityObservedApproximate && strings.HasPrefix(def.Name, "diagnostic.")
}

func metricNamesForScope(counters map[counterKey]uint64, scope Scope, inclusion DomainInclusionMode) []string {
	seen := map[string]struct{}{}
	for key := range counters {
		if key.Scope == scope && key.DomainInclusion == inclusion {
			seen[key.MetricName] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func listScopesForState(state usageState, parent Scope, mode ListMode) ([]Scope, error) {
	seen := map[Scope]struct{}{}
	addSpace := func(spaceID string) {
		scope := SpaceScope(spaceID)
		if err := scope.Validate(); err == nil {
			seen[scope] = struct{}{}
		}
	}
	addDomain := func(spaceID, domainID string) {
		scope := DomainScope(spaceID, domainID)
		if err := scope.Validate(); err == nil {
			seen[scope] = struct{}{}
		}
	}
	switch mode {
	case ListModeSpaces:
		for key := range state.Counters {
			if key.Scope.Type == ScopeTypeSpace {
				addSpace(key.Scope.SpaceID)
			}
		}
	case ListModeDomainsInSpace:
		if parent.Type != ScopeTypeSpace || strings.TrimSpace(parent.SpaceID) == "" {
			return nil, fmt.Errorf("domains_in_space usage list requires a space parent scope")
		}
		for key := range state.Counters {
			if key.Scope.Type == ScopeTypeDomain && key.Scope.SpaceID == parent.SpaceID {
				addDomain(key.Scope.SpaceID, key.Scope.DomainID)
			}
		}
	case ListModeSpaceWithDomainBreakdown:
		if parent.Type != ScopeTypeSpace || strings.TrimSpace(parent.SpaceID) == "" {
			return nil, fmt.Errorf("space_with_domain_breakdown usage list requires a space parent scope")
		}
		addSpace(parent.SpaceID)
		for key := range state.Counters {
			if key.Scope.Type == ScopeTypeDomain && key.Scope.SpaceID == parent.SpaceID {
				addDomain(key.Scope.SpaceID, key.Scope.DomainID)
			}
		}
	default:
		return nil, fmt.Errorf("unsupported usage list mode %q", mode)
	}
	scopes := make([]Scope, 0, len(seen))
	for scope := range seen {
		scopes = append(scopes, scope)
	}
	sort.Slice(scopes, func(i, j int) bool {
		return scopeSortKey(scopes[i]) < scopeSortKey(scopes[j])
	})
	if mode == ListModeSpaceWithDomainBreakdown {
		parentScope := SpaceScope(parent.SpaceID)
		sort.SliceStable(scopes, func(i, j int) bool {
			if scopes[i] == parentScope {
				return true
			}
			if scopes[j] == parentScope {
				return false
			}
			return scopeSortKey(scopes[i]) < scopeSortKey(scopes[j])
		})
	}
	return scopes, nil
}

func scopeSortKey(scope Scope) string {
	return string(scope.Type) + "\x00" + scope.SpaceID + "\x00" + scope.DomainID
}

func rebuildingStatus(scope Scope, state usageState) Status {
	evidence := map[string]string{"state": string(state.AccountingState)}
	if state.RebuildReason != "" {
		evidence["reason"] = state.RebuildReason
	}
	return Status{Reason: StatusReasonCountersRebuilding, Severity: StatusSeverityBlocker, Message: "usage counters are not ready", Scope: scope, Evidence: evidence}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func persistentCounterKey(counter persistentCounter) string {
	return fmt.Sprintf("%s|%s|%s|%s|%s|%d", counter.Scope.Type, counter.Scope.SpaceID, counter.Scope.DomainID, counter.DomainInclusion, counter.MetricName, counter.MetricVersion)
}
