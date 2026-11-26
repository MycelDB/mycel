package service

import (
	"context"
	"fmt"
	"strings"
	"time"
)

const ModuleName = "usage"

const (
	MetricGraphNodeCount                = "graph.node_count"
	MetricGraphEdgeCount                = "graph.edge_count"
	MetricGraphLogicalBytes             = "graph.logical_bytes"
	MetricBlobActiveLogicalBytes        = "blob.active_logical_bytes"
	MetricBlobActiveCount               = "blob.active_count"
	MetricBlobLocalActiveLogicalBytes   = "blob.local_active_logical_bytes"
	MetricBlobLocalActiveCount          = "blob.local_active_count"
	MetricBlobObjectActiveLogicalBytes  = "blob.object_store_active_logical_bytes"
	MetricBlobObjectActiveCount         = "blob.object_store_active_count"
	MetricBlobPendingDeleteLogicalBytes = "blob.pending_delete_logical_bytes"
	MetricBlobPendingDeleteCount        = "blob.pending_delete_count"

	MetricDiagnosticLocalScopePhysicalBytes   = "diagnostic.local_scope_physical_bytes"
	MetricDiagnosticLocalGraphStoreBytes      = "diagnostic.local_graph_store_bytes"
	MetricDiagnosticLocalBlobStoreBytes       = "diagnostic.local_blob_store_bytes"
	MetricDiagnosticLocalBlobMetadataBytes    = "diagnostic.local_blob_metadata_bytes"
	MetricDiagnosticLocalLexicalStoreBytes    = "diagnostic.local_lexical_store_bytes"
	MetricDiagnosticLocalSemanticStoreBytes   = "diagnostic.local_semantic_store_bytes"
	MetricDiagnosticLocalProjectionStoreBytes = "diagnostic.local_projection_store_bytes"
)

type Unit string

const (
	UnitBytes Unit = "bytes"
	UnitCount Unit = "count"
)

type Authority string

const (
	AuthorityRaftCommittedExact   Authority = "raft_committed_exact"
	AuthorityStateMachineExact    Authority = "state_machine_exact"
	AuthorityObservedApproximate  Authority = "observed_approximate"
	AuthorityRebuildableOperation Authority = "rebuildable_operational"
)

type AccountingState string

const (
	AccountingStateReady         AccountingState = "ready"
	AccountingStateRebuilding    AccountingState = "rebuilding"
	AccountingStateReconciling   AccountingState = "reconciling"
	AccountingStateStale         AccountingState = "stale"
	AccountingStateDriftDetected AccountingState = "drift_detected"
	AccountingStateUnavailable   AccountingState = "unavailable"
)

type DomainInclusionMode string

const (
	DomainInclusionUnspecified       DomainInclusionMode = ""
	DomainInclusionRetainedNonSystem DomainInclusionMode = "retained_non_system"
	DomainInclusionRetainedAll       DomainInclusionMode = "retained_all"
	DomainInclusionSystemOnly        DomainInclusionMode = "system_only"
)

type DomainClass string

const (
	DomainClassRetainedNonSystem DomainClass = "retained_non_system"
	DomainClassSystem            DomainClass = "system"
)

type ScopeType string

const (
	ScopeTypeSpace  ScopeType = "space"
	ScopeTypeDomain ScopeType = "domain"
)

type Scope struct {
	Type     ScopeType `json:"type"`
	SpaceID  string    `json:"space_id"`
	DomainID string    `json:"domain_id,omitempty"`
}

func SpaceScope(spaceID string) Scope {
	return Scope{Type: ScopeTypeSpace, SpaceID: strings.TrimSpace(spaceID)}
}

func DomainScope(spaceID, domainID string) Scope {
	return Scope{Type: ScopeTypeDomain, SpaceID: strings.TrimSpace(spaceID), DomainID: strings.TrimSpace(domainID)}
}

func (s Scope) Validate() error {
	switch s.Type {
	case ScopeTypeSpace:
		if strings.TrimSpace(s.SpaceID) == "" {
			return fmt.Errorf("space usage scope requires space_id")
		}
		if strings.TrimSpace(s.DomainID) != "" {
			return fmt.Errorf("space usage scope must not include domain_id")
		}
		return nil
	case ScopeTypeDomain:
		if strings.TrimSpace(s.SpaceID) == "" || strings.TrimSpace(s.DomainID) == "" {
			return fmt.Errorf("domain usage scope requires space_id and domain_id")
		}
		return nil
	default:
		return fmt.Errorf("unsupported usage scope type %q", s.Type)
	}
}

func (s Scope) normalized() Scope {
	s.SpaceID = strings.TrimSpace(s.SpaceID)
	s.DomainID = strings.TrimSpace(s.DomainID)
	return s
}

type MetricDefinition struct {
	Name                           string
	Version                        uint32
	Unit                           Unit
	Authority                      Authority
	QuotaSafe                      bool
	HardLimitEligible              bool
	ReconciliationBackfillEligible bool
}

type MetricDelta struct {
	Name    string
	Version uint32
	Delta   int64
}

type Delta struct {
	MutationID  string
	Source      string
	Revision    string
	Scope       Scope
	DomainClass DomainClass
	Metrics     []MetricDelta
	ObservedAt  time.Time
}

type Revision struct {
	Token   string
	Sources []SourceRevision
}

type SourceRevision struct {
	Source   string
	Revision string
}

type Metric struct {
	Name              string
	Version           uint32
	Value             uint64
	Unit              Unit
	Authority         Authority
	QuotaSafe         bool
	HardLimitEligible bool
	ObservedAt        time.Time
	Revision          Revision
}

type CounterValue struct {
	Scope           Scope
	DomainInclusion DomainInclusionMode
	MetricName      string
	MetricVersion   uint32
	Value           uint64
}

type UsageLimit struct {
	LimitID         string
	Scope           Scope
	DomainInclusion DomainInclusionMode
	MetricName      string
	MetricVersion   uint32
	HardLimit       uint64
	Enabled         bool
	PolicyRef       string
	Description     string
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

type LimitMutationRequest struct {
	Limit UsageLimit
}

type LimitLookupRequest struct {
	LimitID         string
	Scope           Scope
	DomainInclusion DomainInclusionMode
	MetricName      string
	MetricVersion   uint32
}

type LimitListRequest struct {
	Parent     Scope
	PageSize   int
	PageToken  string
	MetricName string
}

type LimitListResponse struct {
	Limits        []UsageLimit
	NextPageToken string
}

type StatusReason string

const (
	StatusReasonCountersRebuilding             StatusReason = "counters_rebuilding"
	StatusReasonCounterStale                   StatusReason = "counter_stale"
	StatusReasonReconciliationDrift            StatusReason = "reconciliation_drift"
	StatusReasonMetricUnavailable              StatusReason = "metric_unavailable"
	StatusReasonMetricNotQuotaSafe             StatusReason = "metric_not_quota_safe"
	StatusReasonUnsupportedScope               StatusReason = "unsupported_scope"
	StatusReasonUnsupportedRevision            StatusReason = "unsupported_revision"
	StatusReasonDiagnosticCollectorUnavailable StatusReason = "diagnostic_collector_unavailable"
)

type StatusSeverity string

const (
	StatusSeverityInfo    StatusSeverity = "info"
	StatusSeverityWarning StatusSeverity = "warning"
	StatusSeverityBlocker StatusSeverity = "blocker"
)

type Status struct {
	Reason   StatusReason      `json:"reason"`
	Severity StatusSeverity    `json:"severity"`
	Message  string            `json:"message,omitempty"`
	Scope    Scope             `json:"scope"`
	Metric   string            `json:"metric_name,omitempty"`
	Evidence map[string]string `json:"evidence,omitempty"`
}

type SnapshotRequest struct {
	Scope                     Scope
	DomainInclusion           DomainInclusionMode
	MetricNames               []string
	RequireQuotaSafe          bool
	IncludeDiagnosticPhysical bool
	MaxStaleness              time.Duration
	MinimumRevision           Revision
}

type Snapshot struct {
	Scope             Scope
	DomainInclusion   DomainInclusionMode
	ObservedAt        time.Time
	Revision          Revision
	State             AccountingState
	QuotaSafeComplete bool
	Metrics           []Metric
	Blockers          []Status
	Warnings          []Status
}

type ListMode string

const (
	ListModeSpaces                   ListMode = "spaces"
	ListModeDomainsInSpace           ListMode = "domains_in_space"
	ListModeSpaceWithDomainBreakdown ListMode = "space_with_domain_breakdown"
)

type ListRequest struct {
	Parent                    Scope
	Mode                      ListMode
	DomainInclusion           DomainInclusionMode
	MetricNames               []string
	RequireQuotaSafe          bool
	IncludeDiagnosticPhysical bool
	MaxStaleness              time.Duration
	MinimumRevision           Revision
	PageSize                  int
	PageToken                 string
}

type ListResponse struct {
	Snapshots     []Snapshot
	NextPageToken string
}

type ReconciliationRequest struct {
	Scope      Scope
	MaxRecords uint64
}

type ReconciliationResult struct {
	Source         string
	Deltas         []Delta
	ScopesScanned  uint64
	RecordsScanned uint64
	Truncated      bool
	Evidence       map[string]string
}

type ReconciliationProvider interface {
	ReconcileUsage(context.Context, ReconciliationRequest) (ReconciliationResult, error)
}

type DiagnosticRequest struct {
	Scope      Scope
	MaxEntries uint64
}

type DiagnosticResult struct {
	Source         string
	Metrics        []Metric
	Statuses       []Status
	EntriesScanned uint64
	Truncated      bool
	Evidence       map[string]string
}

type DiagnosticCollector interface {
	CollectUsageDiagnostics(context.Context, DiagnosticRequest) (DiagnosticResult, error)
}

type HealthRequest struct {
	Scope                       Scope
	IncludeReconciliation       bool
	IncludeDiagnosticCollectors bool
}

type Health struct {
	Scope                               Scope
	ObservedAt                          time.Time
	State                               AccountingState
	Revision                            Revision
	QuotaSafeReady                      bool
	Blockers                            []Status
	Warnings                            []Status
	RebuildingScopeCount                uint64
	StaleScopeCount                     uint64
	DriftScopeCount                     uint64
	DiagnosticCollectorUnavailableCount uint64
}
