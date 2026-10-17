package engine

import (
	"context"
	"errors"
	"time"

	querybinding "github.com/myceldb/mycel/internal/query/binding"
	queryexecution "github.com/myceldb/mycel/internal/query/execution"
	querylogical "github.com/myceldb/mycel/internal/query/logical"
	queryphysical "github.com/myceldb/mycel/internal/query/physical"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
)

const (
	// PlannerName and PlannerVersion identify the extracted planner in neutral
	// and transport diagnostics.
	PlannerName    = "mycel-query-engine"
	PlannerVersion = "qrx6"
)

// ErrUnsupportedPlan is the temporary bridge signal used while plan families
// are still executed by the legacy daemon adapter.
var ErrUnsupportedPlan = errors.New("query engine plan is not supported")

// Facade is the daemon-independent query engine boundary.
type Facade interface {
	Prepare(ctx context.Context, planning PlanningContext, statement querylogical.Statement) (PreparedQuery, error)
	Explain(ctx context.Context, planning PlanningContext, statement querylogical.Statement) (querylogical.Diagnostics, error)
	Execute(ctx context.Context, execution ExecutionContext, prepared PreparedQuery) (querylogical.Result, error)
}

// DomainPolicy and BindingLimits expose neutral binding inputs at the façade
// without making daemon adapters depend on binder implementation details.
type DomainPolicy = querybinding.DomainPolicy
type BindingLimits = querybinding.Limits

// PlanningContext contains request-scoped state needed to bind and plan a
// statement. The complete graph transaction is retained so a later physical
// plan can preserve snapshot and overlay visibility.
type PlanningContext struct {
	Transaction sessionmodel.GraphTransaction
	Schema      *schemamodel.DomainSchema
	Domain      DomainPolicy
	Limits      BindingLimits
	Parameters  map[string]querylogical.Value
}

// ExecutionContext contains state and work limits for one execution. LegacyRows
// is the temporary QRX6 handoff from legacy access selection into neutral
// shaping; nil retains the explicit unsupported-plan bridge.
type ExecutionContext struct {
	Transaction sessionmodel.GraphTransaction
	Budget      WorkBudget
	LegacyRows  []queryexecution.BindingRow
	Now         time.Time
}

// WorkBudget is the shared bounded-runtime resource vocabulary.
type WorkBudget = queryexecution.Budget

// PlanKind identifies the execution owner selected during preparation.
type PlanKind string

const (
	// PlanLegacyBridge means the API adapter must execute the statement exactly
	// once through its existing implementation.
	PlanLegacyBridge PlanKind = "legacy_bridge"
)

// PreparedQuery is the stable handoff between preparation and execution.
type PreparedQuery struct {
	Statement         querylogical.Statement
	Binding           querybinding.BoundStatement
	Physical          queryphysical.Plan
	Planning          PlanningContext
	Kind              PlanKind
	UnsupportedReason string
}

// RequiresLegacy reports whether the existing adapter must remain the sole
// execution owner for this prepared query.
func (p PreparedQuery) RequiresLegacy() bool {
	return p.Kind == PlanLegacyBridge
}

// Engine implements the neutral query facade. QRX6 can execute neutral result
// shaping over an explicitly supplied temporary legacy source. Storage access
// families remain on the adapter-owned legacy bridge until later tranches.
type Engine struct {
	ports Ports
}

// New constructs a query engine from consumer-owned access ports.
func New(ports Ports) *Engine {
	return &Engine{ports: ports}
}

// Capabilities reports which access ports were supplied by composition.
func (e *Engine) Capabilities() CapabilitySet {
	if e == nil {
		return CapabilitySet{}
	}
	return CapabilitySet{
		EntityReads:        e.ports.Entities != nil,
		Traversals:         e.ports.Traversals != nil,
		Indexes:            e.ports.Indexes != nil,
		Catalog:            e.ports.Catalog != nil,
		SemanticCandidates: e.ports.SemanticCandidates != nil,
		Mutations:          e.ports.Mutations != nil,
	}
}

// Prepare binds and physically plans the neutral statement without changing
// production execution ownership. Preparation is side-effect-free and invokes
// no access port.
func (e *Engine) Prepare(ctx context.Context, planning PlanningContext, statement querylogical.Statement) (PreparedQuery, error) {
	if err := contextQueryError("prepare query", ctx.Err()); err != nil {
		return PreparedQuery{}, err
	}
	bound, err := querybinding.Bind(statement, querybinding.Environment{
		Transaction: planning.Transaction,
		Schema:      planning.Schema,
		Domain:      planning.Domain,
		Limits:      planning.Limits,
		Parameters:  planning.Parameters,
	})
	if err != nil {
		return PreparedQuery{}, err
	}
	capabilities := e.Capabilities()
	physicalPlan, err := queryphysical.Build(bound, queryphysical.Environment{
		Schema:        planning.Schema,
		Domain:        planning.Domain,
		BindingLimits: planning.Limits,
		Capabilities: queryphysical.Capabilities{
			EntityReads: capabilities.EntityReads, Traversals: capabilities.Traversals,
			Indexes: capabilities.Indexes, SemanticCandidates: capabilities.SemanticCandidates,
			Mutations: capabilities.Mutations,
		},
	})
	if err != nil {
		return PreparedQuery{}, err
	}
	return PreparedQuery{
		Statement:         statement,
		Binding:           bound,
		Physical:          physicalPlan,
		Planning:          planning,
		Kind:              PlanLegacyBridge,
		UnsupportedReason: physicalPlan.LegacyBridgeReason,
	}, nil
}

// Explain prepares once and serializes that exact physical plan. Static plan
// diagnostics never contain runtime work counters.
func (e *Engine) Explain(ctx context.Context, planning PlanningContext, statement querylogical.Statement) (querylogical.Diagnostics, error) {
	prepared, err := e.Prepare(ctx, planning, statement)
	if err != nil {
		return querylogical.Diagnostics{}, err
	}
	diagnostics := queryphysical.Explain(prepared.Physical)
	diagnostics.Planner = PlannerName
	diagnostics.PlannerVersion = PlannerVersion
	return diagnostics, nil
}

// Execute runs QRX6 neutral shaping only when the caller explicitly supplies a
// temporary legacy binding source. With no source, storage-backed plans are
// rejected before any access port is invoked.
func (e *Engine) Execute(ctx context.Context, execution ExecutionContext, prepared PreparedQuery) (querylogical.Result, error) {
	if err := contextQueryError("execute query", ctx.Err()); err != nil {
		return querylogical.Result{}, err
	}
	if execution.LegacyRows == nil || prepared.Statement.Kind != querylogical.StatementRead || prepared.Statement.Read == nil {
		return querylogical.Result{}, unsupportedPlan(prepared)
	}
	if execution.Transaction != prepared.Planning.Transaction {
		return querylogical.Result{}, querylogical.NewError(
			querylogical.ErrorInvalidQuery,
			"execute query",
			"execution transaction does not match the prepared transaction",
			nil,
		)
	}
	staticBudget := queryexecution.Budget{
		MaxCandidates:         prepared.Physical.Limits.MaxCandidates,
		MaxIndexEntries:       prepared.Physical.Limits.MaxIndexEntries,
		MaxNodesLoaded:        prepared.Physical.Limits.MaxNodesLoaded,
		MaxEdgesLoaded:        prepared.Physical.Limits.MaxEdgesLoaded,
		MaxAdjacencyCalls:     prepared.Physical.Limits.MaxAdjacencyCalls,
		MaxIntermediateRows:   prepared.Physical.Limits.MaxIntermediateRows,
		MaxOutputRows:         prepared.Physical.Limits.MaxOutputRows,
		MaxMemoryBytes:        prepared.Physical.Limits.MaxMemoryBytes,
		MaxSemanticCandidates: prepared.Physical.Limits.MaxSemanticCandidates,
		MaxTraversalDepth:     prepared.Physical.Limits.MaxTraversalDepth,
	}
	budget := queryexecution.ResolveBudget(execution.Budget, staticBudget)
	tracker, err := queryexecution.NewTracker(budget)
	if err != nil {
		return querylogical.Result{}, err
	}
	source := queryexecution.NewLegacySource(execution.LegacyRows, budget.BatchSize, tracker)
	query := *prepared.Statement.Read
	query.Predicate = prepared.Physical.ResidualPredicate
	result, err := queryexecution.Shape(ctx, source, query, tracker, queryexecution.ShapeOptions{
		Parameters: prepared.Planning.Parameters,
		Now:        execution.Now,
	})
	if err != nil {
		return querylogical.Result{}, err
	}
	result.Diagnostics = mergeRuntimeDiagnostics(queryphysical.Explain(prepared.Physical), result.Diagnostics)
	result.Diagnostics.Planner = PlannerName
	result.Diagnostics.PlannerVersion = PlannerVersion
	return result, nil
}

func unsupportedPlan(prepared PreparedQuery) error {
	reason := prepared.UnsupportedReason
	if reason == "" {
		reason = "prepared query has no supported physical plan"
	}
	return querylogical.NewError(querylogical.ErrorUnsupportedPlan, "execute query", reason, ErrUnsupportedPlan)
}

func mergeRuntimeDiagnostics(plan querylogical.Diagnostics, runtime querylogical.Diagnostics) querylogical.Diagnostics {
	plan.ExplainOnly = false
	plan.IndexEntriesScanned = runtime.IndexEntriesScanned
	plan.NodesLoaded = runtime.NodesLoaded
	plan.EdgesLoaded = runtime.EdgesLoaded
	plan.RowsScanned = runtime.RowsScanned
	plan.RowsProduced = runtime.RowsProduced
	plan.RowsReturned = runtime.RowsReturned
	plan.CandidateCount = runtime.CandidateCount
	plan.AdjacencyScanCalls = runtime.AdjacencyScanCalls
	plan.IntermediateRows = runtime.IntermediateRows
	plan.PeakMemoryBytes = runtime.PeakMemoryBytes
	plan.RetainedMemoryBytes = runtime.RetainedMemoryBytes
	plan.TraversalDepth = runtime.TraversalDepth
	plan.ExecutionMillis = runtime.ExecutionMillis
	return plan
}

// IsUnsupportedPlan reports whether an engine error requests the temporary
// legacy execution bridge.
func IsUnsupportedPlan(err error) bool {
	if errors.Is(err, ErrUnsupportedPlan) {
		return true
	}
	var queryErr *querylogical.QueryError
	return errors.As(err, &queryErr) && queryErr.Code == querylogical.ErrorUnsupportedPlan
}

func contextQueryError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return querylogical.NewError(querylogical.ErrorCanceled, operation, "query context canceled", err)
}
