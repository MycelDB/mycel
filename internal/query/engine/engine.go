package engine

import (
	"context"
	"errors"
	"strconv"
	"strings"
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
	PlannerVersion = "qrx9"
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
	// PlanIndexed executes a migrated native read family directly.
	PlanIndexed PlanKind = "indexed"
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

// Engine implements the neutral query facade. Indexed, traversal, and bounded
// broad read plans execute directly; later plan families can still provide an
// explicit temporary legacy source for neutral QRX6 shaping.
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

// Prepare binds and physically plans the neutral statement. Preparation is
// side-effect-free and invokes no access port.
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
	kind := PlanIndexed
	if physicalPlan.RequiresLegacy() {
		kind = PlanLegacyBridge
	}
	return PreparedQuery{
		Statement: statement, Binding: bound, Physical: physicalPlan, Planning: planning,
		Kind: kind, UnsupportedReason: physicalPlan.LegacyBridgeReason,
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

// Execute runs migrated native read plans directly. An explicit legacy
// row source remains available for adapter-owned families that only migrated
// neutral shaping in QRX6.
func (e *Engine) Execute(ctx context.Context, execution ExecutionContext, prepared PreparedQuery) (querylogical.Result, error) {
	if err := contextQueryError("execute query", ctx.Err()); err != nil {
		return querylogical.Result{}, err
	}
	if prepared.Statement.Kind != querylogical.StatementRead || prepared.Statement.Read == nil {
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
	if execution.Now.IsZero() {
		execution.Now = time.Now()
	}
	budget := queryexecution.ResolveBudget(execution.Budget, physicalBudget(prepared.Physical))
	tracker, err := queryexecution.NewTracker(budget)
	if err != nil {
		return querylogical.Result{}, err
	}

	query := *prepared.Statement.Read
	var source queryexecution.Operator
	var cursor indexedCursor
	if execution.LegacyRows != nil {
		source = queryexecution.NewLegacySource(execution.LegacyRows, budget.BatchSize, tracker)
		query.Predicate = prepared.Physical.ResidualPredicate
	} else {
		if prepared.Kind != PlanIndexed {
			return querylogical.Result{}, unsupportedPlan(prepared)
		}
		cursor, err = decodeIndexedCursor(prepared)
		if err != nil {
			return querylogical.Result{}, err
		}
		pageRows := indexedPageRows(query, cursor, prepared.Physical.Family)
		if pageRows == 0 {
			source = queryexecution.NewLegacySource([]queryexecution.BindingRow{}, budget.BatchSize, tracker)
		} else {
			source, err = e.indexedSource(ctx, execution, prepared, tracker, cursor, cursor.RowsReturned+int64(pageRows))
			if err != nil {
				return querylogical.Result{}, err
			}
			if !nativeTraversalFamily(prepared.Physical.Family) {
				source = queryexecution.NewNodePatternFilter(source, query.Start, prepared.Planning.Parameters, execution.Now, tracker)
			}
		}
		query = executionQuery(query, prepared.Physical, cursor)
	}

	result, err := queryexecution.Shape(ctx, source, query, tracker, queryexecution.ShapeOptions{
		Parameters: prepared.Planning.Parameters,
		Now:        execution.Now,
	})
	if err != nil {
		return querylogical.Result{}, err
	}
	if execution.LegacyRows == nil {
		result.NextCursor = wrapIndexedNextCursor(prepared, result.NextCursor)
	}
	result.Diagnostics = mergeRuntimeDiagnostics(queryphysical.Explain(prepared.Physical), result.Diagnostics)
	if prepared.Physical.Family == queryphysical.FamilyIndexedSubtree {
		result.Diagnostics.RootCount = result.Diagnostics.RowsReturned
	}
	result.Diagnostics.Planner = PlannerName
	result.Diagnostics.PlannerVersion = PlannerVersion
	return result, nil
}

func physicalBudget(plan queryphysical.Plan) queryexecution.Budget {
	return queryexecution.Budget{
		MaxCandidates: plan.Limits.MaxCandidates, MaxIndexEntries: plan.Limits.MaxIndexEntries,
		MaxStartNodes: plan.Limits.MaxStartNodes, MaxNodesLoaded: plan.Limits.MaxNodesLoaded, MaxEdgesLoaded: plan.Limits.MaxEdgesLoaded,
		MaxAdjacencyCalls: plan.Limits.MaxAdjacencyCalls, MaxIntermediateRows: plan.Limits.MaxIntermediateRows,
		MaxOutputRows: plan.Limits.MaxOutputRows, MaxMemoryBytes: plan.Limits.MaxMemoryBytes,
		MaxSemanticCandidates: plan.Limits.MaxSemanticCandidates, MaxTraversalDepth: plan.Limits.MaxTraversalDepth,
	}
}

func indexedPageRows(query querylogical.Query, cursor indexedCursor, families ...queryphysical.Family) int {
	pageSize := int(query.Page.Size)
	if pageSize <= 0 || pageSize > 500 {
		pageSize = 500
	}
	if query.Page.Limit <= 0 {
		return pageSize
	}
	if len(families) > 0 && families[0] == queryphysical.FamilyIndexedSubtree {
		if query.Page.Limit < int64(pageSize) {
			return int(query.Page.Limit)
		}
		return pageSize
	}
	remaining := query.Page.Limit - cursor.RowsReturned
	if cursor.Mode == cursorModeOutput {
		remaining = query.Page.Limit - int64(cursor.OutputOffset)
	}
	if remaining <= 0 {
		return 0
	}
	if remaining < int64(pageSize) {
		return int(remaining)
	}
	return pageSize
}

func executionQuery(query querylogical.Query, plan queryphysical.Plan, cursor indexedCursor) querylogical.Query {
	query.Predicate = plan.ResidualPredicate
	if plan.Family == queryphysical.FamilyIndexedSubtree {
		pageSize := query.Page.Size
		if pageSize <= 0 || pageSize > 500 {
			pageSize = 500
		}
		if query.Page.Limit > 0 && query.Page.Limit < int64(pageSize) {
			pageSize = int32(query.Page.Limit)
		}
		query.Page.Size = pageSize
		query.Page.Limit = 0
	}
	if (plan.Family == queryphysical.FamilyIndexedOrder || plan.Family == queryphysical.FamilyIndexedSubtree) && len(query.Aggregates) == 0 {
		query.OrderBy = nil
	}
	if cursor.Mode == cursorModeSource {
		if query.Page.Cursor != "" {
			query.Page.Offset = 0
		}
		query.Page.Cursor = ""
		if query.Page.Limit > 0 && plan.Family != queryphysical.FamilyIndexedSubtree {
			query.Page.Limit -= cursor.RowsReturned
			if query.Page.Limit < 0 {
				query.Page.Limit = 0
			}
		}
		return query
	}
	query.Page.Cursor = strconv.Itoa(cursor.OutputOffset)
	return query
}

func wrapIndexedNextCursor(prepared PreparedQuery, next string) string {
	if next == "" || strings.HasPrefix(next, indexedCursorPrefix) {
		return next
	}
	offset, err := strconv.Atoi(next)
	if err != nil || offset < 0 {
		return ""
	}
	return encodeIndexedCursor(prepared, indexedCursor{Mode: cursorModeOutput, OutputOffset: offset, RowsReturned: int64(offset)})
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
	plan.NodeReadCalls = runtime.NodeReadCalls
	plan.RootCount = runtime.RootCount
	plan.Truncated = runtime.Truncated
	plan.TruncationReason = runtime.TruncationReason
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
