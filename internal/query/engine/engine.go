package engine

import (
	"context"
	"errors"

	querybinding "github.com/myceldb/mycel/internal/query/binding"
	querylogical "github.com/myceldb/mycel/internal/query/logical"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
)

const (
	plannerName    = "mycel-query-engine"
	plannerVersion = "qrx3"
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

// ExecutionContext contains state and work limits for one execution.
type ExecutionContext struct {
	Transaction sessionmodel.GraphTransaction
	Budget      WorkBudget
}

// WorkBudget bounds physical work independently of response page size. Zero
// values mean the caller did not supply a bound; operators introduced by later
// tranches must resolve them to configured defaults before doing work.
type WorkBudget struct {
	MaxIndexEntries       int64
	MaxNodesLoaded        int64
	MaxEdgesLoaded        int64
	MaxIntermediateRows   int64
	MaxSemanticCandidates int64
}

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
	Planning          PlanningContext
	Kind              PlanKind
	UnsupportedReason string
}

// RequiresLegacy reports whether the existing adapter must remain the sole
// execution owner for this prepared query.
func (p PreparedQuery) RequiresLegacy() bool {
	return p.Kind == PlanLegacyBridge
}

// Engine implements the neutral query facade. QRX3 binds every represented
// statement and still prepares it for the legacy bridge; later tranches replace
// that execution decision one plan family at a time.
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

// Prepare binds the neutral statement without changing production execution
// ownership. Binding is side-effect-free and invokes no access port.
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
	return PreparedQuery{
		Statement:         statement,
		Binding:           bound,
		Planning:          planning,
		Kind:              PlanLegacyBridge,
		UnsupportedReason: "no physical plan families have migrated to the query engine",
	}, nil
}

// Explain reports the preparation decision. The daemon adapter keeps its
// existing Explain response authoritative until physical planning migrates.
func (e *Engine) Explain(ctx context.Context, planning PlanningContext, statement querylogical.Statement) (querylogical.Diagnostics, error) {
	prepared, err := e.Prepare(ctx, planning, statement)
	if err != nil {
		return querylogical.Diagnostics{}, err
	}
	return querylogical.Diagnostics{
		Plan:           "LegacyBridge",
		PlanKind:       string(prepared.Kind),
		Planner:        plannerName,
		PlannerVersion: plannerVersion,
		ExplainOnly:    true,
		FallbackMode:   string(PlanLegacyBridge),
	}, nil
}

// Execute rejects legacy-bridge plans before invoking any dependency. The
// caller may then execute the statement once through the legacy path.
func (e *Engine) Execute(ctx context.Context, _ ExecutionContext, prepared PreparedQuery) (querylogical.Result, error) {
	if err := contextQueryError("execute query", ctx.Err()); err != nil {
		return querylogical.Result{}, err
	}
	reason := prepared.UnsupportedReason
	if reason == "" {
		reason = "prepared query has no supported physical plan"
	}
	return querylogical.Result{}, querylogical.NewError(
		querylogical.ErrorUnsupportedPlan,
		"execute query",
		reason,
		ErrUnsupportedPlan,
	)
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
