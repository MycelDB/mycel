package binding

import (
	"fmt"
	"strings"

	querylogical "github.com/myceldb/mycel/internal/query/logical"
	queryschema "github.com/myceldb/mycel/internal/query/schema"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
)

const (
	structuredTraversalDepthCap = 64
	gqlTraversalDepthCap        = 5
)

type binder struct {
	environment  Environment
	source       querylogical.Source
	catalog      queryschema.Catalog
	variables    map[string]Variable
	nodeBindings map[string][]schemamodel.NodeType
	edgeBindings map[string][]schemamodel.EdgeType
	variableSet  map[string]struct{}
	order        []string
}

// Bind validates a neutral statement and returns deterministic binding
// metadata. It performs no graph, index, semantic, or mutation access.
func Bind(statement querylogical.Statement, environment Environment) (BoundStatement, error) {
	b := newBinder(environment)
	b.source = statement.Source
	if statement.Source != querylogical.SourceStructured && statement.Source != querylogical.SourceGQL {
		return BoundStatement{}, b.invalid("unsupported statement source %q", statement.Source)
	}
	if err := b.validateAccess(statement); err != nil {
		return BoundStatement{}, err
	}
	switch statement.Kind {
	case querylogical.StatementRead:
		if statement.Read == nil || statement.Mutation != nil {
			return BoundStatement{}, b.invalid("read statement must contain exactly one read query")
		}
		if statement.Read.Source != statement.Source || statement.Read.AccessMode != statement.AccessMode {
			return BoundStatement{}, b.invalid("read statement metadata does not match its query")
		}
		if err := b.bindRead(*statement.Read, false); err != nil {
			return BoundStatement{}, err
		}
	case querylogical.StatementMutation:
		if statement.Mutation == nil || statement.Read != nil {
			return BoundStatement{}, b.invalid("mutation statement must contain exactly one mutation")
		}
		if err := b.bindMutation(*statement.Mutation); err != nil {
			return BoundStatement{}, err
		}
	default:
		return BoundStatement{}, b.invalid("unsupported statement kind %q", statement.Kind)
	}

	variables := make([]Variable, 0, len(b.order))
	for _, name := range b.order {
		variables = append(variables, b.variables[name])
	}
	bound := BoundStatement{Statement: statement, Variables: variables}
	if environment.Schema != nil {
		bound.SchemaVersion = b.catalog.Version()
	}
	return bound, nil
}

func newBinder(environment Environment) *binder {
	return &binder{
		environment:  environment,
		catalog:      queryschema.NewCatalog(environment.Schema),
		variables:    map[string]Variable{},
		nodeBindings: map[string][]schemamodel.NodeType{},
		edgeBindings: map[string][]schemamodel.EdgeType{},
		variableSet:  map[string]struct{}{},
	}
}

func (b *binder) validateAccess(statement querylogical.Statement) error {
	tx := b.environment.Transaction
	if tx.State != "" && tx.State != sessionmodel.TransactionStateActive {
		return b.invalid("transaction is not active")
	}
	if statement.AccessMode != querylogical.AccessModeReadOnly && statement.AccessMode != querylogical.AccessModeReadWrite {
		return b.invalid("unsupported statement access mode %q", statement.AccessMode)
	}
	if statement.Kind == querylogical.StatementRead && statement.AccessMode != querylogical.AccessModeReadOnly {
		return b.invalid("read statement requires read-only access mode")
	}
	if statement.Kind == querylogical.StatementMutation && statement.AccessMode != querylogical.AccessModeReadWrite {
		return b.invalid("mutation statement requires read-write access mode")
	}
	if statement.AccessMode == querylogical.AccessModeReadWrite {
		if tx.Mode != sessionmodel.TransactionModeReadWrite {
			return b.invalid("statement requires a read-write transaction")
		}
		if b.environment.Domain.ReadOnly {
			return b.unavailable("domain is read-only")
		}
	}
	return nil
}

func (b *binder) bindRead(query querylogical.Query, mutationMatch bool) error {
	if query.AccessMode != querylogical.AccessModeReadOnly && !(mutationMatch && query.AccessMode == querylogical.AccessModeReadWrite) {
		return b.invalid("query access mode %q is incompatible with statement", query.AccessMode)
	}
	startTypes, err := b.bindNodePattern(query.Start, true)
	if err != nil {
		return err
	}
	_ = startTypes
	if query.PathAlias != "" {
		if len(query.Steps) == 0 {
			return b.invalid("path alias %q requires at least one traversal step", query.PathAlias)
		}
		if err := b.declare(Variable{Name: query.PathAlias, Kind: VariablePath}); err != nil {
			return err
		}
	}

	fromAlias := query.Start.Alias
	for i, step := range query.Steps {
		if err := b.validateDepth(step.Depth, query.Source, i); err != nil {
			return err
		}
		if query.Source == querylogical.SourceGQL && step.Edge.Alias != "" && (step.Depth.Min != 1 || step.Depth.Max != 1) {
			return b.invalid("variable-length traversal step %d cannot bind an edge alias", i)
		}
		edgeTypes, err := b.bindEdgePattern(step.Edge)
		if err != nil {
			return err
		}
		toTypes, err := b.bindNodePattern(step.Target, true)
		if err != nil {
			return err
		}
		fromTypes := b.nodeSchemaTypes(fromAlias)
		if err := b.validateEndpoints(edgeTypes, fromTypes, toTypes, step.Edge.Direction, fromAlias, step.Target.Alias); err != nil {
			return err
		}
		fromAlias = step.Target.Alias
	}
	if err := b.validatePredicate(query.Predicate); err != nil {
		return err
	}
	if err := b.validateProjections(query.Returns); err != nil {
		return err
	}
	if err := b.validateAggregates(query.Aggregates); err != nil {
		return err
	}
	if err := b.validateOutputNames(query.Returns, query.Aggregates); err != nil {
		return err
	}
	for i, order := range query.OrderBy {
		if order.Direction != querylogical.SortAscending && order.Direction != querylogical.SortDescending {
			return b.invalid("order item %d has unsupported direction %q", i, order.Direction)
		}
		if err := b.validateValue(order.Value); err != nil {
			return fmt.Errorf("order item %d: %w", i, err)
		}
	}
	if query.Page.Offset < 0 || query.Page.Limit < 0 || query.Page.Size < 0 {
		return b.invalid("pagination values must be non-negative")
	}
	if query.Safety.MaxNodes < 0 || query.Safety.MaxEdges < 0 {
		return b.invalid("query safety limits must be non-negative")
	}
	return nil
}

func (b *binder) bindMutation(mutation querylogical.Mutation) error {
	if mutation.Limit < 0 {
		return b.invalid("mutation limit must be non-negative")
	}
	switch mutation.Kind {
	case querylogical.MutationInsertNode:
		if mutation.Node == nil {
			return b.invalid("insert node pattern is required")
		}
		if len(mutation.Node.Labels) == 0 {
			return b.invalid("insert node requires at least one label")
		}
		if _, err := b.bindNodePattern(*mutation.Node, false); err != nil {
			return err
		}

	case querylogical.MutationMergeNode:
		if mutation.Node == nil {
			return b.invalid("merge node pattern is required")
		}
		if len(mutation.Node.Labels) == 0 && len(mutation.Node.Properties) == 0 {
			return b.invalid("merge node requires at least one label or property")
		}
		if _, err := b.bindNodePattern(*mutation.Node, true); err != nil {
			return err
		}

	case querylogical.MutationMatchSet, querylogical.MutationMatchDelete:
		if mutation.Match == nil {
			return b.invalid("matched mutation requires a match query")
		}
		if mutation.Match.Source != b.source {
			return b.invalid("mutation match source does not match its statement")
		}
		if err := b.bindRead(*mutation.Match, true); err != nil {
			return err
		}

	case querylogical.MutationCreateRelationship, querylogical.MutationMergeRelationship:
		if len(mutation.Nodes) < 2 {
			return b.invalid("relationship mutation requires at least two node patterns")
		}
		for _, node := range mutation.Nodes {
			if mutation.Kind == querylogical.MutationMergeRelationship && len(node.Properties) == 0 {
				return b.invalid("merge relationship match for %q requires at least one identifying property", node.Alias)
			}
			if _, err := b.bindNodePattern(node, true); err != nil {
				return err
			}
		}
		if err := b.bindRelationshipMutation(mutation.Relationship); err != nil {
			return err
		}

	default:
		return b.invalid("unsupported mutation kind %q", mutation.Kind)
	}

	if mutation.Kind == querylogical.MutationMatchSet {
		if len(mutation.Assignments) == 0 {
			return b.invalid("set mutation requires at least one assignment")
		}
		for i, assignment := range mutation.Assignments {
			if err := b.validateField(assignment.Field); err != nil {
				return fmt.Errorf("assignment %d: %w", i, err)
			}
			if assignment.Field.Namespace == querylogical.NamespaceMeta {
				return b.invalid("set mutation does not support meta fields")
			}
			if err := b.validateValue(assignment.Value); err != nil {
				return fmt.Errorf("assignment %d: %w", i, err)
			}
		}
	}
	if mutation.Kind == querylogical.MutationMatchDelete {
		if len(mutation.Targets) == 0 {
			return b.invalid("delete mutation requires at least one target")
		}
		seen := map[string]struct{}{}
		for _, target := range mutation.Targets {
			if _, ok := b.variables[target]; !ok {
				return b.invalid("delete target %q is not defined", target)
			}
			if _, ok := seen[target]; ok {
				return b.invalid("duplicate delete target %q", target)
			}
			seen[target] = struct{}{}
		}
	}
	if err := b.validateProjections(mutation.Returns); err != nil {
		return err
	}
	if err := b.validateAggregates(mutation.Aggregates); err != nil {
		return err
	}
	return b.validateOutputNames(mutation.Returns, mutation.Aggregates)
}

func (b *binder) bindRelationshipMutation(relationship *querylogical.RelationshipMutation) error {
	if relationship == nil {
		return b.invalid("relationship mutation is required")
	}
	if len(relationship.Labels) == 0 {
		return b.invalid("relationship mutation requires at least one label")
	}
	from, fromOK := b.variables[relationship.FromAlias]
	to, toOK := b.variables[relationship.ToAlias]
	if !fromOK || from.Kind != VariableNode {
		return b.invalid("relationship source alias %q is not a node", relationship.FromAlias)
	}
	if !toOK || to.Kind != VariableNode {
		return b.invalid("relationship target alias %q is not a node", relationship.ToAlias)
	}
	edge := querylogical.EdgePattern{Alias: relationship.Alias, Labels: relationship.Labels, Properties: relationship.Properties, Direction: querylogical.DirectionOutgoing}
	edgeTypes, err := b.bindEdgePattern(edge)
	if err != nil {
		return err
	}
	return b.validateEndpoints(edgeTypes, b.nodeSchemaTypes(from.Name), b.nodeSchemaTypes(to.Name), querylogical.DirectionOutgoing, from.Name, to.Name)
}

func (b *binder) declare(variable Variable) error {
	name := strings.TrimSpace(variable.Name)
	if name == "" {
		return b.invalid("variable alias is required")
	}
	if name != variable.Name {
		return b.invalid("variable alias %q must not contain surrounding whitespace", variable.Name)
	}
	if _, exists := b.variableSet[name]; exists {
		return b.invalid("duplicate variable alias %q", name)
	}
	variable.Name = name
	b.variableSet[name] = struct{}{}
	b.variables[name] = variable
	b.order = append(b.order, name)
	return nil
}

func (b *binder) invalid(format string, args ...any) error {
	return querylogical.NewError(querylogical.ErrorInvalidQuery, "bind query", fmt.Sprintf(format, args...), nil)
}

func (b *binder) unavailable(format string, args ...any) error {
	return querylogical.NewError(querylogical.ErrorUnavailableAccessPath, "bind query", fmt.Sprintf(format, args...), nil)
}
