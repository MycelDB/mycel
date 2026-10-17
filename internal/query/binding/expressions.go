package binding

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	querylogical "github.com/myceldb/mycel/internal/query/logical"
)

func (b *binder) validateDepth(depth querylogical.DepthBounds, source querylogical.Source, step int) error {
	if depth.Min < 0 {
		return b.invalid("traversal step %d minimum depth must be non-negative", step)
	}
	cap := b.environment.Limits.MaxTraversalDepth
	if cap <= 0 {
		cap = structuredTraversalDepthCap
		if source == querylogical.SourceGQL {
			cap = gqlTraversalDepthCap
		}
	}
	if depth.Max == -1 {
		openDepth := b.environment.Limits.OpenTraversalDepth
		if openDepth <= 0 && source == querylogical.SourceGQL {
			openDepth = gqlTraversalDepthCap
		}
		if openDepth <= 0 {
			return b.invalid("traversal step %d requires a finite maximum depth", step)
		}
		if depth.Min > openDepth {
			return b.invalid("traversal step %d minimum depth exceeds open traversal cap %d", step, openDepth)
		}
	} else {
		if depth.Max < depth.Min {
			return b.invalid("traversal step %d maximum depth must be greater than or equal to minimum depth", step)
		}
		if depth.Max > cap {
			return b.invalid("traversal step %d maximum depth must not exceed %d", step, cap)
		}
	}
	return nil
}

func (b *binder) validatePredicate(predicate *querylogical.Predicate) error {
	if predicate == nil {
		return nil
	}
	switch predicate.Op {
	case querylogical.PredicateLeafOp:
		if predicate.Leaf == nil || len(predicate.Terms) != 0 {
			return b.invalid("leaf predicate must contain exactly one leaf")
		}
		return b.validateLeaf(*predicate.Leaf)
	case querylogical.PredicateAndOp, querylogical.PredicateOrOp:
		if predicate.Leaf != nil || len(predicate.Terms) == 0 {
			return b.invalid("%s predicate must contain one or more terms", predicate.Op)
		}
		for i := range predicate.Terms {
			if err := b.validatePredicate(&predicate.Terms[i]); err != nil {
				return err
			}
		}
		return nil
	default:
		return b.invalid("unsupported predicate operation %q", predicate.Op)
	}
}

func (b *binder) validateLeaf(leaf querylogical.Leaf) error {
	switch leaf.Kind {
	case querylogical.LeafComparison:
		if leaf.Left == nil || leaf.Right == nil {
			return b.invalid("comparison predicate requires left and right values")
		}
		switch leaf.Operator {
		case "=", "!=", "<", "<=", ">", ">=":
		default:
			return b.invalid("unsupported comparison operator %q", leaf.Operator)
		}
		if err := b.validateValue(*leaf.Left); err != nil {
			return err
		}
		return b.validateValue(*leaf.Right)

	case querylogical.LeafBetween:
		if leaf.Left == nil || leaf.Low == nil || leaf.High == nil {
			return b.invalid("between predicate requires value, low, and high operands")
		}
		for _, value := range []*querylogical.Value{leaf.Left, leaf.Low, leaf.High} {
			if err := b.validateValue(*value); err != nil {
				return err
			}
		}
		return nil

	case querylogical.LeafNull, querylogical.LeafPropertyExists:
		if leaf.Field == nil {
			return b.invalid("%s predicate requires a field", leaf.Kind)
		}
		return b.validateField(*leaf.Field)

	case querylogical.LeafString:
		if leaf.Operand == nil {
			return b.invalid("string predicate requires an operand")
		}
		switch leaf.Operator {
		case "contains", "starts_with", "ends_with":
		default:
			return b.invalid("unsupported string predicate operator %q", leaf.Operator)
		}
		if strings.TrimSpace(leaf.Query) == "" {
			return b.invalid("string predicate query is required")
		}
		return b.validateValue(*leaf.Operand)

	case querylogical.LeafText:
		if leaf.Field == nil || strings.TrimSpace(leaf.Query) == "" {
			return b.invalid("text predicate requires a field and query")
		}
		if !b.environment.Domain.ExplicitSearchable {
			return b.unavailable("domain does not allow text search")
		}
		return b.validateField(*leaf.Field)

	case querylogical.LeafSemantic:
		if leaf.Field == nil || strings.TrimSpace(leaf.Query) == "" {
			return b.invalid("semantic predicate requires an alias and query")
		}
		if !validNamespace(leaf.Field.Namespace) {
			return b.invalid("unsupported field namespace %q", leaf.Field.Namespace)
		}
		variable, ok := b.variables[leaf.Field.Alias]
		if !ok || variable.Kind != VariableNode {
			return b.invalid("semantic predicate alias %q is not a node", leaf.Field.Alias)
		}
		if leaf.Field.Name != "" {
			if err := b.validateField(*leaf.Field); err != nil {
				return err
			}
		}
		if leaf.Limit < 0 || (leaf.Limit == 0 && b.statementSourceRequiresExplicitSemanticLimit()) {
			return b.invalid("semantic predicate limit must be positive")
		}
		if strings.TrimSpace(leaf.RuleRef) != "" {
			ruleID, err := uuid.Parse(strings.TrimSpace(leaf.RuleRef))
			if err != nil || ruleID == uuid.Nil {
				return b.invalid("semantic predicate rule reference must be a UUID")
			}
		}
		if !b.environment.Domain.SemanticSearchable || !b.environment.Domain.SemanticCandidatesAvailable {
			return b.unavailable("semantic candidate access is unavailable")
		}
		return nil

	case querylogical.LeafHasTag:
		if leaf.Field == nil || strings.TrimSpace(leaf.Tag) == "" {
			return b.invalid("tag predicate requires an alias and tag")
		}
		variable, ok := b.variables[leaf.Field.Alias]
		if !ok || variable.Kind != VariableNode {
			return b.invalid("tag predicate alias %q is not a node", leaf.Field.Alias)
		}
		return nil

	default:
		return b.invalid("unsupported predicate kind %q", leaf.Kind)
	}
}

func (b *binder) statementSourceRequiresExplicitSemanticLimit() bool {
	// GQL analysis requires TOP K to be positive. Structured queries preserve a
	// zero value as "use the request/page default" for compatibility.
	return b.source == querylogical.SourceGQL
}

func (b *binder) validateValue(value querylogical.Value) error {
	return b.validateValueWithParameters(value, map[string]struct{}{})
}

func (b *binder) validateValueWithParameters(value querylogical.Value, resolving map[string]struct{}) error {
	switch value.Kind {
	case querylogical.ValueNull, querylogical.ValueBool, querylogical.ValueInteger, querylogical.ValueUnsigned, querylogical.ValueString, querylogical.ValueCurrentDate:
		return nil
	case querylogical.ValueNumber:
		if !value.IsFiniteNumber() {
			return b.invalid("numeric value must be finite")
		}
		return nil
	case querylogical.ValueDate:
		if _, err := time.Parse("2006-01-02", value.String); err != nil {
			return b.invalid("date value %q must use YYYY-MM-DD", value.String)
		}
		return nil
	case querylogical.ValueBinding:
		if _, ok := b.variables[value.Binding]; !ok {
			return b.invalid("value alias %q is not defined", value.Binding)
		}
		return nil
	case querylogical.ValueField:
		if value.Field == nil {
			return b.invalid("field value requires a field reference")
		}
		return b.validateField(*value.Field)
	case querylogical.ValueList:
		for i, item := range value.List {
			if err := b.validateValueWithParameters(item, resolving); err != nil {
				return fmt.Errorf("list item %d: %w", i, err)
			}
		}
		return nil
	case querylogical.ValueObject:
		for _, name := range sortedValueKeys(value.Object) {
			item := value.Object[name]
			if strings.TrimSpace(name) == "" {
				return b.invalid("object field name cannot be empty")
			}
			if err := b.validateValueWithParameters(item, resolving); err != nil {
				return fmt.Errorf("object field %q: %w", name, err)
			}
		}
		return nil
	case querylogical.ValueParameter:
		if strings.TrimSpace(value.Parameter) == "" {
			return b.invalid("parameter name is required")
		}
		resolved, ok := b.environment.Parameters[value.Parameter]
		if !ok {
			return b.invalid("missing parameter %q", value.Parameter)
		}
		if _, cycle := resolving[value.Parameter]; cycle {
			return b.invalid("parameter %q resolves recursively", value.Parameter)
		}
		resolving[value.Parameter] = struct{}{}
		err := b.validateValueWithParameters(resolved, resolving)
		delete(resolving, value.Parameter)
		return err
	default:
		return b.invalid("unsupported value kind %q", value.Kind)
	}
}

func (b *binder) validateProjections(projections []querylogical.Projection) error {
	for i, projection := range projections {
		variable, ok := b.variables[projection.Alias]
		if !ok {
			return b.invalid("projection %d alias %q is not defined", i, projection.Alias)
		}
		switch projection.Kind {
		case querylogical.ProjectionNode, querylogical.ProjectionTree:
			if variable.Kind != VariableNode {
				return b.invalid("projection %d alias %q is not a node", i, projection.Alias)
			}
		case querylogical.ProjectionEdge:
			if variable.Kind != VariableEdge {
				return b.invalid("projection %d alias %q is not an edge", i, projection.Alias)
			}
		case querylogical.ProjectionPath:
			if variable.Kind != VariablePath {
				return b.invalid("projection %d alias %q is not a path", i, projection.Alias)
			}
		case querylogical.ProjectionScalar:
			if projection.Field == nil {
				return b.invalid("scalar projection %d requires a field", i)
			}
			if projection.Field.Alias != projection.Alias {
				return b.invalid("scalar projection %d field alias does not match projection alias", i)
			}
			if projection.Field.Name == "" {
				if variable.Kind != VariableNode {
					return b.invalid("scalar projection %d alias %q is not a node", i, projection.Alias)
				}
				continue
			}
			if err := b.validateField(*projection.Field); err != nil {
				return err
			}
		default:
			return b.invalid("projection %d has unsupported kind %q", i, projection.Kind)
		}
	}
	return nil
}

func (b *binder) validateAggregates(aggregates []querylogical.Aggregate) error {
	for i, aggregate := range aggregates {
		switch aggregate.Function {
		case querylogical.AggregateCount, querylogical.AggregateSum, querylogical.AggregateAvg, querylogical.AggregateMin, querylogical.AggregateMax:
		default:
			return b.invalid("aggregate %d has unsupported function %q", i, aggregate.Function)
		}
		if aggregate.Star {
			if aggregate.Function != querylogical.AggregateCount {
				return b.invalid("aggregate %d only COUNT supports star", i)
			}
			if aggregate.Argument != nil {
				return b.invalid("aggregate %d cannot contain both star and an argument", i)
			}
			continue
		}
		if aggregate.Argument == nil {
			return b.invalid("aggregate %d requires an argument", i)
		}
		if aggregate.Function != querylogical.AggregateCount && aggregate.Argument.Kind == querylogical.ValueBinding {
			return b.invalid("aggregate %d %s requires a value argument", i, aggregate.Function)
		}
		if err := b.validateValue(*aggregate.Argument); err != nil {
			return fmt.Errorf("aggregate %d: %w", i, err)
		}
	}
	return nil
}

func (b *binder) validateOutputNames(projections []querylogical.Projection, aggregates []querylogical.Aggregate) error {
	seen := map[string]struct{}{}
	add := func(name string) error {
		if strings.TrimSpace(name) == "" {
			return b.invalid("query output name cannot be empty")
		}
		if _, ok := seen[name]; ok {
			return b.invalid("duplicate query output name %q", name)
		}
		seen[name] = struct{}{}
		return nil
	}
	for _, projection := range projections {
		name := projection.OutputName
		if name == "" {
			name = projection.Alias
			if projection.Kind == querylogical.ProjectionScalar && projection.Field != nil && projection.Field.Name != "" {
				name = projection.Field.Alias + "." + projection.Field.Name
				if projection.Field.Namespace != querylogical.NamespaceProperties {
					name = projection.Field.Alias + "." + string(projection.Field.Namespace) + "." + projection.Field.Name
				}
			}
		}
		if err := add(name); err != nil {
			return err
		}
	}
	for _, aggregate := range aggregates {
		name := aggregate.OutputName
		if name == "" {
			name = string(aggregate.Function)
		}
		if err := add(name); err != nil {
			return err
		}
	}
	return nil
}
