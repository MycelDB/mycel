package client

import (
	"fmt"
	"strings"

	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	"github.com/myceldb/mycel/internal/query/gql/analysis"
	queryschema "github.com/myceldb/mycel/internal/query/schema"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
)

type structuredQuerySchemaState struct {
	catalog     queryschema.Catalog
	aliases     map[string][]schemamodel.NodeType
	edgeAliases map[string][]schemamodel.EdgeType
}

func validateStructuredGraphQueryWithSchema(query *clientv1.GraphQuery, schemaCtx analysis.SchemaContext) error {
	if query == nil || schemaCtx.Schema == nil {
		return nil
	}
	doc := schemaCtx.Schema.Normalize()
	if doc.Mode != schemamodel.SchemaModeStrict {
		return nil
	}
	state := newStructuredQuerySchemaState(doc)
	if err := state.validatePattern(query.GetMatch()); err != nil {
		return err
	}
	if err := state.validateExpr(query.GetWhere()); err != nil {
		return err
	}
	for _, order := range query.GetOrderBy() {
		if err := state.validateValueExpr(order.GetValue()); err != nil {
			return err
		}
	}
	return nil
}

func newStructuredQuerySchemaState(doc schemamodel.DomainSchema) structuredQuerySchemaState {
	return structuredQuerySchemaState{
		catalog: queryschema.NewCatalog(&doc),
		aliases: map[string][]schemamodel.NodeType{}, edgeAliases: map[string][]schemamodel.EdgeType{},
	}
}

func (s structuredQuerySchemaState) validatePattern(pattern *clientv1.GraphPattern) error {
	if pattern == nil {
		return nil
	}
	if err := s.validateNodePattern(pattern.GetStart()); err != nil {
		return err
	}
	currentAlias := pattern.GetStart().GetAlias()
	for _, step := range pattern.GetSteps() {
		edgeTypes, err := s.validateEdgeLabel(step.GetEdgeKind())
		if err != nil {
			return err
		}
		if alias := strings.TrimSpace(step.GetEdgeAlias()); alias != "" {
			s.edgeAliases[alias] = edgeTypes
		}
		if err := s.validateNodePattern(step.GetTarget()); err != nil {
			return err
		}
		fromAlias, toAlias := currentAlias, step.GetTarget().GetAlias()
		if step.GetDirection() == clientv1.TraversalDirection_TRAVERSAL_DIRECTION_IN {
			fromAlias, toAlias = toAlias, fromAlias
		}
		if err := s.validateTraversalEndpoints(edgeTypes, fromAlias, toAlias); err != nil {
			return err
		}
		currentAlias = step.GetTarget().GetAlias()
	}
	return nil
}

func (s structuredQuerySchemaState) validateNodePattern(pattern *clientv1.NodePattern) error {
	if pattern == nil {
		return nil
	}
	matched, unknown := s.catalog.ResolveNodeLabels(pattern.GetLabels())
	if unknown != "" {
		return fmt.Errorf("unknown node label %q", unknown)
	}
	if alias := strings.TrimSpace(pattern.GetAlias()); alias != "" {
		s.aliases[alias] = matched
	}
	return nil
}

func (s structuredQuerySchemaState) validateEdgeLabel(label string) ([]schemamodel.EdgeType, error) {
	label = strings.TrimSpace(label)
	if label == "" {
		return nil, nil
	}
	types, unknown := s.catalog.ResolveEdgeLabels([]string{label})
	if unknown != "" {
		return nil, fmt.Errorf("unknown edge label %q", unknown)
	}
	return types, nil
}

func (s structuredQuerySchemaState) validateTraversalEndpoints(edgeTypes []schemamodel.EdgeType, fromAlias string, toAlias string) error {
	if s.catalog.EndpointsAllowed(edgeTypes, s.aliases[fromAlias], s.aliases[toAlias]) {
		return nil
	}
	return fmt.Errorf("edge endpoint constraints do not allow %q -> %q", fromAlias, toAlias)
}

func (s structuredQuerySchemaState) validateExpr(expr *clientv1.Expr) error {
	if expr == nil {
		return nil
	}
	switch v := expr.GetExpr().(type) {
	case *clientv1.Expr_And:
		for _, child := range v.And.GetExprs() {
			if err := s.validateExpr(child); err != nil {
				return err
			}
		}
	case *clientv1.Expr_PropertyExists:
		return s.validateNodeField(v.PropertyExists.GetAlias(), "properties", v.PropertyExists.GetName())
	case *clientv1.Expr_PropertyEquals:
		return s.validateNodeField(v.PropertyEquals.GetAlias(), "properties", v.PropertyEquals.GetName())
	case *clientv1.Expr_Between:
		if err := s.validateValueExpr(v.Between.GetValue()); err != nil {
			return err
		}
		if err := s.validateValueExpr(v.Between.GetLow()); err != nil {
			return err
		}
		return s.validateValueExpr(v.Between.GetHigh())
	case *clientv1.Expr_LessThan:
		if err := s.validateValueExpr(v.LessThan.GetLeft()); err != nil {
			return err
		}
		return s.validateValueExpr(v.LessThan.GetRight())
	}
	return nil
}

func (s structuredQuerySchemaState) validateValueExpr(value *clientv1.ValueExpr) error {
	if value == nil {
		return nil
	}
	if prop := value.GetProp(); prop != nil {
		return s.validateNodeField(prop.GetAlias(), "properties", prop.GetName())
	}
	return nil
}

func (s structuredQuerySchemaState) validateNodeField(alias string, namespace string, field string) error {
	if edgeTypes := s.edgeAliases[alias]; len(edgeTypes) > 0 {
		if s.catalog.EdgeFieldKnown(edgeTypes, namespace, field) {
			return nil
		}
		return fmt.Errorf("unknown edge %s field %q", namespace, field)
	}
	if s.catalog.NodeFieldKnown(s.aliases[alias], namespace, field) {
		return nil
	}
	return fmt.Errorf("unknown %s field %q", namespace, field)
}
