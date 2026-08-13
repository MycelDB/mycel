package client

import (
	"fmt"
	"strings"

	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	"github.com/myceldb/mycel/internal/query/gql/analysis"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
)

type structuredQuerySchemaState struct {
	strict      bool
	nodeTypes   map[string][]schemamodel.NodeType
	edgeTypes   map[string][]schemamodel.EdgeType
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
	state := structuredQuerySchemaState{strict: doc.Mode == schemamodel.SchemaModeStrict, nodeTypes: map[string][]schemamodel.NodeType{}, edgeTypes: map[string][]schemamodel.EdgeType{}, aliases: map[string][]schemamodel.NodeType{}, edgeAliases: map[string][]schemamodel.EdgeType{}}
	for _, typ := range doc.NodeTypes {
		state.nodeTypes[typ.Name] = append(state.nodeTypes[typ.Name], typ)
		for _, label := range typ.Labels {
			state.nodeTypes[label] = append(state.nodeTypes[label], typ)
		}
	}
	for _, typ := range doc.EdgeTypes {
		state.edgeTypes[typ.Name] = append(state.edgeTypes[typ.Name], typ)
		for _, label := range typ.Labels {
			state.edgeTypes[label] = append(state.edgeTypes[label], typ)
		}
	}
	return state
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
	var matched []schemamodel.NodeType
	for _, label := range pattern.GetLabels() {
		types := s.nodeTypes[label]
		if len(types) == 0 {
			return fmt.Errorf("unknown node label %q", label)
		}
		matched = appendStructuredNodeTypes(matched, types...)
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
	types := s.edgeTypes[label]
	if len(types) == 0 {
		return nil, fmt.Errorf("unknown edge label %q", label)
	}
	return types, nil
}

func (s structuredQuerySchemaState) validateTraversalEndpoints(edgeTypes []schemamodel.EdgeType, fromAlias string, toAlias string) error {
	if len(edgeTypes) == 0 {
		return nil
	}
	fromTypes := s.aliases[fromAlias]
	toTypes := s.aliases[toAlias]
	if len(fromTypes) == 0 || len(toTypes) == 0 {
		return nil
	}
	for _, edgeType := range edgeTypes {
		if structuredEndpointAllows(edgeType.From, fromTypes) && structuredEndpointAllows(edgeType.To, toTypes) {
			return nil
		}
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
		for _, typ := range edgeTypes {
			if structuredEdgeTypeHasField(typ, namespace, field) {
				return nil
			}
		}
		return fmt.Errorf("unknown edge %s field %q", namespace, field)
	}
	types := s.aliases[alias]
	if len(types) == 0 {
		return nil
	}
	for _, typ := range types {
		if structuredNodeTypeHasField(typ, namespace, field) {
			return nil
		}
	}
	return fmt.Errorf("unknown %s field %q", namespace, field)
}

func structuredNodeTypeHasField(typ schemamodel.NodeType, namespace string, field string) bool {
	switch namespace {
	case "", "properties":
		return structuredFieldExists(typ.Properties, field)
	case "payload":
		return structuredFieldExists(typ.Payload, field)
	case "meta":
		return structuredFieldExists(typ.Meta, field)
	default:
		return false
	}
}

func structuredEdgeTypeHasField(typ schemamodel.EdgeType, namespace string, field string) bool {
	switch namespace {
	case "", "properties":
		return structuredFieldExists(typ.Properties, field)
	case "payload":
		return structuredFieldExists(typ.Payload, field)
	case "meta":
		return structuredFieldExists(typ.Meta, field)
	default:
		return false
	}
}

func structuredFieldExists(fields []schemamodel.FieldSpec, name string) bool {
	for _, field := range fields {
		if field.Name == name {
			return true
		}
	}
	return false
}

func structuredEndpointAllows(endpoint schemamodel.EndpointSpec, nodeTypes []schemamodel.NodeType) bool {
	if len(endpoint.NodeTypes) == 0 && len(endpoint.Labels) == 0 {
		return true
	}
	for _, typ := range nodeTypes {
		if structuredContains(endpoint.NodeTypes, typ.Name) {
			return true
		}
		for _, label := range typ.Labels {
			if structuredContains(endpoint.Labels, label) {
				return true
			}
		}
	}
	return false
}

func appendStructuredNodeTypes(out []schemamodel.NodeType, values ...schemamodel.NodeType) []schemamodel.NodeType {
	seen := map[string]struct{}{}
	for _, typ := range out {
		seen[typ.Name] = struct{}{}
	}
	for _, typ := range values {
		if _, ok := seen[typ.Name]; !ok {
			seen[typ.Name] = struct{}{}
			out = append(out, typ)
		}
	}
	return out
}

func structuredContains(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}
