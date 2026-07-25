package analysis

import (
	"fmt"

	ast "github.com/myceldb/mycel/internal/query/gql/ast/model"
	schema "github.com/myceldb/mycel/internal/schema/model"
)

// SchemaContext optionally supplies a domain schema to semantic analysis.
// In permissive or no-schema mode, schema misses are accepted to preserve GQL's
// dynamic behavior. In strict mode, unknown labels, fields, and relationship
// endpoints fail analysis.
type SchemaContext struct {
	Schema *schema.DomainSchema
}

func (c SchemaContext) enabled() bool { return c.Schema != nil }

func (c SchemaContext) strict() bool {
	return c.Schema != nil && c.Schema.Normalize().Mode == schema.SchemaModeStrict
}

func (c SchemaContext) normalized() schema.DomainSchema {
	if c.Schema == nil {
		return schema.DomainSchema{Mode: schema.SchemaModePermissive}
	}
	return c.Schema.Normalize()
}

type schemaState struct {
	ctx       SchemaContext
	nodeTypes map[string][]schema.NodeType
	edgeTypes map[string][]schema.EdgeType
	vars      map[string][]schema.NodeType
}

func newSchemaState(ctx SchemaContext) schemaState {
	st := schemaState{ctx: ctx, vars: map[string][]schema.NodeType{}}
	if !ctx.enabled() {
		return st
	}
	doc := ctx.normalized()
	st.nodeTypes = map[string][]schema.NodeType{}
	st.edgeTypes = map[string][]schema.EdgeType{}
	for _, typ := range doc.NodeTypes {
		st.nodeTypes[typ.Name] = append(st.nodeTypes[typ.Name], typ)
		for _, label := range typ.Labels {
			st.nodeTypes[label] = append(st.nodeTypes[label], typ)
		}
	}
	for _, typ := range doc.EdgeTypes {
		st.edgeTypes[typ.Name] = append(st.edgeTypes[typ.Name], typ)
		for _, label := range typ.Labels {
			st.edgeTypes[label] = append(st.edgeTypes[label], typ)
		}
	}
	return st
}

func (s schemaState) analyzeNodePattern(pattern ast.NodePattern) error {
	if !s.ctx.enabled() {
		return nil
	}
	var matched []schema.NodeType
	for _, label := range pattern.Labels {
		types := s.nodeTypes[label]
		if len(types) == 0 && s.ctx.strict() {
			return fmt.Errorf("unknown node label %q", label)
		}
		matched = appendNodeTypes(matched, types...)
	}
	if pattern.Variable != "" {
		s.vars[pattern.Variable] = matched
	}
	for _, prop := range pattern.Properties {
		if err := s.validateNodeField(matched, "properties", prop.Key); err != nil {
			return err
		}
	}
	return nil
}

func (s schemaState) analyzeRelationshipPattern(pattern ast.RelationshipPattern) ([]schema.EdgeType, error) {
	if !s.ctx.enabled() {
		return nil, nil
	}
	var matched []schema.EdgeType
	for _, label := range pattern.Labels {
		types := s.edgeTypes[label]
		if len(types) == 0 && s.ctx.strict() {
			return nil, fmt.Errorf("unknown edge label %q", label)
		}
		matched = appendEdgeTypes(matched, types...)
	}
	for _, prop := range pattern.Properties {
		if err := s.validateEdgeField(matched, "properties", prop.Key); err != nil {
			return nil, err
		}
	}
	return matched, nil
}

func (s schemaState) validateReturn(ret ast.ReturnItem) error {
	if !s.ctx.enabled() || ret.Kind == ast.ReturnVariable || ret.Kind == "" {
		return nil
	}
	return s.validateNodeField(s.vars[ret.Variable], namespaceOrDefault(ret.Namespace), ret.Property)
}

func (s schemaState) validateWhereProperty(variable, namespace, property string) error {
	if !s.ctx.enabled() {
		return nil
	}
	return s.validateNodeField(s.vars[variable], namespaceOrDefault(namespace), property)
}

func (s schemaState) validateNodeField(types []schema.NodeType, namespace, field string) error {
	if !s.ctx.strict() || len(types) == 0 {
		return nil
	}
	for _, typ := range types {
		if nodeTypeHasField(typ, namespace, field) {
			return nil
		}
	}
	return fmt.Errorf("unknown %s field %q", namespace, field)
}

func (s schemaState) validateEdgeField(types []schema.EdgeType, namespace, field string) error {
	if !s.ctx.strict() || len(types) == 0 {
		return nil
	}
	for _, typ := range types {
		if edgeTypeHasField(typ, namespace, field) {
			return nil
		}
	}
	return fmt.Errorf("unknown edge %s field %q", namespace, field)
}

func (s schemaState) validateRelationshipEndpoints(edgeTypes []schema.EdgeType, fromVar, toVar string) error {
	if !s.ctx.strict() || len(edgeTypes) == 0 {
		return nil
	}
	fromTypes := s.vars[fromVar]
	toTypes := s.vars[toVar]
	if len(fromTypes) == 0 || len(toTypes) == 0 {
		return nil
	}
	for _, edgeType := range edgeTypes {
		if endpointAllows(edgeType.From, fromTypes) && endpointAllows(edgeType.To, toTypes) {
			return nil
		}
	}
	return fmt.Errorf("relationship endpoint constraints do not allow %q -> %q", fromVar, toVar)
}

func endpointAllows(endpoint schema.EndpointSpec, nodeTypes []schema.NodeType) bool {
	if len(endpoint.NodeTypes) == 0 && len(endpoint.Labels) == 0 {
		return true
	}
	for _, typ := range nodeTypes {
		if containsString(endpoint.NodeTypes, typ.Name) {
			return true
		}
		for _, label := range typ.Labels {
			if containsString(endpoint.Labels, label) {
				return true
			}
		}
	}
	return false
}

func nodeTypeHasField(typ schema.NodeType, namespace, field string) bool {
	switch namespace {
	case "", "properties":
		return fieldSpecExists(typ.Properties, field)
	case "payload":
		return fieldSpecExists(typ.Payload, field)
	case "meta":
		return fieldSpecExists(typ.Meta, field)
	default:
		return false
	}
}

func edgeTypeHasField(typ schema.EdgeType, namespace, field string) bool {
	switch namespace {
	case "", "properties":
		return fieldSpecExists(typ.Properties, field)
	case "payload":
		return fieldSpecExists(typ.Payload, field)
	case "meta":
		return fieldSpecExists(typ.Meta, field)
	default:
		return false
	}
}

func fieldSpecExists(fields []schema.FieldSpec, name string) bool {
	for _, field := range fields {
		if field.Name == name {
			return true
		}
	}
	return false
}

func appendNodeTypes(out []schema.NodeType, values ...schema.NodeType) []schema.NodeType {
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

func appendEdgeTypes(out []schema.EdgeType, values ...schema.EdgeType) []schema.EdgeType {
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

func namespaceOrDefault(namespace string) string {
	if namespace == "" {
		return "properties"
	}
	return namespace
}

func containsString(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}
