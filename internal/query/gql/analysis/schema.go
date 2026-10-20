package analysis

import (
	"fmt"

	ast "github.com/myceldb/mycel/internal/query/gql/ast/model"
	queryschema "github.com/myceldb/mycel/internal/query/schema"
	schema "github.com/myceldb/mycel/internal/schema/model"
)

// SchemaContext optionally supplies a domain schema to semantic analysis.
// In permissive or no-schema mode, schema misses are accepted to preserve GQL's
// dynamic behavior. In strict mode, unknown labels, fields, and relationship
// endpoints fail analysis.
type SchemaContext struct {
	Schema *schema.DomainSchema
}

type schemaState struct {
	catalog queryschema.Catalog
	vars    map[string][]schema.NodeType
}

func newSchemaState(ctx SchemaContext) schemaState {
	return schemaState{catalog: queryschema.NewCatalog(ctx.Schema), vars: map[string][]schema.NodeType{}}
}

func (s schemaState) analyzeNodePattern(pattern ast.NodePattern) error {
	if !s.catalog.Enabled() {
		return nil
	}
	matched, unknown := s.catalog.ResolveNodeLabels(pattern.Labels)
	if unknown != "" && s.catalog.Strict() {
		return fmt.Errorf("unknown node label %q", unknown)
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
	if !s.catalog.Enabled() {
		return nil, nil
	}
	matched, unknown := s.catalog.ResolveEdgeLabels(pattern.Labels)
	if unknown != "" && s.catalog.Strict() {
		return nil, fmt.Errorf("unknown edge label %q", unknown)
	}
	for _, prop := range pattern.Properties {
		if err := s.validateEdgeField(matched, "properties", prop.Key); err != nil {
			return nil, err
		}
	}
	return matched, nil
}

func (s schemaState) validateReturn(ret ast.ReturnItem) error {
	if !s.catalog.Enabled() || ret.Kind == ast.ReturnVariable || ret.Kind == "" {
		return nil
	}
	return s.validateNodeField(s.vars[ret.Variable], namespaceOrDefault(ret.Namespace), ret.Property)
}

func (s schemaState) validateWhereProperty(variable, namespace, property string) error {
	if !s.catalog.Enabled() {
		return nil
	}
	return s.validateNodeField(s.vars[variable], namespaceOrDefault(namespace), property)
}

func (s schemaState) validateNodeField(types []schema.NodeType, namespace, field string) error {
	if s.catalog.NodeFieldKnown(types, namespace, field) {
		return nil
	}
	return fmt.Errorf("unknown %s field %q", namespace, field)
}

func (s schemaState) validateEdgeField(types []schema.EdgeType, namespace, field string) error {
	if s.catalog.EdgeFieldKnown(types, namespace, field) {
		return nil
	}
	return fmt.Errorf("unknown edge %s field %q", namespace, field)
}

func (s schemaState) validateRelationshipEndpoints(edgeTypes []schema.EdgeType, fromVar, toVar string) error {
	if s.catalog.EndpointsAllowed(edgeTypes, s.vars[fromVar], s.vars[toVar]) {
		return nil
	}
	return fmt.Errorf("relationship endpoint constraints do not allow %q -> %q", fromVar, toVar)
}

func namespaceOrDefault(namespace string) string {
	if namespace == "" {
		return "properties"
	}
	return namespace
}
