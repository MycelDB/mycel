package binding

import (
	"fmt"
	"sort"
	"strings"

	querylogical "github.com/myceldb/mycel/internal/query/logical"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
)

func (b *binder) bindNodePattern(pattern querylogical.NodePattern, aliasRequired bool) ([]schemamodel.NodeType, error) {
	if aliasRequired && strings.TrimSpace(pattern.Alias) == "" {
		return nil, b.invalid("node alias is required")
	}
	if err := validateNames("node label", pattern.Labels); err != nil {
		return nil, b.invalid("%s", err)
	}
	for _, nodeID := range pattern.NodeIDs {
		if strings.TrimSpace(nodeID) == "" {
			return nil, b.invalid("node ID cannot be empty")
		}
	}
	types, err := b.matchNodeTypes(pattern.Labels)
	if err != nil {
		return nil, err
	}
	if err := b.validatePatternProperties("node", pattern.Alias, pattern.Properties, func(ref querylogical.FieldRef) error {
		return b.validateFieldAgainstNodeTypes(ref, types)
	}); err != nil {
		return nil, err
	}
	if strings.TrimSpace(pattern.Alias) != "" {
		variable := Variable{Name: pattern.Alias, Kind: VariableNode, NodeTypes: nodeTypeNames(types)}
		if err := b.declare(variable); err != nil {
			return nil, err
		}
		b.nodeBindings[variable.Name] = types
	}
	return types, nil
}

func (b *binder) bindEdgePattern(pattern querylogical.EdgePattern) ([]schemamodel.EdgeType, error) {
	if pattern.Direction == querylogical.DirectionUnspecified {
		return nil, b.invalid("traversal direction is required")
	}
	if pattern.Direction != querylogical.DirectionIncoming && pattern.Direction != querylogical.DirectionOutgoing && pattern.Direction != querylogical.DirectionUndirected {
		return nil, b.invalid("unsupported traversal direction %q", pattern.Direction)
	}
	if err := validateNames("edge label", pattern.Labels); err != nil {
		return nil, b.invalid("%s", err)
	}
	types, err := b.matchEdgeTypes(pattern.Labels)
	if err != nil {
		return nil, err
	}
	if err := b.validatePatternProperties("edge", pattern.Alias, pattern.Properties, func(ref querylogical.FieldRef) error {
		return b.validateFieldAgainstEdgeTypes(ref, types)
	}); err != nil {
		return nil, err
	}
	if strings.TrimSpace(pattern.Alias) != "" {
		variable := Variable{Name: pattern.Alias, Kind: VariableEdge, EdgeTypes: edgeTypeNames(types)}
		if err := b.declare(variable); err != nil {
			return nil, err
		}
		b.edgeBindings[variable.Name] = types
	}
	return types, nil
}

func (b *binder) validatePatternProperties(kind string, alias string, properties map[string]querylogical.Value, validateField func(querylogical.FieldRef) error) error {
	for _, field := range sortedValueKeys(properties) {
		ref := querylogical.FieldRef{Alias: alias, Namespace: querylogical.NamespaceProperties, Name: field}
		if err := validateField(ref); err != nil {
			return err
		}
		if err := b.validateValue(properties[field]); err != nil {
			return fmt.Errorf("%s property %q: %w", kind, field, err)
		}
	}
	return nil
}

func (b *binder) matchNodeTypes(labels []string) ([]schemamodel.NodeType, error) {
	matched, unknown := b.catalog.ResolveNodeLabels(labels)
	if b.catalog.Strict() && unknown != "" {
		return nil, b.invalid("unknown node label %q", unknown)
	}
	return matched, nil
}

func (b *binder) matchEdgeTypes(labels []string) ([]schemamodel.EdgeType, error) {
	matched, unknown := b.catalog.ResolveEdgeLabels(labels)
	if b.catalog.Strict() && unknown != "" {
		return nil, b.invalid("unknown edge label %q", unknown)
	}
	return matched, nil
}

func (b *binder) validateField(field querylogical.FieldRef) error {
	if strings.TrimSpace(field.Alias) == "" {
		return b.invalid("field alias is required")
	}
	if strings.TrimSpace(field.Name) == "" {
		return b.invalid("field name is required")
	}
	if !validNamespace(field.Namespace) {
		return b.invalid("unsupported field namespace %q", field.Namespace)
	}
	variable, ok := b.variables[field.Alias]
	if !ok {
		return b.invalid("field alias %q is not defined", field.Alias)
	}
	switch variable.Kind {
	case VariableNode:
		return b.validateFieldAgainstNodeTypes(field, b.nodeSchemaTypes(field.Alias))
	case VariableEdge:
		return b.validateFieldAgainstEdgeTypes(field, b.edgeSchemaTypes(field.Alias))
	case VariablePath:
		return b.invalid("path alias %q does not support field references", field.Alias)
	default:
		return b.invalid("field alias %q has unsupported kind %q", field.Alias, variable.Kind)
	}
}

func (b *binder) validateFieldAgainstNodeTypes(field querylogical.FieldRef, types []schemamodel.NodeType) error {
	if strings.TrimSpace(field.Name) == "" {
		return b.invalid("field name is required")
	}
	if !validNamespace(field.Namespace) {
		return b.invalid("unsupported field namespace %q", field.Namespace)
	}
	if nodeRuntimeField(field) || b.catalog.NodeFieldKnown(types, string(field.Namespace), field.Name) {
		return nil
	}
	return b.invalid("unknown %s field %q", field.Namespace, field.Name)
}

func (b *binder) validateFieldAgainstEdgeTypes(field querylogical.FieldRef, types []schemamodel.EdgeType) error {
	if strings.TrimSpace(field.Name) == "" {
		return b.invalid("field name is required")
	}
	if !validNamespace(field.Namespace) {
		return b.invalid("unsupported field namespace %q", field.Namespace)
	}
	if edgeRuntimeField(field) || b.catalog.EdgeFieldKnown(types, string(field.Namespace), field.Name) {
		return nil
	}
	return b.invalid("unknown edge %s field %q", field.Namespace, field.Name)
}

func nodeRuntimeField(field querylogical.FieldRef) bool {
	return field.Namespace == querylogical.NamespaceProperties && (field.Name == "node_id" || field.Name == "content")
}

func edgeRuntimeField(field querylogical.FieldRef) bool {
	if field.Namespace != querylogical.NamespaceProperties {
		return false
	}
	switch field.Name {
	case "edge_id", "from_node_id", "to_node_id":
		return true
	default:
		return false
	}
}

func (b *binder) validateEndpoints(edgeTypes []schemamodel.EdgeType, fromTypes []schemamodel.NodeType, toTypes []schemamodel.NodeType, direction querylogical.Direction, fromAlias string, toAlias string) error {
	switch direction {
	case querylogical.DirectionIncoming:
		if b.catalog.EndpointsAllowed(edgeTypes, toTypes, fromTypes) {
			return nil
		}
	case querylogical.DirectionUndirected:
		if b.catalog.EndpointsAllowed(edgeTypes, fromTypes, toTypes) || b.catalog.EndpointsAllowed(edgeTypes, toTypes, fromTypes) {
			return nil
		}
	default:
		if b.catalog.EndpointsAllowed(edgeTypes, fromTypes, toTypes) {
			return nil
		}
	}
	return b.invalid("edge endpoint constraints do not allow %q -> %q", fromAlias, toAlias)
}

func (b *binder) nodeSchemaTypes(alias string) []schemamodel.NodeType {
	return b.nodeBindings[alias]
}

func (b *binder) edgeSchemaTypes(alias string) []schemamodel.EdgeType {
	return b.edgeBindings[alias]
}

func validateNames(kind string, values []string) error {
	seen := map[string]struct{}{}
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			return fmt.Errorf("%s cannot be empty", kind)
		}
		if trimmed != value {
			return fmt.Errorf("%s %q must not contain surrounding whitespace", kind, value)
		}
		if _, ok := seen[value]; ok {
			return fmt.Errorf("duplicate %s %q", kind, value)
		}
		seen[value] = struct{}{}
	}
	return nil
}

func validNamespace(namespace querylogical.FieldNamespace) bool {
	switch namespace {
	case querylogical.NamespaceProperties, querylogical.NamespacePayload, querylogical.NamespaceMeta:
		return true
	default:
		return false
	}
}

func nodeTypeNames(types []schemamodel.NodeType) []string {
	out := make([]string, 0, len(types))
	for _, typ := range types {
		out = append(out, typ.Name)
	}
	return out
}

func edgeTypeNames(types []schemamodel.EdgeType) []string {
	out := make([]string, 0, len(types))
	for _, typ := range types {
		out = append(out, typ.Name)
	}
	return out
}

func sortedValueKeys(values map[string]querylogical.Value) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
