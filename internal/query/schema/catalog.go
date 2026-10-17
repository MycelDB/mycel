// Package schema provides shared, side-effect-free schema lookup semantics for
// query analysis and binding.
package schema

import schemamodel "github.com/myceldb/mycel/internal/schema/model"

// Catalog indexes one optional domain schema by type name and label.
type Catalog struct {
	enabled   bool
	document  schemamodel.DomainSchema
	nodeTypes map[string][]schemamodel.NodeType
	edgeTypes map[string][]schemamodel.EdgeType
}

// NewCatalog constructs an immutable query schema catalog.
func NewCatalog(document *schemamodel.DomainSchema) Catalog {
	if document == nil {
		return Catalog{}
	}
	normalized := cloneDomainSchema(*document).Normalize()
	catalog := Catalog{
		enabled: true, document: normalized,
		nodeTypes: map[string][]schemamodel.NodeType{}, edgeTypes: map[string][]schemamodel.EdgeType{},
	}
	for _, typ := range normalized.NodeTypes {
		catalog.nodeTypes[typ.Name] = append(catalog.nodeTypes[typ.Name], typ)
		for _, label := range typ.Labels {
			catalog.nodeTypes[label] = append(catalog.nodeTypes[label], typ)
		}
	}
	for _, typ := range normalized.EdgeTypes {
		catalog.edgeTypes[typ.Name] = append(catalog.edgeTypes[typ.Name], typ)
		for _, label := range typ.Labels {
			catalog.edgeTypes[label] = append(catalog.edgeTypes[label], typ)
		}
	}
	return catalog
}

// Enabled reports whether a schema document was supplied.
func (c Catalog) Enabled() bool { return c.enabled }

// Strict reports whether unknown schema names must be rejected.
func (c Catalog) Strict() bool {
	return c.enabled && c.document.Mode == schemamodel.SchemaModeStrict
}

// Version returns the normalized schema version.
func (c Catalog) Version() string { return c.document.Version }

// ResolveNodeLabels returns the union of node types addressed by labels and the
// first unknown label, if any.
func (c Catalog) ResolveNodeLabels(labels []string) ([]schemamodel.NodeType, string) {
	return resolveLabels(labels, c.nodeTypes, appendNodeTypes)
}

// ResolveEdgeLabels returns the union of edge types addressed by labels and the
// first unknown label, if any.
func (c Catalog) ResolveEdgeLabels(labels []string) ([]schemamodel.EdgeType, string) {
	return resolveLabels(labels, c.edgeTypes, appendEdgeTypes)
}

func resolveLabels[T any](labels []string, index map[string][]T, appendValues func([]T, ...T) []T) ([]T, string) {
	var matched []T
	var firstUnknown string
	for _, label := range labels {
		values := index[label]
		if len(values) == 0 {
			if firstUnknown == "" {
				firstUnknown = label
			}
			continue
		}
		matched = appendValues(matched, values...)
	}
	return matched, firstUnknown
}

// NodeFieldKnown applies permissive/no-schema behavior and strict field lookup.
func (c Catalog) NodeFieldKnown(types []schemamodel.NodeType, namespace string, name string) bool {
	return fieldKnown(c.Strict(), types, namespace, name, nodeFields)
}

// EdgeFieldKnown applies permissive/no-schema behavior and strict field lookup.
func (c Catalog) EdgeFieldKnown(types []schemamodel.EdgeType, namespace string, name string) bool {
	return fieldKnown(c.Strict(), types, namespace, name, edgeFields)
}

func fieldKnown[T any](strict bool, types []T, namespace string, name string, fields func(T, string) []schemamodel.FieldSpec) bool {
	if !strict || len(types) == 0 {
		return true
	}
	for _, typ := range types {
		if fieldsContain(fields(typ, namespace), name) {
			return true
		}
	}
	return false
}

// EndpointsAllowed reports whether at least one edge type permits the directed
// node-type pair. Unknown endpoint types remain permissive for compatibility.
func (c Catalog) EndpointsAllowed(edgeTypes []schemamodel.EdgeType, fromTypes []schemamodel.NodeType, toTypes []schemamodel.NodeType) bool {
	if !c.Strict() || len(edgeTypes) == 0 || len(fromTypes) == 0 || len(toTypes) == 0 {
		return true
	}
	for _, edgeType := range edgeTypes {
		if endpointAllows(edgeType.From, fromTypes) && endpointAllows(edgeType.To, toTypes) {
			return true
		}
	}
	return false
}

func nodeFields(typ schemamodel.NodeType, namespace string) []schemamodel.FieldSpec {
	return namespacedFields(namespace, typ.Properties, typ.Payload, typ.Meta)
}

func edgeFields(typ schemamodel.EdgeType, namespace string) []schemamodel.FieldSpec {
	return namespacedFields(namespace, typ.Properties, typ.Payload, typ.Meta)
}

func namespacedFields(namespace string, properties []schemamodel.FieldSpec, payload []schemamodel.FieldSpec, meta []schemamodel.FieldSpec) []schemamodel.FieldSpec {
	switch namespace {
	case "", "properties":
		return properties
	case "payload":
		return payload
	case "meta":
		return meta
	default:
		return nil
	}
}

func fieldsContain(fields []schemamodel.FieldSpec, name string) bool {
	for _, field := range fields {
		if field.Name == name {
			return true
		}
	}
	return false
}

func endpointAllows(endpoint schemamodel.EndpointSpec, nodeTypes []schemamodel.NodeType) bool {
	if len(endpoint.NodeTypes) == 0 && len(endpoint.Labels) == 0 {
		return true
	}
	for _, typ := range nodeTypes {
		if contains(endpoint.NodeTypes, typ.Name) {
			return true
		}
		for _, label := range typ.Labels {
			if contains(endpoint.Labels, label) {
				return true
			}
		}
	}
	return false
}

func appendNodeTypes(out []schemamodel.NodeType, values ...schemamodel.NodeType) []schemamodel.NodeType {
	return appendUnique(out, values, func(value schemamodel.NodeType) string { return value.Name })
}

func appendEdgeTypes(out []schemamodel.EdgeType, values ...schemamodel.EdgeType) []schemamodel.EdgeType {
	return appendUnique(out, values, func(value schemamodel.EdgeType) string { return value.Name })
}

func appendUnique[T any](out []T, values []T, name func(T) string) []T {
	seen := make(map[string]struct{}, len(out)+len(values))
	for _, value := range out {
		seen[name(value)] = struct{}{}
	}
	for _, value := range values {
		key := name(value)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, value)
	}
	return out
}

func cloneDomainSchema(document schemamodel.DomainSchema) schemamodel.DomainSchema {
	document.NodeTypes = append([]schemamodel.NodeType(nil), document.NodeTypes...)
	for i := range document.NodeTypes {
		typ := &document.NodeTypes[i]
		typ.Labels = append([]string(nil), typ.Labels...)
		typ.Properties = append([]schemamodel.FieldSpec(nil), typ.Properties...)
		typ.Payload = append([]schemamodel.FieldSpec(nil), typ.Payload...)
		typ.Meta = append([]schemamodel.FieldSpec(nil), typ.Meta...)
	}
	document.EdgeTypes = append([]schemamodel.EdgeType(nil), document.EdgeTypes...)
	for i := range document.EdgeTypes {
		typ := &document.EdgeTypes[i]
		typ.Labels = append([]string(nil), typ.Labels...)
		typ.From.NodeTypes = append([]string(nil), typ.From.NodeTypes...)
		typ.From.Labels = append([]string(nil), typ.From.Labels...)
		typ.To.NodeTypes = append([]string(nil), typ.To.NodeTypes...)
		typ.To.Labels = append([]string(nil), typ.To.Labels...)
		typ.Properties = append([]schemamodel.FieldSpec(nil), typ.Properties...)
		typ.Payload = append([]schemamodel.FieldSpec(nil), typ.Payload...)
		typ.Meta = append([]schemamodel.FieldSpec(nil), typ.Meta...)
	}
	document.Indexes = append([]schemamodel.IndexDefinition(nil), document.Indexes...)
	for i := range document.Indexes {
		document.Indexes[i].Labels = append([]string(nil), document.Indexes[i].Labels...)
	}
	return document
}

func contains(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}
