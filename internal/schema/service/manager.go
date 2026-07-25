package service

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/google/uuid"
	graph "github.com/myceldb/mycel/internal/graph/model"
	schema "github.com/myceldb/mycel/internal/schema/model"
	"github.com/myceldb/mycel/internal/schema/storage"
)

var ErrSchemaNotFound = storage.ErrNotFound

type ValidationSeverity string

const (
	ValidationSeverityWarning ValidationSeverity = "warning"
	ValidationSeverityError   ValidationSeverity = "error"
)

type ValidationIssue struct {
	Severity ValidationSeverity
	Path     string
	Message  string
}

type ValidationResult struct {
	Mode   schema.SchemaMode
	Issues []ValidationIssue
}

func (r ValidationResult) Valid() bool {
	for _, issue := range r.Issues {
		if issue.Severity == ValidationSeverityError {
			return false
		}
	}
	return true
}

// Manager exposes schema lookup, resolution, and graph validation.
type Manager interface {
	GetDomainSchema(ctx context.Context, domainID graph.DomainID) (schema.DomainSchema, error)
	PutDomainSchema(ctx context.Context, schema schema.DomainSchema) error
	ValidateNode(ctx context.Context, domainID graph.DomainID, node graph.Node) (ValidationResult, error)
	ValidateEdge(ctx context.Context, domainID graph.DomainID, edge graph.Edge, from graph.Node, to graph.Node) (ValidationResult, error)
	ResolveNodeLabel(ctx context.Context, domainID graph.DomainID, label string) ([]schema.NodeType, error)
	ResolveEdgeLabel(ctx context.Context, domainID graph.DomainID, label string) ([]schema.EdgeType, error)
}

type SchemaManager struct {
	store storage.Store
	now   func() time.Time
}

func NewManager(store storage.Store) *SchemaManager {
	return &SchemaManager{store: store, now: func() time.Time { return time.Now().UTC() }}
}

func (m *SchemaManager) GetDomainSchema(ctx context.Context, domainID graph.DomainID) (schema.DomainSchema, error) {
	return m.store.GetDomainSchema(ctx, domainID)
}

func (m *SchemaManager) PutDomainSchema(ctx context.Context, value schema.DomainSchema) error {
	value = value.Normalize()
	if value.ID == uuid.Nil {
		value.ID = uuid.New()
	}
	now := m.now()
	if value.CreatedAt.IsZero() {
		value.CreatedAt = now
	}
	value.UpdatedAt = now
	if err := schema.Validate(value); err != nil {
		return err
	}
	return m.store.PutDomainSchema(ctx, value)
}

func (m *SchemaManager) ResolveNodeLabel(ctx context.Context, domainID graph.DomainID, label string) ([]schema.NodeType, error) {
	value, err := m.store.GetDomainSchema(ctx, domainID)
	if err != nil {
		return nil, err
	}
	label = strings.TrimSpace(label)
	var matches []schema.NodeType
	for _, nt := range value.NodeTypes {
		if nt.Name == label || contains(nt.Labels, label) {
			matches = append(matches, nt)
		}
	}
	return matches, nil
}

func (m *SchemaManager) ResolveEdgeLabel(ctx context.Context, domainID graph.DomainID, label string) ([]schema.EdgeType, error) {
	value, err := m.store.GetDomainSchema(ctx, domainID)
	if err != nil {
		return nil, err
	}
	label = strings.TrimSpace(label)
	var matches []schema.EdgeType
	for _, et := range value.EdgeTypes {
		if et.Name == label || contains(et.Labels, label) {
			matches = append(matches, et)
		}
	}
	return matches, nil
}

func (m *SchemaManager) ValidateNode(ctx context.Context, domainID graph.DomainID, node graph.Node) (ValidationResult, error) {
	value, err := m.store.GetDomainSchema(ctx, domainID)
	if errors.Is(err, storage.ErrNotFound) {
		return ValidationResult{Mode: schema.SchemaModePermissive}, nil
	}
	if err != nil {
		return ValidationResult{}, err
	}
	value = value.Normalize()
	result := ValidationResult{Mode: value.Mode}
	if len(node.Labels) == 0 {
		return result, nil
	}
	for _, label := range node.Labels {
		if _, ok := findNodeTypes(value, label); !ok {
			result.add(value.Mode, "labels", fmt.Sprintf("unknown node label %q", label))
		}
	}
	for _, nt := range matchingNodeTypes(value, node.Labels) {
		validateFields(value.Mode, &result, "properties", nt.Properties, node.Properties)
		validateFields(value.Mode, &result, "payload", nt.Payload, node.Payload)
		validateFields(value.Mode, &result, "meta", nt.Meta, node.Meta)
	}
	return result, nil
}

func (m *SchemaManager) ValidateEdge(ctx context.Context, domainID graph.DomainID, edge graph.Edge, from graph.Node, to graph.Node) (ValidationResult, error) {
	value, err := m.store.GetDomainSchema(ctx, domainID)
	if errors.Is(err, storage.ErrNotFound) {
		return ValidationResult{Mode: schema.SchemaModePermissive}, nil
	}
	if err != nil {
		return ValidationResult{}, err
	}
	value = value.Normalize()
	result := ValidationResult{Mode: value.Mode}
	for _, label := range edge.Labels {
		types, ok := findEdgeTypes(value, label)
		if !ok {
			result.add(value.Mode, "labels", fmt.Sprintf("unknown edge label %q", label))
			continue
		}
		for _, et := range types {
			validateFields(value.Mode, &result, "properties", et.Properties, edge.Properties)
			validateFields(value.Mode, &result, "payload", et.Payload, edge.Payload)
			validateFields(value.Mode, &result, "meta", et.Meta, edge.Meta)
			if !endpointMatches(value, et.From, from) {
				result.add(value.Mode, "from", fmt.Sprintf("edge %q does not allow source node labels %v", label, from.Labels))
			}
			if !endpointMatches(value, et.To, to) {
				result.add(value.Mode, "to", fmt.Sprintf("edge %q does not allow target node labels %v", label, to.Labels))
			}
		}
	}
	return result, nil
}

func (r *ValidationResult) add(mode schema.SchemaMode, path, message string) {
	severity := ValidationSeverityWarning
	if mode == schema.SchemaModeStrict {
		severity = ValidationSeverityError
	}
	if mode == schema.SchemaModePermissive || mode == "" {
		return
	}
	r.Issues = append(r.Issues, ValidationIssue{Severity: severity, Path: path, Message: message})
}

func validateFields(mode schema.SchemaMode, result *ValidationResult, bucket string, specs []schema.FieldSpec, values map[string]any) {
	if values == nil {
		values = map[string]any{}
	}
	known := map[string]schema.FieldSpec{}
	for _, spec := range specs {
		known[spec.Name] = spec
		if spec.Required {
			if _, ok := values[spec.Name]; !ok {
				result.add(mode, bucket+"."+spec.Name, "required field is missing")
			}
		}
	}
	for name, value := range values {
		spec, ok := known[name]
		if !ok {
			result.add(mode, bucket+"."+name, fmt.Sprintf("unknown field %q", name))
			continue
		}
		if !valueMatches(spec, value) {
			result.add(mode, bucket+"."+name, fmt.Sprintf("field %q does not match type %q", name, spec.Type))
		}
	}
}

func valueMatches(spec schema.FieldSpec, value any) bool {
	if value == nil {
		return !spec.Required
	}
	if spec.Repeated {
		rv := reflect.ValueOf(value)
		if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
			return false
		}
		for i := 0; i < rv.Len(); i++ {
			if !scalarMatches(spec.Type, rv.Index(i).Interface(), spec.EnumValues) {
				return false
			}
		}
		return true
	}
	return scalarMatches(spec.Type, value, spec.EnumValues)
}

func scalarMatches(t schema.FieldType, value any, enumValues []string) bool {
	switch t {
	case schema.FieldTypeString, schema.FieldTypeDateTime:
		_, ok := value.(string)
		return ok
	case schema.FieldTypeInt:
		switch value.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			return true
		}
		return false
	case schema.FieldTypeFloat:
		switch value.(type) {
		case float32, float64, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			return true
		}
		return false
	case schema.FieldTypeBool:
		_, ok := value.(bool)
		return ok
	case schema.FieldTypeEnum:
		str, ok := value.(string)
		return ok && contains(enumValues, str)
	default:
		return false
	}
}

func matchingNodeTypes(value schema.DomainSchema, labels []string) []schema.NodeType {
	seen := map[string]struct{}{}
	var out []schema.NodeType
	for _, label := range labels {
		types, _ := findNodeTypes(value, label)
		for _, nt := range types {
			if _, ok := seen[nt.Name]; !ok {
				seen[nt.Name] = struct{}{}
				out = append(out, nt)
			}
		}
	}
	return out
}

func findNodeTypes(value schema.DomainSchema, label string) ([]schema.NodeType, bool) {
	var out []schema.NodeType
	for _, nt := range value.NodeTypes {
		if nt.Name == label || contains(nt.Labels, label) {
			out = append(out, nt)
		}
	}
	return out, len(out) > 0
}

func findEdgeTypes(value schema.DomainSchema, label string) ([]schema.EdgeType, bool) {
	var out []schema.EdgeType
	for _, et := range value.EdgeTypes {
		if et.Name == label || contains(et.Labels, label) {
			out = append(out, et)
		}
	}
	return out, len(out) > 0
}

func endpointMatches(value schema.DomainSchema, spec schema.EndpointSpec, node graph.Node) bool {
	if len(spec.NodeTypes) == 0 && len(spec.Labels) == 0 {
		return true
	}
	for _, label := range node.Labels {
		if contains(spec.Labels, label) {
			return true
		}
		types, _ := findNodeTypes(value, label)
		for _, nt := range types {
			if contains(spec.NodeTypes, nt.Name) {
				return true
			}
		}
	}
	return false
}

func contains(values []string, needle string) bool {
	needle = strings.TrimSpace(needle)
	for _, value := range values {
		if strings.TrimSpace(value) == needle {
			return true
		}
	}
	return false
}
