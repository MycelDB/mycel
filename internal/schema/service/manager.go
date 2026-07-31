package service

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	graph "github.com/myceldb/mycel/internal/graph/model"
	schemacompile "github.com/myceldb/mycel/internal/schema/compile"
	"github.com/myceldb/mycel/internal/schema/dsl"
	schema "github.com/myceldb/mycel/internal/schema/model"
	"github.com/myceldb/mycel/internal/schema/storage"
	"github.com/myceldb/mycel/internal/wal"
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
	DeleteDomainSchema(ctx context.Context, domainID graph.DomainID) error
	ValidateNode(ctx context.Context, domainID graph.DomainID, node graph.Node) (ValidationResult, error)
	ValidateEdge(ctx context.Context, domainID graph.DomainID, edge graph.Edge, from graph.Node, to graph.Node) (ValidationResult, error)
	PutDomainSchemaGWL(ctx context.Context, domainID graph.DomainID, source string) error
	ResolveNodeLabel(ctx context.Context, domainID graph.DomainID, label string) ([]schema.NodeType, error)
	ResolveEdgeLabel(ctx context.Context, domainID graph.DomainID, label string) ([]schema.EdgeType, error)
}

type SchemaManager struct {
	store              storage.Store
	now                func() time.Time
	cache              *validationCache
	wal                *wal.Manager
	walProgress        wal.AppliedLSNStore
	walWaiter          *wal.ApplyWaiter
	raftGroups         *consensus.MultiGroup
	raftPartitionCount uint32
}

func NewManager(store storage.Store) *SchemaManager {
	m := &SchemaManager{store: store, now: func() time.Time { return time.Now().UTC() }, cache: newValidationCache()}
	return m
}

func (m *SchemaManager) WarmCache(ctx context.Context) error {
	items, err := m.store.ListDomainSchemas(ctx)
	if err != nil {
		return err
	}
	for _, item := range items {
		compiled, err := schemacompile.Compile(item)
		if err != nil {
			return err
		}
		m.cache.put(item.DomainID, compiled)
	}
	return nil
}

func (m *SchemaManager) GetDomainSchema(ctx context.Context, domainID graph.DomainID) (schema.DomainSchema, error) {
	return m.store.GetDomainSchema(ctx, domainID)
}

func (m *SchemaManager) PutDomainSchemaGWL(ctx context.Context, domainID graph.DomainID, source string) error {
	value, err := dsl.Parse(source)
	if err != nil {
		return err
	}
	value.DomainID = domainID
	value.SourceGWL = source
	value.SourceHash = schemacompile.SourceHash(source)
	return m.PutDomainSchema(ctx, value)
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
	compiled, err := schemacompile.Compile(value)
	if err != nil {
		return err
	}
	if value.SourceHash == "" && value.SourceGWL != "" {
		value.SourceHash = schemacompile.SourceHash(value.SourceGWL)
	}
	_ = compiled
	return m.commitDomainSchema(ctx, value)
}

func (m *SchemaManager) DeleteDomainSchema(ctx context.Context, domainID graph.DomainID) error {
	return m.commitDeleteDomainSchema(ctx, domainID)
}

func (m *SchemaManager) applyDomainSchema(ctx context.Context, value schema.DomainSchema) error {
	compiled, err := schemacompile.Compile(value)
	if err != nil {
		return err
	}
	if err := m.store.PutDomainSchema(ctx, value); err != nil {
		return err
	}
	m.cache.put(value.DomainID, compiled)
	return nil
}

func (m *SchemaManager) applyDeleteDomainSchema(ctx context.Context, domainID graph.DomainID) error {
	if err := m.store.DeleteDomainSchema(ctx, domainID); err != nil {
		return err
	}
	m.cache.delete(domainID)
	return nil
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

func (m *SchemaManager) compiledFor(ctx context.Context, domainID graph.DomainID) (*schemacompile.CompiledSchema, error) {
	if compiled, ok := m.cache.get(domainID); ok {
		return compiled, nil
	}
	value, err := m.store.GetDomainSchema(ctx, domainID)
	if err != nil {
		return nil, err
	}
	compiled, err := schemacompile.Compile(value)
	if err != nil {
		return nil, err
	}
	m.cache.put(domainID, compiled)
	return compiled, nil
}

func (m *SchemaManager) ValidateNode(ctx context.Context, domainID graph.DomainID, node graph.Node) (ValidationResult, error) {
	compiled, err := m.compiledFor(ctx, domainID)
	if errors.Is(err, storage.ErrNotFound) {
		return ValidationResult{Mode: schema.SchemaModePermissive}, nil
	}
	if err != nil {
		return ValidationResult{}, err
	}
	value := compiled.Schema
	result := ValidationResult{Mode: value.Mode}
	matched := schemacompile.NodeTypesFor(compiled, node)
	if len(node.Labels) == 0 && len(matched) == 0 {
		return result, nil
	}
	for _, label := range node.Labels {
		if _, ok := compiled.NodeTypesByLabel[label]; !ok {
			result.add(value.Mode, "labels", fmt.Sprintf("unknown node label %q", label))
		}
	}
	if rt, ok := graph.Property(node, "record_type"); ok {
		if str, ok := rt.(string); ok {
			if compiled.NodeTypesByRecordType[str] == nil {
				result.add(value.Mode, "properties.record_type", fmt.Sprintf("unknown record_type %q", str))
			}
		}
	}
	for _, nt := range matched {
		validateFields(value.Mode, &result, "properties", nt.Properties, node.Properties)
		validateFields(value.Mode, &result, "payload", nt.Payload, node.Payload)
		validateFields(value.Mode, &result, "meta", nt.Meta, node.Meta)
	}
	return result, nil
}

func (m *SchemaManager) ValidateEdge(ctx context.Context, domainID graph.DomainID, edge graph.Edge, from graph.Node, to graph.Node) (ValidationResult, error) {
	compiled, err := m.compiledFor(ctx, domainID)
	if errors.Is(err, storage.ErrNotFound) {
		return ValidationResult{Mode: schema.SchemaModePermissive}, nil
	}
	if err != nil {
		return ValidationResult{}, err
	}
	value := compiled.Schema
	result := ValidationResult{Mode: value.Mode}
	for _, label := range edge.Labels {
		types, ok := compiled.EdgeTypesByLabel[label]
		if !ok {
			result.add(value.Mode, "labels", fmt.Sprintf("unknown edge label %q", label))
			continue
		}
		for _, et := range types {
			validateFields(value.Mode, &result, "properties", et.Properties, edge.Properties)
			validateFields(value.Mode, &result, "payload", et.Payload, edge.Payload)
			validateFields(value.Mode, &result, "meta", et.Meta, edge.Meta)
			if !endpointMatchesCompiled(compiled, et.From, from) {
				result.add(value.Mode, "from", fmt.Sprintf("edge %q does not allow source node", label))
			}
			if !endpointMatchesCompiled(compiled, et.To, to) {
				result.add(value.Mode, "to", fmt.Sprintf("edge %q does not allow target node", label))
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
	case schema.FieldTypeDate:
		str, ok := value.(string)
		return ok && regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`).MatchString(str)
	case schema.FieldTypeObject, schema.FieldTypeMap:
		_, ok := value.(map[string]any)
		return ok
	case schema.FieldTypeJSON:
		return true
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

func endpointMatchesCompiled(compiled *schemacompile.CompiledSchema, spec schema.EndpointSpec, node graph.Node) bool {
	if len(spec.NodeTypes) == 0 && len(spec.Labels) == 0 {
		return true
	}
	for _, label := range node.Labels {
		if contains(spec.Labels, label) {
			return true
		}
	}
	for _, nt := range schemacompile.NodeTypesFor(compiled, node) {
		if contains(spec.NodeTypes, nt.Name) {
			return true
		}
		for _, label := range nt.Labels {
			if contains(spec.Labels, label) {
				return true
			}
		}
	}
	return false
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
