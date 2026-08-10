package model

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// Validate checks schema self-consistency.
func Validate(s DomainSchema) error {
	s = s.Normalize()
	if s.DomainID == uuid.Nil {
		return fmt.Errorf("domain id is required")
	}
	switch s.Mode {
	case "", SchemaModePermissive, SchemaModeWarn, SchemaModeStrict:
	default:
		return fmt.Errorf("invalid schema mode %q", s.Mode)
	}
	nodeTypes := map[string]NodeType{}
	nodeLabels := map[string]string{}
	for _, nt := range s.NodeTypes {
		if nt.Name == "" {
			return fmt.Errorf("node type name is required")
		}
		if _, ok := nodeTypes[nt.Name]; ok {
			return fmt.Errorf("duplicate node type %q", nt.Name)
		}
		nodeTypes[nt.Name] = nt
		for _, label := range nt.Labels {
			if owner, ok := nodeLabels[label]; ok && owner != nt.Name {
				return fmt.Errorf("node label %q is used by both %q and %q", label, owner, nt.Name)
			}
			nodeLabels[label] = nt.Name
		}
		if err := validateFields("node type "+nt.Name+" properties", nt.Properties); err != nil {
			return err
		}
		if err := validateFields("node type "+nt.Name+" payload", nt.Payload); err != nil {
			return err
		}
		if err := validateFields("node type "+nt.Name+" meta", nt.Meta); err != nil {
			return err
		}
		if nt.Indexing.SemanticDirtyCooldown < 0 {
			return fmt.Errorf("node type %s semantic dirty cooldown must be non-negative", nt.Name)
		}
	}
	edgeTypes := map[string]EdgeType{}
	edgeLabels := map[string]string{}
	for _, et := range s.EdgeTypes {
		if et.Name == "" {
			return fmt.Errorf("edge type name is required")
		}
		if _, ok := edgeTypes[et.Name]; ok {
			return fmt.Errorf("duplicate edge type %q", et.Name)
		}
		edgeTypes[et.Name] = et
		for _, label := range et.Labels {
			if owner, ok := edgeLabels[label]; ok && owner != et.Name {
				return fmt.Errorf("edge label %q is used by both %q and %q", label, owner, et.Name)
			}
			edgeLabels[label] = et.Name
		}
		if err := validateEndpoint("edge type "+et.Name+" from", et.From, nodeTypes, nodeLabels); err != nil {
			return err
		}
		if err := validateEndpoint("edge type "+et.Name+" to", et.To, nodeTypes, nodeLabels); err != nil {
			return err
		}
		if err := validateFields("edge type "+et.Name+" properties", et.Properties); err != nil {
			return err
		}
		if err := validateFields("edge type "+et.Name+" payload", et.Payload); err != nil {
			return err
		}
		if err := validateFields("edge type "+et.Name+" meta", et.Meta); err != nil {
			return err
		}
		if et.Indexing.SemanticDirtyCooldown < 0 {
			return fmt.Errorf("edge type %s semantic dirty cooldown must be non-negative", et.Name)
		}
	}
	if err := validateIndexes(s.Indexes, nodeTypes, edgeTypes); err != nil {
		return err
	}
	return nil
}

func validateIndexes(indexes []IndexDefinition, nodeTypes map[string]NodeType, edgeTypes map[string]EdgeType) error {
	seen := map[string]struct{}{}
	for _, idx := range indexes {
		if idx.Name == "" {
			return fmt.Errorf("index name is required")
		}
		if _, ok := seen[idx.Name]; ok {
			return fmt.Errorf("duplicate index %q", idx.Name)
		}
		seen[idx.Name] = struct{}{}
		var fields []FieldSpec
		switch idx.TargetKind {
		case IndexTargetNode:
			nt, ok := nodeTypes[idx.TargetType]
			if !ok {
				return fmt.Errorf("index %q references unknown node type %q", idx.Name, idx.TargetType)
			}
			fields = fieldsForNamespace(nt.Properties, nt.Payload, nt.Meta, idx.Field.Namespace)
		case IndexTargetEdge:
			et, ok := edgeTypes[idx.TargetType]
			if !ok {
				return fmt.Errorf("index %q references unknown edge type %q", idx.Name, idx.TargetType)
			}
			fields = fieldsForNamespace(et.Properties, et.Payload, et.Meta, idx.Field.Namespace)
		default:
			return fmt.Errorf("index %q has invalid target kind %q", idx.Name, idx.TargetKind)
		}
		if idx.Field.Namespace == "" || idx.Field.Name == "" {
			return fmt.Errorf("index %q field path is required", idx.Name)
		}
		if idx.Field.Namespace != "properties" {
			return fmt.Errorf("index %q field namespace %q is not supported", idx.Name, idx.Field.Namespace)
		}
		field, ok := findField(fields, idx.Field.Name)
		if !ok {
			return fmt.Errorf("index %q references unknown field %q.%s", idx.Name, idx.Field.Namespace, idx.Field.Name)
		}
		if field.Repeated {
			return fmt.Errorf("index %q field %q cannot be repeated", idx.Name, idx.Field.Name)
		}
		switch idx.Kind {
		case IndexKindEquality:
			if idx.Direction != "" {
				return fmt.Errorf("index %q direction requires ordered kind", idx.Name)
			}
		case IndexKindOrdered:
			if !isOrderableField(field.Type) {
				return fmt.Errorf("index %q field %q type %q is not orderable", idx.Name, idx.Field.Name, field.Type)
			}
			switch idx.Direction {
			case IndexSortDirectionAsc, IndexSortDirectionDesc:
			default:
				return fmt.Errorf("index %q has invalid direction %q", idx.Name, idx.Direction)
			}
		default:
			return fmt.Errorf("index %q has invalid kind %q", idx.Name, idx.Kind)
		}
	}
	return nil
}

func fieldsForNamespace(properties, payload, meta []FieldSpec, namespace string) []FieldSpec {
	switch namespace {
	case "properties":
		return properties
	case "payload":
		return payload
	case "meta":
		return meta
	default:
		return nil
	}
}

func findField(fields []FieldSpec, name string) (FieldSpec, bool) {
	for _, field := range fields {
		if field.Name == name {
			return field, true
		}
	}
	return FieldSpec{}, false
}

func isOrderableField(fieldType FieldType) bool {
	switch fieldType {
	case FieldTypeString, FieldTypeInt, FieldTypeFloat, FieldTypeBool, FieldTypeDateTime, FieldTypeDate, FieldTypeEnum:
		return true
	default:
		return false
	}
}

func validateEndpoint(scope string, spec EndpointSpec, nodeTypes map[string]NodeType, nodeLabels map[string]string) error {
	for _, name := range spec.NodeTypes {
		if _, ok := nodeTypes[name]; !ok {
			return fmt.Errorf("%s references unknown node type %q", scope, name)
		}
	}
	for _, label := range spec.Labels {
		if _, ok := nodeLabels[label]; !ok {
			return fmt.Errorf("%s references unknown node label %q", scope, label)
		}
	}
	return nil
}

func validateFields(scope string, fields []FieldSpec) error {
	seen := map[string]struct{}{}
	for _, field := range fields {
		name := strings.TrimSpace(field.Name)
		if name == "" {
			return fmt.Errorf("%s contains field with empty name", scope)
		}
		if _, ok := seen[name]; ok {
			return fmt.Errorf("%s contains duplicate field %q", scope, name)
		}
		seen[name] = struct{}{}
		switch field.Type {
		case FieldTypeString, FieldTypeInt, FieldTypeFloat, FieldTypeBool, FieldTypeDateTime, FieldTypeDate, FieldTypeObject, FieldTypeMap, FieldTypeJSON, FieldTypeEnum:
		default:
			return fmt.Errorf("%s field %q has invalid type %q", scope, name, field.Type)
		}
		if field.Type == FieldTypeEnum && len(field.EnumValues) == 0 {
			return fmt.Errorf("%s enum field %q requires enum values", scope, name)
		}
	}
	return nil
}
