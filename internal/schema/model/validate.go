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
	nodeTypes := map[string]struct{}{}
	nodeLabels := map[string]string{}
	for _, nt := range s.NodeTypes {
		if nt.Name == "" {
			return fmt.Errorf("node type name is required")
		}
		if _, ok := nodeTypes[nt.Name]; ok {
			return fmt.Errorf("duplicate node type %q", nt.Name)
		}
		nodeTypes[nt.Name] = struct{}{}
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
	edgeTypes := map[string]struct{}{}
	edgeLabels := map[string]string{}
	for _, et := range s.EdgeTypes {
		if et.Name == "" {
			return fmt.Errorf("edge type name is required")
		}
		if _, ok := edgeTypes[et.Name]; ok {
			return fmt.Errorf("duplicate edge type %q", et.Name)
		}
		edgeTypes[et.Name] = struct{}{}
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
	return nil
}

func validateEndpoint(scope string, spec EndpointSpec, nodeTypes map[string]struct{}, nodeLabels map[string]string) error {
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
