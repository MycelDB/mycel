package model

import (
	"strings"
	"time"

	"github.com/google/uuid"
	graph "github.com/myceldb/mycel/internal/graph/model"
)

// SchemaID identifies a domain schema version.
type SchemaID = uuid.UUID

// SchemaMode controls how schema misses are handled.
type SchemaMode string

const (
	SchemaModePermissive SchemaMode = "permissive"
	SchemaModeWarn       SchemaMode = "warn"
	SchemaModeStrict     SchemaMode = "strict"
)

// FieldType is a scalar schema field type.
type FieldType string

const (
	FieldTypeString   FieldType = "string"
	FieldTypeInt      FieldType = "int"
	FieldTypeFloat    FieldType = "float"
	FieldTypeBool     FieldType = "bool"
	FieldTypeDateTime FieldType = "datetime"
	FieldTypeEnum     FieldType = "enum"
)

// DomainSchema is the active graph schema for a domain.
type DomainSchema struct {
	ID        SchemaID
	DomainID  graph.DomainID
	Name      string
	Version   string
	Mode      SchemaMode
	NodeTypes []NodeType
	EdgeTypes []EdgeType
	Policies  SchemaPolicies
	CreatedAt time.Time
	UpdatedAt time.Time
}

type NodeType struct {
	Name       string
	Labels     []string
	Properties []FieldSpec
	Payload    []FieldSpec
	Meta       []FieldSpec
	Indexing   IndexPolicy
	UI         UIHints
	Reserved   bool
}

type EdgeType struct {
	Name       string
	Labels     []string
	From       EndpointSpec
	To         EndpointSpec
	Properties []FieldSpec
	Payload    []FieldSpec
	Meta       []FieldSpec
	Indexing   IndexPolicy
	Hierarchy  *HierarchyPolicy
	UI         UIHints
	Reserved   bool
}

type FieldSpec struct {
	Name        string
	Type        FieldType
	Required    bool
	Repeated    bool
	EnumValues  []string
	Description string
}

type EndpointSpec struct {
	NodeTypes []string
	Labels    []string
}

type IndexPolicy struct {
	Enabled  bool
	Fields   []string
	FullText bool
	Semantic bool
}

type HierarchyPolicy struct {
	Enabled      bool
	Acyclic      bool
	SingleParent bool
	SameDomain   bool
}

type SchemaPolicies struct {
	DefaultIndexing IndexPolicy
}

type UIHints struct {
	Icon        string
	Color       string
	DisplayName string
}

// Normalize fills defaults and trims type/label/field names.
func (s DomainSchema) Normalize() DomainSchema {
	if s.Mode == "" {
		s.Mode = SchemaModePermissive
	}
	for i := range s.NodeTypes {
		s.NodeTypes[i].Name = strings.TrimSpace(s.NodeTypes[i].Name)
		s.NodeTypes[i].Labels = trimStrings(s.NodeTypes[i].Labels)
		normalizeFields(s.NodeTypes[i].Properties)
		normalizeFields(s.NodeTypes[i].Payload)
		normalizeFields(s.NodeTypes[i].Meta)
	}
	for i := range s.EdgeTypes {
		s.EdgeTypes[i].Name = strings.TrimSpace(s.EdgeTypes[i].Name)
		s.EdgeTypes[i].Labels = trimStrings(s.EdgeTypes[i].Labels)
		s.EdgeTypes[i].From.NodeTypes = trimStrings(s.EdgeTypes[i].From.NodeTypes)
		s.EdgeTypes[i].From.Labels = trimStrings(s.EdgeTypes[i].From.Labels)
		s.EdgeTypes[i].To.NodeTypes = trimStrings(s.EdgeTypes[i].To.NodeTypes)
		s.EdgeTypes[i].To.Labels = trimStrings(s.EdgeTypes[i].To.Labels)
		normalizeFields(s.EdgeTypes[i].Properties)
		normalizeFields(s.EdgeTypes[i].Payload)
		normalizeFields(s.EdgeTypes[i].Meta)
	}
	return s
}

func trimStrings(values []string) []string {
	out := values[:0]
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func normalizeFields(fields []FieldSpec) {
	for i := range fields {
		fields[i].Name = strings.TrimSpace(fields[i].Name)
	}
}
