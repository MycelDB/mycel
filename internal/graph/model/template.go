package graph

import (
	"github.com/google/uuid"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

// TemplateID uniquely identifies a template definition.
type TemplateID = uuid.UUID

// Template defines a reusable, versioned node shape scoped to a space.
//
// Templates provide a schema-like contract for node props and direct child
// relationships. Nodes refer to templates by TemplateID.
const (
	TemplateStateActive   = "active"
	TemplateStateArchived = "archived"
)

type Template struct {
	ID          TemplateID          `json:"id"`
	SpaceID     domainspace.SpaceID `json:"space_id"`
	Key         string              `json:"key"`
	Version     string              `json:"version"`
	DisplayName string              `json:"display_name,omitempty"`
	Description string              `json:"description,omitempty"`
	System      bool                `json:"system,omitempty"`
	State       string              `json:"state,omitempty"`
	Properties  PropertyPolicy      `json:"properties"`
	Children    ChildPolicy         `json:"children"`
}

// PropertyPolicy defines allowed and forbidden node properties for a template.
type PropertyPolicy struct {
	AllowExtra bool               `json:"allow_extra"`
	Allowed    []TemplateProperty `json:"allowed,omitempty"`
	Forbidden  []string           `json:"forbidden,omitempty"`
}

// TemplateProperty defines a single allowed template property.
type TemplateProperty struct {
	Name        string       `json:"name"`
	Type        PropertyType `json:"type"`
	Required    bool         `json:"required,omitempty"`
	Default     any          `json:"default,omitempty"`
	Description string       `json:"description,omitempty"`
}

// PropertyType defines a supported template property value type.
type PropertyType string

const (
	PropertyTypeString PropertyType = "string"
	PropertyTypeNumber PropertyType = "number"
	PropertyTypeBool   PropertyType = "bool"
	PropertyTypeObject PropertyType = "object"
	PropertyTypeArray  PropertyType = "array"
	PropertyTypeDate   PropertyType = "date"
)

// SortDirection defines ascending or descending order.
type SortDirection string

const (
	// SortDirectionAsc sorts in ascending order.
	SortDirectionAsc SortDirection = "asc"
	// SortDirectionDesc sorts in descending order.
	SortDirectionDesc SortDirection = "desc"
)

// ChildOrderMode defines how child ordering is represented.
type ChildOrderMode string

const (
	// ChildOrderModeNone means no explicit child ordering policy is defined.
	ChildOrderModeNone ChildOrderMode = "none"
	// ChildOrderModeEdgeProperty orders children by a property on contains edges.
	ChildOrderModeEdgeProperty ChildOrderMode = "edge_property"
)

// ChildOrderPolicy defines how direct children should be ordered.
type ChildOrderPolicy struct {
	Mode      ChildOrderMode `json:"mode"`
	Property  string         `json:"property,omitempty"`
	Direction SortDirection  `json:"direction,omitempty"`
}

// ChildPolicy defines direct child-node constraints for a template.
type ChildPolicy struct {
	Allowed          bool              `json:"allowed"`
	AllowedTemplates []TemplateRef     `json:"allowed_templates,omitempty"`
	Order            *ChildOrderPolicy `json:"order,omitempty"`
}

// TemplateRef identifies a required child template by key and semver version.
type TemplateRef struct {
	Key     string `json:"key"`
	Version string `json:"version"`
}
