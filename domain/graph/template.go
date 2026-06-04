package graph

import "martinbeauvais.com/mbgit/knotbase/knotdb/domain/identity"

// Template defines a reusable, versioned node shape scoped to a space.
//
// Templates provide a schema-like contract for node props and direct child
// relationships. Nodes refer to templates by TemplateID.
type Template struct {
	ID          TemplateID       `json:"id"`
	SpaceID     identity.SpaceID `json:"space_id"`
	Key         string           `json:"key"`
	Version     string           `json:"version"`
	DisplayName string           `json:"display_name,omitempty"`
	Description string           `json:"description,omitempty"`
	System      bool             `json:"system,omitempty"`
	Properties  PropertyPolicy   `json:"properties"`
	Children    ChildPolicy      `json:"children"`
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

// ChildPolicy defines direct child-node constraints for a template.
type ChildPolicy struct {
	Allowed          bool          `json:"allowed"`
	AllowedTemplates []TemplateRef `json:"allowed_templates,omitempty"`
}

// TemplateRef identifies a required child template by key and semver version.
type TemplateRef struct {
	Key     string `json:"key"`
	Version string `json:"version"`
}
