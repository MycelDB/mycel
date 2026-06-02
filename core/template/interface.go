package template

import (
	"context"

	"martinbeauvais.com/mbgit/knotbase/knotdb/graph"
	"martinbeauvais.com/mbgit/knotbase/knotdb/model"
)

// ImportDocument is the JSON import contract for templates.
type ImportDocument struct {
	SchemaVersion int              `json:"schema_version"`
	Templates     []TemplateImport `json:"templates"`
}

// TemplateImport is the JSON representation of a template to import.
type TemplateImport struct {
	Key         string               `json:"key"`
	Version     string               `json:"version"`
	DisplayName string               `json:"display_name,omitempty"`
	Description string               `json:"description,omitempty"`
	System      bool                 `json:"system,omitempty"`
	Properties  PropertyPolicyImport `json:"properties,omitempty"`
	Children    ChildPolicyImport    `json:"children,omitempty"`
}

// PropertyPolicyImport defines imported property constraints.
type PropertyPolicyImport struct {
	AllowExtra bool                     `json:"allow_extra"`
	Allowed    []TemplatePropertyImport `json:"allowed,omitempty"`
	Forbidden  []string                 `json:"forbidden,omitempty"`
}

// TemplatePropertyImport defines an imported allowed property.
type TemplatePropertyImport struct {
	Name        string             `json:"name"`
	Type        graph.PropertyType `json:"type"`
	Required    bool               `json:"required,omitempty"`
	Default     any                `json:"default,omitempty"`
	Description string             `json:"description,omitempty"`
}

// ChildPolicyImport defines imported direct-child constraints.
type ChildPolicyImport struct {
	Allowed          bool                `json:"allowed"`
	AllowedTemplates []TemplateRefImport `json:"allowed_templates,omitempty"`
}

// TemplateRefImport identifies an imported child-template reference.
type TemplateRefImport struct {
	Key     string `json:"key"`
	Version string `json:"version"`
}

// Manager manages templates for spaces.
type Manager interface {
	Init(ctx context.Context, location string) error
	Import(ctx context.Context, spaceID model.SpaceID, doc ImportDocument) ([]graph.Template, error)
	GetByID(ctx context.Context, id graph.TemplateID) (graph.Template, error)
	Find(ctx context.Context, spaceID model.SpaceID, key string, version string) (graph.Template, error)
}
