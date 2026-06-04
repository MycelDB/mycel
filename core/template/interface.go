package template

import (
	"context"

	"martinbeauvais.com/mbgit/knotbase/knotdb/domain/graph"
	domainspace "martinbeauvais.com/mbgit/knotbase/knotdb/domain/space"
)

// ImportDocument is the JSON import contract for templates.
type ImportDocument = graph.ImportDocument

// TemplateImport is the JSON representation of a template to import.
type TemplateImport = graph.TemplateImport

// PropertyPolicyImport defines imported property constraints.
type PropertyPolicyImport = graph.PropertyPolicyImport

// TemplatePropertyImport defines an imported allowed property.
type TemplatePropertyImport = graph.TemplatePropertyImport

// ChildPolicyImport defines imported direct-child constraints.
type ChildPolicyImport = graph.ChildPolicyImport

// TemplateRefImport identifies an imported child-template reference.
type TemplateRefImport = graph.TemplateRefImport

// Manager manages templates for spaces.
type Manager interface {
	Init(ctx context.Context, location string) error
	Import(ctx context.Context, spaceID domainspace.SpaceID, doc graph.ImportDocument) ([]graph.Template, error)
	ListBySpace(ctx context.Context, spaceID domainspace.SpaceID) ([]graph.Template, error)
	GetByID(ctx context.Context, id graph.TemplateID) (graph.Template, error)
	Find(ctx context.Context, spaceID domainspace.SpaceID, key string, version string) (graph.Template, error)
	DeleteForSpace(ctx context.Context, spaceID domainspace.SpaceID) error
}
