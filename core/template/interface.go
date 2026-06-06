package template

import (
	"context"

	"martinbeauvais.com/mbgit/knotbase/knotdb/domain/graph"
	domainspace "martinbeauvais.com/mbgit/knotbase/knotdb/domain/space"
	domainsession "martinbeauvais.com/mbgit/knotbase/knotdb/session"
)

// ImportDocument is the JSON import contract for templates.
type ImportDocument = domainsession.ImportDocument

// TemplateImport is the JSON representation of a template to import.
type TemplateImport = domainsession.TemplateImport

// PropertyPolicyImport defines imported property constraints.
type PropertyPolicyImport = domainsession.PropertyPolicyImport

// TemplatePropertyImport defines an imported allowed property.
type TemplatePropertyImport = domainsession.TemplatePropertyImport

// ChildPolicyImport defines imported direct-child constraints.
type ChildPolicyImport = domainsession.ChildPolicyImport

// TemplateRefImport identifies an imported child-template reference.
type TemplateRefImport = domainsession.TemplateRefImport

// Manager manages templates for spaces.
type Manager interface {
	Init(ctx context.Context, location string) error
	Import(ctx context.Context, spaceID domainspace.SpaceID, doc domainsession.ImportDocument) ([]graph.Template, error)
	ListBySpace(ctx context.Context, spaceID domainspace.SpaceID) ([]graph.Template, error)
	GetByID(ctx context.Context, id graph.TemplateID) (graph.Template, error)
	Find(ctx context.Context, spaceID domainspace.SpaceID, key string, version string) (graph.Template, error)
	DeleteForSpace(ctx context.Context, spaceID domainspace.SpaceID) error
}
