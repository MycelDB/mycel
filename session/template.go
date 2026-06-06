package session

import coretemplate "martinbeauvais.com/mbgit/knotbase/knotdb/core/template"

// ImportDocument is the JSON import contract for templates.
type ImportDocument = coretemplate.ImportDocument

// TemplateImport is the JSON representation of a template to import.
type TemplateImport = coretemplate.TemplateImport

// PropertyPolicyImport defines imported property constraints.
type PropertyPolicyImport = coretemplate.PropertyPolicyImport

// TemplatePropertyImport defines an imported allowed property.
type TemplatePropertyImport = coretemplate.TemplatePropertyImport

// ChildPolicyImport defines imported direct-child constraints.
type ChildPolicyImport = coretemplate.ChildPolicyImport

// TemplateRefImport identifies an imported child-template reference.
type TemplateRefImport = coretemplate.TemplateRefImport

// ImportTemplatesInput is the session-scoped template import payload.
type ImportTemplatesInput struct {
	Document ImportDocument
}
