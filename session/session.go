package session

import (
	domainspace "martinbeauvais.com/mbgit/knotbase/knotdb/domain/space"
	"martinbeauvais.com/mbgit/knotbase/knotdb/internal/session/filesession"
	sessionapi "martinbeauvais.com/mbgit/knotbase/knotdb/session/api"
	storetemplate "martinbeauvais.com/mbgit/knotbase/knotdb/store/template"
)

type (
	Session                = sessionapi.Session
	Errors                 = sessionapi.Errors
	Permissions            = sessionapi.Permissions
	ImportDocument         = sessionapi.ImportDocument
	TemplateImport         = sessionapi.TemplateImport
	PropertyPolicyImport   = sessionapi.PropertyPolicyImport
	TemplatePropertyImport = sessionapi.TemplatePropertyImport
	ChildPolicyImport      = sessionapi.ChildPolicyImport
	ChildOrderPolicyImport = sessionapi.ChildOrderPolicyImport
	TemplateRefImport      = sessionapi.TemplateRefImport
	ImportTemplatesInput   = sessionapi.ImportTemplatesInput
	AddNodeInput           = sessionapi.AddNodeInput
	AddBlobNodeInput       = sessionapi.AddBlobNodeInput
	GetBlobInput           = sessionapi.GetBlobInput
	GetBlobResult          = sessionapi.GetBlobResult
	UpsertNodeInput        = sessionapi.UpsertNodeInput
	UpdateNodeInput        = sessionapi.UpdateNodeInput
	DeleteNodeInput        = sessionapi.DeleteNodeInput
	AddEdgeInput           = sessionapi.AddEdgeInput
	AddGraphInput          = sessionapi.AddGraphInput
	ApplyGraphInput        = sessionapi.ApplyGraphInput
	ApplyGraphResult       = sessionapi.ApplyGraphResult
	MoveSubtreeInput       = sessionapi.MoveSubtreeInput
	ReorderChildrenInput   = sessionapi.ReorderChildrenInput
)

// NewSession opens a file-backed graph session for a space.
//
// blobsDir is the root directory for per-space content-addressed blob stores.
//
// Most callers should use engine.Engine.OpenSession so engine-level auth,
// access checks, and lifecycle validation are applied before construction.
func NewSession(graphsDir string, blobsDir string, spaceID domainspace.SpaceID, templateManager storetemplate.Manager, permissions Permissions, errs Errors) Session {
	return filesession.New(graphsDir, blobsDir, spaceID, templateManager, permissions, errs)
}
