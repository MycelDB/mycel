package session

import (
	"time"

	"github.com/myceldb/mycel/domain/identity"
	domainspace "github.com/myceldb/mycel/domain/space"
	"github.com/myceldb/mycel/internal/graphstorage"
	"github.com/myceldb/mycel/internal/session/filesession"
	sessionapi "github.com/myceldb/mycel/session/api"
	storeembedding "github.com/myceldb/mycel/store/embedding"
	storetemplate "github.com/myceldb/mycel/store/template"
)

type (
	Session                          = sessionapi.Session
	Tx                               = sessionapi.Tx
	TxOptions                        = sessionapi.TxOptions
	Errors                           = sessionapi.Errors
	Permissions                      = sessionapi.Permissions
	ImportDocument                   = sessionapi.ImportDocument
	TemplateImport                   = sessionapi.TemplateImport
	PropertyPolicyImport             = sessionapi.PropertyPolicyImport
	TemplatePropertyImport           = sessionapi.TemplatePropertyImport
	ChildPolicyImport                = sessionapi.ChildPolicyImport
	ChildOrderPolicyImport           = sessionapi.ChildOrderPolicyImport
	TemplateRefImport                = sessionapi.TemplateRefImport
	ImportTemplatesInput             = sessionapi.ImportTemplatesInput
	AddNodeInput                     = sessionapi.AddNodeInput
	AddBlobNodeInput                 = sessionapi.AddBlobNodeInput
	GetBlobInput                     = sessionapi.GetBlobInput
	GetBlobResult                    = sessionapi.GetBlobResult
	UpsertNodeInput                  = sessionapi.UpsertNodeInput
	UpdateNodeInput                  = sessionapi.UpdateNodeInput
	UpdateNodeAndCreateSiblingInput  = sessionapi.UpdateNodeAndCreateSiblingInput
	UpdateNodeAndCreateSiblingResult = sessionapi.UpdateNodeAndCreateSiblingResult
	DeleteNodeInput                  = sessionapi.DeleteNodeInput
	DeleteEdgeInput                  = sessionapi.DeleteEdgeInput
	AddEdgeInput                     = sessionapi.AddEdgeInput
	AddGraphInput                    = sessionapi.AddGraphInput
	ApplyGraphInput                  = sessionapi.ApplyGraphInput
	ApplyGraphResult                 = sessionapi.ApplyGraphResult
	MoveSubtreeInput                 = sessionapi.MoveSubtreeInput
	ReorderChildrenInput             = sessionapi.ReorderChildrenInput
	GenerateNodeEmbeddingInput       = sessionapi.GenerateNodeEmbeddingInput
	GenerateNodeEmbeddingsInput      = sessionapi.GenerateNodeEmbeddingsInput
	GenerateNodeEmbeddingBatchInput  = sessionapi.GenerateNodeEmbeddingBatchInput
	GenerateNodeEmbeddingBatchResult = sessionapi.GenerateNodeEmbeddingBatchResult
	EmbeddingBatchSkipped            = sessionapi.EmbeddingBatchSkipped
	EmbeddingBatchFailure            = sessionapi.EmbeddingBatchFailure
	ListNodeEmbeddingsInput          = sessionapi.ListNodeEmbeddingsInput
	SemanticSearchInput              = sessionapi.SemanticSearchInput
	TagMatchMode                     = sessionapi.TagMatchMode
	TagSummary                       = sessionapi.TagSummary
	FindNodesByTagInput              = sessionapi.FindNodesByTagInput
	PropertyOperator                 = sessionapi.PropertyOperator
	PropertySummary                  = sessionapi.PropertySummary
	FindNodesByPropertyInput         = sessionapi.FindNodesByPropertyInput
	BlobLimits                       = sessionapi.BlobLimits
)

var (
	ErrBlobTooLarge            = sessionapi.ErrBlobTooLarge
	ErrBlobTypeDisallowed      = sessionapi.ErrBlobTypeDisallowed
	ErrTransactionsUnsupported = sessionapi.ErrTransactionsUnsupported
	ErrTransactionClosed       = sessionapi.ErrTransactionClosed
	ErrReadOnlyTransaction     = sessionapi.ErrReadOnlyTransaction
	TagMatchAny                = sessionapi.TagMatchAny
	TagMatchAll                = sessionapi.TagMatchAll
	PropertyOperatorExists     = sessionapi.PropertyOperatorExists
	PropertyOperatorEqual      = sessionapi.PropertyOperatorEqual
)

// Config carries runtime session knobs supplied by the engine.
type Config struct {
	BlobLimits       BlobLimits
	BlobStaleTmpAge  time.Duration
	CurrentUserID    identity.UserID
	EmbeddingManager storeembedding.Manager
}

// NewSession opens a file-backed graph session for a space.
//
// blobsDir is the root directory for per-space content-addressed blob stores.
//
// Most callers should use engine.Engine.OpenSession so engine-level auth,
// access checks, and lifecycle validation are applied before construction.
func NewSession(graphsDir string, blobsDir string, spaceID domainspace.SpaceID, templateManager storetemplate.Manager, permissions Permissions, errs Errors) Session {
	return filesession.New(graphsDir, blobsDir, spaceID, templateManager, permissions, errs)
}

// NewSessionWithStore opens a file-backed session over an engine-owned graph store.
func NewSessionWithStore(graphsDir string, blobsDir string, spaceID domainspace.SpaceID, templateManager storetemplate.Manager, permissions Permissions, errs Errors, store *graphstorage.LocalStore) Session {
	return filesession.NewWithStore(graphsDir, blobsDir, spaceID, templateManager, permissions, errs, store)
}

// NewSessionWithStoreConfig opens a file-backed session over an engine-owned graph store.
func NewSessionWithStoreConfig(graphsDir string, blobsDir string, spaceID domainspace.SpaceID, templateManager storetemplate.Manager, permissions Permissions, errs Errors, store *graphstorage.LocalStore, cfg Config) Session {
	return filesession.NewWithStoreConfig(graphsDir, blobsDir, spaceID, templateManager, permissions, errs, store, filesession.Config{
		BlobLimits:       cfg.BlobLimits,
		BlobStaleTmpAge:  cfg.BlobStaleTmpAge,
		CurrentUserID:    cfg.CurrentUserID,
		EmbeddingManager: cfg.EmbeddingManager,
	})
}
