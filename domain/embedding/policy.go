package embedding

import (
	"github.com/myceldb/mycel/domain/graph"
	domainspace "github.com/myceldb/mycel/domain/space"
)

// DomainEmbeddingRefreshMode controls how a domain's embeddings are refreshed.
type DomainEmbeddingRefreshMode string

const (
	DomainEmbeddingRefreshManual DomainEmbeddingRefreshMode = "manual"
	DomainEmbeddingRefreshDirty  DomainEmbeddingRefreshMode = "dirty"
)

// DomainEmbeddingPolicy configures semantic indexing defaults for one domain.
// Profiles remain user-owned; ProfileID is optional and callers may still
// provide an explicit profile at generation/search time.
type DomainEmbeddingPolicy struct {
	SpaceID            domainspace.SpaceID
	DomainID           graph.DomainID
	Enabled            bool
	ProfileID          *ProfileID
	SourceMode         SourceMode
	TargetTemplateKeys []string
	IncludeProps       []string
	MaxDepth           *int
	MinimumTextLength  int
	RefreshMode        DomainEmbeddingRefreshMode
}
