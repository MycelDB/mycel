package filesession

import (
	"context"
	"fmt"

	"github.com/myceldb/mycel/domain/graph"
	domainembedding "github.com/myceldb/mycel/internal/embedding/domain"
	sessionapi "github.com/myceldb/mycel/internal/session/api"
)

var errLegacyEmbeddingUnsupported = fmt.Errorf("legacy embedding profile APIs are no longer supported; use daemon semantic index APIs")

// GenerateNodeEmbedding is the deprecated pre-daemon embedding API. Embedding
// generation now runs through semantic indexes, daemon inference credentials,
// credential grants, and vector backends.
func (s *FileSession) GenerateNodeEmbedding(ctx context.Context, in sessionapi.GenerateNodeEmbeddingInput) (domainembedding.EmbeddingRecord, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return domainembedding.EmbeddingRecord{}, err
	}
	if err := s.ensureSpaceLive(); err != nil {
		return domainembedding.EmbeddingRecord{}, err
	}
	if err := s.ensureWrite(); err != nil {
		return domainembedding.EmbeddingRecord{}, err
	}
	return domainembedding.EmbeddingRecord{}, errLegacyEmbeddingUnsupported
}

// GenerateNodeEmbeddings is the deprecated pre-daemon embedding API.
func (s *FileSession) GenerateNodeEmbeddings(ctx context.Context, in sessionapi.GenerateNodeEmbeddingsInput) ([]domainembedding.EmbeddingRecord, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return nil, err
	}
	if err := s.ensureSpaceLive(); err != nil {
		return nil, err
	}
	if err := s.ensureWrite(); err != nil {
		return nil, err
	}
	return nil, errLegacyEmbeddingUnsupported
}

// GenerateNodeEmbeddingBatch is the deprecated pre-daemon embedding API.
func (s *FileSession) GenerateNodeEmbeddingBatch(ctx context.Context, in sessionapi.GenerateNodeEmbeddingBatchInput) (sessionapi.GenerateNodeEmbeddingBatchResult, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return sessionapi.GenerateNodeEmbeddingBatchResult{}, err
	}
	if err := s.ensureSpaceLive(); err != nil {
		return sessionapi.GenerateNodeEmbeddingBatchResult{}, err
	}
	if err := s.ensureWrite(); err != nil {
		return sessionapi.GenerateNodeEmbeddingBatchResult{}, err
	}
	return sessionapi.GenerateNodeEmbeddingBatchResult{}, errLegacyEmbeddingUnsupported
}

// ListNodeEmbeddings is the deprecated pre-daemon embedding API.
func (s *FileSession) ListNodeEmbeddings(ctx context.Context, in sessionapi.ListNodeEmbeddingsInput) ([]domainembedding.EmbeddingRecord, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return nil, err
	}
	if err := s.ensureSpaceLive(); err != nil {
		return nil, err
	}
	if err := s.ensureRead(); err != nil {
		return nil, err
	}
	return nil, errLegacyEmbeddingUnsupported
}

// SemanticSearch is the deprecated pre-daemon embedding-profile search API. Use
// AdvancedSemanticSearch, which is backed by semantic indexes and vectorstore.
func (s *FileSession) SemanticSearch(ctx context.Context, in sessionapi.SemanticSearchInput) ([]domainembedding.SemanticSearchResult, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return nil, err
	}
	if err := s.ensureSpaceLive(); err != nil {
		return nil, err
	}
	if err := s.ensureRead(); err != nil {
		return nil, err
	}
	return nil, errLegacyEmbeddingUnsupported
}

func (s *FileSession) selectEmbeddingBatchNodes(ctx context.Context, in sessionapi.GenerateNodeEmbeddingBatchInput) ([]graph.NodeID, error) {
	return nil, errLegacyEmbeddingUnsupported
}
