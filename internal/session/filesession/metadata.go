package filesession

import (
	"context"

	"github.com/myceldb/mycel/domain/graph"
	"github.com/myceldb/mycel/internal/session/metadataindex"
	sessionapi "github.com/myceldb/mycel/session/api"
)

func (s *FileSession) ListTags(ctx context.Context) ([]sessionapi.TagSummary, error) {
	idx, err := s.metadataIndex(ctx)
	if err != nil {
		return nil, err
	}
	return idx.TagSummaries(), nil
}

func (s *FileSession) FindNodesByTag(ctx context.Context, in sessionapi.FindNodesByTagInput) ([]graph.Node, error) {
	idx, err := s.metadataIndex(ctx)
	if err != nil {
		return nil, err
	}
	nodes, err := idx.FindByTags(in.Tags, in.Match, in.Limit)
	if err != nil {
		return nil, err
	}
	return cloneNodes(nodes), nil
}

func (s *FileSession) ListPropertyNames(ctx context.Context) ([]sessionapi.PropertySummary, error) {
	idx, err := s.metadataIndex(ctx)
	if err != nil {
		return nil, err
	}
	return idx.PropertySummaries(), nil
}

func (s *FileSession) FindNodesByProperty(ctx context.Context, in sessionapi.FindNodesByPropertyInput) ([]graph.Node, error) {
	idx, err := s.metadataIndex(ctx)
	if err != nil {
		return nil, err
	}
	nodes, err := idx.FindByProperty(in)
	if err != nil {
		return nil, err
	}
	return cloneNodes(nodes), nil
}

func (s *FileSession) metadataIndex(ctx context.Context) (*metadataindex.Index, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return nil, err
	}
	if err := s.ensureSpaceLive(); err != nil {
		return nil, err
	}
	if err := s.ensureRead(); err != nil {
		return nil, err
	}
	nodes, err := s.readNodes()
	if err != nil {
		return nil, err
	}
	return metadataindex.Build(nodes), nil
}
