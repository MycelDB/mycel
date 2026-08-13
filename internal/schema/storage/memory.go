package storage

import (
	"context"
	"errors"
	"sync"

	graph "github.com/myceldb/mycel/internal/graph/model"
	schema "github.com/myceldb/mycel/internal/schema/model"
)

var ErrNotFound = errors.New("schema not found")

// Store persists active domain schemas.
type Store interface {
	GetDomainSchema(ctx context.Context, domainID graph.DomainID) (schema.DomainSchema, error)
	PutDomainSchema(ctx context.Context, schema schema.DomainSchema) error
	DeleteDomainSchema(ctx context.Context, domainID graph.DomainID) error
	ListDomainSchemas(ctx context.Context) ([]schema.DomainSchema, error)
}

// MemoryStore is an in-memory schema store suitable for daemon tests and the
// initial subsystem skeleton. Durable storage can implement Store later.
type MemoryStore struct {
	mu      sync.RWMutex
	schemas map[graph.DomainID]schema.DomainSchema
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{schemas: map[graph.DomainID]schema.DomainSchema{}}
}

func (s *MemoryStore) GetDomainSchema(ctx context.Context, domainID graph.DomainID) (schema.DomainSchema, error) {
	if err := ctx.Err(); err != nil {
		return schema.DomainSchema{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.schemas[domainID]
	if !ok {
		return schema.DomainSchema{}, ErrNotFound
	}
	return cloneSchema(value), nil
}

func (s *MemoryStore) ListDomainSchemas(ctx context.Context) ([]schema.DomainSchema, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]schema.DomainSchema, 0, len(s.schemas))
	for _, value := range s.schemas {
		out = append(out, cloneSchema(value))
	}
	return out, nil
}

func (s *MemoryStore) PutDomainSchema(ctx context.Context, value schema.DomainSchema) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.schemas == nil {
		s.schemas = map[graph.DomainID]schema.DomainSchema{}
	}
	s.schemas[value.DomainID] = cloneSchema(value)
	return nil
}

func (s *MemoryStore) DeleteDomainSchema(ctx context.Context, domainID graph.DomainID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.schemas[domainID]; !ok {
		return ErrNotFound
	}
	delete(s.schemas, domainID)
	return nil
}

func cloneSchema(value schema.DomainSchema) schema.DomainSchema {
	value.NodeTypes = append([]schema.NodeType(nil), value.NodeTypes...)
	for i := range value.NodeTypes {
		value.NodeTypes[i].Labels = append([]string(nil), value.NodeTypes[i].Labels...)
		value.NodeTypes[i].Properties = append([]schema.FieldSpec(nil), value.NodeTypes[i].Properties...)
		value.NodeTypes[i].Payload = append([]schema.FieldSpec(nil), value.NodeTypes[i].Payload...)
		value.NodeTypes[i].Meta = append([]schema.FieldSpec(nil), value.NodeTypes[i].Meta...)
	}
	value.Indexes = append([]schema.IndexDefinition(nil), value.Indexes...)
	for i := range value.Indexes {
		value.Indexes[i].Labels = append([]string(nil), value.Indexes[i].Labels...)
	}
	value.EdgeTypes = append([]schema.EdgeType(nil), value.EdgeTypes...)
	for i := range value.EdgeTypes {
		value.EdgeTypes[i].Labels = append([]string(nil), value.EdgeTypes[i].Labels...)
		value.EdgeTypes[i].From.NodeTypes = append([]string(nil), value.EdgeTypes[i].From.NodeTypes...)
		value.EdgeTypes[i].From.Labels = append([]string(nil), value.EdgeTypes[i].From.Labels...)
		value.EdgeTypes[i].To.NodeTypes = append([]string(nil), value.EdgeTypes[i].To.NodeTypes...)
		value.EdgeTypes[i].To.Labels = append([]string(nil), value.EdgeTypes[i].To.Labels...)
		value.EdgeTypes[i].Properties = append([]schema.FieldSpec(nil), value.EdgeTypes[i].Properties...)
		value.EdgeTypes[i].Payload = append([]schema.FieldSpec(nil), value.EdgeTypes[i].Payload...)
		value.EdgeTypes[i].Meta = append([]schema.FieldSpec(nil), value.EdgeTypes[i].Meta...)
		if value.EdgeTypes[i].Hierarchy != nil {
			copy := *value.EdgeTypes[i].Hierarchy
			value.EdgeTypes[i].Hierarchy = &copy
		}
	}
	return value
}
