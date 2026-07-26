package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	graph "github.com/myceldb/mycel/internal/graph/model"
	schema "github.com/myceldb/mycel/internal/schema/model"
)

// FileStore persists each domain's active schema as JSON under a directory.
type FileStore struct {
	dir string
}

func NewFileStore(dir string) *FileStore {
	return &FileStore{dir: dir}
}

func (s *FileStore) GetDomainSchema(ctx context.Context, domainID graph.DomainID) (schema.DomainSchema, error) {
	if err := ctx.Err(); err != nil {
		return schema.DomainSchema{}, err
	}
	data, err := os.ReadFile(s.path(domainID))
	if errors.Is(err, os.ErrNotExist) {
		return schema.DomainSchema{}, ErrNotFound
	}
	if err != nil {
		return schema.DomainSchema{}, err
	}
	var value schema.DomainSchema
	if err := json.Unmarshal(data, &value); err != nil {
		return schema.DomainSchema{}, fmt.Errorf("decode schema %s: %w", domainID, err)
	}
	return value.Normalize(), nil
}

func (s *FileStore) ListDomainSchemas(ctx context.Context) ([]schema.DomainSchema, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(s.dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out := []schema.DomainSchema{}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(s.dir, entry.Name()))
		if err != nil {
			return nil, err
		}
		var value schema.DomainSchema
		if err := json.Unmarshal(data, &value); err != nil {
			return nil, fmt.Errorf("decode schema %s: %w", entry.Name(), err)
		}
		out = append(out, value.Normalize())
	}
	return out, nil
}

func (s *FileStore) PutDomainSchema(ctx context.Context, value schema.DomainSchema) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.MkdirAll(s.dir, 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(value.Normalize(), "", "  ")
	if err != nil {
		return fmt.Errorf("encode schema %s: %w", value.DomainID, err)
	}
	path := s.path(value.DomainID)
	tmp, err := os.CreateTemp(s.dir, ".schema-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write([]byte("\n")); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

func (s *FileStore) DeleteDomainSchema(ctx context.Context, domainID graph.DomainID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Remove(s.path(domainID)); errors.Is(err, os.ErrNotExist) {
		return ErrNotFound
	} else if err != nil {
		return err
	}
	return nil
}

func (s *FileStore) path(domainID graph.DomainID) string {
	return filepath.Join(s.dir, domainID.String()+".json")
}
