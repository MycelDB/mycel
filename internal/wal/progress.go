package wal

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/myceldb/mycel/internal/filestore"
)

// AppliedLSNStore persists the highest WAL LSN fully applied to durable state.
type AppliedLSNStore interface {
	AppliedLSN(ctx context.Context) (LSN, error)
	SetAppliedLSN(ctx context.Context, lsn LSN) error
}

type FileProgressStore struct {
	mu   sync.Mutex
	path string
}

type progressDocument struct {
	AppliedLSN LSN       `json:"applied_lsn"`
	UpdatedAt  time.Time `json:"updated_at"`
}

func NewFileProgressStore(path string) *FileProgressStore { return &FileProgressStore{path: path} }

func (s *FileProgressStore) AppliedLSN(ctx context.Context) (LSN, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var doc progressDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		return 0, err
	}
	return doc.AppliedLSN, nil
}

func (s *FileProgressStore) SetAppliedLSN(ctx context.Context, lsn LSN) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return err
	}
	doc := progressDocument{AppliedLSN: lsn, UpdatedAt: time.Now().UTC()}
	raw, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	return filestore.WriteFileAtomic(s.path, raw, 0o600)
}
