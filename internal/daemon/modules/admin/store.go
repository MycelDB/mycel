package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

const StoreFilename = "admins.json"

var ErrDuplicateAdmin = errors.New("admin already exists")
var ErrStoreNotFound = errors.New("admin store not found")

type Store interface {
	List(context.Context) ([]Admin, error)
	Create(context.Context, Admin) error
}

type FileStore struct {
	path string
	mu   sync.Mutex
}

type storeDocument struct {
	Admins []Admin `json:"admins"`
}

func OpenStore(dir string) (*FileStore, bool, error) {
	path := filepath.Join(dir, StoreFilename)
	if _, err := os.Stat(path); err == nil {
		return &FileStore{path: path}, false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, false, fmt.Errorf("stat admin store: %w", err)
	}
	store := &FileStore{path: path}
	if err := store.write(storeDocument{Admins: []Admin{}}); err != nil {
		return nil, false, err
	}
	return store, true, nil
}

func OpenExistingStore(dir string) (*FileStore, error) {
	path := filepath.Join(dir, StoreFilename)
	if _, err := os.Stat(path); err == nil {
		return &FileStore{path: path}, nil
	} else if errors.Is(err, os.ErrNotExist) {
		return nil, ErrStoreNotFound
	} else {
		return nil, fmt.Errorf("stat admin store: %w", err)
	}
}

func (s *FileStore) List(ctx context.Context) ([]Admin, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, err := s.read()
	if err != nil {
		return nil, err
	}
	admins := append([]Admin(nil), doc.Admins...)
	sort.Slice(admins, func(i, j int) bool { return admins[i].Username < admins[j].Username })
	return admins, nil
}

func (s *FileStore) Create(ctx context.Context, admin Admin) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, err := s.read()
	if err != nil {
		return err
	}
	for _, existing := range doc.Admins {
		if strings.EqualFold(existing.Username, admin.Username) || existing.ID == admin.ID {
			return ErrDuplicateAdmin
		}
	}
	doc.Admins = append(doc.Admins, admin)
	return s.write(doc)
}

func (s *FileStore) read() (storeDocument, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		return storeDocument{}, fmt.Errorf("read admin store: %w", err)
	}
	var doc storeDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		return storeDocument{}, fmt.Errorf("decode admin store: %w", err)
	}
	if doc.Admins == nil {
		doc.Admins = []Admin{}
	}
	return doc, nil
}

func (s *FileStore) write(doc storeDocument) error {
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return fmt.Errorf("encode admin store: %w", err)
	}
	data = append(data, '\n')
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("write admin store: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("replace admin store: %w", err)
	}
	return nil
}
