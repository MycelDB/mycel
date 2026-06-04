package space

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"martinbeauvais.com/mbgit/knotbase/knotdb/domain/identity"
	"martinbeauvais.com/mbgit/knotbase/knotdb/internal/filestore"
)

const spacesStoreFile = "spaces.json"

type storedSpace struct {
	SpaceID  identity.SpaceID       `json:"space_id"`
	OwnerID  identity.UserID        `json:"owner_id"`
	Name     string                 `json:"name"`
	Status   string                 `json:"status"`
	Settings identity.SpaceSettings `json:"settings,omitempty"`
}

type defaultManager struct {
	location    string
	storePath   string
	spaces      []storedSpace
	indexByID   map[identity.SpaceID]int
	indexByName map[string]int
}

// NewManager creates the default file-backed Manager implementation.
func NewManager() Manager {
	return &defaultManager{indexByID: map[identity.SpaceID]int{}, indexByName: map[string]int{}}
}

func (m *defaultManager) Init(ctx context.Context, location string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(location) == "" {
		return fmt.Errorf("%w: location is required", ErrInvalidInput)
	}
	if err := os.MkdirAll(location, 0o755); err != nil {
		return err
	}
	m.location = location
	m.storePath = filepath.Join(location, spacesStoreFile)

	if _, err := os.Stat(m.storePath); err != nil {
		if os.IsNotExist(err) {
			m.spaces = []storedSpace{}
			m.rebuildIndex()
			return m.persist()
		}
		return err
	}

	raw, err := os.ReadFile(m.storePath)
	if err != nil {
		return err
	}
	var spaces []storedSpace
	if err := json.Unmarshal(raw, &spaces); err != nil {
		return err
	}
	m.spaces = spaces
	m.rebuildIndex()
	return nil
}

func (m *defaultManager) ExistsByID(ctx context.Context, id identity.SpaceID) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if id == uuid.Nil {
		return false, fmt.Errorf("%w: space_id is required", ErrInvalidInput)
	}
	_, ok := m.indexByID[id]
	return ok, nil
}

func (m *defaultManager) GetByID(ctx context.Context, id identity.SpaceID) (identity.Space, error) {
	if err := ctx.Err(); err != nil {
		return identity.Space{}, err
	}
	if id == uuid.Nil {
		return identity.Space{}, fmt.Errorf("%w: space_id is required", ErrInvalidInput)
	}
	idx, ok := m.indexByID[id]
	if !ok {
		return identity.Space{}, ErrSpaceNotFound
	}
	return m.spaces[idx].toModel(), nil
}

func (m *defaultManager) List(ctx context.Context) ([]identity.Space, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := make([]identity.Space, 0, len(m.spaces))
	for _, s := range m.spaces {
		out = append(out, s.toModel())
	}
	return out, nil
}

func (m *defaultManager) ListByOwner(ctx context.Context, ownerID identity.UserID) ([]identity.Space, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if ownerID == uuid.Nil {
		return nil, fmt.Errorf("%w: owner_id is required", ErrInvalidInput)
	}
	out := []identity.Space{}
	for _, s := range m.spaces {
		if s.OwnerID == ownerID {
			out = append(out, s.toModel())
		}
	}
	return out, nil
}

func (m *defaultManager) FindByOwnerAndName(ctx context.Context, ownerID identity.UserID, name string) (identity.Space, error) {
	if err := ctx.Err(); err != nil {
		return identity.Space{}, err
	}
	if ownerID == uuid.Nil {
		return identity.Space{}, fmt.Errorf("%w: owner_id is required", ErrInvalidInput)
	}
	if strings.TrimSpace(name) == "" {
		return identity.Space{}, fmt.Errorf("%w: name is required", ErrInvalidInput)
	}
	idx, ok := m.indexByName[ownerNameKey(ownerID, name)]
	if !ok {
		return identity.Space{}, ErrSpaceNotFound
	}
	return m.spaces[idx].toModel(), nil
}

func (m *defaultManager) Create(ctx context.Context, in CreateInput) (identity.Space, error) {
	if err := ctx.Err(); err != nil {
		return identity.Space{}, err
	}
	if in.OwnerID == uuid.Nil {
		return identity.Space{}, fmt.Errorf("%w: owner_id is required", ErrInvalidInput)
	}
	if strings.TrimSpace(in.Name) == "" {
		return identity.Space{}, fmt.Errorf("%w: name is required", ErrInvalidInput)
	}
	if existing, err := m.FindByOwnerAndName(ctx, in.OwnerID, in.Name); err == nil {
		return existing, nil
	} else if err != nil && err != ErrSpaceNotFound {
		return identity.Space{}, err
	}

	status := in.Status
	if status == "" {
		status = "active"
	}
	s := storedSpace{
		SpaceID:  uuid.New(),
		OwnerID:  in.OwnerID,
		Name:     in.Name,
		Status:   status,
		Settings: in.Settings,
	}
	m.spaces = append(m.spaces, s)
	m.rebuildIndex()
	if err := m.persist(); err != nil {
		return identity.Space{}, err
	}
	return s.toModel(), nil
}

func (m *defaultManager) DeleteByID(ctx context.Context, id identity.SpaceID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if id == uuid.Nil {
		return fmt.Errorf("%w: space_id is required", ErrInvalidInput)
	}
	idx, ok := m.indexByID[id]
	if !ok {
		return ErrSpaceNotFound
	}
	oldSpaces := append([]storedSpace(nil), m.spaces...)
	m.spaces = append(m.spaces[:idx], m.spaces[idx+1:]...)
	m.rebuildIndex()
	if err := m.persist(); err != nil {
		m.spaces = oldSpaces
		m.rebuildIndex()
		return err
	}
	return nil
}

func (m *defaultManager) rebuildIndex() {
	m.indexByID = map[identity.SpaceID]int{}
	m.indexByName = map[string]int{}
	for i, s := range m.spaces {
		m.indexByID[s.SpaceID] = i
		m.indexByName[ownerNameKey(s.OwnerID, s.Name)] = i
	}
}

func (m *defaultManager) persist() error {
	b, err := json.MarshalIndent(m.spaces, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return filestore.WriteFileAtomic(m.storePath, b, 0o600)
}

func (s storedSpace) toModel() identity.Space {
	return identity.Space{
		SpaceID:  s.SpaceID,
		OwnerID:  s.OwnerID,
		Name:     s.Name,
		Status:   s.Status,
		Settings: s.Settings,
	}
}

func ownerNameKey(ownerID identity.UserID, name string) string {
	return ownerID.String() + ":" + strings.ToLower(strings.TrimSpace(name))
}
