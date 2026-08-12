package spaces

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/filestore"
	"github.com/myceldb/mycel/internal/identity/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

const spacesStoreFile = "spaces.json"

type storedSpace struct {
	SpaceID   domainspace.SpaceID       `json:"space_id"`
	OwnerID   identity.PrincipalID      `json:"owner_id"`
	Name      string                    `json:"name"`
	Status    string                    `json:"status"`
	Settings  domainspace.SpaceSettings `json:"settings,omitempty"`
	CreatedAt time.Time                 `json:"created_at,omitempty"`
	UpdatedAt time.Time                 `json:"updated_at,omitempty"`
}

type defaultManager struct {
	location    string
	storePath   string
	spaces      []storedSpace
	indexByID   map[domainspace.SpaceID]int
	indexByName map[string]int
}

// NewManager creates the default file-backed Manager implementation.
func NewManager() Manager {
	return &defaultManager{indexByID: map[domainspace.SpaceID]int{}, indexByName: map[string]int{}}
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

func (m *defaultManager) ExistsByID(ctx context.Context, id domainspace.SpaceID) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if id == uuid.Nil {
		return false, fmt.Errorf("%w: space_id is required", ErrInvalidInput)
	}
	_, ok := m.indexByID[id]
	return ok, nil
}

func (m *defaultManager) GetByID(ctx context.Context, id domainspace.SpaceID) (domainspace.Space, error) {
	if err := ctx.Err(); err != nil {
		return domainspace.Space{}, err
	}
	if id == uuid.Nil {
		return domainspace.Space{}, fmt.Errorf("%w: space_id is required", ErrInvalidInput)
	}
	idx, ok := m.indexByID[id]
	if !ok {
		return domainspace.Space{}, ErrSpaceNotFound
	}
	return m.spaces[idx].toModel(), nil
}

func (m *defaultManager) List(ctx context.Context) ([]domainspace.Space, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := make([]domainspace.Space, 0, len(m.spaces))
	for _, s := range m.spaces {
		out = append(out, s.toModel())
	}
	return out, nil
}

func (m *defaultManager) ListByOwner(ctx context.Context, ownerID identity.PrincipalID) ([]domainspace.Space, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(string(ownerID)) == "" {
		return nil, fmt.Errorf("%w: owner_id is required", ErrInvalidInput)
	}
	out := []domainspace.Space{}
	for _, s := range m.spaces {
		if s.OwnerID == ownerID {
			out = append(out, s.toModel())
		}
	}
	return out, nil
}

func (m *defaultManager) FindByOwnerAndName(ctx context.Context, ownerID identity.PrincipalID, name string) (domainspace.Space, error) {
	if err := ctx.Err(); err != nil {
		return domainspace.Space{}, err
	}
	if strings.TrimSpace(string(ownerID)) == "" {
		return domainspace.Space{}, fmt.Errorf("%w: owner_id is required", ErrInvalidInput)
	}
	if strings.TrimSpace(name) == "" {
		return domainspace.Space{}, fmt.Errorf("%w: name is required", ErrInvalidInput)
	}
	idx, ok := m.indexByName[ownerNameKey(ownerID, name)]
	if !ok {
		return domainspace.Space{}, ErrSpaceNotFound
	}
	return m.spaces[idx].toModel(), nil
}

func (m *defaultManager) Create(ctx context.Context, in CreateInput) (domainspace.Space, error) {
	if err := ctx.Err(); err != nil {
		return domainspace.Space{}, err
	}
	if strings.TrimSpace(string(in.OwnerID)) == "" {
		return domainspace.Space{}, fmt.Errorf("%w: owner_id is required", ErrInvalidInput)
	}
	if strings.TrimSpace(in.Name) == "" {
		return domainspace.Space{}, fmt.Errorf("%w: name is required", ErrInvalidInput)
	}
	if existing, err := m.FindByOwnerAndName(ctx, in.OwnerID, in.Name); err == nil {
		return existing, nil
	} else if err != nil && err != ErrSpaceNotFound {
		return domainspace.Space{}, err
	}

	status := in.Status
	if status == "" {
		status = "active"
	}
	now := time.Now().UTC()
	s := storedSpace{
		SpaceID:   uuid.New(),
		OwnerID:   in.OwnerID,
		Name:      in.Name,
		Status:    status,
		Settings:  in.Settings,
		CreatedAt: now,
		UpdatedAt: now,
	}
	m.spaces = append(m.spaces, s)
	m.rebuildIndex()
	if err := m.persist(); err != nil {
		return domainspace.Space{}, err
	}
	return s.toModel(), nil
}

func (m *defaultManager) ApplyCreate(ctx context.Context, space domainspace.Space) (domainspace.Space, error) {
	if err := ctx.Err(); err != nil {
		return domainspace.Space{}, err
	}
	if space.SpaceID == uuid.Nil {
		return domainspace.Space{}, fmt.Errorf("%w: space_id is required", ErrInvalidInput)
	}
	if strings.TrimSpace(string(space.OwnerID)) == "" {
		return domainspace.Space{}, fmt.Errorf("%w: owner_id is required", ErrInvalidInput)
	}
	if strings.TrimSpace(space.Name) == "" {
		return domainspace.Space{}, fmt.Errorf("%w: name is required", ErrInvalidInput)
	}
	if idx, ok := m.indexByID[space.SpaceID]; ok {
		existing := m.spaces[idx].toModel()
		if existing.OwnerID == space.OwnerID && strings.EqualFold(strings.TrimSpace(existing.Name), strings.TrimSpace(space.Name)) {
			return existing, nil
		}
		return domainspace.Space{}, fmt.Errorf("%w: conflicting space id", ErrInvalidInput)
	}
	if _, ok := m.indexByName[ownerNameKey(space.OwnerID, space.Name)]; ok {
		return domainspace.Space{}, fmt.Errorf("%w: conflicting owner/name", ErrInvalidInput)
	}
	status := space.Status
	if status == "" {
		status = "active"
	}
	s := storedSpace{SpaceID: space.SpaceID, OwnerID: space.OwnerID, Name: space.Name, Status: status, Settings: space.Settings, CreatedAt: space.CreatedAt, UpdatedAt: space.UpdatedAt}
	m.spaces = append(m.spaces, s)
	m.rebuildIndex()
	if err := m.persist(); err != nil {
		m.spaces = m.spaces[:len(m.spaces)-1]
		m.rebuildIndex()
		return domainspace.Space{}, err
	}
	return s.toModel(), nil
}

func (m *defaultManager) DeleteByID(ctx context.Context, id domainspace.SpaceID) error {
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

func (m *defaultManager) ApplyDelete(ctx context.Context, id domainspace.SpaceID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if id == uuid.Nil {
		return fmt.Errorf("%w: space_id is required", ErrInvalidInput)
	}
	idx, ok := m.indexByID[id]
	if !ok {
		return nil
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
	m.indexByID = map[domainspace.SpaceID]int{}
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

func (s storedSpace) toModel() domainspace.Space {
	createdAt := s.CreatedAt
	updatedAt := s.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = createdAt
	}
	return domainspace.Space{
		SpaceID:   s.SpaceID,
		OwnerID:   s.OwnerID,
		Name:      s.Name,
		Status:    s.Status,
		Settings:  s.Settings,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}
}

func ownerNameKey(ownerID identity.PrincipalID, name string) string {
	return string(ownerID) + ":" + strings.ToLower(strings.TrimSpace(name))
}
