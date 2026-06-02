package spacemgmt

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"knot_db/model"
)

const spacesStoreFile = "spaces.json"

type storedSpace struct {
	SpaceID  model.SpaceID       `json:"space_id"`
	OwnerID  model.UserID        `json:"owner_id"`
	Name     string              `json:"name"`
	Status   string              `json:"status"`
	Settings model.SpaceSettings `json:"settings,omitempty"`
}

type defaultSpaceManager struct {
	location    string
	storePath   string
	spaces      []storedSpace
	indexByID   map[model.SpaceID]int
	indexByName map[string]int
}

// NewSpaceManager creates the default file-backed SpaceManager implementation.
func NewSpaceManager() SpaceManager {
	return &defaultSpaceManager{indexByID: map[model.SpaceID]int{}, indexByName: map[string]int{}}
}

func (m *defaultSpaceManager) Init(ctx context.Context, location string) error {
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

func (m *defaultSpaceManager) ExistsByID(ctx context.Context, id model.SpaceID) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if id == uuid.Nil {
		return false, fmt.Errorf("%w: space_id is required", ErrInvalidInput)
	}
	_, ok := m.indexByID[id]
	return ok, nil
}

func (m *defaultSpaceManager) GetByID(ctx context.Context, id model.SpaceID) (model.Space, error) {
	if err := ctx.Err(); err != nil {
		return model.Space{}, err
	}
	if id == uuid.Nil {
		return model.Space{}, fmt.Errorf("%w: space_id is required", ErrInvalidInput)
	}
	idx, ok := m.indexByID[id]
	if !ok {
		return model.Space{}, ErrSpaceNotFound
	}
	return m.spaces[idx].toModel(), nil
}

func (m *defaultSpaceManager) FindByOwnerAndName(ctx context.Context, ownerID model.UserID, name string) (model.Space, error) {
	if err := ctx.Err(); err != nil {
		return model.Space{}, err
	}
	if ownerID == uuid.Nil {
		return model.Space{}, fmt.Errorf("%w: owner_id is required", ErrInvalidInput)
	}
	if strings.TrimSpace(name) == "" {
		return model.Space{}, fmt.Errorf("%w: name is required", ErrInvalidInput)
	}
	idx, ok := m.indexByName[ownerNameKey(ownerID, name)]
	if !ok {
		return model.Space{}, ErrSpaceNotFound
	}
	return m.spaces[idx].toModel(), nil
}

func (m *defaultSpaceManager) Create(ctx context.Context, in CreateSpaceInput) (model.Space, error) {
	if err := ctx.Err(); err != nil {
		return model.Space{}, err
	}
	if in.OwnerID == uuid.Nil {
		return model.Space{}, fmt.Errorf("%w: owner_id is required", ErrInvalidInput)
	}
	if strings.TrimSpace(in.Name) == "" {
		return model.Space{}, fmt.Errorf("%w: name is required", ErrInvalidInput)
	}
	if existing, err := m.FindByOwnerAndName(ctx, in.OwnerID, in.Name); err == nil {
		return existing, nil
	} else if err != nil && err != ErrSpaceNotFound {
		return model.Space{}, err
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
		return model.Space{}, err
	}
	return s.toModel(), nil
}

func (m *defaultSpaceManager) rebuildIndex() {
	m.indexByID = map[model.SpaceID]int{}
	m.indexByName = map[string]int{}
	for i, s := range m.spaces {
		m.indexByID[s.SpaceID] = i
		m.indexByName[ownerNameKey(s.OwnerID, s.Name)] = i
	}
}

func (m *defaultSpaceManager) persist() error {
	b, err := json.MarshalIndent(m.spaces, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(m.storePath, b, 0o600)
}

func (s storedSpace) toModel() model.Space {
	return model.Space{
		SpaceID:  s.SpaceID,
		OwnerID:  s.OwnerID,
		Name:     s.Name,
		Status:   s.Status,
		Settings: s.Settings,
	}
}

func ownerNameKey(ownerID model.UserID, name string) string {
	return ownerID.String() + ":" + strings.ToLower(strings.TrimSpace(name))
}
