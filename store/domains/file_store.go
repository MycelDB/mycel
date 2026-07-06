package domains

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/domain/graph"
	domainspace "github.com/myceldb/mycel/domain/space"
	"github.com/myceldb/mycel/internal/filestore"
)

const domainsStoreFile = "domains.json"

type storedDomain struct {
	ID          graph.DomainID      `json:"id"`
	SpaceID     domainspace.SpaceID `json:"space_id"`
	Key         string              `json:"key"`
	Name        string              `json:"name"`
	Description string              `json:"description,omitempty"`
	Default     bool                `json:"default"`
	CreatedAt   time.Time           `json:"created_at"`
	UpdatedAt   time.Time           `json:"updated_at"`
}

type storedState struct {
	Domains []storedDomain `json:"domains"`
}

type defaultManager struct {
	location    string
	storePath   string
	domains     []storedDomain
	indexByID   map[graph.DomainID]int
	indexByKey  map[string]int
	defaultBySp map[domainspace.SpaceID]int
}

// NewManager creates the default file-backed Manager implementation.
func NewManager() Manager {
	return &defaultManager{indexByID: map[graph.DomainID]int{}, indexByKey: map[string]int{}, defaultBySp: map[domainspace.SpaceID]int{}}
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
	m.storePath = filepath.Join(location, domainsStoreFile)
	if _, err := os.Stat(m.storePath); err != nil {
		if os.IsNotExist(err) {
			m.domains = []storedDomain{}
			m.rebuildIndex()
			return m.persist()
		}
		return err
	}
	raw, err := os.ReadFile(m.storePath)
	if err != nil {
		return err
	}
	var state storedState
	if err := json.Unmarshal(raw, &state); err != nil {
		// Older development snapshots may contain just the domains array.
		var domains []storedDomain
		if arrErr := json.Unmarshal(raw, &domains); arrErr != nil {
			return err
		}
		state.Domains = domains
	}
	m.domains = state.Domains
	m.rebuildIndex()
	return nil
}

func (m *defaultManager) Create(ctx context.Context, in CreateInput) (graph.Domain, error) {
	if err := ctx.Err(); err != nil {
		return graph.Domain{}, err
	}
	if in.SpaceID == uuid.Nil {
		return graph.Domain{}, fmt.Errorf("%w: space_id is required", ErrInvalidInput)
	}
	key := normalizeKey(in.Key)
	if key == "" {
		return graph.Domain{}, fmt.Errorf("%w: key is required", ErrInvalidInput)
	}
	name := strings.TrimSpace(in.Name)
	if name == "" {
		name = key
	}
	if existing, err := m.FindBySpaceAndKey(ctx, in.SpaceID, key); err == nil {
		return existing, nil
	} else if err != nil && err != ErrDomainNotFound {
		return graph.Domain{}, err
	}
	if in.Default {
		if _, ok := m.defaultBySp[in.SpaceID]; ok {
			return graph.Domain{}, fmt.Errorf("%w: default domain already exists", ErrConflict)
		}
	}
	now := time.Now().UTC()
	d := storedDomain{ID: uuid.New(), SpaceID: in.SpaceID, Key: key, Name: name, Description: strings.TrimSpace(in.Description), Default: in.Default, CreatedAt: now, UpdatedAt: now}
	m.domains = append(m.domains, d)
	m.rebuildIndex()
	if err := m.persist(); err != nil {
		m.domains = m.domains[:len(m.domains)-1]
		m.rebuildIndex()
		return graph.Domain{}, err
	}
	return d.toModel(), nil
}

func (m *defaultManager) EnsureDefault(ctx context.Context, spaceID domainspace.SpaceID) (graph.Domain, error) {
	if err := ctx.Err(); err != nil {
		return graph.Domain{}, err
	}
	if spaceID == uuid.Nil {
		return graph.Domain{}, fmt.Errorf("%w: space_id is required", ErrInvalidInput)
	}
	if d, err := m.GetDefault(ctx, spaceID); err == nil {
		return d, nil
	} else if err != nil && err != ErrDomainNotFound {
		return graph.Domain{}, err
	}
	return m.Create(ctx, CreateInput{SpaceID: spaceID, Key: graph.DefaultDomainKey, Name: graph.DefaultDomainName, Default: true})
}

func (m *defaultManager) GetByID(ctx context.Context, id graph.DomainID) (graph.Domain, error) {
	if err := ctx.Err(); err != nil {
		return graph.Domain{}, err
	}
	if id == uuid.Nil {
		return graph.Domain{}, fmt.Errorf("%w: domain_id is required", ErrInvalidInput)
	}
	idx, ok := m.indexByID[id]
	if !ok {
		return graph.Domain{}, ErrDomainNotFound
	}
	return m.domains[idx].toModel(), nil
}

func (m *defaultManager) FindBySpaceAndKey(ctx context.Context, spaceID domainspace.SpaceID, key string) (graph.Domain, error) {
	if err := ctx.Err(); err != nil {
		return graph.Domain{}, err
	}
	if spaceID == uuid.Nil {
		return graph.Domain{}, fmt.Errorf("%w: space_id is required", ErrInvalidInput)
	}
	key = normalizeKey(key)
	if key == "" {
		return graph.Domain{}, fmt.Errorf("%w: key is required", ErrInvalidInput)
	}
	idx, ok := m.indexByKey[spaceKey(spaceID, key)]
	if !ok {
		return graph.Domain{}, ErrDomainNotFound
	}
	return m.domains[idx].toModel(), nil
}

func (m *defaultManager) GetDefault(ctx context.Context, spaceID domainspace.SpaceID) (graph.Domain, error) {
	if err := ctx.Err(); err != nil {
		return graph.Domain{}, err
	}
	if spaceID == uuid.Nil {
		return graph.Domain{}, fmt.Errorf("%w: space_id is required", ErrInvalidInput)
	}
	idx, ok := m.defaultBySp[spaceID]
	if !ok {
		return graph.Domain{}, ErrDomainNotFound
	}
	return m.domains[idx].toModel(), nil
}

func (m *defaultManager) ListBySpace(ctx context.Context, spaceID domainspace.SpaceID) ([]graph.Domain, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if spaceID == uuid.Nil {
		return nil, fmt.Errorf("%w: space_id is required", ErrInvalidInput)
	}
	out := []graph.Domain{}
	for _, d := range m.domains {
		if d.SpaceID == spaceID {
			out = append(out, d.toModel())
		}
	}
	return out, nil
}

func (m *defaultManager) Update(ctx context.Context, in UpdateInput) (graph.Domain, error) {
	if err := ctx.Err(); err != nil {
		return graph.Domain{}, err
	}
	if in.DomainID == uuid.Nil {
		return graph.Domain{}, fmt.Errorf("%w: domain_id is required", ErrInvalidInput)
	}
	idx, ok := m.indexByID[in.DomainID]
	if !ok {
		return graph.Domain{}, ErrDomainNotFound
	}
	old := m.domains[idx]
	updated := old
	if in.Name != nil {
		name := strings.TrimSpace(*in.Name)
		if name == "" {
			return graph.Domain{}, fmt.Errorf("%w: name is required", ErrInvalidInput)
		}
		updated.Name = name
	}
	if in.Description != nil {
		updated.Description = strings.TrimSpace(*in.Description)
	}
	updated.UpdatedAt = time.Now().UTC()
	m.domains[idx] = updated
	if err := m.persist(); err != nil {
		m.domains[idx] = old
		return graph.Domain{}, err
	}
	return updated.toModel(), nil
}

func (m *defaultManager) DeleteByID(ctx context.Context, id graph.DomainID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if id == uuid.Nil {
		return fmt.Errorf("%w: domain_id is required", ErrInvalidInput)
	}
	idx, ok := m.indexByID[id]
	if !ok {
		return ErrDomainNotFound
	}
	oldDomains := append([]storedDomain(nil), m.domains...)
	m.domains = append(m.domains[:idx], m.domains[idx+1:]...)
	m.rebuildIndex()
	if err := m.persist(); err != nil {
		m.domains = oldDomains
		m.rebuildIndex()
		return err
	}
	return nil
}

func (m *defaultManager) DeleteForSpace(ctx context.Context, spaceID domainspace.SpaceID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if spaceID == uuid.Nil {
		return fmt.Errorf("%w: space_id is required", ErrInvalidInput)
	}
	oldDomains := append([]storedDomain(nil), m.domains...)
	domains := m.domains[:0]
	for _, d := range m.domains {
		if d.SpaceID != spaceID {
			domains = append(domains, d)
		}
	}
	m.domains = domains
	m.rebuildIndex()
	if err := m.persist(); err != nil {
		m.domains = oldDomains
		m.rebuildIndex()
		return err
	}
	return nil
}

func (m *defaultManager) rebuildIndex() {
	m.indexByID = map[graph.DomainID]int{}
	m.indexByKey = map[string]int{}
	m.defaultBySp = map[domainspace.SpaceID]int{}
	for i, d := range m.domains {
		m.indexByID[d.ID] = i
		m.indexByKey[spaceKey(d.SpaceID, d.Key)] = i
		if d.Default {
			m.defaultBySp[d.SpaceID] = i
		}
	}
}

func (m *defaultManager) persist() error {
	b, err := json.MarshalIndent(storedState{Domains: m.domains}, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return filestore.WriteFileAtomic(m.storePath, b, 0o600)
}

func (d storedDomain) toModel() graph.Domain {
	return graph.Domain{ID: d.ID, SpaceID: d.SpaceID, Key: d.Key, Name: d.Name, Description: d.Description, Default: d.Default, CreatedAt: d.CreatedAt, UpdatedAt: d.UpdatedAt}
}

func normalizeKey(key string) string {
	key = strings.ToLower(strings.TrimSpace(key))
	key = strings.ReplaceAll(key, " ", "-")
	return key
}

func spaceKey(spaceID domainspace.SpaceID, key string) string {
	return spaceID.String() + ":" + normalizeKey(key)
}
