package template

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/google/uuid"
	"martinbeauvais.com/mbgit/knotbase/knotdb/graph"
	"martinbeauvais.com/mbgit/knotbase/knotdb/model"
)

const (
	templatesStoreSuffix = ".json"
	supportedSchema      = 1
)

var semverRe = regexp.MustCompile(`^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$`)

type storedTemplate struct {
	ID          graph.TemplateID     `json:"id"`
	SpaceID     model.SpaceID        `json:"space_id"`
	Key         string               `json:"key"`
	Version     string               `json:"version"`
	DisplayName string               `json:"display_name,omitempty"`
	Description string               `json:"description,omitempty"`
	System      bool                 `json:"system,omitempty"`
	Properties  graph.PropertyPolicy `json:"properties"`
	Children    graph.ChildPolicy    `json:"children"`
}

type defaultManager struct {
	location               string
	templates              []storedTemplate
	indexByID              map[graph.TemplateID]int
	indexBySpaceKeyVersion map[string]int
}

// NewManager creates the default file-backed Manager implementation.
func NewManager() Manager {
	return &defaultManager{indexByID: map[graph.TemplateID]int{}, indexBySpaceKeyVersion: map[string]int{}}
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
	m.templates = []storedTemplate{}

	entries, err := os.ReadDir(location)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() || !isTemplateStoreFile(entry.Name()) {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(location, entry.Name()))
		if err != nil {
			return err
		}
		var templates []storedTemplate
		if err := json.Unmarshal(raw, &templates); err != nil {
			return err
		}
		m.templates = append(m.templates, templates...)
	}
	m.rebuildIndex()
	return nil
}

func (m *defaultManager) Import(ctx context.Context, spaceID model.SpaceID, doc ImportDocument) ([]graph.Template, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if spaceID == uuid.Nil {
		return nil, fmt.Errorf("%w: space_id is required", ErrInvalidInput)
	}
	if err := validateImportDocument(doc); err != nil {
		return nil, err
	}

	seen := map[string]struct{}{}
	for _, in := range doc.Templates {
		key := spaceKeyVersion(spaceID, in.Key, in.Version)
		if _, ok := seen[key]; ok {
			return nil, fmt.Errorf("%w: %s@%s", ErrDuplicateTemplateVersion, in.Key, in.Version)
		}
		seen[key] = struct{}{}
		if _, exists := m.indexBySpaceKeyVersion[key]; exists {
			return nil, fmt.Errorf("%w: %s@%s", ErrDuplicateTemplateVersion, in.Key, in.Version)
		}
	}

	created := make([]storedTemplate, 0, len(doc.Templates))
	for _, in := range doc.Templates {
		t := storedTemplate{
			ID:          uuid.New(),
			SpaceID:     spaceID,
			Key:         strings.TrimSpace(in.Key),
			Version:     strings.TrimSpace(in.Version),
			DisplayName: in.DisplayName,
			Description: in.Description,
			System:      in.System,
			Properties:  toPropertyPolicy(in.Properties),
			Children:    toChildPolicy(in.Children),
		}
		created = append(created, t)
	}

	m.templates = append(m.templates, created...)
	m.rebuildIndex()
	if err := m.persistSpace(spaceID); err != nil {
		return nil, err
	}

	out := make([]graph.Template, 0, len(created))
	for _, t := range created {
		out = append(out, t.toModel())
	}
	return out, nil
}

func (m *defaultManager) GetByID(ctx context.Context, id graph.TemplateID) (graph.Template, error) {
	if err := ctx.Err(); err != nil {
		return graph.Template{}, err
	}
	if id == uuid.Nil {
		return graph.Template{}, fmt.Errorf("%w: template_id is required", ErrInvalidInput)
	}
	idx, ok := m.indexByID[id]
	if !ok {
		return graph.Template{}, ErrTemplateNotFound
	}
	return m.templates[idx].toModel(), nil
}

func (m *defaultManager) Find(ctx context.Context, spaceID model.SpaceID, key string, version string) (graph.Template, error) {
	if err := ctx.Err(); err != nil {
		return graph.Template{}, err
	}
	if spaceID == uuid.Nil {
		return graph.Template{}, fmt.Errorf("%w: space_id is required", ErrInvalidInput)
	}
	if strings.TrimSpace(key) == "" {
		return graph.Template{}, fmt.Errorf("%w: key is required", ErrInvalidInput)
	}
	if !validSemver(version) {
		return graph.Template{}, fmt.Errorf("%w: version must be semver", ErrInvalidInput)
	}
	idx, ok := m.indexBySpaceKeyVersion[spaceKeyVersion(spaceID, key, version)]
	if !ok {
		return graph.Template{}, ErrTemplateNotFound
	}
	return m.templates[idx].toModel(), nil
}

func (m *defaultManager) rebuildIndex() {
	m.indexByID = map[graph.TemplateID]int{}
	m.indexBySpaceKeyVersion = map[string]int{}
	for i, t := range m.templates {
		m.indexByID[t.ID] = i
		m.indexBySpaceKeyVersion[spaceKeyVersion(t.SpaceID, t.Key, t.Version)] = i
	}
}

func (m *defaultManager) persistSpace(spaceID model.SpaceID) error {
	items := []storedTemplate{}
	for _, t := range m.templates {
		if t.SpaceID == spaceID {
			items = append(items, t)
		}
	}
	b, err := json.MarshalIndent(items, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(templateStorePath(m.location, spaceID), b, 0o600)
}

func (t storedTemplate) toModel() graph.Template {
	return graph.Template{
		ID:          t.ID,
		SpaceID:     t.SpaceID,
		Key:         t.Key,
		Version:     t.Version,
		DisplayName: t.DisplayName,
		Description: t.Description,
		System:      t.System,
		Properties:  t.Properties,
		Children:    t.Children,
	}
}

func validateImportDocument(doc ImportDocument) error {
	if doc.SchemaVersion != supportedSchema {
		return fmt.Errorf("%w: unsupported schema_version %d", ErrInvalidInput, doc.SchemaVersion)
	}
	seenTemplates := map[string]struct{}{}
	for _, t := range doc.Templates {
		key := strings.TrimSpace(t.Key)
		version := strings.TrimSpace(t.Version)
		if key == "" {
			return fmt.Errorf("%w: template key is required", ErrInvalidInput)
		}
		if !validSemver(version) {
			return fmt.Errorf("%w: template %s version must be semver", ErrInvalidInput, key)
		}
		kv := strings.ToLower(key) + "@" + version
		if _, ok := seenTemplates[kv]; ok {
			return fmt.Errorf("%w: %s", ErrDuplicateTemplateVersion, kv)
		}
		seenTemplates[kv] = struct{}{}
		if err := validatePropertyPolicy(t.Properties); err != nil {
			return fmt.Errorf("template %s@%s: %w", key, version, err)
		}
		if err := validateChildPolicy(t.Children); err != nil {
			return fmt.Errorf("template %s@%s: %w", key, version, err)
		}
	}
	return nil
}

func validatePropertyPolicy(policy PropertyPolicyImport) error {
	allowed := map[string]struct{}{}
	for _, prop := range policy.Allowed {
		name := strings.TrimSpace(prop.Name)
		if name == "" {
			return fmt.Errorf("%w: property name is required", ErrInvalidInput)
		}
		nameKey := strings.ToLower(name)
		if _, exists := allowed[nameKey]; exists {
			return fmt.Errorf("%w: duplicate property %q", ErrInvalidInput, name)
		}
		allowed[nameKey] = struct{}{}
		if !validPropertyType(prop.Type) {
			return fmt.Errorf("%w: unsupported property type %q", ErrInvalidInput, prop.Type)
		}
	}
	for _, forbidden := range policy.Forbidden {
		name := strings.TrimSpace(forbidden)
		if name == "" {
			return fmt.Errorf("%w: forbidden property name is required", ErrInvalidInput)
		}
		if _, exists := allowed[strings.ToLower(name)]; exists {
			return fmt.Errorf("%w: property %q cannot be both allowed and forbidden", ErrInvalidInput, name)
		}
	}
	return nil
}

func validateChildPolicy(policy ChildPolicyImport) error {
	if !policy.Allowed && len(policy.AllowedTemplates) > 0 {
		return fmt.Errorf("%w: allowed_templates requires children.allowed=true", ErrInvalidInput)
	}
	seen := map[string]struct{}{}
	for _, ref := range policy.AllowedTemplates {
		key := strings.TrimSpace(ref.Key)
		version := strings.TrimSpace(ref.Version)
		if key == "" {
			return fmt.Errorf("%w: child template key is required", ErrInvalidInput)
		}
		if !validSemver(version) {
			return fmt.Errorf("%w: child template %s version must be semver", ErrInvalidInput, key)
		}
		kv := strings.ToLower(key) + "@" + version
		if _, exists := seen[kv]; exists {
			return fmt.Errorf("%w: duplicate child template %s", ErrInvalidInput, kv)
		}
		seen[kv] = struct{}{}
	}
	return nil
}

func toPropertyPolicy(in PropertyPolicyImport) graph.PropertyPolicy {
	allowed := make([]graph.TemplateProperty, 0, len(in.Allowed))
	for _, p := range in.Allowed {
		allowed = append(allowed, graph.TemplateProperty{
			Name:        strings.TrimSpace(p.Name),
			Type:        p.Type,
			Required:    p.Required,
			Default:     p.Default,
			Description: p.Description,
		})
	}
	forbidden := make([]string, 0, len(in.Forbidden))
	for _, f := range in.Forbidden {
		forbidden = append(forbidden, strings.TrimSpace(f))
	}
	return graph.PropertyPolicy{AllowExtra: in.AllowExtra, Allowed: allowed, Forbidden: forbidden}
}

func toChildPolicy(in ChildPolicyImport) graph.ChildPolicy {
	refs := make([]graph.TemplateRef, 0, len(in.AllowedTemplates))
	for _, ref := range in.AllowedTemplates {
		refs = append(refs, graph.TemplateRef{Key: strings.TrimSpace(ref.Key), Version: strings.TrimSpace(ref.Version)})
	}
	return graph.ChildPolicy{Allowed: in.Allowed, AllowedTemplates: refs}
}

func validPropertyType(t graph.PropertyType) bool {
	switch t {
	case graph.PropertyTypeString, graph.PropertyTypeNumber, graph.PropertyTypeBool, graph.PropertyTypeObject, graph.PropertyTypeArray, graph.PropertyTypeDate:
		return true
	default:
		return false
	}
}

func validSemver(version string) bool {
	return semverRe.MatchString(strings.TrimSpace(version))
}

func templateStorePath(location string, spaceID model.SpaceID) string {
	return filepath.Join(location, spaceID.String()+templatesStoreSuffix)
}

func isTemplateStoreFile(name string) bool {
	return strings.HasSuffix(name, templatesStoreSuffix)
}

func spaceKeyVersion(spaceID model.SpaceID, key string, version string) string {
	return spaceID.String() + ":" + strings.ToLower(strings.TrimSpace(key)) + "@" + strings.TrimSpace(version)
}
