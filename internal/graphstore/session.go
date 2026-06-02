package graphstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	coretemplate "martinbeauvais.com/mbgit/knotbase/knotdb/core/template"
	"martinbeauvais.com/mbgit/knotbase/knotdb/graph"
	"martinbeauvais.com/mbgit/knotbase/knotdb/model"
)

// Errors defines public errors returned by sessions.
type Errors struct {
	Closed       error
	NotFound     error
	Unauthorized error
}

// Permissions defines read/write capabilities for a session.
type Permissions struct {
	Read  bool
	Write bool
}

type session struct {
	graphsDir       string
	spaceID         model.SpaceID
	templateManager coretemplate.Manager
	permissions     Permissions
	errors          Errors
	closed          bool
}

// NewSession opens a file-backed graph session for a space.
func NewSession(graphsDir string, spaceID model.SpaceID, templateManager coretemplate.Manager, permissions Permissions, errs Errors) graph.Session {
	return &session{graphsDir: graphsDir, spaceID: spaceID, templateManager: templateManager, permissions: permissions, errors: errs}
}

func (s *session) AddNode(ctx context.Context, in graph.NodeInput) (graph.Node, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return graph.Node{}, err
	}
	if err := s.ensureWrite(); err != nil {
		return graph.Node{}, err
	}
	nodes, err := s.readNodes()
	if err != nil {
		return graph.Node{}, err
	}

	nodeID := uuid.New()
	if in.ID != nil {
		nodeID = *in.ID
	}

	var nodeTemplate *graph.Template
	props := copyProps(in.Props)
	if in.TemplateID != nil {
		t, err := s.templateManager.GetByID(ctx, *in.TemplateID)
		if err != nil {
			if errors.Is(err, coretemplate.ErrTemplateNotFound) {
				return graph.Node{}, fmt.Errorf("%w: template not found", s.errors.NotFound)
			}
			return graph.Node{}, err
		}
		if t.SpaceID != s.spaceID {
			return graph.Node{}, fmt.Errorf("%w: template not found in space", s.errors.NotFound)
		}
		if err := validateProps(&props, t); err != nil {
			return graph.Node{}, err
		}
		nodeTemplate = &t
	}

	if in.ParentID != nil {
		parent, ok := findNode(nodes, *in.ParentID)
		if !ok {
			return graph.Node{}, fmt.Errorf("%w: parent node not found", s.errors.NotFound)
		}
		if err := s.validateChild(ctx, parent, nodeTemplate); err != nil {
			return graph.Node{}, err
		}
	}

	n := graph.Node{
		ID:         nodeID,
		TemplateID: in.TemplateID,
		ParentID:   in.ParentID,
		Content:    in.Content,
		Props:      props,
	}
	nodes = append(nodes, n)
	if err := s.writeNodes(nodes); err != nil {
		return graph.Node{}, err
	}
	return n, nil
}

func (s *session) AddEdge(ctx context.Context, in graph.EdgeInput) (graph.Edge, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return graph.Edge{}, err
	}
	if err := s.ensureWrite(); err != nil {
		return graph.Edge{}, err
	}
	nodes, err := s.readNodes()
	if err != nil {
		return graph.Edge{}, err
	}
	if _, ok := findNode(nodes, in.FromID); !ok {
		return graph.Edge{}, fmt.Errorf("%w: from node not found", s.errors.NotFound)
	}
	if _, ok := findNode(nodes, in.ToID); !ok {
		return graph.Edge{}, fmt.Errorf("%w: to node not found", s.errors.NotFound)
	}

	edges, err := s.readEdges()
	if err != nil {
		return graph.Edge{}, err
	}
	edgeID := uuid.New()
	if in.ID != nil {
		edgeID = *in.ID
	}
	e := graph.Edge{ID: edgeID, FromID: in.FromID, ToID: in.ToID, Kind: in.Kind, Props: in.Props}
	edges = append(edges, e)
	if err := s.writeEdges(edges); err != nil {
		return graph.Edge{}, err
	}
	return e, nil
}

func (s *session) AddGraph(ctx context.Context, in graph.GraphInput) error {
	if err := s.ensureOpen(ctx); err != nil {
		return err
	}
	if err := s.ensureWrite(); err != nil {
		return err
	}
	for _, n := range in.Nodes {
		if _, err := s.AddNode(ctx, n); err != nil {
			return err
		}
	}
	for _, e := range in.Edges {
		if _, err := s.AddEdge(ctx, e); err != nil {
			return err
		}
	}
	return nil
}

func (s *session) GetNode(ctx context.Context, id graph.NodeID) (graph.Node, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return graph.Node{}, err
	}
	if err := s.ensureRead(); err != nil {
		return graph.Node{}, err
	}
	nodes, err := s.readNodes()
	if err != nil {
		return graph.Node{}, err
	}
	n, ok := findNode(nodes, id)
	if !ok {
		return graph.Node{}, s.errors.NotFound
	}
	return n, nil
}

func (s *session) Close() error {
	s.closed = true
	return nil
}

func (s *session) ensureOpen(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if s.closed {
		return s.errors.Closed
	}
	return nil
}

func (s *session) ensureRead() error {
	if !s.permissions.Read {
		return s.errors.Unauthorized
	}
	return nil
}

func (s *session) ensureWrite() error {
	if !s.permissions.Write {
		return s.errors.Unauthorized
	}
	return nil
}

func (s *session) spacePath() string {
	return filepath.Join(s.graphsDir, safeID(s.spaceID))
}

func (s *session) nodesPath() string {
	return filepath.Join(s.spacePath(), "nodes.json")
}

func (s *session) edgesPath() string {
	return filepath.Join(s.spacePath(), "edges.json")
}

func (s *session) readNodes() ([]graph.Node, error) {
	path := s.nodesPath()
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return []graph.Node{}, nil
		}
		return nil, err
	}
	var out []graph.Node
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *session) writeNodes(nodes []graph.Node) error {
	if err := os.MkdirAll(s.spacePath(), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(nodes, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(s.nodesPath(), b, 0o600)
}

func (s *session) readEdges() ([]graph.Edge, error) {
	path := s.edgesPath()
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return []graph.Edge{}, nil
		}
		return nil, err
	}
	var out []graph.Edge
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *session) writeEdges(edges []graph.Edge) error {
	if err := os.MkdirAll(s.spacePath(), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(edges, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(s.edgesPath(), b, 0o600)
}

func (s *session) validateChild(ctx context.Context, parent graph.Node, childTemplate *graph.Template) error {
	if parent.TemplateID == nil {
		return nil
	}
	parentTemplate, err := s.templateManager.GetByID(ctx, *parent.TemplateID)
	if err != nil {
		if errors.Is(err, coretemplate.ErrTemplateNotFound) {
			return fmt.Errorf("%w: parent template not found", s.errors.NotFound)
		}
		return err
	}
	if parentTemplate.SpaceID != s.spaceID {
		return fmt.Errorf("%w: parent template not found in space", s.errors.NotFound)
	}
	if !parentTemplate.Children.Allowed {
		return fmt.Errorf("%w: parent template does not allow children", coretemplate.ErrInvalidInput)
	}
	if len(parentTemplate.Children.AllowedTemplates) == 0 {
		return nil
	}
	if childTemplate == nil {
		return fmt.Errorf("%w: child template is required", coretemplate.ErrInvalidInput)
	}
	for _, ref := range parentTemplate.Children.AllowedTemplates {
		if ref.Key == childTemplate.Key && ref.Version == childTemplate.Version {
			return nil
		}
	}
	return fmt.Errorf("%w: child template %s@%s is not allowed", coretemplate.ErrInvalidInput, childTemplate.Key, childTemplate.Version)
}

func validateProps(props *map[string]any, tmpl graph.Template) error {
	if *props == nil {
		*props = map[string]any{}
	}
	allowed := map[string]graph.TemplateProperty{}
	for _, prop := range tmpl.Properties.Allowed {
		allowed[prop.Name] = prop
		if _, ok := (*props)[prop.Name]; !ok && prop.Default != nil {
			(*props)[prop.Name] = prop.Default
		}
	}
	for _, name := range tmpl.Properties.Forbidden {
		if _, ok := (*props)[name]; ok {
			return fmt.Errorf("%w: property %q is forbidden", coretemplate.ErrInvalidInput, name)
		}
	}
	for name, value := range *props {
		prop, ok := allowed[name]
		if !ok {
			if !tmpl.Properties.AllowExtra {
				return fmt.Errorf("%w: property %q is not allowed", coretemplate.ErrInvalidInput, name)
			}
			continue
		}
		if err := validatePropertyValue(prop, value); err != nil {
			return err
		}
	}
	for _, prop := range tmpl.Properties.Allowed {
		if !prop.Required {
			continue
		}
		value, ok := (*props)[prop.Name]
		if !ok || value == nil {
			return fmt.Errorf("%w: required property %q is missing", coretemplate.ErrInvalidInput, prop.Name)
		}
	}
	return nil
}

func validatePropertyValue(prop graph.TemplateProperty, value any) error {
	if value == nil {
		return fmt.Errorf("%w: property %q cannot be null", coretemplate.ErrInvalidInput, prop.Name)
	}
	valid := false
	switch prop.Type {
	case graph.PropertyTypeString:
		_, valid = value.(string)
	case graph.PropertyTypeNumber:
		valid = isNumber(value)
	case graph.PropertyTypeBool:
		_, valid = value.(bool)
	case graph.PropertyTypeObject:
		_, valid = value.(map[string]any)
	case graph.PropertyTypeArray:
		_, valid = value.([]any)
	case graph.PropertyTypeDate:
		valid = isDate(value)
	default:
		return fmt.Errorf("%w: unsupported property type %q", coretemplate.ErrInvalidInput, prop.Type)
	}
	if !valid {
		return fmt.Errorf("%w: property %q must be %s", coretemplate.ErrInvalidInput, prop.Name, prop.Type)
	}
	return nil
}

func isNumber(value any) bool {
	switch n := value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return true
	case json.Number:
		_, err := n.Float64()
		return err == nil
	default:
		return false
	}
}

func isDate(value any) bool {
	s, ok := value.(string)
	if !ok {
		return false
	}
	if _, err := time.Parse(time.RFC3339, s); err == nil {
		return true
	}
	_, err := time.Parse("2006-01-02", s)
	return err == nil
}

func copyProps(in map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range in {
		out[k] = v
	}
	return out
}

func findNode(nodes []graph.Node, id graph.NodeID) (graph.Node, bool) {
	for _, n := range nodes {
		if n.ID == id {
			return n, true
		}
	}
	return graph.Node{}, false
}

func safeID(id model.SpaceID) string {
	repl := strings.NewReplacer(":", "_", "/", "_", "\\", "_")
	return repl.Replace(id.String())
}
