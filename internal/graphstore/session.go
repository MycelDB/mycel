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
	"martinbeauvais.com/mbgit/knotbase/knotdb/domain/graph"
	domainspace "martinbeauvais.com/mbgit/knotbase/knotdb/domain/space"
	domainsession "martinbeauvais.com/mbgit/knotbase/knotdb/session"
)

// Errors defines public errors returned by sessions.
type Errors struct {
	Closed       error
	NotFound     error
	Unauthorized error
	Conflict     error
}

// Permissions defines read/write capabilities for a session.
type Permissions struct {
	Read  bool
	Write bool
	Admin bool
}

type session struct {
	graphsDir       string
	spaceID         domainspace.SpaceID
	templateManager coretemplate.Manager
	permissions     Permissions
	errors          Errors
	closed          bool
}

// NewSession opens a file-backed graph session for a space.
func NewSession(graphsDir string, spaceID domainspace.SpaceID, templateManager coretemplate.Manager, permissions Permissions, errs Errors) domainsession.Session {
	return &session{graphsDir: graphsDir, spaceID: spaceID, templateManager: templateManager, permissions: permissions, errors: errs}
}

func (s *session) ImportTemplates(ctx context.Context, in domainsession.ImportTemplatesInput) ([]graph.Template, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return nil, err
	}
	if err := s.ensureSpaceLive(); err != nil {
		return nil, err
	}
	if err := s.ensureAdmin(); err != nil {
		return nil, err
	}
	return s.templateManager.Import(ctx, s.spaceID, in.Document)
}

func (s *session) ListTemplates(ctx context.Context) ([]graph.Template, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return nil, err
	}
	if err := s.ensureSpaceLive(); err != nil {
		return nil, err
	}
	if err := s.ensureRead(); err != nil {
		return nil, err
	}
	return s.templateManager.ListBySpace(ctx, s.spaceID)
}

func (s *session) AddNode(ctx context.Context, in domainsession.AddNodeInput) (graph.Node, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return graph.Node{}, err
	}
	if err := s.ensureSpaceLive(); err != nil {
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
	n, err := s.buildNode(ctx, nodes, nodeID, in.TemplateID, in.ParentID, in.Content, in.Props)
	if err != nil {
		return graph.Node{}, err
	}
	nodes = append(nodes, n)
	if err := s.writeNodes(nodes); err != nil {
		return graph.Node{}, err
	}
	return n, nil
}

func (s *session) ListNodes(ctx context.Context) ([]graph.Node, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return nil, err
	}
	if err := s.ensureSpaceLive(); err != nil {
		return nil, err
	}
	if err := s.ensureRead(); err != nil {
		return nil, err
	}
	nodes, err := s.readNodes()
	if err != nil {
		return nil, err
	}
	return cloneNodes(nodes), nil
}

func (s *session) UpdateNode(ctx context.Context, in domainsession.UpdateNodeInput) (graph.Node, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return graph.Node{}, err
	}
	if err := s.ensureSpaceLive(); err != nil {
		return graph.Node{}, err
	}
	if err := s.ensureWrite(); err != nil {
		return graph.Node{}, err
	}
	if in.ID == uuid.Nil {
		return graph.Node{}, fmt.Errorf("%w: node_id is required", s.errors.NotFound)
	}
	nodes, err := s.readNodes()
	if err != nil {
		return graph.Node{}, err
	}
	idx := findNodeIndex(nodes, in.ID)
	if idx < 0 {
		return graph.Node{}, s.errors.NotFound
	}
	candidateNodes := append([]graph.Node(nil), nodes[:idx]...)
	candidateNodes = append(candidateNodes, nodes[idx+1:]...)
	n, err := s.buildNode(ctx, candidateNodes, in.ID, in.TemplateID, in.ParentID, in.Content, in.Props)
	if err != nil {
		return graph.Node{}, err
	}
	if err := s.validateExistingChildren(ctx, n, candidateNodes); err != nil {
		return graph.Node{}, err
	}
	nodes[idx] = n
	if err := s.writeNodes(nodes); err != nil {
		return graph.Node{}, err
	}
	return n, nil
}

func (s *session) UpsertNode(ctx context.Context, in domainsession.UpsertNodeInput) (graph.Node, error) {
	if in.ID == nil {
		return s.AddNode(ctx, domainsession.AddNodeInput{TemplateID: in.TemplateID, ParentID: in.ParentID, Content: in.Content, Props: in.Props})
	}
	if err := s.ensureOpen(ctx); err != nil {
		return graph.Node{}, err
	}
	if err := s.ensureSpaceLive(); err != nil {
		return graph.Node{}, err
	}
	if err := s.ensureWrite(); err != nil {
		return graph.Node{}, err
	}
	nodes, err := s.readNodes()
	if err != nil {
		return graph.Node{}, err
	}
	if findNodeIndex(nodes, *in.ID) >= 0 {
		return s.UpdateNode(ctx, domainsession.UpdateNodeInput{ID: *in.ID, TemplateID: in.TemplateID, ParentID: in.ParentID, Content: in.Content, Props: in.Props})
	}
	n, err := s.buildNode(ctx, nodes, *in.ID, in.TemplateID, in.ParentID, in.Content, in.Props)
	if err != nil {
		return graph.Node{}, err
	}
	nodes = append(nodes, n)
	if err := s.writeNodes(nodes); err != nil {
		return graph.Node{}, err
	}
	return n, nil
}

func (s *session) AddEdge(ctx context.Context, in domainsession.AddEdgeInput) (graph.Edge, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return graph.Edge{}, err
	}
	if err := s.ensureSpaceLive(); err != nil {
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

func (s *session) AddGraph(ctx context.Context, in domainsession.AddGraphInput) error {
	if err := s.ensureOpen(ctx); err != nil {
		return err
	}
	if err := s.ensureSpaceLive(); err != nil {
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
	if err := s.ensureSpaceLive(); err != nil {
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

func (s *session) DeleteNode(ctx context.Context, in domainsession.DeleteNodeInput) error {
	if err := s.ensureOpen(ctx); err != nil {
		return err
	}
	if err := s.ensureSpaceLive(); err != nil {
		return err
	}
	if err := s.ensureWrite(); err != nil {
		return err
	}
	if in.ID == uuid.Nil {
		return fmt.Errorf("%w: node_id is required", s.errors.NotFound)
	}
	nodes, err := s.readNodes()
	if err != nil {
		return err
	}
	if _, ok := findNode(nodes, in.ID); !ok {
		return s.errors.NotFound
	}
	deleteIDs := map[graph.NodeID]struct{}{in.ID: {}}
	if in.Recursive {
		changed := true
		for changed {
			changed = false
			for _, n := range nodes {
				if n.ParentID == nil {
					continue
				}
				if _, parentDeleted := deleteIDs[*n.ParentID]; parentDeleted {
					if _, already := deleteIDs[n.ID]; !already {
						deleteIDs[n.ID] = struct{}{}
						changed = true
					}
				}
			}
		}
	} else {
		for _, n := range nodes {
			if n.ParentID != nil && *n.ParentID == in.ID {
				if s.errors.Conflict != nil {
					return fmt.Errorf("%w: node has child nodes", s.errors.Conflict)
				}
				return fmt.Errorf("node has child nodes")
			}
		}
	}

	remainingNodes := make([]graph.Node, 0, len(nodes))
	for _, n := range nodes {
		if _, deleted := deleteIDs[n.ID]; !deleted {
			remainingNodes = append(remainingNodes, n)
		}
	}
	edges, err := s.readEdges()
	if err != nil {
		return err
	}
	remainingEdges := make([]graph.Edge, 0, len(edges))
	for _, edge := range edges {
		_, fromDeleted := deleteIDs[edge.FromID]
		_, toDeleted := deleteIDs[edge.ToID]
		if !fromDeleted && !toDeleted {
			remainingEdges = append(remainingEdges, edge)
		}
	}
	if err := s.writeNodes(remainingNodes); err != nil {
		return err
	}
	return s.writeEdges(remainingEdges)
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

func (s *session) ensureAdmin() error {
	if !s.permissions.Admin {
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

func (s *session) markerPath() string {
	return filepath.Join(s.spacePath(), ".space")
}

func (s *session) ensureSpaceLive() error {
	if _, err := os.Stat(s.markerPath()); err != nil {
		if os.IsNotExist(err) {
			return s.errors.NotFound
		}
		return err
	}
	return nil
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
	if err := s.ensureSpaceLive(); err != nil {
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
	if err := s.ensureSpaceLive(); err != nil {
		return err
	}
	b, err := json.MarshalIndent(edges, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(s.edgesPath(), b, 0o600)
}

func (s *session) buildNode(ctx context.Context, nodes []graph.Node, nodeID graph.NodeID, templateID *graph.TemplateID, parentID *graph.NodeID, content string, inputProps map[string]any) (graph.Node, error) {
	if nodeID == uuid.Nil {
		return graph.Node{}, fmt.Errorf("%w: node_id is required", s.errors.NotFound)
	}
	var nodeTemplate *graph.Template
	props := copyProps(inputProps)
	if templateID != nil {
		t, err := s.templateManager.GetByID(ctx, *templateID)
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
	if parentID != nil {
		if *parentID == nodeID {
			return graph.Node{}, fmt.Errorf("%w: node cannot be its own parent", coretemplate.ErrInvalidInput)
		}
		parent, ok := findNode(nodes, *parentID)
		if !ok {
			return graph.Node{}, fmt.Errorf("%w: parent node not found", s.errors.NotFound)
		}
		if err := s.validateChild(ctx, parent, nodeTemplate); err != nil {
			return graph.Node{}, err
		}
	}
	return graph.Node{ID: nodeID, TemplateID: templateID, ParentID: parentID, Content: content, Props: props}, nil
}

func (s *session) validateExistingChildren(ctx context.Context, parent graph.Node, nodes []graph.Node) error {
	for _, child := range nodes {
		if child.ParentID == nil || *child.ParentID != parent.ID {
			continue
		}
		var childTemplate *graph.Template
		if child.TemplateID != nil {
			t, err := s.templateManager.GetByID(ctx, *child.TemplateID)
			if err != nil {
				if errors.Is(err, coretemplate.ErrTemplateNotFound) {
					return fmt.Errorf("%w: child template not found", s.errors.NotFound)
				}
				return err
			}
			if t.SpaceID != s.spaceID {
				return fmt.Errorf("%w: child template not found in space", s.errors.NotFound)
			}
			childTemplate = &t
		}
		if err := s.validateChild(ctx, parent, childTemplate); err != nil {
			return err
		}
	}
	return nil
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
	idx := findNodeIndex(nodes, id)
	if idx < 0 {
		return graph.Node{}, false
	}
	return nodes[idx], true
}

func findNodeIndex(nodes []graph.Node, id graph.NodeID) int {
	for i, n := range nodes {
		if n.ID == id {
			return i
		}
	}
	return -1
}

func cloneNodes(nodes []graph.Node) []graph.Node {
	out := make([]graph.Node, 0, len(nodes))
	for _, node := range nodes {
		node.Props = copyProps(node.Props)
		out = append(out, node)
	}
	return out
}

func safeID(id domainspace.SpaceID) string {
	repl := strings.NewReplacer(":", "_", "/", "_", "\\", "_")
	return repl.Replace(id.String())
}
