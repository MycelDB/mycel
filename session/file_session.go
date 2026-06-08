package session

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
	"martinbeauvais.com/mbgit/knotbase/knotdb/query"
)

// FileSession is the default file-backed Session implementation.
type FileSession struct {
	graphsDir       string
	spaceID         domainspace.SpaceID
	templateManager TemplateManager
	permissions     Permissions
	errors          Errors
	closed          bool
}

// Query starts a programmatic graph query over this session.
func (s *FileSession) Query() *query.Builder { return query.NewBuilder(s) }

func (s *FileSession) ImportTemplates(ctx context.Context, in ImportTemplatesInput) ([]graph.Template, error) {
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

func (s *FileSession) ListTemplates(ctx context.Context) ([]graph.Template, error) {
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

func (s *FileSession) AddNode(ctx context.Context, in AddNodeInput) (graph.Node, error) {
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
	n, err := s.buildNode(ctx, nodes, nodeID, in.TemplateID, in.Content, in.Props)
	if err != nil {
		return graph.Node{}, err
	}
	nodes = append(nodes, n)
	if err := s.writeNodes(nodes); err != nil {
		return graph.Node{}, err
	}
	return n, nil
}

func (s *FileSession) ListNodes(ctx context.Context) ([]graph.Node, error) {
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

func (s *FileSession) UpdateNode(ctx context.Context, in UpdateNodeInput) (graph.Node, error) {
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
	n, err := s.buildNode(ctx, nodes, in.ID, in.TemplateID, in.Content, in.Props)
	if err != nil {
		return graph.Node{}, err
	}
	candidateNodes := append([]graph.Node(nil), nodes...)
	candidateNodes[idx] = n
	edges, err := s.readEdges()
	if err != nil {
		return graph.Node{}, err
	}
	if err := s.validateIncidentContains(ctx, n, candidateNodes, edges); err != nil {
		return graph.Node{}, err
	}
	nodes[idx] = n
	if err := s.writeNodes(nodes); err != nil {
		return graph.Node{}, err
	}
	return n, nil
}

func (s *FileSession) UpsertNode(ctx context.Context, in UpsertNodeInput) (graph.Node, error) {
	if in.ID == nil {
		return s.AddNode(ctx, AddNodeInput{TemplateID: in.TemplateID, Content: in.Content, Props: in.Props})
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
		return s.UpdateNode(ctx, UpdateNodeInput{ID: *in.ID, TemplateID: in.TemplateID, Content: in.Content, Props: in.Props})
	}
	n, err := s.buildNode(ctx, nodes, *in.ID, in.TemplateID, in.Content, in.Props)
	if err != nil {
		return graph.Node{}, err
	}
	nodes = append(nodes, n)
	if err := s.writeNodes(nodes); err != nil {
		return graph.Node{}, err
	}
	return n, nil
}

func (s *FileSession) AddEdge(ctx context.Context, in AddEdgeInput) (graph.Edge, error) {
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
	from, ok := findNode(nodes, in.FromID)
	if !ok {
		return graph.Edge{}, fmt.Errorf("%w: from node not found", s.errors.NotFound)
	}
	to, ok := findNode(nodes, in.ToID)
	if !ok {
		return graph.Edge{}, fmt.Errorf("%w: to node not found", s.errors.NotFound)
	}

	edges, err := s.readEdges()
	if err != nil {
		return graph.Edge{}, err
	}
	if err := s.validateNewEdge(ctx, from, to, in.Kind, edges); err != nil {
		return graph.Edge{}, err
	}
	edgeID := uuid.New()
	if in.ID != nil {
		edgeID = *in.ID
	}
	e := graph.Edge{ID: edgeID, FromID: in.FromID, ToID: in.ToID, Kind: in.Kind, Props: copyProps(in.Props)}
	edges = append(edges, e)
	if err := s.writeEdges(edges); err != nil {
		return graph.Edge{}, err
	}
	return e, nil
}

func (s *FileSession) ListEdges(ctx context.Context) ([]graph.Edge, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return nil, err
	}
	if err := s.ensureSpaceLive(); err != nil {
		return nil, err
	}
	if err := s.ensureRead(); err != nil {
		return nil, err
	}
	edges, err := s.readEdges()
	if err != nil {
		return nil, err
	}
	return cloneEdges(edges), nil
}

func (s *FileSession) AddGraph(ctx context.Context, in AddGraphInput) error {
	_, err := s.ApplyGraph(ctx, ApplyGraphInput{AddNodes: in.Nodes, AddEdges: in.Edges, Atomic: in.Atomic})
	return err
}

func (s *FileSession) ApplyGraph(ctx context.Context, in ApplyGraphInput) (ApplyGraphResult, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return ApplyGraphResult{}, err
	}
	if err := s.ensureSpaceLive(); err != nil {
		return ApplyGraphResult{}, err
	}
	if err := s.ensureWrite(); err != nil {
		return ApplyGraphResult{}, err
	}
	nodes, err := s.readNodes()
	if err != nil {
		return ApplyGraphResult{}, err
	}
	edges, err := s.readEdges()
	if err != nil {
		return ApplyGraphResult{}, err
	}

	candidateNodes := cloneNodes(nodes)
	candidateEdges := cloneEdges(edges)
	result := ApplyGraphResult{}

	for _, del := range in.DeleteNodes {
		deletedIDs, newNodes, newEdges, err := s.applyDeleteNode(candidateNodes, candidateEdges, del)
		if err != nil {
			return ApplyGraphResult{}, err
		}
		candidateNodes = newNodes
		candidateEdges = newEdges
		result.DeletedNodeIDs = append(result.DeletedNodeIDs, deletedIDs...)
	}

	nodeIndex := indexNodes(candidateNodes)
	for _, add := range in.AddNodes {
		nodeID := uuid.New()
		if add.ID != nil {
			nodeID = *add.ID
		}
		if nodeID == uuid.Nil {
			return ApplyGraphResult{}, fmt.Errorf("%w: node_id is required", s.errors.NotFound)
		}
		if _, exists := nodeIndex[nodeID]; exists {
			return ApplyGraphResult{}, fmt.Errorf("%w: duplicate node_id %s", coretemplate.ErrInvalidInput, nodeID)
		}
		node, err := s.buildNode(ctx, candidateNodes, nodeID, add.TemplateID, add.Content, add.Props)
		if err != nil {
			return ApplyGraphResult{}, err
		}
		candidateNodes = append(candidateNodes, node)
		nodeIndex[node.ID] = len(candidateNodes) - 1
		result.AddedNodes = append(result.AddedNodes, node)
	}

	edgeIndex := indexEdges(candidateEdges)
	for _, add := range in.AddEdges {
		edgeID := uuid.New()
		if add.ID != nil {
			edgeID = *add.ID
		}
		if edgeID == uuid.Nil {
			return ApplyGraphResult{}, fmt.Errorf("%w: edge_id is required", s.errors.NotFound)
		}
		if _, exists := edgeIndex[edgeID]; exists {
			return ApplyGraphResult{}, fmt.Errorf("%w: duplicate edge_id %s", coretemplate.ErrInvalidInput, edgeID)
		}
		fromIdx, ok := nodeIndex[add.FromID]
		if !ok {
			return ApplyGraphResult{}, fmt.Errorf("%w: from node not found", s.errors.NotFound)
		}
		toIdx, ok := nodeIndex[add.ToID]
		if !ok {
			return ApplyGraphResult{}, fmt.Errorf("%w: to node not found", s.errors.NotFound)
		}
		from := candidateNodes[fromIdx]
		to := candidateNodes[toIdx]
		if err := s.validateNewEdge(ctx, from, to, add.Kind, candidateEdges); err != nil {
			return ApplyGraphResult{}, err
		}
		edge := graph.Edge{ID: edgeID, FromID: add.FromID, ToID: add.ToID, Kind: add.Kind, Props: copyProps(add.Props)}
		candidateEdges = append(candidateEdges, edge)
		edgeIndex[edge.ID] = len(candidateEdges) - 1
		result.AddedEdges = append(result.AddedEdges, edge)
	}

	if err := s.writeNodes(candidateNodes); err != nil {
		return ApplyGraphResult{}, err
	}
	if err := s.writeEdges(candidateEdges); err != nil {
		return ApplyGraphResult{}, err
	}
	return result, nil
}

func (s *FileSession) GetNode(ctx context.Context, id graph.NodeID) (graph.Node, error) {
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

func (s *FileSession) DeleteNode(ctx context.Context, in DeleteNodeInput) error {
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
	edges, err := s.readEdges()
	if err != nil {
		return err
	}
	deleteIDs := map[graph.NodeID]struct{}{in.ID: {}}
	if in.Recursive {
		changed := true
		for changed {
			changed = false
			for _, edge := range edges {
				if edge.Kind != graph.EdgeKindContains {
					continue
				}
				if _, parentDeleted := deleteIDs[edge.FromID]; parentDeleted {
					if _, already := deleteIDs[edge.ToID]; !already {
						deleteIDs[edge.ToID] = struct{}{}
						changed = true
					}
				}
			}
		}
	} else {
		for _, edge := range edges {
			if edge.Kind == graph.EdgeKindContains && edge.FromID == in.ID {
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

func (s *FileSession) applyDeleteNode(nodes []graph.Node, edges []graph.Edge, in DeleteNodeInput) ([]graph.NodeID, []graph.Node, []graph.Edge, error) {
	if in.ID == uuid.Nil {
		return nil, nil, nil, fmt.Errorf("%w: node_id is required", s.errors.NotFound)
	}
	if _, ok := findNode(nodes, in.ID); !ok {
		return nil, nil, nil, s.errors.NotFound
	}
	deleteIDs := map[graph.NodeID]struct{}{in.ID: {}}
	if in.Recursive {
		changed := true
		for changed {
			changed = false
			for _, edge := range edges {
				if edge.Kind != graph.EdgeKindContains {
					continue
				}
				if _, parentDeleted := deleteIDs[edge.FromID]; parentDeleted {
					if _, already := deleteIDs[edge.ToID]; !already {
						deleteIDs[edge.ToID] = struct{}{}
						changed = true
					}
				}
			}
		}
	} else {
		for _, edge := range edges {
			if edge.Kind == graph.EdgeKindContains && edge.FromID == in.ID {
				if s.errors.Conflict != nil {
					return nil, nil, nil, fmt.Errorf("%w: node has child nodes", s.errors.Conflict)
				}
				return nil, nil, nil, fmt.Errorf("node has child nodes")
			}
		}
	}

	deleted := make([]graph.NodeID, 0, len(deleteIDs))
	remainingNodes := make([]graph.Node, 0, len(nodes))
	for _, n := range nodes {
		if _, isDeleted := deleteIDs[n.ID]; isDeleted {
			deleted = append(deleted, n.ID)
			continue
		}
		remainingNodes = append(remainingNodes, n)
	}
	remainingEdges := make([]graph.Edge, 0, len(edges))
	for _, edge := range edges {
		_, fromDeleted := deleteIDs[edge.FromID]
		_, toDeleted := deleteIDs[edge.ToID]
		if !fromDeleted && !toDeleted {
			remainingEdges = append(remainingEdges, edge)
		}
	}
	return deleted, remainingNodes, remainingEdges, nil
}

func (s *FileSession) Close() error {
	s.closed = true
	return nil
}

func (s *FileSession) ensureOpen(ctx context.Context) error {
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

func (s *FileSession) ensureRead() error {
	if !s.permissions.Read {
		return s.errors.Unauthorized
	}
	return nil
}

func (s *FileSession) ensureWrite() error {
	if !s.permissions.Write {
		return s.errors.Unauthorized
	}
	return nil
}

func (s *FileSession) ensureAdmin() error {
	if !s.permissions.Admin {
		return s.errors.Unauthorized
	}
	return nil
}

func (s *FileSession) spacePath() string {
	return filepath.Join(s.graphsDir, safeID(s.spaceID))
}

func (s *FileSession) nodesPath() string {
	return filepath.Join(s.spacePath(), "nodes.json")
}

func (s *FileSession) edgesPath() string {
	return filepath.Join(s.spacePath(), "edges.json")
}

func (s *FileSession) markerPath() string {
	return filepath.Join(s.spacePath(), ".space")
}

func (s *FileSession) ensureSpaceLive() error {
	if _, err := os.Stat(s.markerPath()); err != nil {
		if os.IsNotExist(err) {
			return s.errors.NotFound
		}
		return err
	}
	return nil
}

func (s *FileSession) readNodes() ([]graph.Node, error) {
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

func (s *FileSession) writeNodes(nodes []graph.Node) error {
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

func (s *FileSession) readEdges() ([]graph.Edge, error) {
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

func (s *FileSession) writeEdges(edges []graph.Edge) error {
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

func (s *FileSession) buildNode(ctx context.Context, nodes []graph.Node, nodeID graph.NodeID, templateID *graph.TemplateID, content string, inputProps map[string]any) (graph.Node, error) {
	if nodeID == uuid.Nil {
		return graph.Node{}, fmt.Errorf("%w: node_id is required", s.errors.NotFound)
	}
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
	}
	return graph.Node{ID: nodeID, TemplateID: templateID, Content: content, Props: props}, nil
}

func (s *FileSession) validateNewEdge(ctx context.Context, from graph.Node, to graph.Node, kind graph.EdgeKind, edges []graph.Edge) error {
	if kind != graph.EdgeKindContains {
		return nil
	}
	if from.ID == to.ID {
		return fmt.Errorf("%w: contains edge cannot target itself", coretemplate.ErrInvalidInput)
	}
	for _, edge := range edges {
		if edge.Kind == graph.EdgeKindContains && edge.ToID == to.ID {
			return fmt.Errorf("%w: node already has a contains parent", coretemplate.ErrInvalidInput)
		}
	}
	if containsPath(edges, to.ID, from.ID) {
		return fmt.Errorf("%w: contains edge would create a cycle", coretemplate.ErrInvalidInput)
	}
	childTemplate, err := s.nodeTemplate(ctx, to, "child")
	if err != nil {
		return err
	}
	return s.validateChild(ctx, from, childTemplate)
}

func (s *FileSession) validateIncidentContains(ctx context.Context, node graph.Node, nodes []graph.Node, edges []graph.Edge) error {
	for _, edge := range edges {
		if edge.Kind != graph.EdgeKindContains {
			continue
		}
		if edge.FromID == node.ID {
			child, ok := findNode(nodes, edge.ToID)
			if !ok {
				continue
			}
			childTemplate, err := s.nodeTemplate(ctx, child, "child")
			if err != nil {
				return err
			}
			if err := s.validateChild(ctx, node, childTemplate); err != nil {
				return err
			}
		}
		if edge.ToID == node.ID {
			parent, ok := findNode(nodes, edge.FromID)
			if !ok {
				continue
			}
			childTemplate, err := s.nodeTemplate(ctx, node, "child")
			if err != nil {
				return err
			}
			if err := s.validateChild(ctx, parent, childTemplate); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *FileSession) nodeTemplate(ctx context.Context, node graph.Node, label string) (*graph.Template, error) {
	if node.TemplateID == nil {
		return nil, nil
	}
	t, err := s.templateManager.GetByID(ctx, *node.TemplateID)
	if err != nil {
		if errors.Is(err, coretemplate.ErrTemplateNotFound) {
			return nil, fmt.Errorf("%w: %s template not found", s.errors.NotFound, label)
		}
		return nil, err
	}
	if t.SpaceID != s.spaceID {
		return nil, fmt.Errorf("%w: %s template not found in space", s.errors.NotFound, label)
	}
	return &t, nil
}

func containsPath(edges []graph.Edge, from graph.NodeID, target graph.NodeID) bool {
	visited := map[graph.NodeID]struct{}{}
	var visit func(graph.NodeID) bool
	visit = func(id graph.NodeID) bool {
		if id == target {
			return true
		}
		if _, ok := visited[id]; ok {
			return false
		}
		visited[id] = struct{}{}
		for _, edge := range edges {
			if edge.Kind == graph.EdgeKindContains && edge.FromID == id {
				if visit(edge.ToID) {
					return true
				}
			}
		}
		return false
	}
	return visit(from)
}

func (s *FileSession) validateChild(ctx context.Context, parent graph.Node, childTemplate *graph.Template) error {
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

func indexNodes(nodes []graph.Node) map[graph.NodeID]int {
	out := make(map[graph.NodeID]int, len(nodes))
	for i, node := range nodes {
		out[node.ID] = i
	}
	return out
}

func indexEdges(edges []graph.Edge) map[graph.EdgeID]int {
	out := make(map[graph.EdgeID]int, len(edges))
	for i, edge := range edges {
		out[edge.ID] = i
	}
	return out
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

func cloneEdges(edges []graph.Edge) []graph.Edge {
	out := make([]graph.Edge, 0, len(edges))
	for _, edge := range edges {
		edge.Props = copyProps(edge.Props)
		out = append(out, edge)
	}
	return out
}

func safeID(id domainspace.SpaceID) string {
	repl := strings.NewReplacer(":", "_", "/", "_", "\\", "_")
	return repl.Replace(id.String())
}
