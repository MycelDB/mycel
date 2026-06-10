package filesession

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/google/uuid"
	"martinbeauvais.com/mbgit/knotbase/knotdb/domain/graph"
	domainspace "martinbeauvais.com/mbgit/knotbase/knotdb/domain/space"
	"martinbeauvais.com/mbgit/knotbase/knotdb/internal/graphstorage"
	"martinbeauvais.com/mbgit/knotbase/knotdb/query"
	sessionapi "martinbeauvais.com/mbgit/knotbase/knotdb/session/api"
	storetemplate "martinbeauvais.com/mbgit/knotbase/knotdb/store/template"
)

// New opens the default file-backed session implementation.
func New(graphsDir string, spaceID domainspace.SpaceID, templateManager storetemplate.Manager, permissions sessionapi.Permissions, errs sessionapi.Errors) sessionapi.Session {
	return &FileSession{graphsDir: graphsDir, spaceID: spaceID, templateManager: templateManager, permissions: permissions, errors: errs}
}

// FileSession is the default file-backed Session implementation.
type FileSession struct {
	graphsDir       string
	spaceID         domainspace.SpaceID
	templateManager storetemplate.Manager
	permissions     sessionapi.Permissions
	errors          sessionapi.Errors
	store           *graphstorage.LocalStore
	closed          bool
}

// Query starts a programmatic graph query over this session.
func (s *FileSession) Query() *query.Builder { return query.NewBuilder(s) }

func (s *FileSession) ImportTemplates(ctx context.Context, in sessionapi.ImportTemplatesInput) ([]graph.Template, error) {
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

func (s *FileSession) AddNode(ctx context.Context, in sessionapi.AddNodeInput) (graph.Node, error) {
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
	nodeID, err := newGraphUUID()
	if err != nil {
		return graph.Node{}, err
	}
	if in.ID != nil {
		nodeID = *in.ID
	}
	n, err := s.buildNode(ctx, nodes, nodeID, in.TemplateID, in.Content, in.Props)
	if err != nil {
		return graph.Node{}, err
	}
	now := time.Now().UTC()
	n.CreatedAt = now
	n.UpdatedAt = now
	if err := s.commitGraph(ctx, []graph.Node{n}, nil, nil, nil); err != nil {
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

func (s *FileSession) UpdateNode(ctx context.Context, in sessionapi.UpdateNodeInput) (graph.Node, error) {
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
	n.CreatedAt = nodes[idx].CreatedAt
	if n.CreatedAt.IsZero() {
		n.CreatedAt = time.Now().UTC()
	}
	n.UpdatedAt = time.Now().UTC()
	candidateNodes := append([]graph.Node(nil), nodes...)
	candidateNodes[idx] = n
	edges, err := s.readEdges()
	if err != nil {
		return graph.Node{}, err
	}
	if err := s.validateIncidentContains(ctx, n, candidateNodes, edges); err != nil {
		return graph.Node{}, err
	}
	if err := s.commitGraph(ctx, []graph.Node{n}, nil, nil, nil); err != nil {
		return graph.Node{}, err
	}
	return n, nil
}

func (s *FileSession) UpsertNode(ctx context.Context, in sessionapi.UpsertNodeInput) (graph.Node, error) {
	if in.ID == nil {
		return s.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: in.TemplateID, Content: in.Content, Props: in.Props})
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
		return s.UpdateNode(ctx, sessionapi.UpdateNodeInput{ID: *in.ID, TemplateID: in.TemplateID, Content: in.Content, Props: in.Props})
	}
	n, err := s.buildNode(ctx, nodes, *in.ID, in.TemplateID, in.Content, in.Props)
	if err != nil {
		return graph.Node{}, err
	}
	now := time.Now().UTC()
	n.CreatedAt = now
	n.UpdatedAt = now
	if err := s.commitGraph(ctx, []graph.Node{n}, nil, nil, nil); err != nil {
		return graph.Node{}, err
	}
	return n, nil
}

func (s *FileSession) AddEdge(ctx context.Context, in sessionapi.AddEdgeInput) (graph.Edge, error) {
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
	edgeID, err := newGraphUUID()
	if err != nil {
		return graph.Edge{}, err
	}
	if in.ID != nil {
		edgeID = *in.ID
	}
	e := graph.Edge{ID: edgeID, FromID: in.FromID, ToID: in.ToID, Kind: in.Kind, Props: copyProps(in.Props)}
	if err := s.commitGraph(ctx, nil, []graph.Edge{e}, nil, nil); err != nil {
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

func (s *FileSession) AddGraph(ctx context.Context, in sessionapi.AddGraphInput) error {
	_, err := s.ApplyGraph(ctx, sessionapi.ApplyGraphInput{AddNodes: in.Nodes, AddEdges: in.Edges, Atomic: in.Atomic})
	return err
}

func (s *FileSession) ApplyGraph(ctx context.Context, in sessionapi.ApplyGraphInput) (sessionapi.ApplyGraphResult, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return sessionapi.ApplyGraphResult{}, err
	}
	if err := s.ensureSpaceLive(); err != nil {
		return sessionapi.ApplyGraphResult{}, err
	}
	if err := s.ensureWrite(); err != nil {
		return sessionapi.ApplyGraphResult{}, err
	}
	nodes, err := s.readNodes()
	if err != nil {
		return sessionapi.ApplyGraphResult{}, err
	}
	edges, err := s.readEdges()
	if err != nil {
		return sessionapi.ApplyGraphResult{}, err
	}

	candidateNodes := cloneNodes(nodes)
	candidateEdges := cloneEdges(edges)
	result := sessionapi.ApplyGraphResult{}

	for _, del := range in.DeleteNodes {
		deletedIDs, newNodes, newEdges, err := s.applyDeleteNode(candidateNodes, candidateEdges, del)
		if err != nil {
			return sessionapi.ApplyGraphResult{}, err
		}
		candidateNodes = newNodes
		candidateEdges = newEdges
		result.DeletedNodeIDs = append(result.DeletedNodeIDs, deletedIDs...)
	}

	nodeIndex := indexNodes(candidateNodes)
	for _, add := range in.AddNodes {
		nodeID, err := newGraphUUID()
		if err != nil {
			return sessionapi.ApplyGraphResult{}, err
		}
		if add.ID != nil {
			nodeID = *add.ID
		}
		if nodeID == uuid.Nil {
			return sessionapi.ApplyGraphResult{}, fmt.Errorf("%w: node_id is required", s.errors.NotFound)
		}
		if _, exists := nodeIndex[nodeID]; exists {
			return sessionapi.ApplyGraphResult{}, fmt.Errorf("%w: duplicate node_id %s", storetemplate.ErrInvalidInput, nodeID)
		}
		node, err := s.buildNode(ctx, candidateNodes, nodeID, add.TemplateID, add.Content, add.Props)
		if err != nil {
			return sessionapi.ApplyGraphResult{}, err
		}
		now := time.Now().UTC()
		node.CreatedAt = now
		node.UpdatedAt = now
		candidateNodes = append(candidateNodes, node)
		nodeIndex[node.ID] = len(candidateNodes) - 1
		result.AddedNodes = append(result.AddedNodes, node)
	}

	edgeIndex := indexEdges(candidateEdges)
	for _, add := range in.AddEdges {
		edgeID, err := newGraphUUID()
		if err != nil {
			return sessionapi.ApplyGraphResult{}, err
		}
		if add.ID != nil {
			edgeID = *add.ID
		}
		if edgeID == uuid.Nil {
			return sessionapi.ApplyGraphResult{}, fmt.Errorf("%w: edge_id is required", s.errors.NotFound)
		}
		if _, exists := edgeIndex[edgeID]; exists {
			return sessionapi.ApplyGraphResult{}, fmt.Errorf("%w: duplicate edge_id %s", storetemplate.ErrInvalidInput, edgeID)
		}
		fromIdx, ok := nodeIndex[add.FromID]
		if !ok {
			return sessionapi.ApplyGraphResult{}, fmt.Errorf("%w: from node not found", s.errors.NotFound)
		}
		toIdx, ok := nodeIndex[add.ToID]
		if !ok {
			return sessionapi.ApplyGraphResult{}, fmt.Errorf("%w: to node not found", s.errors.NotFound)
		}
		from := candidateNodes[fromIdx]
		to := candidateNodes[toIdx]
		if err := s.validateNewEdge(ctx, from, to, add.Kind, candidateEdges); err != nil {
			return sessionapi.ApplyGraphResult{}, err
		}
		edge := graph.Edge{ID: edgeID, FromID: add.FromID, ToID: add.ToID, Kind: add.Kind, Props: copyProps(add.Props)}
		candidateEdges = append(candidateEdges, edge)
		edgeIndex[edge.ID] = len(candidateEdges) - 1
		result.AddedEdges = append(result.AddedEdges, edge)
	}

	deletedEdgeIDs := deletedEdges(edges, candidateEdges)
	if err := s.commitGraph(ctx, result.AddedNodes, result.AddedEdges, result.DeletedNodeIDs, deletedEdgeIDs); err != nil {
		return sessionapi.ApplyGraphResult{}, err
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

func (s *FileSession) DeleteNode(ctx context.Context, in sessionapi.DeleteNodeInput) error {
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
	deletedEdges := deletedEdges(edges, remainingEdges)
	return s.commitGraph(ctx, nil, nil, mapKeys(deleteIDs), deletedEdges)
}

func (s *FileSession) applyDeleteNode(nodes []graph.Node, edges []graph.Edge, in sessionapi.DeleteNodeInput) ([]graph.NodeID, []graph.Node, []graph.Edge, error) {
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
	if s.store != nil {
		return s.store.Close()
	}
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

func (s *FileSession) graphStore() (*graphstorage.LocalStore, error) {
	if s.store != nil {
		return s.store, nil
	}
	store, err := graphstorage.Open(context.Background(), s.spacePath())
	if err != nil {
		return nil, err
	}
	s.store = store
	return s.store, nil
}

func (s *FileSession) readNodes() ([]graph.Node, error) {
	store, err := s.graphStore()
	if err != nil {
		return nil, err
	}
	return store.ListNodes(context.Background())
}

func (s *FileSession) readEdges() ([]graph.Edge, error) {
	store, err := s.graphStore()
	if err != nil {
		return nil, err
	}
	return store.ListEdges(context.Background())
}

func (s *FileSession) commitGraph(ctx context.Context, putNodes []graph.Node, putEdges []graph.Edge, deleteNodes []graph.NodeID, deleteEdges []graph.EdgeID) error {
	store, err := s.graphStore()
	if err != nil {
		return err
	}
	txn, err := store.Begin(ctx)
	if err != nil {
		return err
	}
	for _, node := range putNodes {
		if err := txn.PutNode(node); err != nil {
			_ = txn.Rollback()
			return err
		}
	}
	for _, edge := range putEdges {
		if err := txn.PutEdge(edge); err != nil {
			_ = txn.Rollback()
			return err
		}
	}
	for _, id := range deleteEdges {
		if err := txn.DeleteEdge(id); err != nil {
			_ = txn.Rollback()
			return err
		}
	}
	for _, id := range deleteNodes {
		if err := txn.DeleteNode(id); err != nil {
			_ = txn.Rollback()
			return err
		}
	}
	return txn.Commit()
}

func (s *FileSession) buildNode(ctx context.Context, nodes []graph.Node, nodeID graph.NodeID, templateID *graph.TemplateID, content string, inputProps map[string]any) (graph.Node, error) {
	if nodeID == uuid.Nil {
		return graph.Node{}, fmt.Errorf("%w: node_id is required", s.errors.NotFound)
	}
	props := copyProps(inputProps)
	if templateID != nil {
		t, err := s.templateManager.GetByID(ctx, *templateID)
		if err != nil {
			if errors.Is(err, storetemplate.ErrTemplateNotFound) {
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
		return fmt.Errorf("%w: contains edge cannot target itself", storetemplate.ErrInvalidInput)
	}
	for _, edge := range edges {
		if edge.Kind == graph.EdgeKindContains && edge.ToID == to.ID {
			return fmt.Errorf("%w: node already has a contains parent", storetemplate.ErrInvalidInput)
		}
	}
	if containsPath(edges, to.ID, from.ID) {
		return fmt.Errorf("%w: contains edge would create a cycle", storetemplate.ErrInvalidInput)
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
		if errors.Is(err, storetemplate.ErrTemplateNotFound) {
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
		if errors.Is(err, storetemplate.ErrTemplateNotFound) {
			return fmt.Errorf("%w: parent template not found", s.errors.NotFound)
		}
		return err
	}
	if parentTemplate.SpaceID != s.spaceID {
		return fmt.Errorf("%w: parent template not found in space", s.errors.NotFound)
	}
	if !parentTemplate.Children.Allowed {
		return fmt.Errorf("%w: parent template does not allow children", storetemplate.ErrInvalidInput)
	}
	if len(parentTemplate.Children.AllowedTemplates) == 0 {
		return nil
	}
	if childTemplate == nil {
		return fmt.Errorf("%w: child template is required", storetemplate.ErrInvalidInput)
	}
	for _, ref := range parentTemplate.Children.AllowedTemplates {
		if ref.Key == childTemplate.Key && ref.Version == childTemplate.Version {
			return nil
		}
	}
	return fmt.Errorf("%w: child template %s@%s is not allowed", storetemplate.ErrInvalidInput, childTemplate.Key, childTemplate.Version)
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
			return fmt.Errorf("%w: property %q is forbidden", storetemplate.ErrInvalidInput, name)
		}
	}
	for name, value := range *props {
		prop, ok := allowed[name]
		if !ok {
			if !tmpl.Properties.AllowExtra {
				return fmt.Errorf("%w: property %q is not allowed", storetemplate.ErrInvalidInput, name)
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
			return fmt.Errorf("%w: required property %q is missing", storetemplate.ErrInvalidInput, prop.Name)
		}
	}
	return nil
}

func validatePropertyValue(prop graph.TemplateProperty, value any) error {
	if value == nil {
		return fmt.Errorf("%w: property %q cannot be null", storetemplate.ErrInvalidInput, prop.Name)
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
		return fmt.Errorf("%w: unsupported property type %q", storetemplate.ErrInvalidInput, prop.Type)
	}
	if !valid {
		return fmt.Errorf("%w: property %q must be %s", storetemplate.ErrInvalidInput, prop.Name, prop.Type)
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

func newGraphUUID() (uuid.UUID, error) {
	return graphstorage.NewUUIDv7()
}

func changedEdges(original []graph.Edge, candidate []graph.Edge) []graph.Edge {
	byID := map[graph.EdgeID]graph.Edge{}
	for _, edge := range original {
		byID[edge.ID] = edge
	}
	out := []graph.Edge{}
	for _, edge := range candidate {
		old, ok := byID[edge.ID]
		if !ok || !edgesEqual(old, edge) {
			out = append(out, edge)
		}
	}
	return out
}

func edgesEqual(left graph.Edge, right graph.Edge) bool {
	return left.ID == right.ID && left.FromID == right.FromID && left.ToID == right.ToID && left.Kind == right.Kind && reflect.DeepEqual(left.Props, right.Props)
}

func deletedEdges(original []graph.Edge, remaining []graph.Edge) []graph.EdgeID {
	live := map[graph.EdgeID]struct{}{}
	for _, edge := range remaining {
		live[edge.ID] = struct{}{}
	}
	out := []graph.EdgeID{}
	for _, edge := range original {
		if _, ok := live[edge.ID]; !ok {
			out = append(out, edge.ID)
		}
	}
	return out
}

func mapKeys(m map[graph.NodeID]struct{}) []graph.NodeID {
	out := make([]graph.NodeID, 0, len(m))
	for id := range m {
		out = append(out, id)
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
