package graphstore

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"martinbeauvais.com/mbgit/knotbase/knotdb/graph"
	"martinbeauvais.com/mbgit/knotbase/knotdb/model"
)

// Errors defines public errors returned by sessions.
type Errors struct {
	Closed   error
	NotFound error
}

type session struct {
	dataDir string
	spaceID model.SpaceID
	errors  Errors
	closed  bool
}

// NewSession opens a file-backed graph session for a space.
func NewSession(dataDir string, spaceID model.SpaceID, errs Errors) graph.Session {
	return &session{dataDir: dataDir, spaceID: spaceID, errors: errs}
}

func (s *session) AddNode(ctx context.Context, in graph.NodeInput) (graph.Node, error) {
	if err := s.ensureOpen(ctx); err != nil {
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
	if in.ParentID != nil {
		if _, ok := findNode(nodes, *in.ParentID); !ok {
			return graph.Node{}, fmt.Errorf("%w: parent node not found", s.errors.NotFound)
		}
	}

	n := graph.Node{
		ID:         nodeID,
		TemplateID: in.TemplateID,
		ParentID:   in.ParentID,
		Content:    in.Content,
		Props:      in.Props,
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

func (s *session) nodesPath() string {
	return filepath.Join(s.dataDir, "nodes_"+safeID(s.spaceID)+".json")
}

func (s *session) edgesPath() string {
	return filepath.Join(s.dataDir, "edges_"+safeID(s.spaceID)+".json")
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
	b, err := json.MarshalIndent(edges, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(s.edgesPath(), b, 0o600)
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
