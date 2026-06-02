package client

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"knot_db/api"
	"knot_db/model"
)

type defaultGraphSession struct {
	dataDir string
	spaceID model.SpaceID
	closed  bool
}

func (s *defaultGraphSession) AddNode(ctx context.Context, in api.NodeInput) (api.Node, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return api.Node{}, err
	}
	nodes, err := s.readNodes()
	if err != nil {
		return api.Node{}, err
	}

	nodeID := uuid.New()
	if in.ID != nil {
		nodeID = *in.ID
	}
	if in.ParentID != nil {
		if _, ok := findNode(nodes, *in.ParentID); !ok {
			return api.Node{}, fmt.Errorf("%w: parent node not found", ErrNotFound)
		}
	}

	n := api.Node{
		ID:         nodeID,
		TemplateID: in.TemplateID,
		ParentID:   in.ParentID,
		Content:    in.Content,
		Props:      in.Props,
	}
	nodes = append(nodes, n)
	if err := s.writeNodes(nodes); err != nil {
		return api.Node{}, err
	}
	return n, nil
}

func (s *defaultGraphSession) AddEdge(ctx context.Context, in api.EdgeInput) (api.Edge, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return api.Edge{}, err
	}
	nodes, err := s.readNodes()
	if err != nil {
		return api.Edge{}, err
	}
	if _, ok := findNode(nodes, in.FromID); !ok {
		return api.Edge{}, fmt.Errorf("%w: from node not found", ErrNotFound)
	}
	if _, ok := findNode(nodes, in.ToID); !ok {
		return api.Edge{}, fmt.Errorf("%w: to node not found", ErrNotFound)
	}

	edges, err := s.readEdges()
	if err != nil {
		return api.Edge{}, err
	}
	edgeID := uuid.New()
	if in.ID != nil {
		edgeID = *in.ID
	}
	e := api.Edge{ID: edgeID, FromID: in.FromID, ToID: in.ToID, Kind: in.Kind, Props: in.Props}
	edges = append(edges, e)
	if err := s.writeEdges(edges); err != nil {
		return api.Edge{}, err
	}
	return e, nil
}

func (s *defaultGraphSession) AddGraph(ctx context.Context, in api.GraphInput) error {
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

func (s *defaultGraphSession) GetNode(ctx context.Context, id api.NodeID) (api.Node, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return api.Node{}, err
	}
	nodes, err := s.readNodes()
	if err != nil {
		return api.Node{}, err
	}
	n, ok := findNode(nodes, id)
	if !ok {
		return api.Node{}, ErrNotFound
	}
	return n, nil
}

func (s *defaultGraphSession) Close() error {
	s.closed = true
	return nil
}

func (s *defaultGraphSession) ensureOpen(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if s.closed {
		return ErrClosed
	}
	return nil
}

func (s *defaultGraphSession) nodesPath() string {
	return filepath.Join(s.dataDir, "nodes_"+safeID(s.spaceID)+".json")
}

func (s *defaultGraphSession) edgesPath() string {
	return filepath.Join(s.dataDir, "edges_"+safeID(s.spaceID)+".json")
}

func (s *defaultGraphSession) readNodes() ([]api.Node, error) {
	path := s.nodesPath()
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return []api.Node{}, nil
		}
		return nil, err
	}
	var out []api.Node
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *defaultGraphSession) writeNodes(nodes []api.Node) error {
	b, err := json.MarshalIndent(nodes, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(s.nodesPath(), b, 0o600)
}

func (s *defaultGraphSession) readEdges() ([]api.Edge, error) {
	path := s.edgesPath()
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return []api.Edge{}, nil
		}
		return nil, err
	}
	var out []api.Edge
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *defaultGraphSession) writeEdges(edges []api.Edge) error {
	b, err := json.MarshalIndent(edges, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(s.edgesPath(), b, 0o600)
}

func findNode(nodes []api.Node, id api.NodeID) (api.Node, bool) {
	for _, n := range nodes {
		if n.ID == id {
			return n, true
		}
	}
	return api.Node{}, false
}

func safeID(id model.SpaceID) string {
	repl := strings.NewReplacer(":", "_", "/", "_", "\\", "_")
	return repl.Replace(id.String())
}
