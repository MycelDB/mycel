package session

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/google/uuid"
	coretemplate "martinbeauvais.com/mbgit/knotbase/knotdb/core/template"
	"martinbeauvais.com/mbgit/knotbase/knotdb/domain/graph"
)

// MoveSubtree rewires the incoming contains edge for a node so the node and
// all of its descendants are contained by a new parent.
func (s *FileSession) MoveSubtree(ctx context.Context, in MoveSubtreeInput) (graph.Edge, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return graph.Edge{}, err
	}
	if err := s.ensureSpaceLive(); err != nil {
		return graph.Edge{}, err
	}
	if err := s.ensureWrite(); err != nil {
		return graph.Edge{}, err
	}
	if in.NodeID == uuid.Nil {
		return graph.Edge{}, fmt.Errorf("%w: node_id is required", s.errors.NotFound)
	}
	if in.NewParentID == uuid.Nil {
		return graph.Edge{}, fmt.Errorf("%w: new_parent_id is required", s.errors.NotFound)
	}
	if in.Order != nil && *in.Order < 0 {
		return graph.Edge{}, fmt.Errorf("%w: order must be non-negative", coretemplate.ErrInvalidInput)
	}

	nodes, err := s.readNodes()
	if err != nil {
		return graph.Edge{}, err
	}
	node, ok := findNode(nodes, in.NodeID)
	if !ok {
		return graph.Edge{}, fmt.Errorf("%w: node not found", s.errors.NotFound)
	}
	newParent, ok := findNode(nodes, in.NewParentID)
	if !ok {
		return graph.Edge{}, fmt.Errorf("%w: new parent not found", s.errors.NotFound)
	}
	if in.NodeID == in.NewParentID {
		return graph.Edge{}, fmt.Errorf("%w: cannot move a node under itself", coretemplate.ErrInvalidInput)
	}

	edges, err := s.readEdges()
	if err != nil {
		return graph.Edge{}, err
	}
	oldParentIndexes := containsParentEdgeIndexes(edges, in.NodeID)
	if len(oldParentIndexes) > 1 {
		return graph.Edge{}, fmt.Errorf("%w: node has multiple contains parents", coretemplate.ErrInvalidInput)
	}
	if containsPath(edges, in.NodeID, in.NewParentID) {
		return graph.Edge{}, fmt.Errorf("%w: move would create a contains cycle", coretemplate.ErrInvalidInput)
	}
	childTemplate, err := s.nodeTemplate(ctx, node, "child")
	if err != nil {
		return graph.Edge{}, err
	}
	if err := s.validateChild(ctx, newParent, childTemplate); err != nil {
		return graph.Edge{}, err
	}

	oldParentID := graph.NodeID(uuid.Nil)
	if len(oldParentIndexes) == 1 {
		oldEdge := edges[oldParentIndexes[0]]
		oldParentID = oldEdge.FromID
		if oldParentID == in.NewParentID {
			if in.Order == nil {
				return cloneEdge(oldEdge), nil
			}
			updated, err := setChildPosition(edges, in.NewParentID, in.NodeID, in.Order)
			if err != nil {
				return graph.Edge{}, err
			}
			if err := s.writeEdges(edges); err != nil {
				return graph.Edge{}, err
			}
			return updated, nil
		}

		edges[oldParentIndexes[0]].FromID = in.NewParentID
		edges[oldParentIndexes[0]].ToID = in.NodeID
		edges[oldParentIndexes[0]].Kind = graph.EdgeKindContains
		edges[oldParentIndexes[0]].Props = copyProps(edges[oldParentIndexes[0]].Props)
	} else {
		edges = append(edges, graph.Edge{ID: graph.EdgeID(uuid.New()), FromID: in.NewParentID, ToID: in.NodeID, Kind: graph.EdgeKindContains, Props: map[string]any{}})
	}

	if oldParentID != uuid.Nil {
		normalizeChildrenOrder(edges, oldParentID)
	}
	updated, err := setChildPosition(edges, in.NewParentID, in.NodeID, in.Order)
	if err != nil {
		return graph.Edge{}, err
	}
	if err := s.writeEdges(edges); err != nil {
		return graph.Edge{}, err
	}
	return updated, nil
}

// ReorderChildren rewrites contains edge order properties for all direct
// children of a parent. The caller must provide the complete child list.
func (s *FileSession) ReorderChildren(ctx context.Context, in ReorderChildrenInput) ([]graph.Edge, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return nil, err
	}
	if err := s.ensureSpaceLive(); err != nil {
		return nil, err
	}
	if err := s.ensureWrite(); err != nil {
		return nil, err
	}
	if in.ParentID == uuid.Nil {
		return nil, fmt.Errorf("%w: parent_id is required", s.errors.NotFound)
	}

	nodes, err := s.readNodes()
	if err != nil {
		return nil, err
	}
	if _, ok := findNode(nodes, in.ParentID); !ok {
		return nil, fmt.Errorf("%w: parent not found", s.errors.NotFound)
	}
	edges, err := s.readEdges()
	if err != nil {
		return nil, err
	}
	childEdgeByID, err := validateCompleteChildOrder(edges, in.ParentID, in.ChildIDs)
	if err != nil {
		return nil, err
	}

	updated := make([]graph.Edge, 0, len(in.ChildIDs))
	for order, childID := range in.ChildIDs {
		edgeIndex := childEdgeByID[childID]
		ensureEdgeProps(&edges[edgeIndex])
		edges[edgeIndex].Props["order"] = order
		updated = append(updated, cloneEdge(edges[edgeIndex]))
	}
	if err := s.writeEdges(edges); err != nil {
		return nil, err
	}
	return updated, nil
}

func containsParentEdgeIndexes(edges []graph.Edge, childID graph.NodeID) []int {
	indexes := []int{}
	for i, edge := range edges {
		if edge.Kind == graph.EdgeKindContains && edge.ToID == childID {
			indexes = append(indexes, i)
		}
	}
	return indexes
}

func orderedContainsEdgeIndexes(edges []graph.Edge, parentID graph.NodeID) []int {
	indexes := []int{}
	for i, edge := range edges {
		if edge.Kind == graph.EdgeKindContains && edge.FromID == parentID {
			indexes = append(indexes, i)
		}
	}
	sort.SliceStable(indexes, func(i, j int) bool {
		left, leftOK := edgeOrderNumber(edges[indexes[i]])
		right, rightOK := edgeOrderNumber(edges[indexes[j]])
		if leftOK && rightOK && left != right {
			return left < right
		}
		if leftOK != rightOK {
			return leftOK
		}
		return indexes[i] < indexes[j]
	})
	return indexes
}

func edgeOrderNumber(edge graph.Edge) (float64, bool) {
	if edge.Props == nil {
		return 0, false
	}
	switch v := edge.Props["order"].(type) {
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	case json.Number:
		f, err := v.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}

func normalizeChildrenOrder(edges []graph.Edge, parentID graph.NodeID) {
	for order, edgeIndex := range orderedContainsEdgeIndexes(edges, parentID) {
		ensureEdgeProps(&edges[edgeIndex])
		edges[edgeIndex].Props["order"] = order
	}
}

func setChildPosition(edges []graph.Edge, parentID graph.NodeID, childID graph.NodeID, order *int) (graph.Edge, error) {
	indexes := orderedContainsEdgeIndexes(edges, parentID)
	currentPos := -1
	for i, edgeIndex := range indexes {
		if edges[edgeIndex].ToID == childID {
			currentPos = i
			break
		}
	}
	if currentPos < 0 {
		return graph.Edge{}, fmt.Errorf("%w: child is not contained by parent", coretemplate.ErrInvalidInput)
	}
	movedIndex := indexes[currentPos]
	indexes = append(indexes[:currentPos], indexes[currentPos+1:]...)
	target := len(indexes)
	if order != nil && *order < target {
		target = *order
	}
	indexes = append(indexes, 0)
	copy(indexes[target+1:], indexes[target:])
	indexes[target] = movedIndex
	for pos, edgeIndex := range indexes {
		ensureEdgeProps(&edges[edgeIndex])
		edges[edgeIndex].Props["order"] = pos
	}
	return cloneEdge(edges[movedIndex]), nil
}

func validateCompleteChildOrder(edges []graph.Edge, parentID graph.NodeID, childIDs []graph.NodeID) (map[graph.NodeID]int, error) {
	childEdgeByID := map[graph.NodeID]int{}
	for _, edgeIndex := range orderedContainsEdgeIndexes(edges, parentID) {
		childID := edges[edgeIndex].ToID
		if _, exists := childEdgeByID[childID]; exists {
			return nil, fmt.Errorf("%w: duplicate contains child %s", coretemplate.ErrInvalidInput, childID)
		}
		childEdgeByID[childID] = edgeIndex
	}
	if len(childIDs) != len(childEdgeByID) {
		return nil, fmt.Errorf("%w: child_ids must include exactly all children", coretemplate.ErrInvalidInput)
	}
	seen := map[graph.NodeID]struct{}{}
	for _, childID := range childIDs {
		if childID == uuid.Nil {
			return nil, fmt.Errorf("%w: child_id is required", coretemplate.ErrInvalidInput)
		}
		if _, duplicate := seen[childID]; duplicate {
			return nil, fmt.Errorf("%w: duplicate child_id %s", coretemplate.ErrInvalidInput, childID)
		}
		seen[childID] = struct{}{}
		if _, ok := childEdgeByID[childID]; !ok {
			return nil, fmt.Errorf("%w: child_id %s is not contained by parent", coretemplate.ErrInvalidInput, childID)
		}
	}
	return childEdgeByID, nil
}

func ensureEdgeProps(edge *graph.Edge) {
	if edge.Props == nil {
		edge.Props = map[string]any{}
	}
}

func cloneEdge(edge graph.Edge) graph.Edge {
	edge.Props = copyProps(edge.Props)
	return edge
}
