package filesession

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/graph/model"
	sessionapi "github.com/myceldb/mycel/internal/session/api"
)

const childOrderStep = 1024

// MoveSubtree rewires the incoming contains edge for a node so the node and
// all of its descendants are contained by a new parent.
func (s *FileSession) MoveSubtree(ctx context.Context, in sessionapi.MoveSubtreeInput) (graph.Edge, error) {
	var out graph.Edge
	err := s.Tx(ctx, sessionapi.TxOptions{}, func(tx sessionapi.Tx) error {
		var err error
		out, err = tx.MoveSubtree(ctx, in)
		return err
	})
	return out, err
}

func (s *FileSession) ReorderChildren(ctx context.Context, in sessionapi.ReorderChildrenInput) ([]graph.Edge, error) {
	var out []graph.Edge
	err := s.Tx(ctx, sessionapi.TxOptions{}, func(tx sessionapi.Tx) error {
		var err error
		out, err = tx.ReorderChildren(ctx, in)
		return err
	})
	return out, err
}

func (s *FileSession) hierarchyParentEdgeIndexes(ctx context.Context, edges []graph.Edge, childID graph.NodeID) ([]int, error) {
	indexes := []int{}
	for i, edge := range edges {
		isHierarchy, err := s.isHierarchyEdge(ctx, edge)
		if err != nil {
			return nil, err
		}
		if isHierarchy && edge.ToID == childID {
			indexes = append(indexes, i)
		}
	}
	return indexes, nil
}

func (s *FileSession) orderedHierarchyEdgeIndexes(ctx context.Context, edges []graph.Edge, parentID graph.NodeID) ([]int, error) {
	indexes := []int{}
	for i, edge := range edges {
		isHierarchy, err := s.isHierarchyEdge(ctx, edge)
		if err != nil {
			return nil, err
		}
		if isHierarchy && edge.FromID == parentID {
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
	return indexes, nil
}

func edgeOrderNumber(edge graph.Edge) (float64, bool) {
	if edge.Properties == nil {
		return 0, false
	}
	switch v := edge.Properties["order"].(type) {
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

func (s *FileSession) normalizeChildrenOrder(ctx context.Context, edges []graph.Edge, parentID graph.NodeID) error {
	indexes, err := s.orderedHierarchyEdgeIndexes(ctx, edges, parentID)
	if err != nil {
		return err
	}
	for order, edgeIndex := range indexes {
		ensureEdgeProps(&edges[edgeIndex])
		edges[edgeIndex].Properties["order"] = order * childOrderStep
	}
	return nil
}

func (s *FileSession) childIDsInOrder(ctx context.Context, edges []graph.Edge, parentID graph.NodeID) ([]graph.NodeID, error) {
	indexes, err := s.orderedHierarchyEdgeIndexes(ctx, edges, parentID)
	if err != nil {
		return nil, err
	}
	out := make([]graph.NodeID, 0, len(indexes))
	for _, edgeIndex := range indexes {
		out = append(out, edges[edgeIndex].ToID)
	}
	return out, nil
}

func (s *FileSession) setCompleteChildOrder(ctx context.Context, edges []graph.Edge, parentID graph.NodeID, childIDs []graph.NodeID) error {
	childEdgeByID, err := s.validateCompleteChildOrder(ctx, edges, parentID, childIDs)
	if err != nil {
		return err
	}
	for order, childID := range childIDs {
		edgeIndex := childEdgeByID[childID]
		ensureEdgeProps(&edges[edgeIndex])
		edges[edgeIndex].Properties["order"] = order * childOrderStep
	}
	return nil
}

func (s *FileSession) setChildPosition(ctx context.Context, edges []graph.Edge, parentID graph.NodeID, childID graph.NodeID, order *int) (graph.Edge, error) {
	indexes, err := s.orderedHierarchyEdgeIndexes(ctx, edges, parentID)
	if err != nil {
		return graph.Edge{}, err
	}
	currentPos := -1
	for i, edgeIndex := range indexes {
		if edges[edgeIndex].ToID == childID {
			currentPos = i
			break
		}
	}
	if currentPos < 0 {
		return graph.Edge{}, fmt.Errorf("%w: child is not contained by parent", errInvalidInput)
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
		edges[edgeIndex].Properties["order"] = pos * childOrderStep
	}
	return cloneEdge(edges[movedIndex]), nil
}

func (s *FileSession) validateCompleteChildOrder(ctx context.Context, edges []graph.Edge, parentID graph.NodeID, childIDs []graph.NodeID) (map[graph.NodeID]int, error) {
	childEdgeByID := map[graph.NodeID]int{}
	indexes, err := s.orderedHierarchyEdgeIndexes(ctx, edges, parentID)
	if err != nil {
		return nil, err
	}
	for _, edgeIndex := range indexes {
		childID := edges[edgeIndex].ToID
		if _, exists := childEdgeByID[childID]; exists {
			return nil, fmt.Errorf("%w: duplicate contains child %s", errInvalidInput, childID)
		}
		childEdgeByID[childID] = edgeIndex
	}
	if len(childIDs) != len(childEdgeByID) {
		return nil, fmt.Errorf("%w: child_ids must include exactly all children", errInvalidInput)
	}
	seen := map[graph.NodeID]struct{}{}
	for _, childID := range childIDs {
		if childID == uuid.Nil {
			return nil, fmt.Errorf("%w: child_id is required", errInvalidInput)
		}
		if _, duplicate := seen[childID]; duplicate {
			return nil, fmt.Errorf("%w: duplicate child_id %s", errInvalidInput, childID)
		}
		seen[childID] = struct{}{}
		if _, ok := childEdgeByID[childID]; !ok {
			return nil, fmt.Errorf("%w: child_id %s is not contained by parent", errInvalidInput, childID)
		}
	}
	return childEdgeByID, nil
}

func ensureEdgeProps(edge *graph.Edge) {
	if edge.Properties == nil {
		edge.Properties = map[string]any{}
	}
}

func cloneEdge(edge graph.Edge) graph.Edge {
	edge.Properties = copyProps(edge.Properties)
	return edge
}
