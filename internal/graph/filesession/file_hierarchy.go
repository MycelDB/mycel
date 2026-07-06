package filesession

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/graph/model"
	storetemplate "github.com/myceldb/mycel/internal/graph/template/storage"
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
		edges[edgeIndex].Props["order"] = order * childOrderStep
	}
}

func childIDsInOrder(edges []graph.Edge, parentID graph.NodeID) []graph.NodeID {
	indexes := orderedContainsEdgeIndexes(edges, parentID)
	out := make([]graph.NodeID, 0, len(indexes))
	for _, edgeIndex := range indexes {
		out = append(out, edges[edgeIndex].ToID)
	}
	return out
}

func setCompleteChildOrder(edges []graph.Edge, parentID graph.NodeID, childIDs []graph.NodeID) error {
	childEdgeByID, err := validateCompleteChildOrder(edges, parentID, childIDs)
	if err != nil {
		return err
	}
	for order, childID := range childIDs {
		edgeIndex := childEdgeByID[childID]
		ensureEdgeProps(&edges[edgeIndex])
		edges[edgeIndex].Props["order"] = order * childOrderStep
	}
	return nil
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
		return graph.Edge{}, fmt.Errorf("%w: child is not contained by parent", storetemplate.ErrInvalidInput)
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
		edges[edgeIndex].Props["order"] = pos * childOrderStep
	}
	return cloneEdge(edges[movedIndex]), nil
}

func validateCompleteChildOrder(edges []graph.Edge, parentID graph.NodeID, childIDs []graph.NodeID) (map[graph.NodeID]int, error) {
	childEdgeByID := map[graph.NodeID]int{}
	for _, edgeIndex := range orderedContainsEdgeIndexes(edges, parentID) {
		childID := edges[edgeIndex].ToID
		if _, exists := childEdgeByID[childID]; exists {
			return nil, fmt.Errorf("%w: duplicate contains child %s", storetemplate.ErrInvalidInput, childID)
		}
		childEdgeByID[childID] = edgeIndex
	}
	if len(childIDs) != len(childEdgeByID) {
		return nil, fmt.Errorf("%w: child_ids must include exactly all children", storetemplate.ErrInvalidInput)
	}
	seen := map[graph.NodeID]struct{}{}
	for _, childID := range childIDs {
		if childID == uuid.Nil {
			return nil, fmt.Errorf("%w: child_id is required", storetemplate.ErrInvalidInput)
		}
		if _, duplicate := seen[childID]; duplicate {
			return nil, fmt.Errorf("%w: duplicate child_id %s", storetemplate.ErrInvalidInput, childID)
		}
		seen[childID] = struct{}{}
		if _, ok := childEdgeByID[childID]; !ok {
			return nil, fmt.Errorf("%w: child_id %s is not contained by parent", storetemplate.ErrInvalidInput, childID)
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
