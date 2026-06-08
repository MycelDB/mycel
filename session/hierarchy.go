package session

import "martinbeauvais.com/mbgit/knotbase/knotdb/domain/graph"

// MoveSubtreeInput moves NodeID and all of its descendants under NewParentID
// by rewiring the node's incoming contains edge. If Order is nil, the moved
// subtree is appended to the new parent's children.
type MoveSubtreeInput struct {
	NodeID      graph.NodeID
	NewParentID graph.NodeID
	Order       *int
}

// ReorderChildrenInput replaces the complete sibling order for a parent.
// ChildIDs must contain exactly the current direct contains children of
// ParentID, with no missing, extra, or duplicate IDs.
type ReorderChildrenInput struct {
	ParentID graph.NodeID
	ChildIDs []graph.NodeID
}
