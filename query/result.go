package query

import (
	"sort"

	"github.com/myceldb/mycel/domain/graph"
)

// ResultSet contains query result rows.
type ResultSet struct {
	Rows []Row
}

// Row contains one query result row.
type Row struct {
	values map[string]any
}

// Node returns a projected node by name.
func (r Row) Node(name string) (graph.Node, bool) {
	value, ok := r.values[name]
	if !ok {
		return graph.Node{}, false
	}
	node, ok := value.(graph.Node)
	return node, ok
}

// Tree returns a projected tree by name.
func (r Row) Tree(name string) ([]TreeNode, bool) {
	value, ok := r.values[name]
	if !ok {
		return nil, false
	}
	tree, ok := value.([]TreeNode)
	return tree, ok
}

// TreeNode is a nested node projection preserving contains-edge structure.
type TreeNode struct {
	Node     graph.Node
	Children []TreeNode
}

// ReturnExpr describes a projected value.
type ReturnExpr interface {
	alias() string
	project(row executionRow) (any, error)
}

// Var projects the first node bound to alias.
func Var(alias string) VarReturn { return VarReturn{name: alias, out: alias} }

// VarReturn projects a node binding.
type VarReturn struct {
	name string
	out  string
}

// As changes the output field name.
func (v VarReturn) As(name string) VarReturn {
	v.out = name
	return v
}

func (v VarReturn) alias() string { return v.out }
func (v VarReturn) project(row executionRow) (any, error) {
	nodes := row.bindings[v.name]
	if len(nodes) == 0 {
		return graph.Node{}, nil
	}
	return nodes[0], nil
}

// Tree projects matched nodes as a nested forest.
func Tree(alias string) TreeReturn { return TreeReturn{name: alias, out: alias} }

// TreeReturn projects a matched node set as a tree.
type TreeReturn struct {
	name string
	out  string
}

// As changes the output field name.
func (t TreeReturn) As(name string) TreeReturn {
	t.out = name
	return t
}

func (t TreeReturn) alias() string { return t.out }
func (t TreeReturn) project(row executionRow) (any, error) {
	return buildForest(row.bindings[t.name], row.parentByChild, row.orderByChild), nil
}

func buildForest(nodes []graph.Node, parentByChild map[graph.NodeID]graph.NodeID, orderByChild map[graph.NodeID]any) []TreeNode {
	if len(nodes) == 0 {
		return nil
	}
	byID := map[graph.NodeID]graph.Node{}
	children := map[graph.NodeID][]graph.Node{}
	for _, n := range nodes {
		byID[n.ID] = n
	}
	roots := []graph.Node{}
	for _, n := range nodes {
		if parentID, ok := parentByChild[n.ID]; ok {
			if _, parentMatched := byID[parentID]; parentMatched {
				children[parentID] = append(children[parentID], n)
				continue
			}
		}
		roots = append(roots, n)
	}
	sortNodesByOrder(roots, orderByChild)
	for parentID := range children {
		sortNodesByOrder(children[parentID], orderByChild)
	}
	var build func(graph.Node) TreeNode
	build = func(n graph.Node) TreeNode {
		out := TreeNode{Node: n}
		for _, child := range children[n.ID] {
			out.Children = append(out.Children, build(child))
		}
		return out
	}
	forest := make([]TreeNode, 0, len(roots))
	for _, root := range roots {
		forest = append(forest, build(root))
	}
	return forest
}

func sortNodesByOrder(nodes []graph.Node, orderByChild map[graph.NodeID]any) {
	sort.SliceStable(nodes, func(i, j int) bool {
		cmp, err := compareValues(orderByChild[nodes[i].ID], orderByChild[nodes[j].ID])
		if err != nil || cmp == 0 {
			return false
		}
		return cmp < 0
	})
}
