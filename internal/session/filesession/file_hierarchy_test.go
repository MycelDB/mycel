package filesession

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	storetemplate "martinbeauvais.com/mbgit/knotbase/knotdb/store/template"
	"martinbeauvais.com/mbgit/knotbase/knotdb/domain/graph"
	domainspace "martinbeauvais.com/mbgit/knotbase/knotdb/domain/space"
	q "martinbeauvais.com/mbgit/knotbase/knotdb/query"
	sessionapi "martinbeauvais.com/mbgit/knotbase/knotdb/session/api"
)

type hierarchyTemplateManager struct {
	templates map[graph.TemplateID]graph.Template
}

func (m hierarchyTemplateManager) Import(ctx context.Context, spaceID domainspace.SpaceID, doc storetemplate.ImportDocument) ([]graph.Template, error) {
	return nil, nil
}

func (m hierarchyTemplateManager) ListBySpace(ctx context.Context, spaceID domainspace.SpaceID) ([]graph.Template, error) {
	out := []graph.Template{}
	for _, tmpl := range m.templates {
		if tmpl.SpaceID == spaceID {
			out = append(out, tmpl)
		}
	}
	return out, nil
}

func (m hierarchyTemplateManager) GetByID(ctx context.Context, id graph.TemplateID) (graph.Template, error) {
	tmpl, ok := m.templates[id]
	if !ok {
		return graph.Template{}, storetemplate.ErrTemplateNotFound
	}
	return tmpl, nil
}

func (m hierarchyTemplateManager) Init(ctx context.Context, location string) error { return nil }

func (m hierarchyTemplateManager) Find(ctx context.Context, spaceID domainspace.SpaceID, key string, version string) (graph.Template, error) {
	for _, tmpl := range m.templates {
		if tmpl.SpaceID == spaceID && tmpl.Key == key && tmpl.Version == version {
			return tmpl, nil
		}
	}
	return graph.Template{}, storetemplate.ErrTemplateNotFound
}

func (m hierarchyTemplateManager) DeleteForSpace(ctx context.Context, spaceID domainspace.SpaceID) error {
	return nil
}

func TestFileSessionNodeTimestamps(t *testing.T) {
	ctx := context.Background()
	sess, tmplID := newHierarchyTestSession(t)
	node, err := sess.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &tmplID, Content: "created"})
	if err != nil {
		t.Fatalf("add node failed: %v", err)
	}
	if node.CreatedAt.IsZero() || node.UpdatedAt.IsZero() {
		t.Fatalf("expected create timestamps, got %+v", node)
	}
	if !node.CreatedAt.Equal(node.UpdatedAt) {
		t.Fatalf("expected created and updated to match on create, got created_at=%s updated_at=%s", node.CreatedAt, node.UpdatedAt)
	}
	time.Sleep(time.Millisecond)
	updated, err := sess.UpdateNode(ctx, sessionapi.UpdateNodeInput{ID: node.ID, TemplateID: node.TemplateID, Content: "updated", Props: node.Props})
	if err != nil {
		t.Fatalf("update node failed: %v", err)
	}
	if !updated.CreatedAt.Equal(node.CreatedAt) {
		t.Fatalf("expected created_at preserved, got %s want %s", updated.CreatedAt, node.CreatedAt)
	}
	if !updated.UpdatedAt.After(node.UpdatedAt) {
		t.Fatalf("expected updated_at to advance, got %s after %s", updated.UpdatedAt, node.UpdatedAt)
	}
}

func TestFileSessionMoveSubtreeMovesWholeSubtreeAndPreservesEdge(t *testing.T) {
	ctx := context.Background()
	sess, tmplID := newHierarchyTestSession(t)
	rootID, aID, bID, cID, dID, eID := nodeID(), nodeID(), nodeID(), nodeID(), nodeID(), nodeID()
	bEdgeID := graph.EdgeID(uuid.New())
	if _, err := sess.ApplyGraph(ctx, sessionapi.ApplyGraphInput{
		AddNodes: []sessionapi.AddNodeInput{
			{ID: &rootID, TemplateID: &tmplID, Content: "root"},
			{ID: &aID, TemplateID: &tmplID, Content: "A"},
			{ID: &bID, TemplateID: &tmplID, Content: "B"},
			{ID: &cID, TemplateID: &tmplID, Content: "C"},
			{ID: &dID, TemplateID: &tmplID, Content: "D"},
			{ID: &eID, TemplateID: &tmplID, Content: "E"},
		},
		AddEdges: []sessionapi.AddEdgeInput{
			containsInput(rootID, aID, 0),
			containsInput(rootID, cID, 1),
			{ID: &bEdgeID, FromID: aID, ToID: bID, Kind: graph.EdgeKindContains, Props: map[string]any{"order": 0, "source": "test"}},
			containsInput(bID, dID, 0),
			containsInput(cID, eID, 0),
		},
	}); err != nil {
		t.Fatalf("build graph failed: %v", err)
	}

	moved, err := sess.MoveSubtree(ctx, sessionapi.MoveSubtreeInput{NodeID: bID, NewParentID: cID})
	if err != nil {
		t.Fatalf("move subtree failed: %v", err)
	}
	if moved.ID != bEdgeID || moved.FromID != cID || moved.ToID != bID {
		t.Fatalf("expected moved edge id/from/to to be preserved and updated, got %+v", moved)
	}
	if moved.Props["source"] != "test" || moved.Props["order"] != 1 {
		t.Fatalf("expected moved edge props to preserve source and append order=1, got %+v", moved.Props)
	}
	assertChildren(t, sess, aID)
	assertChildren(t, sess, cID, eID, bID)
	assertChildren(t, sess, bID, dID)
}

func TestFileSessionMoveSubtreeInsertsAtExplicitOrderAndNormalizes(t *testing.T) {
	ctx := context.Background()
	sess, tmplID := newHierarchyTestSession(t)
	rootID, aID, bID, cID, dID, eID := nodeID(), nodeID(), nodeID(), nodeID(), nodeID(), nodeID()
	if _, err := sess.ApplyGraph(ctx, sessionapi.ApplyGraphInput{
		AddNodes: []sessionapi.AddNodeInput{
			{ID: &rootID, TemplateID: &tmplID, Content: "root"},
			{ID: &aID, TemplateID: &tmplID, Content: "A"},
			{ID: &bID, TemplateID: &tmplID, Content: "B"},
			{ID: &cID, TemplateID: &tmplID, Content: "C"},
			{ID: &dID, TemplateID: &tmplID, Content: "D"},
			{ID: &eID, TemplateID: &tmplID, Content: "E"},
		},
		AddEdges: []sessionapi.AddEdgeInput{
			containsInput(rootID, aID, 0),
			containsInput(rootID, cID, 1),
			containsInput(aID, bID, 0),
			containsInput(aID, dID, 1),
			containsInput(cID, eID, 0),
		},
	}); err != nil {
		t.Fatalf("build graph failed: %v", err)
	}

	order := 0
	if _, err := sess.MoveSubtree(ctx, sessionapi.MoveSubtreeInput{NodeID: dID, NewParentID: cID, Order: &order}); err != nil {
		t.Fatalf("move subtree failed: %v", err)
	}
	assertChildren(t, sess, aID, bID)
	assertChildren(t, sess, cID, dID, eID)
}

func TestFileSessionMoveSubtreeWithinSameParentReorders(t *testing.T) {
	ctx := context.Background()
	sess, tmplID := newHierarchyTestSession(t)
	rootID, aID, bID, cID := nodeID(), nodeID(), nodeID(), nodeID()
	if _, err := sess.ApplyGraph(ctx, sessionapi.ApplyGraphInput{
		AddNodes: []sessionapi.AddNodeInput{{ID: &rootID, TemplateID: &tmplID}, {ID: &aID, TemplateID: &tmplID}, {ID: &bID, TemplateID: &tmplID}, {ID: &cID, TemplateID: &tmplID}},
		AddEdges: []sessionapi.AddEdgeInput{containsInput(rootID, aID, 0), containsInput(rootID, bID, 1), containsInput(rootID, cID, 2)},
	}); err != nil {
		t.Fatalf("build graph failed: %v", err)
	}
	order := 0
	if _, err := sess.MoveSubtree(ctx, sessionapi.MoveSubtreeInput{NodeID: cID, NewParentID: rootID, Order: &order}); err != nil {
		t.Fatalf("same-parent move failed: %v", err)
	}
	assertChildren(t, sess, rootID, cID, aID, bID)
}

func TestFileSessionMoveRootNodeUnderParent(t *testing.T) {
	ctx := context.Background()
	sess, tmplID := newHierarchyTestSession(t)
	parentID, rootID := nodeID(), nodeID()
	if _, err := sess.ApplyGraph(ctx, sessionapi.ApplyGraphInput{AddNodes: []sessionapi.AddNodeInput{{ID: &parentID, TemplateID: &tmplID}, {ID: &rootID, TemplateID: &tmplID}}}); err != nil {
		t.Fatalf("build graph failed: %v", err)
	}
	moved, err := sess.MoveSubtree(ctx, sessionapi.MoveSubtreeInput{NodeID: rootID, NewParentID: parentID})
	if err != nil {
		t.Fatalf("move root failed: %v", err)
	}
	if moved.FromID != parentID || moved.ToID != rootID || moved.Props["order"] != 0 {
		t.Fatalf("unexpected moved root edge: %+v", moved)
	}
	assertChildren(t, sess, parentID, rootID)
}

func TestFileSessionMoveSubtreeRejectsInvalidMoves(t *testing.T) {
	ctx := context.Background()
	sess, tmplID := newHierarchyTestSession(t)
	rootID, aID, bID := nodeID(), nodeID(), nodeID()
	if _, err := sess.ApplyGraph(ctx, sessionapi.ApplyGraphInput{
		AddNodes: []sessionapi.AddNodeInput{{ID: &rootID, TemplateID: &tmplID}, {ID: &aID, TemplateID: &tmplID}, {ID: &bID, TemplateID: &tmplID}},
		AddEdges: []sessionapi.AddEdgeInput{containsInput(rootID, aID, 0), containsInput(aID, bID, 0)},
	}); err != nil {
		t.Fatalf("build graph failed: %v", err)
	}
	if _, err := sess.MoveSubtree(ctx, sessionapi.MoveSubtreeInput{NodeID: aID, NewParentID: bID}); !errors.Is(err, storetemplate.ErrInvalidInput) {
		t.Fatalf("expected cycle move invalid input, got %v", err)
	}
	if _, err := sess.MoveSubtree(ctx, sessionapi.MoveSubtreeInput{NodeID: aID, NewParentID: aID}); !errors.Is(err, storetemplate.ErrInvalidInput) {
		t.Fatalf("expected self move invalid input, got %v", err)
	}
	missingID := nodeID()
	if _, err := sess.MoveSubtree(ctx, sessionapi.MoveSubtreeInput{NodeID: missingID, NewParentID: rootID}); err == nil {
		t.Fatal("expected missing moved node error")
	}
	if _, err := sess.MoveSubtree(ctx, sessionapi.MoveSubtreeInput{NodeID: aID, NewParentID: missingID}); err == nil {
		t.Fatal("expected missing new parent error")
	}
	badOrder := -1
	if _, err := sess.MoveSubtree(ctx, sessionapi.MoveSubtreeInput{NodeID: aID, NewParentID: rootID, Order: &badOrder}); !errors.Is(err, storetemplate.ErrInvalidInput) {
		t.Fatalf("expected negative order invalid input, got %v", err)
	}
}

func TestFileSessionMoveSubtreeValidatesChildTemplatePolicy(t *testing.T) {
	ctx := context.Background()
	spaceID := domainspace.SpaceID(uuid.New())
	graphsDir := t.TempDir()
	prepareSpaceDir(t, graphsDir, spaceID)
	parentTemplateID := graph.TemplateID(uuid.New())
	allowedTemplateID := graph.TemplateID(uuid.New())
	forbiddenTemplateID := graph.TemplateID(uuid.New())
	manager := hierarchyTemplateManager{templates: map[graph.TemplateID]graph.Template{
		parentTemplateID:    {ID: parentTemplateID, SpaceID: spaceID, Key: "parent", Version: "1", Children: graph.ChildPolicy{Allowed: true, AllowedTemplates: []graph.TemplateRef{{Key: "allowed", Version: "1"}}}},
		allowedTemplateID:   {ID: allowedTemplateID, SpaceID: spaceID, Key: "allowed", Version: "1", Children: graph.ChildPolicy{Allowed: true}},
		forbiddenTemplateID: {ID: forbiddenTemplateID, SpaceID: spaceID, Key: "forbidden", Version: "1", Children: graph.ChildPolicy{Allowed: true}},
	}}
	sess := New(graphsDir, t.TempDir(), spaceID, manager, sessionapi.Permissions{Read: true, Write: true, Admin: true}, sessionapi.Errors{NotFound: errors.New("not found")})
	parentID, oldParentID, childID := nodeID(), nodeID(), nodeID()
	if _, err := sess.ApplyGraph(ctx, sessionapi.ApplyGraphInput{
		AddNodes: []sessionapi.AddNodeInput{{ID: &parentID, TemplateID: &parentTemplateID}, {ID: &oldParentID, TemplateID: &allowedTemplateID}, {ID: &childID, TemplateID: &forbiddenTemplateID}},
		AddEdges: []sessionapi.AddEdgeInput{containsInput(oldParentID, childID, 0)},
	}); err != nil {
		t.Fatalf("build graph failed: %v", err)
	}
	if _, err := sess.MoveSubtree(ctx, sessionapi.MoveSubtreeInput{NodeID: childID, NewParentID: parentID}); !errors.Is(err, storetemplate.ErrInvalidInput) {
		t.Fatalf("expected template policy invalid input, got %v", err)
	}
}

func TestFileSessionReorderChildrenRequiresCompleteListAndQueryUsesOrder(t *testing.T) {
	ctx := context.Background()
	sess, tmplID := newHierarchyTestSession(t)
	rootID, aID, bID, cID := nodeID(), nodeID(), nodeID(), nodeID()
	if _, err := sess.ApplyGraph(ctx, sessionapi.ApplyGraphInput{
		AddNodes: []sessionapi.AddNodeInput{
			{ID: &rootID, TemplateID: &tmplID, Content: "root"},
			{ID: &aID, TemplateID: &tmplID, Content: "A"},
			{ID: &bID, TemplateID: &tmplID, Content: "B"},
			{ID: &cID, TemplateID: &tmplID, Content: "C"},
		},
		AddEdges: []sessionapi.AddEdgeInput{containsInput(rootID, aID, 0), containsInput(rootID, bID, 1), containsInput(rootID, cID, 2)},
	}); err != nil {
		t.Fatalf("build graph failed: %v", err)
	}
	updated, err := sess.ReorderChildren(ctx, sessionapi.ReorderChildrenInput{ParentID: rootID, ChildIDs: []graph.NodeID{cID, aID, bID}})
	if err != nil {
		t.Fatalf("reorder failed: %v", err)
	}
	if len(updated) != 3 || updated[0].ToID != cID || updated[0].Props["order"] != 0 || updated[2].ToID != bID || updated[2].Props["order"] != 2 {
		t.Fatalf("unexpected updated edges: %+v", updated)
	}
	assertChildren(t, sess, rootID, cID, aID, bID)

	rows, err := sess.Query().Match(q.Pattern().Node("parent", q.Template("entry")).Out("contains", q.Depth(1, 1)).Node("child", q.Template("entry"))).Return(q.Var("parent"), q.Tree("child").As("children")).Execute(ctx)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	var children []q.TreeNode
	for _, row := range rows.Rows {
		parent, ok := row.Node("parent")
		if ok && parent.ID == rootID {
			children, _ = row.Tree("children")
			break
		}
	}
	if len(children) != 3 || children[0].Node.ID != cID || children[1].Node.ID != aID || children[2].Node.ID != bID {
		t.Fatalf("expected query tree to reflect reordered children, got %#v", children)
	}

	if _, err := sess.ReorderChildren(ctx, sessionapi.ReorderChildrenInput{ParentID: rootID, ChildIDs: []graph.NodeID{cID, aID}}); !errors.Is(err, storetemplate.ErrInvalidInput) {
		t.Fatalf("expected missing child invalid input, got %v", err)
	}
	if _, err := sess.ReorderChildren(ctx, sessionapi.ReorderChildrenInput{ParentID: rootID, ChildIDs: []graph.NodeID{cID, aID, aID}}); !errors.Is(err, storetemplate.ErrInvalidInput) {
		t.Fatalf("expected duplicate child invalid input, got %v", err)
	}
	extraID := nodeID()
	if _, err := sess.ReorderChildren(ctx, sessionapi.ReorderChildrenInput{ParentID: rootID, ChildIDs: []graph.NodeID{cID, aID, extraID}}); !errors.Is(err, storetemplate.ErrInvalidInput) {
		t.Fatalf("expected extra child invalid input, got %v", err)
	}
	missingParentID := nodeID()
	if _, err := sess.ReorderChildren(ctx, sessionapi.ReorderChildrenInput{ParentID: missingParentID, ChildIDs: nil}); err == nil {
		t.Fatal("expected missing parent error")
	}
}

func newHierarchyTestSession(t *testing.T) (sessionapi.Session, graph.TemplateID) {
	t.Helper()
	spaceID := domainspace.SpaceID(uuid.New())
	graphsDir := t.TempDir()
	prepareSpaceDir(t, graphsDir, spaceID)
	tmplID := graph.TemplateID(uuid.New())
	manager := hierarchyTemplateManager{templates: map[graph.TemplateID]graph.Template{
		tmplID: {ID: tmplID, SpaceID: spaceID, Key: "entry", Version: "1", Children: graph.ChildPolicy{Allowed: true, Order: &graph.ChildOrderPolicy{Mode: graph.ChildOrderModeEdgeProperty, Property: "order", Direction: graph.SortDirectionAsc}}, Properties: graph.PropertyPolicy{AllowExtra: true}},
	}}
	return New(graphsDir, t.TempDir(), spaceID, manager, sessionapi.Permissions{Read: true, Write: true, Admin: true}, sessionapi.Errors{Closed: errors.New("closed"), NotFound: errors.New("not found"), Unauthorized: errors.New("unauthorized"), Conflict: errors.New("conflict")}), tmplID
}

func prepareSpaceDir(t *testing.T, graphsDir string, spaceID domainspace.SpaceID) {
	t.Helper()
	spacePath := filepath.Join(graphsDir, safeID(spaceID))
	if err := os.MkdirAll(spacePath, 0o700); err != nil {
		t.Fatalf("create space dir failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(spacePath, ".space"), []byte(""), 0o600); err != nil {
		t.Fatalf("create marker failed: %v", err)
	}
}

func nodeID() graph.NodeID { return graph.NodeID(uuid.New()) }

func containsInput(fromID, toID graph.NodeID, order int) sessionapi.AddEdgeInput {
	return sessionapi.AddEdgeInput{FromID: fromID, ToID: toID, Kind: graph.EdgeKindContains, Props: map[string]any{"order": order}}
}

func assertChildren(t *testing.T, sess sessionapi.Session, parentID graph.NodeID, expected ...graph.NodeID) {
	t.Helper()
	edges, err := sess.ListEdges(context.Background())
	if err != nil {
		t.Fatalf("list edges failed: %v", err)
	}
	childEdges := []graph.Edge{}
	for _, edge := range edges {
		if edge.Kind == graph.EdgeKindContains && edge.FromID == parentID {
			childEdges = append(childEdges, edge)
		}
	}
	if len(childEdges) != len(expected) {
		t.Fatalf("expected %d children for %s, got %d: %+v", len(expected), parentID, len(childEdges), childEdges)
	}
	for _, edge := range childEdges {
		orderValue, ok := edgeOrderNumber(edge)
		if !ok {
			t.Fatalf("expected numeric order prop on edge %+v", edge)
		}
		order := int(orderValue)
		if orderValue != float64(order) || order < 0 || order >= len(expected) {
			t.Fatalf("order out of range on edge %+v", edge)
		}
		if edge.ToID != expected[order] {
			t.Fatalf("expected child at order %d to be %s, got edge %+v", order, expected[order], edge)
		}
	}
}
