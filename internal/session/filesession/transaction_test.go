package filesession

import (
	"context"
	"errors"
	"strings"
	"testing"

	"martinbeauvais.com/mbgit/knotbase/knotdb/domain/graph"
	"martinbeauvais.com/mbgit/knotbase/knotdb/query"
	sessionapi "martinbeauvais.com/mbgit/knotbase/knotdb/session/api"
)

func TestFileSessionTransactionPhase2ReadYourWrites(t *testing.T) {
	sess, tmplID := newHierarchyTestSession(t)
	ctx := context.Background()

	tx, err := sess.Begin(ctx, sessionapi.TxOptions{})
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	root, err := tx.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &tmplID, Content: "root", Props: map[string]any{}})
	if err != nil {
		t.Fatalf("add root failed: %v", err)
	}
	child, err := tx.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &tmplID, Content: "child", Props: map[string]any{"k": "v"}})
	if err != nil {
		t.Fatalf("add child failed: %v", err)
	}
	if _, err := tx.AddEdge(ctx, containsInput(root.ID, child.ID, 0)); err != nil {
		t.Fatalf("add edge failed: %v", err)
	}

	seen, err := tx.GetNode(ctx, child.ID)
	if err != nil {
		t.Fatalf("get child inside tx failed: %v", err)
	}
	if seen.Content != "child" || seen.Props["k"] != "v" {
		t.Fatalf("unexpected child in tx: %+v", seen)
	}
	children, err := tx.Children(ctx, root.ID)
	if err != nil {
		t.Fatalf("children inside tx failed: %v", err)
	}
	if len(children) != 1 || children[0].ToID != child.ID {
		t.Fatalf("expected staged child edge, got %+v", children)
	}
	parent, err := tx.Parent(ctx, child.ID)
	if err != nil {
		t.Fatalf("parent inside tx failed: %v", err)
	}
	if parent == nil || parent.FromID != root.ID {
		t.Fatalf("expected staged parent edge, got %+v", parent)
	}
	updated, err := tx.UpdateNode(ctx, sessionapi.UpdateNodeInput{ID: child.ID, TemplateID: child.TemplateID, Content: "updated", Props: map[string]any{"k": "v2"}})
	if err != nil {
		t.Fatalf("update child failed: %v", err)
	}
	if updated.Content != "updated" || updated.Props["k"] != "v2" {
		t.Fatalf("unexpected updated child: %+v", updated)
	}

	if _, err := sess.GetNode(ctx, child.ID); err == nil {
		t.Fatalf("base session should not see staged child before commit")
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback failed: %v", err)
	}
	if _, err := tx.ListNodes(ctx); !errors.Is(err, sessionapi.ErrTransactionClosed) {
		t.Fatalf("expected transaction closed after rollback, got %v", err)
	}
}

func TestFileSessionTransactionPhase2DeleteEdgeOverlay(t *testing.T) {
	sess, tmplID := newHierarchyTestSession(t)
	ctx := context.Background()
	root, err := sess.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &tmplID, Content: "root", Props: map[string]any{}})
	if err != nil {
		t.Fatalf("add root failed: %v", err)
	}
	child, err := sess.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &tmplID, Content: "child", Props: map[string]any{}})
	if err != nil {
		t.Fatalf("add child failed: %v", err)
	}
	edge, err := sess.AddEdge(ctx, containsInput(root.ID, child.ID, 0))
	if err != nil {
		t.Fatalf("add edge failed: %v", err)
	}

	tx, err := sess.Begin(ctx, sessionapi.TxOptions{})
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	if err := tx.DeleteEdge(ctx, sessionapi.DeleteEdgeInput{ID: edge.ID}); err != nil {
		t.Fatalf("delete edge failed: %v", err)
	}
	children, err := tx.Children(ctx, root.ID)
	if err != nil {
		t.Fatalf("children failed: %v", err)
	}
	if len(children) != 0 {
		t.Fatalf("expected deleted edge hidden in tx, got %+v", children)
	}
	baseChildren, err := sess.Children(ctx, root.ID)
	if err != nil {
		t.Fatalf("base children failed: %v", err)
	}
	if len(baseChildren) != 1 || baseChildren[0].ID != edge.ID {
		t.Fatalf("base session should still see edge, got %+v", baseChildren)
	}
}

func TestFileSessionTransactionPhase3CommitPersistsStagedGraphDelta(t *testing.T) {
	sess, tmplID := newHierarchyTestSession(t)
	ctx := context.Background()
	tx, err := sess.Begin(ctx, sessionapi.TxOptions{})
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	root, err := tx.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &tmplID, Content: "root", Props: map[string]any{}})
	if err != nil {
		t.Fatalf("add root failed: %v", err)
	}
	child, err := tx.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &tmplID, Content: "child", Props: map[string]any{"k": "v"}})
	if err != nil {
		t.Fatalf("add child failed: %v", err)
	}
	if _, err := tx.AddEdge(ctx, containsInput(root.ID, child.ID, 0)); err != nil {
		t.Fatalf("add edge failed: %v", err)
	}
	if _, err := tx.UpdateNode(ctx, sessionapi.UpdateNodeInput{ID: child.ID, TemplateID: child.TemplateID, Content: "updated", Props: map[string]any{"k": "v2"}}); err != nil {
		t.Fatalf("update child failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	persisted, err := sess.GetNode(ctx, child.ID)
	if err != nil {
		t.Fatalf("get persisted child failed: %v", err)
	}
	if persisted.Content != "updated" || persisted.Props["k"] != "v2" {
		t.Fatalf("unexpected persisted child: %+v", persisted)
	}
	children, err := sess.Children(ctx, root.ID)
	if err != nil {
		t.Fatalf("children failed: %v", err)
	}
	if len(children) != 1 || children[0].ToID != child.ID {
		t.Fatalf("expected persisted child edge, got %+v", children)
	}
	if err := tx.Rollback(ctx); !errors.Is(err, sessionapi.ErrTransactionClosed) {
		t.Fatalf("expected closed tx after commit, got %v", err)
	}
}

func TestFileSessionTransactionPhase3CommitDeletesStagedNodesAndEdges(t *testing.T) {
	sess, tmplID := newHierarchyTestSession(t)
	ctx := context.Background()
	root, err := sess.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &tmplID, Content: "root", Props: map[string]any{}})
	if err != nil {
		t.Fatalf("add root failed: %v", err)
	}
	child, err := sess.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &tmplID, Content: "child", Props: map[string]any{}})
	if err != nil {
		t.Fatalf("add child failed: %v", err)
	}
	edge, err := sess.AddEdge(ctx, containsInput(root.ID, child.ID, 0))
	if err != nil {
		t.Fatalf("add edge failed: %v", err)
	}
	tx, err := sess.Begin(ctx, sessionapi.TxOptions{})
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	if err := tx.DeleteEdge(ctx, sessionapi.DeleteEdgeInput{ID: edge.ID}); err != nil {
		t.Fatalf("delete edge failed: %v", err)
	}
	if err := tx.DeleteNode(ctx, sessionapi.DeleteNodeInput{ID: child.ID}); err != nil {
		t.Fatalf("delete child failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	if _, err := sess.GetNode(ctx, child.ID); err == nil {
		t.Fatalf("expected child deleted after commit")
	}
	children, err := sess.Children(ctx, root.ID)
	if err != nil {
		t.Fatalf("children failed: %v", err)
	}
	if len(children) != 0 {
		t.Fatalf("expected edge deleted after commit, got %+v", children)
	}
}

func TestFileSessionTransactionPhase3CallbackRollback(t *testing.T) {
	sess, _ := newHierarchyTestSession(t)
	ctx := context.Background()
	callbackErr := errors.New("callback failed")
	called := false
	err := sess.Tx(ctx, sessionapi.TxOptions{}, func(tx sessionapi.Tx) error {
		called = true
		return callbackErr
	})
	if !called {
		t.Fatalf("expected callback to be called")
	}
	if !errors.Is(err, callbackErr) {
		t.Fatalf("expected callback error, got %v", err)
	}
}

func TestFileSessionTransactionPhase2ReadOnlyRejectsWrites(t *testing.T) {
	sess, tmplID := newHierarchyTestSession(t)
	ctx := context.Background()
	tx, err := sess.Begin(ctx, sessionapi.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("begin read-only failed: %v", err)
	}
	_, err = tx.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &tmplID, Content: "nope", Props: map[string]any{}})
	if !errors.Is(err, sessionapi.ErrReadOnlyTransaction) {
		t.Fatalf("expected ErrReadOnlyTransaction, got %v", err)
	}
}

func TestFileSessionTransactionPhase2QuerySeesStagedNodes(t *testing.T) {
	sess, tmplID := newHierarchyTestSession(t)
	ctx := context.Background()
	tx, err := sess.Begin(ctx, sessionapi.TxOptions{})
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	if _, err := tx.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &tmplID, Content: "query me", Props: map[string]any{}}); err != nil {
		t.Fatalf("add node failed: %v", err)
	}
	rows, err := tx.Query().
		Match(query.Pattern().Node("n", query.Template("entry"))).
		Return(query.Var("n")).
		Execute(ctx)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(rows.Rows) != 1 {
		t.Fatalf("expected query to see staged node, got %d rows", len(rows.Rows))
	}
}

func TestFileSessionTransactionPhase4MoveSubtreeCommitAndRollback(t *testing.T) {
	sess, tmplID := newHierarchyTestSession(t)
	ctx := context.Background()
	root, err := sess.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &tmplID, Content: "root", Props: map[string]any{}})
	if err != nil {
		t.Fatalf("add root failed: %v", err)
	}
	other, err := sess.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &tmplID, Content: "other", Props: map[string]any{}})
	if err != nil {
		t.Fatalf("add other failed: %v", err)
	}
	child, err := sess.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &tmplID, Content: "child", Props: map[string]any{}})
	if err != nil {
		t.Fatalf("add child failed: %v", err)
	}
	if _, err := sess.AddEdge(ctx, containsInput(root.ID, child.ID, 0)); err != nil {
		t.Fatalf("add edge failed: %v", err)
	}
	tx, err := sess.Begin(ctx, sessionapi.TxOptions{})
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	if _, err := tx.MoveSubtree(ctx, sessionapi.MoveSubtreeInput{NodeID: child.ID, NewParentID: other.ID}); err != nil {
		t.Fatalf("move in tx failed: %v", err)
	}
	if children, _ := tx.Children(ctx, other.ID); len(children) != 1 || children[0].ToID != child.ID {
		t.Fatalf("tx should see moved child, got %+v", children)
	}
	if children, _ := sess.Children(ctx, other.ID); len(children) != 0 {
		t.Fatalf("base should not see uncommitted move, got %+v", children)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback failed: %v", err)
	}
	if children, _ := sess.Children(ctx, root.ID); len(children) != 1 || children[0].ToID != child.ID {
		t.Fatalf("rollback should preserve original parent, got %+v", children)
	}

	tx, err = sess.Begin(ctx, sessionapi.TxOptions{})
	if err != nil {
		t.Fatalf("begin 2 failed: %v", err)
	}
	if _, err := tx.MoveSubtree(ctx, sessionapi.MoveSubtreeInput{NodeID: child.ID, NewParentID: other.ID}); err != nil {
		t.Fatalf("move 2 failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	if children, _ := sess.Children(ctx, other.ID); len(children) != 1 || children[0].ToID != child.ID {
		t.Fatalf("commit should persist moved child, got %+v", children)
	}
}

func TestFileSessionTransactionPhase4ReorderChildrenCommit(t *testing.T) {
	sess, tmplID := newHierarchyTestSession(t)
	ctx := context.Background()
	root, _ := sess.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &tmplID, Content: "root", Props: map[string]any{}})
	a, _ := sess.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &tmplID, Content: "a", Props: map[string]any{}})
	b, _ := sess.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &tmplID, Content: "b", Props: map[string]any{}})
	c, _ := sess.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &tmplID, Content: "c", Props: map[string]any{}})
	_, _ = sess.AddEdge(ctx, containsInput(root.ID, a.ID, 0))
	_, _ = sess.AddEdge(ctx, containsInput(root.ID, b.ID, 1))
	_, _ = sess.AddEdge(ctx, containsInput(root.ID, c.ID, 2))
	tx, err := sess.Begin(ctx, sessionapi.TxOptions{})
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	if _, err := tx.ReorderChildren(ctx, sessionapi.ReorderChildrenInput{ParentID: root.ID, ChildIDs: []graph.NodeID{c.ID, a.ID, b.ID}}); err != nil {
		t.Fatalf("reorder failed: %v", err)
	}
	children, _ := tx.Children(ctx, root.ID)
	if got := []graph.NodeID{children[0].ToID, children[1].ToID, children[2].ToID}; got[0] != c.ID || got[1] != a.ID || got[2] != b.ID {
		t.Fatalf("unexpected tx order: %+v", got)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	children, _ = sess.Children(ctx, root.ID)
	if got := []graph.NodeID{children[0].ToID, children[1].ToID, children[2].ToID}; got[0] != c.ID || got[1] != a.ID || got[2] != b.ID {
		t.Fatalf("unexpected committed order: %+v", got)
	}
}

func TestFileSessionTransactionPhase4ApplyGraphStagesAndCommits(t *testing.T) {
	sess, tmplID := newHierarchyTestSession(t)
	ctx := context.Background()
	rootID := nodeID()
	childID := nodeID()
	tx, err := sess.Begin(ctx, sessionapi.TxOptions{})
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	result, err := tx.ApplyGraph(ctx, sessionapi.ApplyGraphInput{
		AddNodes: []sessionapi.AddNodeInput{
			{ID: &rootID, TemplateID: &tmplID, Content: "root", Props: map[string]any{}},
			{ID: &childID, TemplateID: &tmplID, Content: "child", Props: map[string]any{}},
		},
		AddEdges: []sessionapi.AddEdgeInput{containsInput(rootID, childID, 0)},
	})
	if err != nil {
		t.Fatalf("apply graph failed: %v", err)
	}
	if len(result.AddedNodes) != 2 || len(result.AddedEdges) != 1 {
		t.Fatalf("unexpected apply result: %+v", result)
	}
	if children, _ := tx.Children(ctx, rootID); len(children) != 1 || children[0].ToID != childID {
		t.Fatalf("tx should see apply graph child, got %+v", children)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	if children, _ := sess.Children(ctx, rootID); len(children) != 1 || children[0].ToID != childID {
		t.Fatalf("base should see committed apply graph, got %+v", children)
	}
}

func TestFileSessionTransactionPhase4ApplyGraphRestoresOverlayOnError(t *testing.T) {
	sess, tmplID := newHierarchyTestSession(t)
	ctx := context.Background()
	rootID := nodeID()
	tx, err := sess.Begin(ctx, sessionapi.TxOptions{})
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	if _, err := tx.ApplyGraph(ctx, sessionapi.ApplyGraphInput{
		AddNodes: []sessionapi.AddNodeInput{{ID: &rootID, TemplateID: &tmplID, Content: "root", Props: map[string]any{}}},
		AddEdges: []sessionapi.AddEdgeInput{containsInput(rootID, nodeID(), 0)},
	}); err == nil {
		t.Fatalf("expected apply graph failure")
	}
	if _, err := tx.GetNode(ctx, rootID); err == nil {
		t.Fatalf("overlay should be restored after failed apply graph")
	}
}

func TestFileSessionTransactionPhase5QueryHidesStagedDeletes(t *testing.T) {
	sess, tmplID := newHierarchyTestSession(t)
	ctx := context.Background()
	node, err := sess.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &tmplID, Content: "delete me", Props: map[string]any{}})
	if err != nil {
		t.Fatalf("add node failed: %v", err)
	}
	tx, err := sess.Begin(ctx, sessionapi.TxOptions{})
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	if err := tx.DeleteNode(ctx, sessionapi.DeleteNodeInput{ID: node.ID}); err != nil {
		t.Fatalf("delete node failed: %v", err)
	}
	rows, err := tx.Query().
		Match(query.Pattern().Node("n", query.Template("entry"))).
		Return(query.Var("n")).
		Execute(ctx)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(rows.Rows) != 0 {
		t.Fatalf("expected tx query to hide staged delete, got %d rows", len(rows.Rows))
	}
	baseRows, err := sess.Query().
		Match(query.Pattern().Node("n", query.Template("entry"))).
		Return(query.Var("n")).
		Execute(ctx)
	if err != nil {
		t.Fatalf("base query failed: %v", err)
	}
	if len(baseRows.Rows) != 1 {
		t.Fatalf("expected base query to still see node, got %d rows", len(baseRows.Rows))
	}
}

func TestFileSessionTransactionPhase5QuerySeesMovedHierarchy(t *testing.T) {
	sess, tmplID := newHierarchyTestSession(t)
	ctx := context.Background()
	root, _ := sess.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &tmplID, Content: "root", Props: map[string]any{}})
	other, _ := sess.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &tmplID, Content: "other", Props: map[string]any{}})
	child, _ := sess.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &tmplID, Content: "child", Props: map[string]any{}})
	_, _ = sess.AddEdge(ctx, containsInput(root.ID, child.ID, 0))
	tx, err := sess.Begin(ctx, sessionapi.TxOptions{})
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	if _, err := tx.MoveSubtree(ctx, sessionapi.MoveSubtreeInput{NodeID: child.ID, NewParentID: other.ID}); err != nil {
		t.Fatalf("move failed: %v", err)
	}
	rows, err := tx.Query().
		Match(query.Pattern().Node("parent", query.Template("entry")).Out("contains", query.Depth(1, 1)).Node("child", query.Template("entry"))).
		Return(query.Var("parent"), query.Var("child")).
		Execute(ctx)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	matched := false
	for _, row := range rows.Rows {
		parentNode, parentOK := row.Node("parent")
		childNode, childOK := row.Node("child")
		if parentOK && childOK && parentNode.ID == other.ID && childNode.ID == child.ID {
			matched = true
		}
		if parentOK && childOK && parentNode.ID == root.ID && childNode.ID == child.ID {
			t.Fatalf("query still saw child under old parent")
		}
	}
	if !matched {
		t.Fatalf("query did not see child under new parent: %+v", rows.Rows)
	}
}

func TestFileSessionTransactionPhase5QueryTreeUsesStagedOrder(t *testing.T) {
	sess, tmplID := newHierarchyTestSession(t)
	ctx := context.Background()
	root, _ := sess.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &tmplID, Content: "root", Props: map[string]any{}})
	a, _ := sess.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &tmplID, Content: "a", Props: map[string]any{}})
	b, _ := sess.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &tmplID, Content: "b", Props: map[string]any{}})
	c, _ := sess.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &tmplID, Content: "c", Props: map[string]any{}})
	_, _ = sess.AddEdge(ctx, containsInput(root.ID, a.ID, 0))
	_, _ = sess.AddEdge(ctx, containsInput(root.ID, b.ID, 1))
	_, _ = sess.AddEdge(ctx, containsInput(root.ID, c.ID, 2))
	tx, err := sess.Begin(ctx, sessionapi.TxOptions{})
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	if _, err := tx.ReorderChildren(ctx, sessionapi.ReorderChildrenInput{ParentID: root.ID, ChildIDs: []graph.NodeID{c.ID, a.ID, b.ID}}); err != nil {
		t.Fatalf("reorder failed: %v", err)
	}
	rows, err := tx.Query().
		Match(query.Pattern().Node("root", query.Template("entry")).Out("contains", query.Depth(1, query.Unbounded)).Node("entry", query.Template("entry"))).
		Return(query.Tree("entry").As("entries")).
		Execute(ctx)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	for _, row := range rows.Rows {
		tree, ok := row.Tree("entries")
		if !ok || len(tree) != 3 {
			continue
		}
		if tree[0].Node.ID != c.ID || tree[1].Node.ID != a.ID || tree[2].Node.ID != b.ID {
			t.Fatalf("query tree did not use staged order: %+v", tree)
		}
		return
	}
	t.Fatalf("did not find root query row with three entries")
}

func TestFileSessionPhase6UpdateNodeAndCreateSiblingIsTransactional(t *testing.T) {
	sess, tmplID := newHierarchyTestSession(t)
	ctx := context.Background()
	root, err := sess.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &tmplID, Content: "root", Props: map[string]any{}})
	if err != nil {
		t.Fatalf("add root failed: %v", err)
	}
	first, err := sess.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &tmplID, Content: "first", Props: map[string]any{}})
	if err != nil {
		t.Fatalf("add first failed: %v", err)
	}
	if _, err := sess.AddEdge(ctx, containsInput(root.ID, first.ID, 0)); err != nil {
		t.Fatalf("add edge failed: %v", err)
	}
	result, err := sess.UpdateNodeAndCreateSibling(ctx, sessionapi.UpdateNodeAndCreateSiblingInput{NodeID: first.ID, Content: "updated", Props: map[string]any{}, SiblingTemplateID: &tmplID, SiblingContent: "second", SiblingProps: map[string]any{}})
	if err != nil {
		t.Fatalf("update/create sibling failed: %v", err)
	}
	updated, err := sess.GetNode(ctx, first.ID)
	if err != nil {
		t.Fatalf("get updated failed: %v", err)
	}
	if updated.Content != "updated" || result.CreatedNode.Content != "second" {
		t.Fatalf("unexpected result updated=%+v result=%+v", updated, result)
	}
	children, err := sess.Children(ctx, root.ID)
	if err != nil {
		t.Fatalf("children failed: %v", err)
	}
	if len(children) != 2 || children[0].ToID != first.ID || children[1].ToID != result.CreatedNode.ID {
		t.Fatalf("unexpected child order: %+v", children)
	}
}

func TestFileSessionPhase6TransactionUpdateNodeAndCreateSiblingRollback(t *testing.T) {
	sess, tmplID := newHierarchyTestSession(t)
	ctx := context.Background()
	root, _ := sess.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &tmplID, Content: "root", Props: map[string]any{}})
	first, _ := sess.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &tmplID, Content: "first", Props: map[string]any{}})
	_, _ = sess.AddEdge(ctx, containsInput(root.ID, first.ID, 0))
	tx, err := sess.Begin(ctx, sessionapi.TxOptions{})
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	result, err := tx.UpdateNodeAndCreateSibling(ctx, sessionapi.UpdateNodeAndCreateSiblingInput{NodeID: first.ID, Content: "updated", Props: map[string]any{}, SiblingTemplateID: &tmplID, SiblingContent: "second", SiblingProps: map[string]any{}})
	if err != nil {
		t.Fatalf("update/create sibling failed: %v", err)
	}
	if _, err := tx.GetNode(ctx, result.CreatedNode.ID); err != nil {
		t.Fatalf("tx should see created sibling: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback failed: %v", err)
	}
	persisted, err := sess.GetNode(ctx, first.ID)
	if err != nil {
		t.Fatalf("get first failed: %v", err)
	}
	if persisted.Content != "first" {
		t.Fatalf("rollback should restore original content, got %+v", persisted)
	}
	if _, err := sess.GetNode(ctx, result.CreatedNode.ID); err == nil {
		t.Fatalf("rollback should not persist sibling")
	}
}

func TestFileSessionPhase6ApplyGraphFailureDoesNotPersistPartialNode(t *testing.T) {
	sess, tmplID := newHierarchyTestSession(t)
	ctx := context.Background()
	rootID := nodeID()
	missingID := nodeID()
	_, err := sess.ApplyGraph(ctx, sessionapi.ApplyGraphInput{
		AddNodes: []sessionapi.AddNodeInput{{ID: &rootID, TemplateID: &tmplID, Content: "root", Props: map[string]any{}}},
		AddEdges: []sessionapi.AddEdgeInput{containsInput(rootID, missingID, 0)},
	})
	if err == nil {
		t.Fatalf("expected apply graph failure")
	}
	if _, err := sess.GetNode(ctx, rootID); err == nil {
		t.Fatalf("failed apply graph should not persist partial node")
	}
}

func TestFileSessionTransactionPhase8ConflictOnStaleCommit(t *testing.T) {
	sess, tmplID := newHierarchyTestSession(t)
	ctx := context.Background()
	tx1, err := sess.Begin(ctx, sessionapi.TxOptions{})
	if err != nil {
		t.Fatalf("begin tx1 failed: %v", err)
	}
	tx2, err := sess.Begin(ctx, sessionapi.TxOptions{})
	if err != nil {
		t.Fatalf("begin tx2 failed: %v", err)
	}
	if _, err := tx1.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &tmplID, Content: "tx1", Props: map[string]any{}}); err != nil {
		t.Fatalf("tx1 add failed: %v", err)
	}
	if _, err := tx2.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &tmplID, Content: "tx2", Props: map[string]any{}}); err != nil {
		t.Fatalf("tx2 add failed: %v", err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("tx1 commit failed: %v", err)
	}
	if err := tx2.Commit(ctx); err == nil || !strings.Contains(err.Error(), "conflict") {
		t.Fatalf("expected stale tx2 commit conflict, got %v", err)
	}
}

func TestFileSessionTransactionPhase8ReadOnlyCommitDoesNotConflict(t *testing.T) {
	sess, tmplID := newHierarchyTestSession(t)
	ctx := context.Background()
	readOnly, err := sess.Begin(ctx, sessionapi.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("begin read-only failed: %v", err)
	}
	writer, err := sess.Begin(ctx, sessionapi.TxOptions{})
	if err != nil {
		t.Fatalf("begin writer failed: %v", err)
	}
	if _, err := writer.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &tmplID, Content: "writer", Props: map[string]any{}}); err != nil {
		t.Fatalf("writer add failed: %v", err)
	}
	if err := writer.Commit(ctx); err != nil {
		t.Fatalf("writer commit failed: %v", err)
	}
	if err := readOnly.Commit(ctx); err != nil {
		t.Fatalf("read-only commit should not conflict, got %v", err)
	}
}
