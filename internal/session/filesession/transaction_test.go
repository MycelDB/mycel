package filesession

import (
	"context"
	"errors"
	"testing"

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
