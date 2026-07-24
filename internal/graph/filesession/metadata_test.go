package filesession

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/graph/model"
	sessionapi "github.com/myceldb/mycel/internal/session/api"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestMetadataIndexTags(t *testing.T) {
	ctx := context.Background()
	sess, tmplID := newHierarchyTestSession(t)

	first, err := sess.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &tmplID, Content: "first", Properties: map[string]any{"tags": []string{"Project", "Urgent"}}})
	if err != nil {
		t.Fatalf("add first failed: %v", err)
	}
	second, err := sess.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &tmplID, Content: "second", Properties: map[string]any{"tags": []any{"project"}}})
	if err != nil {
		t.Fatalf("add second failed: %v", err)
	}
	third, err := sess.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &tmplID, Content: "third", Properties: map[string]any{"tags": []string{"archive"}}})
	if err != nil {
		t.Fatalf("add third failed: %v", err)
	}

	tags, err := sess.ListTags(ctx)
	if err != nil {
		t.Fatalf("list tags failed: %v", err)
	}
	assertTagSummaries(t, tags, map[string]int{"project": 2, "urgent": 1, "archive": 1})

	anyMatches, err := sess.FindNodesByTag(ctx, sessionapi.FindNodesByTagInput{Tags: []string{"urgent", "archive"}, Match: sessionapi.TagMatchAny})
	if err != nil {
		t.Fatalf("find any tags failed: %v", err)
	}
	if got := nodeIDs(anyMatches); !reflect.DeepEqual(got, []graph.NodeID{first.ID, third.ID}) {
		t.Fatalf("expected urgent/archive matches, got %v", got)
	}

	all, err := sess.FindNodesByTag(ctx, sessionapi.FindNodesByTagInput{Tags: []string{"project", "urgent"}, Match: sessionapi.TagMatchAll})
	if err != nil {
		t.Fatalf("find all tags failed: %v", err)
	}
	if got := nodeIDs(all); !reflect.DeepEqual(got, []graph.NodeID{first.ID}) {
		t.Fatalf("expected first all-tag match, got %v", got)
	}

	updated, err := sess.UpdateNode(ctx, sessionapi.UpdateNodeInput{ID: second.ID, TemplateID: second.TemplateID, Content: second.Content, Properties: map[string]any{"tags": []string{"waiting"}}})
	if err != nil {
		t.Fatalf("update second failed: %v", err)
	}
	matches, err := sess.FindNodesByTag(ctx, sessionapi.FindNodesByTagInput{Tags: []string{"project"}})
	if err != nil {
		t.Fatalf("find project failed: %v", err)
	}
	if got := nodeIDs(matches); !reflect.DeepEqual(got, []graph.NodeID{first.ID}) {
		t.Fatalf("expected only first after update, got %v (updated=%v)", got, updated.ID)
	}
	if err := sess.DeleteNode(ctx, sessionapi.DeleteNodeInput{ID: first.ID}); err != nil {
		t.Fatalf("delete first failed: %v", err)
	}
	matches, err = sess.FindNodesByTag(ctx, sessionapi.FindNodesByTagInput{Tags: []string{"project"}})
	if err != nil {
		t.Fatalf("find project after delete failed: %v", err)
	}
	if len(matches) != 0 {
		t.Fatalf("expected no project matches after delete, got %+v", matches)
	}
}

func TestMetadataIndexRebuildsLegacyShapesAfterReopen(t *testing.T) {
	ctx := context.Background()
	spaceID := domainspace.SpaceID(uuid.New())
	graphsDir := t.TempDir()
	blobsDir := t.TempDir()
	prepareSpaceDir(t, graphsDir, spaceID)
	tmplID := graph.TemplateID(uuid.New())
	manager := hierarchyTemplateManager{templates: map[graph.TemplateID]graph.Template{
		tmplID: {ID: tmplID, SpaceID: spaceID, Key: "entry", Version: "1", Children: graph.ChildPolicy{Allowed: true}, Properties: graph.PropertyPolicy{AllowExtra: true}},
	}}
	errs := sessionapi.Errors{Closed: errors.New("closed"), NotFound: errors.New("not found"), Unauthorized: errors.New("unauthorized"), Conflict: errors.New("conflict")}
	sess := New(graphsDir, blobsDir, spaceID, manager, sessionapi.Permissions{Read: true, Write: true, Admin: true}, errs)
	node, err := sess.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &tmplID, Content: "legacy", Properties: map[string]any{
		"tags":       []any{"Project", 42, "#Urgent", "project", ""},
		"properties": map[string]any{"Due Date": " 2026-06-20 ", "Priority": "High", "nested": map[string]any{"ignored": true}},
	}})
	if err != nil {
		t.Fatalf("add legacy node failed: %v", err)
	}
	if err := sess.Close(); err != nil {
		t.Fatalf("close session failed: %v", err)
	}

	reopened := New(graphsDir, blobsDir, spaceID, manager, sessionapi.Permissions{Read: true, Write: true, Admin: true}, errs)
	defer reopened.Close()
	tags, err := reopened.ListTags(ctx)
	if err != nil {
		t.Fatalf("list tags failed: %v", err)
	}
	assertTagSummaries(t, tags, map[string]int{"project": 1, "urgent": 1})
	assertTagMatches(t, reopened, "project", node.ID)
	properties, err := reopened.ListPropertyNames(ctx)
	if err != nil {
		t.Fatalf("list property names failed: %v", err)
	}
	assertPropertySummaries(t, properties, map[string]int{"due date": 1, "priority": 1})
	assertPropertyMatches(t, reopened, sessionapi.FindNodesByPropertyInput{Name: "due date", Operator: sessionapi.PropertyOperatorEqual, Value: "2026-06-20"}, node.ID)
}

func TestMetadataIndexTransactionsAreCommitVisible(t *testing.T) {
	ctx := context.Background()
	sess, tmplID := newHierarchyTestSession(t)

	rolledBackCreate, err := sess.Begin(ctx, sessionapi.TxOptions{})
	if err != nil {
		t.Fatalf("begin rollback create tx failed: %v", err)
	}
	if _, err := rolledBackCreate.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &tmplID, Content: "rollback", Properties: map[string]any{"tags": []string{"draft"}, "properties": map[string]any{"status": "draft"}}}); err != nil {
		t.Fatalf("tx add rollback node failed: %v", err)
	}
	if err := rolledBackCreate.Rollback(ctx); err != nil {
		t.Fatalf("rollback create tx failed: %v", err)
	}
	assertNoTagMatches(t, sess, "draft")
	assertNoPropertyMatches(t, sess, "status")

	committedCreate, err := sess.Begin(ctx, sessionapi.TxOptions{})
	if err != nil {
		t.Fatalf("begin commit create tx failed: %v", err)
	}
	node, err := committedCreate.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &tmplID, Content: "commit", Properties: map[string]any{"tags": []string{"project"}, "properties": map[string]any{"status": "active"}}})
	if err != nil {
		t.Fatalf("tx add commit node failed: %v", err)
	}
	if err := committedCreate.Commit(ctx); err != nil {
		t.Fatalf("commit create tx failed: %v", err)
	}
	assertTagMatches(t, sess, "project", node.ID)
	assertPropertyMatches(t, sess, sessionapi.FindNodesByPropertyInput{Name: "status", Operator: sessionapi.PropertyOperatorEqual, Value: "active"}, node.ID)

	rolledBackUpdate, err := sess.Begin(ctx, sessionapi.TxOptions{})
	if err != nil {
		t.Fatalf("begin rollback update tx failed: %v", err)
	}
	if _, err := rolledBackUpdate.UpdateNode(ctx, sessionapi.UpdateNodeInput{ID: node.ID, TemplateID: node.TemplateID, Content: node.Content, Properties: map[string]any{"tags": []string{"archived"}, "properties": map[string]any{"status": "archived"}}}); err != nil {
		t.Fatalf("tx update rollback node failed: %v", err)
	}
	if err := rolledBackUpdate.Rollback(ctx); err != nil {
		t.Fatalf("rollback update tx failed: %v", err)
	}
	assertTagMatches(t, sess, "project", node.ID)
	assertNoTagMatches(t, sess, "archived")
	assertPropertyMatches(t, sess, sessionapi.FindNodesByPropertyInput{Name: "status", Operator: sessionapi.PropertyOperatorEqual, Value: "active"}, node.ID)

	committedUpdate, err := sess.Begin(ctx, sessionapi.TxOptions{})
	if err != nil {
		t.Fatalf("begin commit update tx failed: %v", err)
	}
	updated, err := committedUpdate.UpdateNode(ctx, sessionapi.UpdateNodeInput{ID: node.ID, TemplateID: node.TemplateID, Content: node.Content, Properties: map[string]any{"tags": []string{"archived"}, "properties": map[string]any{"status": "archived"}}})
	if err != nil {
		t.Fatalf("tx update commit node failed: %v", err)
	}
	if err := committedUpdate.Commit(ctx); err != nil {
		t.Fatalf("commit update tx failed: %v", err)
	}
	assertNoTagMatches(t, sess, "project")
	assertTagMatches(t, sess, "archived", updated.ID)
	assertPropertyMatches(t, sess, sessionapi.FindNodesByPropertyInput{Name: "status", Operator: sessionapi.PropertyOperatorEqual, Value: "archived"}, updated.ID)

	rolledBackDelete, err := sess.Begin(ctx, sessionapi.TxOptions{})
	if err != nil {
		t.Fatalf("begin rollback delete tx failed: %v", err)
	}
	if err := rolledBackDelete.DeleteNode(ctx, sessionapi.DeleteNodeInput{ID: updated.ID}); err != nil {
		t.Fatalf("tx delete rollback node failed: %v", err)
	}
	if err := rolledBackDelete.Rollback(ctx); err != nil {
		t.Fatalf("rollback delete tx failed: %v", err)
	}
	assertTagMatches(t, sess, "archived", updated.ID)

	committedDelete, err := sess.Begin(ctx, sessionapi.TxOptions{})
	if err != nil {
		t.Fatalf("begin commit delete tx failed: %v", err)
	}
	if err := committedDelete.DeleteNode(ctx, sessionapi.DeleteNodeInput{ID: updated.ID}); err != nil {
		t.Fatalf("tx delete commit node failed: %v", err)
	}
	if err := committedDelete.Commit(ctx); err != nil {
		t.Fatalf("commit delete tx failed: %v", err)
	}
	assertNoTagMatches(t, sess, "archived")
	assertNoPropertyMatches(t, sess, "status")
}

func TestMetadataIndexProperties(t *testing.T) {
	ctx := context.Background()
	sess, tmplID := newHierarchyTestSession(t)

	first, err := sess.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &tmplID, Content: "first", Properties: map[string]any{"properties": map[string]any{"Priority": " high ", "Rating": 5, "Flagged": true}}})
	if err != nil {
		t.Fatalf("add first failed: %v", err)
	}
	second, err := sess.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: &tmplID, Content: "second", Properties: map[string]any{"properties": map[string]any{"priority": "low"}}})
	if err != nil {
		t.Fatalf("add second failed: %v", err)
	}

	properties, err := sess.ListPropertyNames(ctx)
	if err != nil {
		t.Fatalf("list properties failed: %v", err)
	}
	assertPropertySummaries(t, properties, map[string]int{"priority": 2, "rating": 1, "flagged": 1})

	exists, err := sess.FindNodesByProperty(ctx, sessionapi.FindNodesByPropertyInput{Name: " priority ", Operator: sessionapi.PropertyOperatorExists})
	if err != nil {
		t.Fatalf("find property exists failed: %v", err)
	}
	if got := nodeIDs(exists); !reflect.DeepEqual(got, []graph.NodeID{first.ID, second.ID}) {
		t.Fatalf("expected both priority nodes, got %v", got)
	}

	equal, err := sess.FindNodesByProperty(ctx, sessionapi.FindNodesByPropertyInput{Name: "priority", Operator: sessionapi.PropertyOperatorEqual, Value: " high "})
	if err != nil {
		t.Fatalf("find property eq failed: %v", err)
	}
	if got := nodeIDs(equal); !reflect.DeepEqual(got, []graph.NodeID{first.ID}) {
		t.Fatalf("expected high priority node, got %v", got)
	}

	equal, err = sess.FindNodesByProperty(ctx, sessionapi.FindNodesByPropertyInput{Name: "rating", Operator: sessionapi.PropertyOperatorEqual, Value: 5.0})
	if err != nil {
		t.Fatalf("find numeric property eq failed: %v", err)
	}
	if got := nodeIDs(equal); !reflect.DeepEqual(got, []graph.NodeID{first.ID}) {
		t.Fatalf("expected rating node, got %v", got)
	}
}

func assertTagMatches(t *testing.T, sess sessionapi.Session, tag string, expected ...graph.NodeID) {
	t.Helper()
	matches, err := sess.FindNodesByTag(context.Background(), sessionapi.FindNodesByTagInput{Tags: []string{tag}})
	if err != nil {
		t.Fatalf("find tag %q failed: %v", tag, err)
	}
	if expected == nil {
		expected = []graph.NodeID{}
	}
	if got := nodeIDs(matches); !reflect.DeepEqual(got, expected) {
		t.Fatalf("expected tag %q matches %v, got %v", tag, expected, got)
	}
}

func assertNoTagMatches(t *testing.T, sess sessionapi.Session, tag string) {
	t.Helper()
	assertTagMatches(t, sess, tag)
}

func assertPropertyMatches(t *testing.T, sess sessionapi.Session, input sessionapi.FindNodesByPropertyInput, expected ...graph.NodeID) {
	t.Helper()
	matches, err := sess.FindNodesByProperty(context.Background(), input)
	if err != nil {
		t.Fatalf("find property %#v failed: %v", input, err)
	}
	if expected == nil {
		expected = []graph.NodeID{}
	}
	if got := nodeIDs(matches); !reflect.DeepEqual(got, expected) {
		t.Fatalf("expected property %#v matches %v, got %v", input, expected, got)
	}
}

func assertNoPropertyMatches(t *testing.T, sess sessionapi.Session, name string) {
	t.Helper()
	assertPropertyMatches(t, sess, sessionapi.FindNodesByPropertyInput{Name: name, Operator: sessionapi.PropertyOperatorExists})
}

func assertTagSummaries(t *testing.T, summaries []sessionapi.TagSummary, expected map[string]int) {
	t.Helper()
	got := map[string]int{}
	for _, summary := range summaries {
		got[summary.Tag] = summary.Count
	}
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("expected tag summaries %#v, got %#v", expected, got)
	}
}

func assertPropertySummaries(t *testing.T, summaries []sessionapi.PropertySummary, expected map[string]int) {
	t.Helper()
	got := map[string]int{}
	for _, summary := range summaries {
		got[summary.Name] = summary.Count
	}
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("expected property summaries %#v, got %#v", expected, got)
	}
}

func nodeIDs(nodes []graph.Node) []graph.NodeID {
	out := make([]graph.NodeID, 0, len(nodes))
	for _, node := range nodes {
		out = append(out, node.ID)
	}
	return out
}
