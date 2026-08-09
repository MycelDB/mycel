package metadataindex

import (
	"reflect"
	"testing"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/graph/model"
)

func TestIndexTags(t *testing.T) {
	first := testNode("first", map[string]any{graph.NodePropTags: []string{"Project", "Urgent"}})
	second := testNode("second", map[string]any{graph.NodePropTags: []any{"project"}})
	third := testNode("third", map[string]any{graph.NodePropTags: []string{"archive"}})
	idx := Build([]graph.Node{first, second, third})

	assertTagSummaries(t, idx.TagSummaries(), []TagSummary{{Tag: "project", Count: 2}, {Tag: "archive", Count: 1}, {Tag: "urgent", Count: 1}})

	anyMatches, err := idx.FindByTags([]string{"urgent", "archive"}, TagMatchAny, 0)
	if err != nil {
		t.Fatalf("find any tags failed: %v", err)
	}
	if got := nodeIDs(anyMatches); !reflect.DeepEqual(got, []graph.NodeID{first.ID, third.ID}) {
		t.Fatalf("expected urgent/archive matches, got %v", got)
	}

	allMatches, err := idx.FindByTags([]string{"project", "urgent"}, TagMatchAll, 0)
	if err != nil {
		t.Fatalf("find all tags failed: %v", err)
	}
	if got := nodeIDs(allMatches); !reflect.DeepEqual(got, []graph.NodeID{first.ID}) {
		t.Fatalf("expected project+urgent match, got %v", got)
	}

	defaultMatches, err := idx.FindByTags([]string{"archive"}, "", 0)
	if err != nil {
		t.Fatalf("find default tags failed: %v", err)
	}
	if got := nodeIDs(defaultMatches); !reflect.DeepEqual(got, []graph.NodeID{third.ID}) {
		t.Fatalf("expected default any match, got %v", got)
	}

	if _, err := idx.FindByTags([]string{"project"}, TagMatchMode("unsupported"), 0); err == nil {
		t.Fatalf("expected unsupported tag match mode error")
	}
}

func TestIndexLegacyShapesAreNormalizedAndMalformedValuesIgnored(t *testing.T) {
	node := testNode("legacy", map[string]any{
		graph.NodePropTags:             []any{"Project", 42, "#Urgent", "project", ""},
		graph.NodePropCustomProperties: map[string]any{"Due Date": " 2026-06-20 ", "Priority": "High", "nested": map[string]any{"ignored": true}},
	})
	idx := Build([]graph.Node{node})

	assertTagSummaries(t, idx.TagSummaries(), []TagSummary{{Tag: "project", Count: 1}, {Tag: "urgent", Count: 1}})
	assertPropertySummaries(t, idx.PropertySummaries(), []PropertySummary{{Name: "due date", Count: 1}, {Name: "priority", Count: 1}})
	assertPropertyMatches(t, idx, FindNodesByPropertyInput{Name: "due date", Operator: PropertyOperatorEqual, Value: "2026-06-20"}, node.ID)
}

func TestIndexProperties(t *testing.T) {
	first := testNode("first", map[string]any{graph.NodePropCustomProperties: map[string]any{"Priority": " high ", "Rating": 5, "Flagged": true}})
	second := testNode("second", map[string]any{graph.NodePropCustomProperties: map[string]any{"priority": "low"}})
	idx := Build([]graph.Node{first, second})

	assertPropertySummaries(t, idx.PropertySummaries(), []PropertySummary{{Name: "priority", Count: 2}, {Name: "flagged", Count: 1}, {Name: "rating", Count: 1}})
	assertPropertyMatches(t, idx, FindNodesByPropertyInput{Name: " priority ", Operator: PropertyOperatorExists}, first.ID, second.ID)
	assertPropertyMatches(t, idx, FindNodesByPropertyInput{Name: "priority", Operator: PropertyOperatorEqual, Value: " high "}, first.ID)
	assertPropertyMatches(t, idx, FindNodesByPropertyInput{Name: "rating", Operator: PropertyOperatorEqual, Value: 5.0}, first.ID)

	limited, err := idx.FindByProperty(FindNodesByPropertyInput{Name: "priority", Operator: PropertyOperatorExists, Limit: 1})
	if err != nil {
		t.Fatalf("limited property query failed: %v", err)
	}
	if got := nodeIDs(limited); !reflect.DeepEqual(got, []graph.NodeID{first.ID}) {
		t.Fatalf("expected limited first match, got %v", got)
	}

	if _, err := idx.FindByProperty(FindNodesByPropertyInput{Name: "priority", Operator: PropertyOperator("unsupported")}); err == nil {
		t.Fatalf("expected unsupported property operator error")
	}
}

func testNode(content string, props map[string]any) graph.Node {
	return graph.Node{ID: graph.NodeID(uuid.New()), Content: content, Props: props}
}

func assertTagSummaries(t *testing.T, got []TagSummary, expected []TagSummary) {
	t.Helper()
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("expected tag summaries %#v, got %#v", expected, got)
	}
}

func assertPropertySummaries(t *testing.T, got []PropertySummary, expected []PropertySummary) {
	t.Helper()
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("expected property summaries %#v, got %#v", expected, got)
	}
}

func assertPropertyMatches(t *testing.T, idx *Index, input FindNodesByPropertyInput, expected ...graph.NodeID) {
	t.Helper()
	matches, err := idx.FindByProperty(input)
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

func nodeIDs(nodes []graph.Node) []graph.NodeID {
	out := make([]graph.NodeID, 0, len(nodes))
	for _, node := range nodes {
		out = append(out, node.ID)
	}
	return out
}
