package hybrid

import (
	"testing"

	"github.com/google/uuid"
	graph "github.com/myceldb/mycel/internal/graph/model"
)

func TestMatchesFiltersLabelsAndNodeIDs(t *testing.T) {
	nodeID := uuid.New()
	node := graph.Node{ID: graph.NodeID(nodeID), Labels: []string{"Note", "Incident"}}
	matched, err := MatchesFilters(node, Filters{NodeLabels: []string{"Note"}, NodeIDs: []string{nodeID.String()}})
	if err != nil || !matched {
		t.Fatalf("MatchesFilters() = %v, %v; want true, nil", matched, err)
	}
	matched, err = MatchesFilters(node, Filters{NodeLabels: []string{"Missing"}})
	if err != nil || matched {
		t.Fatalf("missing label MatchesFilters() = %v, %v; want false, nil", matched, err)
	}
	matched, err = MatchesFilters(node, Filters{NodeIDs: []string{uuid.NewString()}})
	if err != nil || matched {
		t.Fatalf("missing node id MatchesFilters() = %v, %v; want false, nil", matched, err)
	}
}

func TestMatchesFiltersPropertyOperators(t *testing.T) {
	node := graph.Node{ID: graph.NodeID(uuid.New()), Properties: map[string]any{
		"status": "published",
		"tags":   []any{"k3s", "raft"},
		"meta": map[string]any{
			"owner": "ops",
		},
		"title": "raft recovery notes",
	}}
	cases := []struct {
		name    string
		filter  PropertyFilter
		matched bool
	}{
		{name: "exists", filter: PropertyFilter{Path: "status", Operator: FilterExists}, matched: true},
		{name: "equals", filter: PropertyFilter{Path: "status", Operator: FilterEquals, Values: []string{"published"}}, matched: true},
		{name: "not equals", filter: PropertyFilter{Path: "status", Operator: FilterNotEquals, Values: []string{"draft"}}, matched: true},
		{name: "in", filter: PropertyFilter{Path: "status", Operator: FilterIn, Values: []string{"draft", "published"}}, matched: true},
		{name: "contains array", filter: PropertyFilter{Path: "tags", Operator: FilterContains, Values: []string{"k3s"}}, matched: true},
		{name: "contains string", filter: PropertyFilter{Path: "title", Operator: FilterContains, Values: []string{"recovery"}}, matched: true},
		{name: "nested path", filter: PropertyFilter{Path: "meta.owner", Operator: FilterEquals, Values: []string{"ops"}}, matched: true},
		{name: "missing not equals is false", filter: PropertyFilter{Path: "missing", Operator: FilterNotEquals, Values: []string{"draft"}}, matched: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			matched, err := MatchesFilters(node, Filters{Properties: []PropertyFilter{tc.filter}})
			if err != nil || matched != tc.matched {
				t.Fatalf("MatchesFilters() = %v, %v; want %v, nil", matched, err, tc.matched)
			}
		})
	}
}

func TestMatchesFiltersLegacyPropertiesFallback(t *testing.T) {
	node := graph.Node{ID: graph.NodeID(uuid.New()), Props: map[string]any{"properties": map[string]any{"status": "legacy"}}}
	matched, err := MatchesFilters(node, Filters{Properties: []PropertyFilter{{Path: "status", Operator: FilterEquals, Values: []string{"legacy"}}}})
	if err != nil || !matched {
		t.Fatalf("MatchesFilters() = %v, %v; want true, nil", matched, err)
	}
}

func TestMatchesFiltersRejectsInvalidPropertyFilters(t *testing.T) {
	node := graph.Node{ID: graph.NodeID(uuid.New()), Properties: map[string]any{"status": "published"}}
	cases := []PropertyFilter{
		{Operator: FilterEquals, Values: []string{"published"}},
		{Path: "status", Operator: ""},
		{Path: "status", Operator: "bad"},
		{Path: "status", Operator: FilterEquals, Values: []string{"a", "b"}},
		{Path: "status", Operator: FilterIn},
		{Path: "status", Operator: FilterContains, Values: []string{"a", "b"}},
	}
	for _, filter := range cases {
		if matched, err := MatchesFilters(node, Filters{Properties: []PropertyFilter{filter}}); err == nil || matched {
			t.Fatalf("MatchesFilters(%#v) = %v, %v; want false, error", filter, matched, err)
		}
	}
}
