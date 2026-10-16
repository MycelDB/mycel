package logical

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/myceldb/mycel/internal/query/gql"
)

func TestFromGQLPlanPreservesTypedReadShape(t *testing.T) {
	plan, err := gql.Compile("MATCH (n:Note) WHERE n.score >= 10 AND n.status IS NOT NULL AND n.title STARTS WITH 'Al' RETURN DISTINCT n.payload.text AS text, COUNT(*) AS total, SUM(n.score) AS score ORDER BY n.meta.rank DESC OFFSET 1 FETCH FIRST 5 ROWS ONLY")
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	query, ok := FromGQLPlan(plan, NormalizeOptions{PageSize: 25, Cursor: "cursor-1", Safety: SafetyLimits{MaxNodes: 50, MaxEdges: 100}})
	if !ok {
		t.Fatal("FromGQLPlan() ok = false")
	}
	if query.AccessMode != AccessModeReadOnly || query.Start.Alias != "n" || !reflect.DeepEqual(query.Start.Labels, []string{"Note"}) {
		t.Fatalf("query header = %+v", query)
	}
	if query.Page != (PageSpec{Offset: 1, Limit: 5, Size: 25, Cursor: "cursor-1"}) || query.Safety != (SafetyLimits{MaxNodes: 50, MaxEdges: 100}) || !query.Distinct {
		t.Fatalf("query shaping = page %+v safety %+v distinct=%v", query.Page, query.Safety, query.Distinct)
	}
	if len(query.Returns) != 1 || query.Returns[0].Kind != ProjectionScalar || query.Returns[0].Field == nil || query.Returns[0].Field.Namespace != NamespacePayload || query.Returns[0].Field.Name != "text" {
		t.Fatalf("returns = %+v", query.Returns)
	}
	if len(query.Aggregates) != 2 || query.Aggregates[0].Function != AggregateCount || !query.Aggregates[0].Star || query.Aggregates[1].Function != AggregateSum || query.Aggregates[1].Argument == nil || query.Aggregates[1].Argument.Kind != ValueField {
		t.Fatalf("aggregates = %+v", query.Aggregates)
	}
	if len(query.OrderBy) != 1 || query.OrderBy[0].Direction != SortDescending || query.OrderBy[0].Value.Field == nil || query.OrderBy[0].Value.Field.Namespace != NamespaceMeta {
		t.Fatalf("order = %+v", query.OrderBy)
	}
	if got := len(query.PredicatePlan.PushdownEligible); got != 1 {
		t.Fatalf("pushdown count = %d, want 1: %+v", got, query.PredicatePlan)
	}
	if got := len(query.PredicatePlan.Residual); got != 2 {
		t.Fatalf("residual count = %d, want 2: %+v", got, query.PredicatePlan)
	}
}

func TestFromGQLPlanPreservesPathNodeAndEdgePatterns(t *testing.T) {
	plan, err := gql.Compile("MATCH path = (a:Note {rank: 1})-[:REFERENCES*1..3 {weight: 0.5}]->(b:Note {active: true}) RETURN GRAPH path, b.title AS title FETCH FIRST 10 ROWS ONLY")
	if err != nil {
		t.Fatalf("Compile(path) error = %v", err)
	}
	query, ok := FromGQLPlan(plan)
	if !ok {
		t.Fatal("FromGQLPlan(path) ok = false")
	}
	if query.PathAlias != "path" || !query.ReturnGraph || query.Page.Limit != 10 || len(query.Steps) != 1 {
		t.Fatalf("path query = %+v", query)
	}
	step := query.Steps[0]
	if !reflect.DeepEqual(step.Edge.Labels, []string{"REFERENCES"}) || step.Edge.Direction != DirectionOutgoing || step.Depth != (DepthBounds{Min: 1, Max: 3}) {
		t.Fatalf("step = %+v", step)
	}
	if got := step.Edge.Properties["weight"]; got.Kind != ValueNumber || got.Number != 0.5 {
		t.Fatalf("anonymous edge weight = %+v", got)
	}
	if len(step.Target.Properties) != 0 {
		t.Fatalf("bound target properties should be canonical predicates: %+v", step.Target.Properties)
	}
	if len(query.Returns) != 2 || query.Returns[0].Kind != ProjectionPath || query.Returns[1].Kind != ProjectionScalar {
		t.Fatalf("path returns = %+v", query.Returns)
	}

	plan, err = gql.Compile("MATCH (a:Note)-[r:REFERENCES {weight: 0.5}]->(b:Note {active: true}) RETURN r, b")
	if err != nil {
		t.Fatalf("Compile(pattern) error = %v", err)
	}
	query, ok = FromGQLPlan(plan)
	if !ok {
		t.Fatal("FromGQLPlan(pattern) ok = false")
	}
	if len(query.Steps) != 1 || query.Steps[0].Edge.Alias != "r" || len(query.Steps[0].Edge.Properties) != 0 {
		t.Fatalf("pattern step = %+v", query.Steps)
	}
	if got := len(query.PredicatePlan.PushdownEligible); got != 2 {
		t.Fatalf("bound edge/target property candidates = %d, want 2: %+v", got, query.PredicatePlan)
	}
	if len(query.Returns) != 2 || query.Returns[0].Kind != ProjectionEdge || query.Returns[1].Kind != ProjectionNode {
		t.Fatalf("pattern returns = %+v", query.Returns)
	}
}

func TestFromGQLPlanPreservesNamespacedTextAndSemanticPredicates(t *testing.T) {
	plan, err := gql.Compile("MATCH (n:Note) WHERE (TEXT_CONTAINS(n.payload.text, 'alpha') OR SEMANTIC_SIMILAR(n, 'memory', TOP 7)) AND n.meta.archived IS NULL RETURN n.meta.owner AS owner")
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	query, ok := FromGQLPlan(plan)
	if !ok {
		t.Fatal("FromGQLPlan() ok = false")
	}
	if len(query.Returns) != 1 || query.Returns[0].Field == nil || query.Returns[0].Field.Namespace != NamespaceMeta {
		t.Fatalf("returns = %+v", query.Returns)
	}
	if query.Predicate == nil || query.Predicate.Op != PredicateAndOp || len(query.Predicate.Terms) != 2 || (query.Predicate.Terms[0].Op != PredicateOrOp && query.Predicate.Terms[1].Op != PredicateOrOp) {
		t.Fatalf("predicate tree = %+v", query.Predicate)
	}
	leaves := append(append([]Leaf(nil), query.PredicatePlan.PushdownEligible...), query.PredicatePlan.Residual...)
	seenText, seenSemantic, seenMetaNull := false, false, false
	for _, leaf := range leaves {
		switch leaf.Kind {
		case LeafText:
			seenText = leaf.Field != nil && leaf.Field.Namespace == NamespacePayload && leaf.Field.Name == "text" && leaf.Query == "alpha"
		case LeafSemantic:
			seenSemantic = leaf.Field != nil && leaf.Field.Alias == "n" && leaf.Query == "memory" && leaf.Limit == 7
		case LeafNull:
			seenMetaNull = leaf.Field != nil && leaf.Field.Namespace == NamespaceMeta && leaf.Field.Name == "archived" && leaf.IsNull
		}
	}
	if !seenText || !seenSemantic || !seenMetaNull {
		t.Fatalf("predicate leaves = %+v", leaves)
	}
}

func TestFromGQLPlanRejectsMutationAsReadQuery(t *testing.T) {
	plan, err := gql.Compile("INSERT (:Note {title: 'new'})")
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	query, ok := FromGQLPlan(plan)
	if ok {
		t.Fatalf("FromGQLPlan() = %+v, true; want unsupported read shape", query)
	}
	if query.AccessMode != AccessModeReadWrite {
		t.Fatalf("access mode = %q, want %q", query.AccessMode, AccessModeReadWrite)
	}
}

func TestClassifyPredicatesSeparatesCandidatesAndResiduals(t *testing.T) {
	statusField := ParseFieldRef("n", "status")
	statusValue := NativeValue("active")
	statusOperand := FieldValue("n", "status")
	titleField := ParseFieldRef("n", "title")
	semanticField := ParseFieldRef("n", "payload.text")
	predicate := CombinePredicate(PredicateAndOp, []Predicate{
		*LeafPredicate(Leaf{Kind: LeafComparison, Left: &statusOperand, Right: &statusValue, Operator: "="}),
		*LeafPredicate(Leaf{Kind: LeafPropertyExists, Field: &statusField}),
		*LeafPredicate(Leaf{Kind: LeafString, Field: &titleField, Operator: "contains", Query: "alpha"}),
		*LeafPredicate(Leaf{Kind: LeafSemantic, Field: &semanticField, Query: "memory", Limit: 10}),
	})
	plan := ClassifyPredicates(predicate)
	if len(plan.PushdownEligible) != 3 || len(plan.Residual) != 1 {
		t.Fatalf("predicate plan = %+v", plan)
	}
}

func TestNeutralResultAndErrorVocabulary(t *testing.T) {
	largeSigned := NativeValue(int64(1 << 60))
	largeUnsigned := NativeValue(uint64(1 << 63))
	largeJSON := NativeValue(json.Number("1152921504606846976"))
	if largeSigned.Kind != ValueInteger || largeSigned.Integer != 1<<60 || largeUnsigned.Kind != ValueUnsigned || largeUnsigned.Unsigned != 1<<63 || !reflect.DeepEqual(largeJSON, largeSigned) {
		t.Fatalf("large typed values: signed=%+v unsigned=%+v json=%+v", largeSigned, largeUnsigned, largeJSON)
	}

	scalar := NativeValue(map[string]any{"ok": true, "scores": []any{1, 2.5}})
	node := Node{ID: "node-1", Properties: map[string]Value{"value": NativeValue(42)}}
	edge := Edge{ID: "edge-1", FromNodeID: "node-1", ToNodeID: "node-2"}
	path := Path{Nodes: []Node{node}, Edges: []Edge{edge}}
	tree := Tree{Roots: []TreeNode{{Node: node, Children: []TreeNode{{Node: Node{ID: "node-2"}}}}}}
	result := Result{
		Rows: []Row{{Fields: map[string]Cell{
			"null":   {Kind: CellNull},
			"scalar": {Kind: CellScalar, Scalar: &scalar},
			"node":   {Kind: CellNode, Node: &node},
			"edge":   {Kind: CellEdge, Edge: &edge},
			"path":   {Kind: CellPath, Path: &path},
			"tree":   {Kind: CellTree, Tree: &tree},
		}}},
		Graph: ResultGraph{Nodes: []Node{node}, Edges: []Edge{edge}},
		Counters: Counters{
			RowsReturned: 1, NodesInserted: 2, NodesUpdated: 3, NodesDeleted: 4,
			EdgesInserted: 5, EdgesUpdated: 6, EdgesDeleted: 7,
		},
		NextCursor:   "cursor-2",
		ReadMetadata: ReadMetadata{Consistency: "strong", ObservedRevision: 9},
		Diagnostics:  Diagnostics{PlanKind: "indexed", RowsReturned: 1, CandidateCount: 2},
	}
	if result.Rows[0].Fields["scalar"].Scalar.Object["scores"].List[1].Number != 2.5 || result.Rows[0].Fields["path"].Path.Edges[0].ID != "edge-1" || result.Rows[0].Fields["tree"].Tree.Roots[0].Children[0].Node.ID != "node-2" {
		t.Fatalf("result = %+v", result)
	}

	cause := errors.New("backend unavailable")
	err := NewError(ErrorDependencyFailure, "scan nodes", "", cause)
	if !errors.Is(err, cause) || err.Code != ErrorDependencyFailure || err.Error() != "scan nodes: backend unavailable" {
		t.Fatalf("query error = %#v (%v)", err, err)
	}
}
