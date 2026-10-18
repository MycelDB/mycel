package engine_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
	graph "github.com/myceldb/mycel/internal/graph/model"
	graphquery "github.com/myceldb/mycel/internal/graph/query"
	queryengine "github.com/myceldb/mycel/internal/query/engine"
	querylogical "github.com/myceldb/mycel/internal/query/logical"
	queryphysical "github.com/myceldb/mycel/internal/query/physical"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
)

type indexedTestAccess struct {
	nodes          map[string]graph.Node
	labels         map[string][]graph.Node
	tags           map[string][]graph.Node
	indexes        map[string][]graph.Node
	getCalls       int
	labelCalls     int
	tagCalls       int
	orderedCalls   int
	configureCalls int
	limits         []int
	orderedScans   []graphquery.OrderedNodePropertyScan
	txs            []sessionmodel.GraphTransaction
	err            error
}

func (a *indexedTestAccess) ReadViewIdentity(ctx context.Context, tx sessionmodel.GraphTransaction) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	a.txs = append(a.txs, tx)
	return "0", nil
}

func (a *indexedTestAccess) GetNode(_ context.Context, tx sessionmodel.GraphTransaction, id string) (graph.Node, error) {
	a.getCalls++
	a.txs = append(a.txs, tx)
	if a.err != nil {
		return graph.Node{}, a.err
	}
	node, ok := a.nodes[id]
	if !ok {
		return graph.Node{}, fmt.Errorf("node %s not found", id)
	}
	return node, nil
}

func (a *indexedTestAccess) GetEdge(context.Context, sessionmodel.GraphTransaction, string) (graph.Edge, error) {
	return graph.Edge{}, errors.New("unexpected edge read")
}
func (a *indexedTestAccess) ListNodes(context.Context, sessionmodel.GraphTransaction, int, string) ([]graph.Node, string, error) {
	return nil, "", errors.New("unexpected node list")
}
func (a *indexedTestAccess) ScanNodes(context.Context, sessionmodel.GraphTransaction, graphquery.NodeScan) ([]graph.Node, string, graphquery.IndexedReadStats, error) {
	return nil, "", graphquery.IndexedReadStats{}, errors.New("unexpected node scan")
}
func (a *indexedTestAccess) ListEdges(context.Context, sessionmodel.GraphTransaction, int, string) ([]graph.Edge, string, error) {
	return nil, "", errors.New("unexpected edge list")
}

func (a *indexedTestAccess) ConfigureIndexes(_ context.Context, tx sessionmodel.GraphTransaction, _ string, _ []schemamodel.IndexDefinition) error {
	a.configureCalls++
	a.txs = append(a.txs, tx)
	return a.err
}

func (a *indexedTestAccess) ScanLabel(_ context.Context, tx sessionmodel.GraphTransaction, scan graphquery.LabelScan) ([]graph.Node, string, graphquery.IndexedReadStats, error) {
	a.labelCalls++
	a.txs = append(a.txs, tx)
	a.limits = append(a.limits, scan.Limit)
	return indexedTestPage(a.labels[scan.Label], scan.Limit, scan.Cursor, "LabelIndexScan", "label:"+scan.Label, a.err)
}

func (a *indexedTestAccess) ScanTag(_ context.Context, tx sessionmodel.GraphTransaction, scan graphquery.TagScan) ([]graph.Node, string, graphquery.IndexedReadStats, error) {
	a.tagCalls++
	a.txs = append(a.txs, tx)
	a.limits = append(a.limits, scan.Limit)
	return indexedTestPage(a.tags[scan.Tag], scan.Limit, scan.Cursor, "TagIndexScan", "tag:"+scan.Tag, a.err)
}

func (a *indexedTestAccess) ScanNodePropertyOrdered(_ context.Context, tx sessionmodel.GraphTransaction, scan graphquery.OrderedNodePropertyScan) ([]graph.Node, string, graphquery.IndexedReadStats, error) {
	a.orderedCalls++
	a.txs = append(a.txs, tx)
	a.limits = append(a.limits, scan.Limit)
	a.orderedScans = append(a.orderedScans, scan)
	nodes := append([]graph.Node(nil), a.indexes[scan.IndexName]...)
	if scan.Direction == schemamodel.IndexSortDirectionDesc {
		for left, right := 0, len(nodes)-1; left < right; left, right = left+1, right-1 {
			nodes[left], nodes[right] = nodes[right], nodes[left]
		}
	}
	return indexedTestPage(nodes, scan.Limit, scan.Cursor, "OrderedNodePropertyIndexScan", scan.IndexName, a.err)
}

func indexedTestPage(nodes []graph.Node, limit int, cursor string, plan string, index string, injected error) ([]graph.Node, string, graphquery.IndexedReadStats, error) {
	if injected != nil {
		return nil, "", graphquery.IndexedReadStats{}, injected
	}
	if limit <= 0 {
		return nil, "", graphquery.IndexedReadStats{}, fmt.Errorf("unbounded limit %d", limit)
	}
	start := 0
	if cursor != "" {
		if cursor == "opaque-index-key" {
			cursor = "0"
		}
		parsed, err := strconv.Atoi(cursor)
		if err != nil || parsed < 0 || parsed > len(nodes) {
			return nil, "", graphquery.IndexedReadStats{}, fmt.Errorf("bad cursor %q", cursor)
		}
		start = parsed
	}
	end := start + limit
	if end > len(nodes) {
		end = len(nodes)
	}
	next := ""
	if end < len(nodes) {
		next = strconv.Itoa(end)
	}
	page := append([]graph.Node(nil), nodes[start:end]...)
	return page, next, graphquery.IndexedReadStats{Plan: plan, IndexName: index, IndexEntriesScanned: len(page), NodesLoaded: len(page), NextCursorKind: "index_key"}, nil
}

type traversalTestAccess struct {
	*indexedTestAccess
	edges          map[string][]graph.Edge
	adjacencyCalls int
	adjacencyScans []graphquery.AdjacencyScan
}

func (a *traversalTestAccess) ScanAdjacency(_ context.Context, tx sessionmodel.GraphTransaction, scan graphquery.AdjacencyScan) ([]graph.Edge, string, graphquery.IndexedReadStats, error) {
	a.adjacencyCalls++
	a.txs = append(a.txs, tx)
	a.adjacencyScans = append(a.adjacencyScans, scan)
	a.limits = append(a.limits, scan.Limit)
	if a.err != nil {
		return nil, "", graphquery.IndexedReadStats{}, a.err
	}
	if scan.Limit <= 0 {
		return nil, "", graphquery.IndexedReadStats{}, fmt.Errorf("unbounded adjacency limit %d", scan.Limit)
	}
	edges := a.edges[scan.NodeID]
	start := 0
	if scan.Cursor != "" {
		parsed, err := strconv.Atoi(scan.Cursor)
		if err != nil || parsed < 0 || parsed > len(edges) {
			return nil, "", graphquery.IndexedReadStats{}, fmt.Errorf("bad adjacency cursor %q", scan.Cursor)
		}
		start = parsed
	}
	end := start + scan.Limit
	if end > len(edges) {
		end = len(edges)
	}
	next := ""
	if end < len(edges) {
		next = strconv.Itoa(end)
	}
	page := append([]graph.Edge(nil), edges[start:end]...)
	return page, next, graphquery.IndexedReadStats{IndexName: string(scan.Direction) + ":" + scan.Label, IndexEntriesScanned: len(page), EdgesLoaded: len(page), AdjacencyScanCalls: 1}, nil
}

type indexedTestSemantic struct {
	batch queryengine.SemanticCandidateBatch
	err   error
	calls int
	input queryengine.SemanticCandidateRequest
}

func (s *indexedTestSemantic) SearchCandidates(_ context.Context, input queryengine.SemanticCandidateRequest) (queryengine.SemanticCandidateBatch, error) {
	s.calls++
	s.input = input
	return s.batch, s.err
}

func TestEngineExecutesNodeIDLookupWithPlanBoundContinuation(t *testing.T) {
	nodes := indexedNodes("a", "b", "c", "d")
	access := newIndexedTestAccess(nodes)
	engine := queryengine.New(queryengine.Ports{Entities: access, Indexes: access})
	tx := indexedTransaction()
	query := indexedBaseQuery()
	query.Start.NodeIDs = []string{nodes[0].ID.String(), nodes[1].ID.String(), nodes[2].ID.String(), nodes[3].ID.String()}
	query.Page.Size = 1
	query.Page.Limit = 3

	firstPrepared := prepareIndexed(t, engine, tx, query)
	if firstPrepared.Physical.Family != queryphysical.FamilyNodeIDLookup || firstPrepared.RequiresLegacy() {
		t.Fatalf("prepared = %#v", firstPrepared)
	}
	first, err := engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx, Budget: queryengine.WorkBudget{BatchSize: 2}}, firstPrepared)
	if err != nil {
		t.Fatalf("first execute: %v", err)
	}
	if got := resultNames(first); fmt.Sprint(got) != "[a]" || first.NextCursor == "" || first.Diagnostics.CandidateCount != 2 {
		t.Fatalf("first result = %#v, names=%v", first, got)
	}
	if access.getCalls != 2 {
		t.Fatalf("first GetNode calls = %d, want bounded lookahead batch 2", access.getCalls)
	}

	query.Page.Cursor = first.NextCursor
	secondPrepared := prepareIndexed(t, engine, tx, query)
	second, err := engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx, Budget: queryengine.WorkBudget{BatchSize: 2}}, secondPrepared)
	if err != nil {
		t.Fatalf("second execute: %v", err)
	}
	if got := resultNames(second); fmt.Sprint(got) != "[b]" || second.NextCursor == "" {
		t.Fatalf("second result = %#v, names=%v", second, got)
	}

	query.Page.Cursor = second.NextCursor
	thirdPrepared := prepareIndexed(t, engine, tx, query)
	third, err := engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx, Budget: queryengine.WorkBudget{BatchSize: 2}}, thirdPrepared)
	if err != nil {
		t.Fatalf("third execute: %v", err)
	}
	if got := resultNames(third); fmt.Sprint(got) != "[c]" || third.NextCursor != "" {
		t.Fatalf("third result = %#v, names=%v", third, got)
	}
	for _, seenTx := range access.txs {
		if seenTx != tx {
			t.Fatalf("access transaction = %#v, want %#v", seenTx, tx)
		}
	}
}

func TestEngineLabelAndTagScansUseNativeSources(t *testing.T) {
	nodes := indexedNodes("a", "b", "c", "d")
	nodes[0].Properties[graph.NodePropTags] = []string{"red"}
	nodes[1].Properties[graph.NodePropTags] = []string{"red", "blue"}
	nodes[2].Properties[graph.NodePropTags] = []string{"blue"}
	nodes[3].Properties[graph.NodePropTags] = []string{"green"}
	access := newIndexedTestAccess(nodes)
	access.tags["red"] = []graph.Node{nodes[0], nodes[1]}
	access.tags["blue"] = []graph.Node{nodes[1], nodes[2]}
	engine := queryengine.New(queryengine.Ports{Entities: access, Indexes: access})
	tx := indexedTransaction()

	labelQuery := indexedBaseQuery()
	labelQuery.Page.Size = 1
	labelQuery.Page.Cursor = "1" // QRX6 raw offset compatibility.
	labelPrepared := prepareIndexed(t, engine, tx, labelQuery)
	labelResult, err := engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx, Budget: queryengine.WorkBudget{BatchSize: 2}}, labelPrepared)
	if err != nil {
		t.Fatalf("label execute: %v", err)
	}
	if got := resultNames(labelResult); fmt.Sprint(got) != "[b]" || labelResult.NextCursor == "" || access.labelCalls == 0 {
		t.Fatalf("label result=%#v names=%v calls=%d", labelResult, got, access.labelCalls)
	}
	labelQuery.Page.Cursor = "shapeidx:eyJpbmRleF9jdXJzb3IiOiIyIiwicm93c19yZXR1cm5lZCI6Mn0"
	labelPrepared = prepareIndexed(t, engine, tx, labelQuery)
	_, err = engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx, Budget: queryengine.WorkBudget{BatchSize: 2}}, labelPrepared)
	assertEngineQueryCode(t, err, querylogical.ErrorStaleCursor)

	tagField := querylogical.FieldRef{Alias: "n", Namespace: querylogical.NamespaceProperties, Name: "tags"}
	red := querylogical.LeafPredicate(querylogical.Leaf{Kind: querylogical.LeafHasTag, Field: &tagField, Tag: "red"})
	blue := querylogical.LeafPredicate(querylogical.Leaf{Kind: querylogical.LeafHasTag, Field: &tagField, Tag: "blue"})
	for _, test := range []struct {
		name      string
		predicate *querylogical.Predicate
		want      string
	}{
		{name: "intersection", predicate: querylogical.CombinePredicate(querylogical.PredicateAndOp, []querylogical.Predicate{*red, *blue}), want: "[b]"},
		{name: "union", predicate: querylogical.CombinePredicate(querylogical.PredicateOrOp, []querylogical.Predicate{*red, *blue}), want: "[a b c]"},
	} {
		t.Run(test.name, func(t *testing.T) {
			query := indexedBaseQuery()
			query.Predicate = test.predicate
			query.Page.Size = 10
			prepared := prepareIndexed(t, engine, tx, query)
			result, err := engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx, Budget: queryengine.WorkBudget{BatchSize: 1}}, prepared)
			if err != nil {
				t.Fatalf("execute: %v", err)
			}
			if got := resultNames(result); fmt.Sprint(got) != test.want {
				t.Fatalf("names = %v, want %s", got, test.want)
			}
		})
	}
	if access.tagCalls == 0 {
		t.Fatal("tag scans were not called")
	}

	tagQuery := indexedBaseQuery()
	tagQuery.Predicate = red
	tagQuery.Page.Size = 1
	tagPrepared := prepareIndexed(t, engine, tx, tagQuery)
	firstTag, err := engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx, Budget: queryengine.WorkBudget{BatchSize: 1}}, tagPrepared)
	if err != nil || fmt.Sprint(resultNames(firstTag)) != "[a]" || firstTag.NextCursor == "" {
		t.Fatalf("first tag page=%#v error=%v", firstTag, err)
	}
	tagQuery.Page.Cursor = firstTag.NextCursor
	tagPrepared = prepareIndexed(t, engine, tx, tagQuery)
	secondTag, err := engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx, Budget: queryengine.WorkBudget{BatchSize: 1}}, tagPrepared)
	if err != nil || fmt.Sprint(resultNames(secondTag)) != "[b]" || secondTag.NextCursor != "" {
		t.Fatalf("second tag page=%#v error=%v", secondTag, err)
	}
}

func TestEngineRejectsIndexedCursorForChangedSnapshotOrRequest(t *testing.T) {
	nodes := indexedNodes("a", "b", "c")
	access := newIndexedTestAccess(nodes)
	engine := queryengine.New(queryengine.Ports{Entities: access, Indexes: access})
	tx := indexedTransaction()
	query := indexedBaseQuery()
	query.Page.Size = 1
	prepared := prepareIndexed(t, engine, tx, query)
	first, err := engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx, Budget: queryengine.WorkBudget{BatchSize: 2}}, prepared)
	if err != nil || first.NextCursor == "" {
		t.Fatalf("first result=%#v error=%v", first, err)
	}

	query.Page.Cursor = first.NextCursor
	for _, field := range []struct {
		name  string
		value any
	}{
		{name: "mode", value: "output"},
		{name: "page_cursor", value: "forged"},
		{name: "page_offset", value: float64(0)},
		{name: "rows_returned", value: float64(999)},
	} {
		tamperedQuery := query
		tamperedQuery.Page.Cursor = tamperIndexedCursor(t, first.NextCursor, field.name, field.value)
		tampered := prepareIndexed(t, engine, tx, tamperedQuery)
		_, err = engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx}, tampered)
		assertEngineQueryCode(t, err, querylogical.ErrorStaleCursor)
	}

	query.Returns[0].OutputName = "changed"
	changed := prepareIndexed(t, engine, tx, query)
	_, err = engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx}, changed)
	assertEngineQueryCode(t, err, querylogical.ErrorStaleCursor)

	query.Returns[0].OutputName = "name"
	changedTx := tx
	changedTx.BaseRevision++
	changed = prepareIndexed(t, engine, changedTx, query)
	_, err = engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: changedTx}, changed)
	assertEngineQueryCode(t, err, querylogical.ErrorStaleCursor)

	schema := indexedSchema()
	schema.Version = "qrx7-schema-2"
	changed, err = engine.Prepare(context.Background(), queryengine.PlanningContext{
		Transaction: tx, Schema: &schema,
		Domain: queryengine.DomainPolicy{BroadSearchable: true, ExplicitSearchable: true, SemanticSearchable: true, SemanticCandidatesAvailable: true},
	}, querylogical.ReadStatement(query))
	if err != nil {
		t.Fatalf("prepare changed schema: %v", err)
	}
	_, err = engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx}, changed)
	assertEngineQueryCode(t, err, querylogical.ErrorStaleCursor)
}

func TestEngineRejectsIndexedCursorWhenParametersChange(t *testing.T) {
	nodes := indexedNodes("a", "b", "c")
	access := newIndexedTestAccess(nodes)
	access.indexes["document_name"] = nodes
	engine := queryengine.New(queryengine.Ports{Entities: access, Indexes: access})
	tx := indexedTransaction()
	query := indexedBaseQuery()
	left := querylogical.FieldValue("n", "name")
	right := querylogical.Value{Kind: querylogical.ValueParameter, Parameter: "wanted"}
	query.Predicate = querylogical.LeafPredicate(querylogical.Leaf{Kind: querylogical.LeafComparison, Left: &left, Right: &right, Operator: "="})
	query.Page.Size = 1
	schema := indexedSchema()
	planning := queryengine.PlanningContext{
		Transaction: tx, Schema: &schema, Parameters: map[string]querylogical.Value{"wanted": querylogical.NativeValue("b")},
		Domain: queryengine.DomainPolicy{BroadSearchable: true, ExplicitSearchable: true},
	}
	prepared, err := engine.Prepare(context.Background(), planning, querylogical.ReadStatement(query))
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	first, err := engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx, Budget: queryengine.WorkBudget{BatchSize: 2}}, prepared)
	if err != nil || first.NextCursor == "" {
		t.Fatalf("first=%#v error=%v", first, err)
	}
	query.Page.Cursor = first.NextCursor
	planning.Parameters["wanted"] = querylogical.NativeValue("c")
	prepared, err = engine.Prepare(context.Background(), planning, querylogical.ReadStatement(query))
	if err != nil {
		t.Fatalf("reprepare: %v", err)
	}
	_, err = engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx}, prepared)
	assertEngineQueryCode(t, err, querylogical.ErrorStaleCursor)
}

func TestEngineOrderedIndexFollowsPagesUntilResidualDemand(t *testing.T) {
	nodes := indexedNodes("keep-a", "drop-b", "drop-c", "keep-d", "keep-e")
	access := newIndexedTestAccess(nodes)
	access.indexes["document_rank"] = nodes
	engine := queryengine.New(queryengine.Ports{Entities: access, Indexes: access})
	tx := indexedTransaction()
	query := indexedBaseQuery()
	query.Source = querylogical.SourceGQL
	query.OrderBy = []querylogical.Order{{Value: querylogical.FieldValue("n", "rank"), Direction: querylogical.SortAscending}}
	name := querylogical.FieldValue("n", "name")
	query.Predicate = querylogical.LeafPredicate(querylogical.Leaf{Kind: querylogical.LeafString, Operator: "starts_with", Operand: &name, Query: "keep"})
	query.Page.Size = 2
	prepared := prepareIndexed(t, engine, tx, query)
	if prepared.Physical.Family != queryphysical.FamilyIndexedOrder || prepared.Physical.ResidualPredicate == nil {
		t.Fatalf("plan = %#v", prepared.Physical)
	}
	result, err := engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx, Budget: queryengine.WorkBudget{BatchSize: 2}}, prepared)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if got := resultNames(result); fmt.Sprint(got) != "[keep-a keep-d]" || result.NextCursor == "" {
		t.Fatalf("result=%#v names=%v", result, got)
	}
	if access.orderedCalls != 3 || access.configureCalls != 1 {
		t.Fatalf("ordered/configure calls = %d/%d, want 3/1", access.orderedCalls, access.configureCalls)
	}
	for _, limit := range access.limits {
		if limit <= 0 || limit > 2 {
			t.Fatalf("scan limit = %d, want finite batch limit", limit)
		}
	}
}

func TestEngineAcceptsCompatibleLegacyShapedOrderedCursor(t *testing.T) {
	nodes := indexedNodes("a", "b", "c")
	access := newIndexedTestAccess(nodes)
	access.indexes["document_name"] = nodes
	engine := queryengine.New(queryengine.Ports{Entities: access, Indexes: access})
	tx := indexedTransaction()
	query := indexedBaseQuery()
	query.Predicate = indexedComparison("n", "name", "=", querylogical.NativeValue("a"))
	query.Page.Size = 1
	query.Page.Cursor = "shapeidx:eyJpbmRleF9jdXJzb3IiOiIwIiwicm93c19yZXR1cm5lZCI6MH0"
	prepared := prepareIndexed(t, engine, tx, query)
	result, err := engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx, Budget: queryengine.WorkBudget{BatchSize: 2}}, prepared)
	if err != nil {
		t.Fatalf("execute shaped cursor: %v", err)
	}
	if got := resultNames(result); fmt.Sprint(got) != "[a]" || !strings.HasPrefix(result.NextCursor, "qrx7:") {
		t.Fatalf("result=%#v names=%v", result, got)
	}
}

func TestEngineAcceptsRawCursorOnlyForCompatibleOrderedFamily(t *testing.T) {
	nodes := indexedNodes("a", "b", "c")
	access := newIndexedTestAccess(nodes)
	access.indexes["document_name"] = nodes
	engine := queryengine.New(queryengine.Ports{Entities: access, Indexes: access})
	tx := indexedTransaction()

	equality := indexedBaseQuery()
	equality.Predicate = indexedComparison("n", "name", "=", querylogical.NativeValue("a"))
	equality.Page.Size = 1
	equality.Page.Cursor = "opaque-index-key"
	prepared := prepareIndexed(t, engine, tx, equality)
	result, err := engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx, Budget: queryengine.WorkBudget{BatchSize: 2}}, prepared)
	if err != nil || fmt.Sprint(resultNames(result)) != "[a]" || !strings.HasPrefix(result.NextCursor, "qrx7:") {
		t.Fatalf("compatible raw result=%#v error=%v", result, err)
	}

	label := indexedBaseQuery()
	label.Page.Cursor = "opaque-index-key"
	prepared = prepareIndexed(t, engine, tx, label)
	_, err = engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx}, prepared)
	assertEngineQueryCode(t, err, querylogical.ErrorStaleCursor)
}

func TestEngineExecutesEqualityAndRangeBoundsThroughOrderedIndex(t *testing.T) {
	nodes := indexedNodes("a", "b", "c")
	tx := indexedTransaction()
	for _, test := range []struct {
		name       string
		predicate  *querylogical.Predicate
		family     queryphysical.Family
		index      string
		scanNodes  []graph.Node
		wantLow    bool
		wantHigh   bool
		wantLowEx  bool
		wantHighEx bool
	}{
		{name: "equality", predicate: indexedComparison("n", "name", "=", querylogical.NativeValue("b")), family: queryphysical.FamilyIndexedEquality, index: "document_name", scanNodes: nodes[1:2], wantLow: true, wantHigh: true},
		{name: "lower range", predicate: indexedComparison("n", "rank", ">", querylogical.NativeValue(1)), family: queryphysical.FamilyIndexedPredicate, index: "document_rank", scanNodes: nodes[1:], wantLow: true, wantLowEx: true},
		{name: "upper range", predicate: indexedComparison("n", "rank", "<=", querylogical.NativeValue(2)), family: queryphysical.FamilyIndexedPredicate, index: "document_rank", scanNodes: nodes[:2], wantHigh: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			access := newIndexedTestAccess(nodes)
			access.indexes[test.index] = test.scanNodes
			engine := queryengine.New(queryengine.Ports{Entities: access, Indexes: access})
			query := indexedBaseQuery()
			query.Predicate = test.predicate
			query.Page.Size = 10
			prepared := prepareIndexed(t, engine, tx, query)
			if prepared.Physical.Family != test.family || prepared.RequiresLegacy() {
				t.Fatalf("plan = %#v", prepared.Physical)
			}
			result, err := engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx, Budget: queryengine.WorkBudget{BatchSize: 2}}, prepared)
			if err != nil {
				t.Fatalf("execute: %v", err)
			}
			if len(result.Rows) != len(test.scanNodes) || len(access.orderedScans) == 0 {
				t.Fatalf("rows=%d scans=%#v", len(result.Rows), access.orderedScans)
			}
			scan := access.orderedScans[0]
			if scan.HasLow != test.wantLow || scan.HasHigh != test.wantHigh || scan.LowExclusive != test.wantLowEx || scan.HighExclusive != test.wantHighEx {
				t.Fatalf("scan bounds = %#v", scan)
			}
		})
	}
}

func TestEngineCompoundIndexIntersectsAndUnionsDeterministically(t *testing.T) {
	nodes := indexedNodes("a", "b", "c", "d")
	access := newIndexedTestAccess(nodes)
	access.indexes["document_name"] = []graph.Node{nodes[0], nodes[1], nodes[2]}
	access.indexes["document_rank"] = []graph.Node{nodes[1], nodes[2], nodes[3]}
	engine := queryengine.New(queryengine.Ports{Entities: access, Indexes: access})
	tx := indexedTransaction()
	query := indexedBaseQuery()
	nameEq := indexedComparison("n", "name", "=", querylogical.NativeValue("b"))
	rankLow := indexedComparison("n", "rank", ">=", querylogical.NativeValue(2))
	rankHigh := indexedComparison("n", "rank", "<=", querylogical.NativeValue(3))
	query.Predicate = querylogical.CombinePredicate(querylogical.PredicateOrOp, []querylogical.Predicate{
		*nameEq,
		*querylogical.CombinePredicate(querylogical.PredicateAndOp, []querylogical.Predicate{*rankLow, *rankHigh}),
	})
	query.Page.Size = 10
	prepared := prepareIndexed(t, engine, tx, query)
	if prepared.Physical.Family != queryphysical.FamilyIndexedPredicate {
		t.Fatalf("plan = %#v", prepared.Physical)
	}
	result, err := engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx, Budget: queryengine.WorkBudget{BatchSize: 2}}, prepared)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if got := resultNames(result); fmt.Sprint(got) != "[b c]" {
		t.Fatalf("names = %v, result=%#v", got, result)
	}
	if result.Diagnostics.RetainedMemoryBytes != 0 || access.orderedCalls == 0 {
		t.Fatalf("diagnostics=%#v calls=%d", result.Diagnostics, access.orderedCalls)
	}
	for _, limit := range access.limits {
		if limit == 0 {
			t.Fatal("compound scan used unbounded limit")
		}
	}
}

func TestEngineSemanticCandidatesPreserveRankAndFailClosedHydration(t *testing.T) {
	nodes := indexedNodes("first", "second", "third")
	access := newIndexedTestAccess(nodes)
	semantic := &indexedTestSemantic{batch: queryengine.SemanticCandidateBatch{Candidates: []queryengine.SemanticCandidate{
		{NodeID: nodes[1].ID.String(), Score: .9}, {NodeID: nodes[0].ID.String(), Score: .8}, {NodeID: nodes[1].ID.String(), Score: .7}, {NodeID: nodes[2].ID.String(), Score: .6},
	}}}
	engine := queryengine.New(queryengine.Ports{Entities: access, Indexes: access, SemanticCandidates: semantic})
	tx := indexedTransaction()
	query := indexedBaseQuery()
	field := querylogical.FieldRef{Alias: "n", Namespace: querylogical.NamespaceProperties}
	query.Predicate = querylogical.LeafPredicate(querylogical.Leaf{Kind: querylogical.LeafSemantic, Field: &field, Query: "graph", Limit: 4})
	query.Page.Size = 1
	prepared := prepareIndexed(t, engine, tx, query)
	result, err := engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx, Budget: queryengine.WorkBudget{BatchSize: 8}}, prepared)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if got := resultNames(result); fmt.Sprint(got) != "[second]" || result.NextCursor == "" || semantic.calls != 1 || access.getCalls != 2 {
		t.Fatalf("result=%#v names=%v semantic calls=%d hydration calls=%d", result, got, semantic.calls, access.getCalls)
	}
	query.Page.Cursor = result.NextCursor
	preparedNext := prepareIndexed(t, engine, tx, query)
	next, err := engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx, Budget: queryengine.WorkBudget{BatchSize: 8}}, preparedNext)
	if err != nil || fmt.Sprint(resultNames(next)) != "[first]" {
		t.Fatalf("continuation=%#v error=%v", next, err)
	}

	query.Page.Cursor = ""
	prepared = prepareIndexed(t, engine, tx, query)
	access.err = errors.New("hydrate failed")
	_, err = engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx}, prepared)
	assertEngineQueryCode(t, err, querylogical.ErrorDependencyFailure)
}

func TestEngineExecutesAdjacencyWithSecureSourceContinuation(t *testing.T) {
	nodes := indexedNodes("start", "first", "second")
	base := newIndexedTestAccess(nodes)
	access := &traversalTestAccess{indexedTestAccess: base, edges: map[string][]graph.Edge{}}
	access.edges[nodes[0].ID.String()] = []graph.Edge{
		indexedEdge(1, nodes[0], nodes[1], "LINKS"),
		indexedEdge(2, nodes[0], nodes[2], "LINKS"),
	}
	engine := queryengine.New(queryengine.Ports{Entities: access, Indexes: access, Traversals: access})
	tx := indexedTransaction()
	field := querylogical.FieldRef{Alias: "m", Namespace: querylogical.NamespaceProperties, Name: "name"}
	query := querylogical.Query{
		Source: querylogical.SourceStructured, AccessMode: querylogical.AccessModeReadOnly,
		Start:   querylogical.NodePattern{Alias: "n", NodeIDs: []string{nodes[0].ID.String()}, Labels: []string{"Document"}},
		Steps:   []querylogical.Step{{Edge: querylogical.EdgePattern{Alias: "e", Labels: []string{"LINKS"}, Direction: querylogical.DirectionOutgoing}, Depth: querylogical.DepthBounds{Min: 1, Max: 1}, Target: querylogical.NodePattern{Alias: "m", Labels: []string{"Document"}}}},
		Returns: []querylogical.Projection{{Kind: querylogical.ProjectionScalar, Alias: "m", Field: &field, OutputName: "name"}},
		Page:    querylogical.PageSpec{Size: 1},
	}
	prepared := prepareIndexed(t, engine, tx, query)
	if prepared.Physical.Family != queryphysical.FamilyIndexedAdjacency || prepared.RequiresLegacy() {
		t.Fatalf("prepared = %#v", prepared)
	}
	first, err := engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx, Budget: queryengine.WorkBudget{BatchSize: 1}}, prepared)
	if err != nil || fmt.Sprint(resultNames(first)) != "[first]" || !strings.HasPrefix(first.NextCursor, "qrx7:") {
		t.Fatalf("first=%#v err=%v", first, err)
	}
	query.Page.Cursor = first.NextCursor
	second, err := engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx, Budget: queryengine.WorkBudget{BatchSize: 1}}, prepareIndexed(t, engine, tx, query))
	if err != nil || fmt.Sprint(resultNames(second)) != "[second]" || second.NextCursor != "" {
		t.Fatalf("second=%#v err=%v", second, err)
	}
	query.Page.Cursor = "1" // legacy raw adjacency cursor
	legacy, err := engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx, Budget: queryengine.WorkBudget{BatchSize: 1}}, prepareIndexed(t, engine, tx, query))
	if err != nil || fmt.Sprint(resultNames(legacy)) != "[second]" {
		t.Fatalf("legacy=%#v err=%v", legacy, err)
	}
	for _, scan := range access.adjacencyScans {
		if scan.Limit <= 0 {
			t.Fatalf("unbounded adjacency scan: %#v", scan)
		}
	}
	for _, got := range access.txs {
		if got != tx {
			t.Fatalf("transaction = %#v, want %#v", got, tx)
		}
	}
}

func TestEngineExecutesBoundedPathsWithOutputContinuation(t *testing.T) {
	nodes := indexedNodes("start", "left", "right", "leaf")
	base := newIndexedTestAccess(nodes)
	access := &traversalTestAccess{indexedTestAccess: base, edges: map[string][]graph.Edge{}}
	access.edges[nodes[0].ID.String()] = []graph.Edge{indexedEdge(1, nodes[0], nodes[1], "LINKS"), indexedEdge(2, nodes[0], nodes[2], "LINKS")}
	access.edges[nodes[1].ID.String()] = []graph.Edge{indexedEdge(3, nodes[1], nodes[3], "LINKS")}
	access.edges[nodes[2].ID.String()] = []graph.Edge{indexedEdge(4, nodes[2], nodes[3], "LINKS")}
	access.edges[nodes[3].ID.String()] = []graph.Edge{indexedEdge(5, nodes[3], nodes[0], "LINKS")}
	engine := queryengine.New(queryengine.Ports{Entities: access, Indexes: access, Traversals: access})
	tx := indexedTransaction()
	field := querylogical.FieldRef{Alias: "m", Namespace: querylogical.NamespaceProperties, Name: "name"}
	query := querylogical.Query{
		Source: querylogical.SourceStructured, AccessMode: querylogical.AccessModeReadOnly,
		Start: querylogical.NodePattern{Alias: "n", NodeIDs: []string{nodes[0].ID.String()}, Labels: []string{"Document"}}, PathAlias: "path",
		Steps:   []querylogical.Step{{Edge: querylogical.EdgePattern{Alias: "e", Labels: []string{"LINKS"}, Direction: querylogical.DirectionOutgoing}, Depth: querylogical.DepthBounds{Min: 1, Max: 3}, Target: querylogical.NodePattern{Alias: "m", Labels: []string{"Document"}}}},
		Returns: []querylogical.Projection{{Kind: querylogical.ProjectionScalar, Alias: "m", Field: &field, OutputName: "name"}, {Kind: querylogical.ProjectionPath, Alias: "path", OutputName: "path"}},
		Page:    querylogical.PageSpec{Size: 2},
	}
	prepared := prepareIndexed(t, engine, tx, query)
	if prepared.Physical.Family != queryphysical.FamilyIndexedPath || prepared.RequiresLegacy() {
		t.Fatalf("prepared = %#v", prepared)
	}
	first, err := engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx, Budget: queryengine.WorkBudget{BatchSize: 2}}, prepared)
	if err != nil || fmt.Sprint(resultNames(first)) != "[left leaf]" || first.NextCursor == "" {
		t.Fatalf("first=%#v err=%v", first, err)
	}
	if path := first.Rows[1].Fields["path"].Path; path == nil || len(path.Nodes) != 3 || len(path.Edges) != 2 {
		t.Fatalf("path = %#v", path)
	}
	query.Page.Cursor = first.NextCursor
	second, err := engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx, Budget: queryengine.WorkBudget{BatchSize: 2}}, prepareIndexed(t, engine, tx, query))
	if err != nil || fmt.Sprint(resultNames(second)) != "[right leaf]" || second.NextCursor != "" {
		t.Fatalf("second=%#v err=%v", second, err)
	}
	if first.Diagnostics.TraversalDepth != 2 || access.adjacencyCalls == 0 {
		t.Fatalf("diagnostics=%#v calls=%d", first.Diagnostics, access.adjacencyCalls)
	}

	query.Page = querylogical.PageSpec{Size: 10}
	query.Predicate = indexedComparison("m", "name", "=", querylogical.Value{Kind: querylogical.ValueString, String: "leaf"})
	filtered, err := engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx, Budget: queryengine.WorkBudget{BatchSize: 2}}, prepareIndexed(t, engine, tx, query))
	if err != nil || fmt.Sprint(resultNames(filtered)) != "[leaf leaf]" {
		t.Fatalf("target residual=%#v err=%v", filtered, err)
	}
}

func TestEngineClampsSingleTagAndPropertyPathStartsBeforeNextIndexRead(t *testing.T) {
	nodes := indexedNodes("first", "second")
	for index := range nodes {
		nodes[index].Properties[graph.NodePropTags] = []string{"project"}
	}
	for _, test := range []struct {
		name      string
		predicate *querylogical.Predicate
		configure func(*indexedTestAccess)
		calls     func(*indexedTestAccess) int
	}{
		{
			name: "tag",
			predicate: querylogical.LeafPredicate(querylogical.Leaf{
				Kind: querylogical.LeafHasTag, Field: &querylogical.FieldRef{Alias: "n", Namespace: querylogical.NamespaceProperties, Name: graph.NodePropTags}, Tag: "project",
			}),
			configure: func(access *indexedTestAccess) { access.tags["project"] = nodes },
			calls:     func(access *indexedTestAccess) int { return access.tagCalls },
		},
		{
			name:      "property",
			predicate: indexedComparison("n", "rank", ">=", querylogical.NativeValue(1)),
			configure: func(access *indexedTestAccess) { access.indexes["document_rank"] = nodes },
			calls:     func(access *indexedTestAccess) int { return access.orderedCalls },
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			base := newIndexedTestAccess(nodes)
			test.configure(base)
			access := &traversalTestAccess{indexedTestAccess: base, edges: map[string][]graph.Edge{}}
			engine := queryengine.New(queryengine.Ports{Entities: access, Indexes: access, Traversals: access})
			tx := indexedTransaction()
			field := querylogical.FieldRef{Alias: "m", Namespace: querylogical.NamespaceProperties, Name: "name"}
			query := querylogical.Query{
				Source: querylogical.SourceStructured, AccessMode: querylogical.AccessModeReadOnly,
				Start: querylogical.NodePattern{Alias: "n", Labels: []string{"Document"}}, Predicate: test.predicate, PathAlias: "path",
				Steps: []querylogical.Step{{
					Edge:  querylogical.EdgePattern{Labels: []string{"LINKS"}, Direction: querylogical.DirectionOutgoing},
					Depth: querylogical.DepthBounds{Min: 1, Max: 1}, Target: querylogical.NodePattern{Alias: "m", Labels: []string{"Document"}},
				}},
				Returns: []querylogical.Projection{{Kind: querylogical.ProjectionScalar, Alias: "m", Field: &field, OutputName: "name"}},
				Page:    querylogical.PageSpec{Size: 10},
			}
			prepared := prepareIndexed(t, engine, tx, query)
			if prepared.Physical.Family != queryphysical.FamilyIndexedPath {
				t.Fatalf("plan = %#v", prepared.Physical)
			}
			_, err := engine.Execute(context.Background(), queryengine.ExecutionContext{
				Transaction: tx,
				Budget:      queryengine.WorkBudget{BatchSize: 8, MaxStartNodes: 1, MaxNodesLoaded: 10},
			}, prepared)
			assertEngineQueryCode(t, err, querylogical.ErrorResourceExhausted)
			if got := test.calls(base); got != 1 {
				t.Fatalf("index calls = %d, want 1", got)
			}
			if !reflect.DeepEqual(base.limits, []int{1}) {
				t.Fatalf("index limits = %v, want [1]", base.limits)
			}
		})
	}
}

func TestEngineAccountsCompoundPathRootsAfterFinalDeduplication(t *testing.T) {
	nodes := indexedNodes("a", "b", "c")
	base := newIndexedTestAccess(nodes)
	base.indexes["document_name"] = nodes[1:2]
	base.indexes["document_rank"] = nodes[1:]
	access := &traversalTestAccess{indexedTestAccess: base, edges: map[string][]graph.Edge{}}
	engine := queryengine.New(queryengine.Ports{Entities: access, Indexes: access, Traversals: access})
	tx := indexedTransaction()
	name := indexedComparison("n", "name", "=", querylogical.NativeValue("b"))
	rank := indexedComparison("n", "rank", ">=", querylogical.NativeValue(2))
	field := querylogical.FieldRef{Alias: "m", Namespace: querylogical.NamespaceProperties, Name: "name"}
	query := querylogical.Query{
		Source: querylogical.SourceStructured, AccessMode: querylogical.AccessModeReadOnly,
		Start: querylogical.NodePattern{Alias: "n", Labels: []string{"Document"}}, PathAlias: "path",
		Predicate: querylogical.CombinePredicate(querylogical.PredicateOrOp, []querylogical.Predicate{*name, *rank}),
		Steps: []querylogical.Step{{
			Edge:  querylogical.EdgePattern{Labels: []string{"LINKS"}, Direction: querylogical.DirectionOutgoing},
			Depth: querylogical.DepthBounds{Min: 1, Max: 1}, Target: querylogical.NodePattern{Alias: "m", Labels: []string{"Document"}},
		}},
		Returns: []querylogical.Projection{{Kind: querylogical.ProjectionScalar, Alias: "m", Field: &field, OutputName: "name"}},
		Page:    querylogical.PageSpec{Size: 10},
	}
	prepared := prepareIndexed(t, engine, tx, query)
	if prepared.Physical.Family != queryphysical.FamilyIndexedPath || prepared.Physical.Access.Kind != queryphysical.AccessCompoundProperty {
		t.Fatalf("plan = %#v", prepared.Physical)
	}
	_, err := engine.Execute(context.Background(), queryengine.ExecutionContext{
		Transaction: tx,
		Budget:      queryengine.WorkBudget{BatchSize: 8, MaxStartNodes: 1, MaxNodesLoaded: 10},
	}, prepared)
	assertEngineQueryCode(t, err, querylogical.ErrorResourceExhausted)
	if access.adjacencyCalls != 0 {
		t.Fatalf("adjacency calls = %d, want no expansion past start budget", access.adjacencyCalls)
	}
}

func TestEngineResolvesOpenGQLPathDepthBeforeExecution(t *testing.T) {
	nodes := indexedNodes("start", "one", "two", "three", "four", "five", "six")
	base := newIndexedTestAccess(nodes)
	access := &traversalTestAccess{indexedTestAccess: base, edges: map[string][]graph.Edge{}}
	for index := 0; index < len(nodes)-1; index++ {
		access.edges[nodes[index].ID.String()] = []graph.Edge{indexedEdge(index+1, nodes[index], nodes[index+1], "LINKS")}
	}
	engine := queryengine.New(queryengine.Ports{Entities: access, Indexes: access, Traversals: access})
	tx := indexedTransaction()
	field := querylogical.FieldRef{Alias: "m", Namespace: querylogical.NamespaceProperties, Name: "name"}
	query := querylogical.Query{
		Source: querylogical.SourceGQL, AccessMode: querylogical.AccessModeReadOnly,
		Start: querylogical.NodePattern{Alias: "n", NodeIDs: []string{nodes[0].ID.String()}, Labels: []string{"Document"}}, PathAlias: "path",
		Steps:   []querylogical.Step{{Edge: querylogical.EdgePattern{Labels: []string{"LINKS"}, Direction: querylogical.DirectionOutgoing}, Depth: querylogical.DepthBounds{Min: 1, Max: -1}, Target: querylogical.NodePattern{Alias: "m", Labels: []string{"Document"}}}},
		Returns: []querylogical.Projection{{Kind: querylogical.ProjectionScalar, Alias: "m", Field: &field, OutputName: "name"}},
		Page:    querylogical.PageSpec{Size: 10},
	}
	prepared := prepareIndexed(t, engine, tx, query)
	if got := prepared.Physical.Access.Steps[0].MaxDepth; got != 5 {
		t.Fatalf("resolved max depth = %d, want 5", got)
	}
	result, err := engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx, Budget: queryengine.WorkBudget{BatchSize: 2}}, prepared)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := fmt.Sprint(resultNames(result)); got != "[one two three four five]" {
		t.Fatalf("path targets = %s", got)
	}
	if result.Diagnostics.TraversalDepth != 5 {
		t.Fatalf("diagnostics = %#v", result.Diagnostics)
	}
}

func TestEngineExecutesOrderedSubtreesWithTreeAndGraphProjection(t *testing.T) {
	nodes := indexedNodes("root-a", "root-b", "child-a", "child-b")
	base := newIndexedTestAccess(nodes)
	base.indexes["document_name"] = nodes[:2]
	edgeA := indexedEdge(1, nodes[0], nodes[2], "contains")
	edgeB := indexedEdge(2, nodes[1], nodes[3], "contains")
	edgeA.Properties["keep"] = true
	edgeB.Properties["keep"] = false
	nodes[2].Properties["kind"] = "child"
	nodes[3].Properties["kind"] = "child"
	base.nodes[nodes[2].ID.String()] = nodes[2]
	base.nodes[nodes[3].ID.String()] = nodes[3]
	access := &traversalTestAccess{indexedTestAccess: base, edges: map[string][]graph.Edge{
		nodes[0].ID.String(): {edgeA},
		nodes[1].ID.String(): {edgeB},
	}}
	engine := queryengine.New(queryengine.Ports{Entities: access, Indexes: access, Traversals: access})
	tx := indexedTransaction()
	rootField := querylogical.FieldRef{Alias: "n", Namespace: querylogical.NamespaceProperties, Name: "name"}
	query := querylogical.Query{
		Source: querylogical.SourceStructured, AccessMode: querylogical.AccessModeReadOnly,
		Start: querylogical.NodePattern{Alias: "n", Labels: []string{"Document"}},
		Steps: []querylogical.Step{{
			Edge:   querylogical.EdgePattern{Labels: []string{"contains"}, Direction: querylogical.DirectionOutgoing, Properties: map[string]querylogical.Value{"keep": {Kind: querylogical.ValueBool, Bool: true}}},
			Depth:  querylogical.DepthBounds{Min: 1, Max: 2},
			Target: querylogical.NodePattern{Alias: "m", Labels: []string{"Document"}, Properties: map[string]querylogical.Value{"kind": {Kind: querylogical.ValueString, String: "child"}}},
		}},
		Returns: []querylogical.Projection{{Kind: querylogical.ProjectionScalar, Alias: "n", Field: &rootField, OutputName: "name"}, {Kind: querylogical.ProjectionTree, Alias: "m", OutputName: "tree"}},
		OrderBy: []querylogical.Order{{Value: querylogical.FieldValue("n", "name"), Direction: querylogical.SortAscending}},
		Page:    querylogical.PageSpec{Size: 1},
	}
	prepared := prepareIndexed(t, engine, tx, query)
	if prepared.Physical.Family != queryphysical.FamilyIndexedSubtree || prepared.RequiresLegacy() {
		t.Fatalf("prepared = %#v", prepared)
	}
	first, err := engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx, Budget: queryengine.WorkBudget{BatchSize: 2}}, prepared)
	if err != nil || fmt.Sprint(resultNames(first)) != "[root-a]" || first.NextCursor == "" {
		t.Fatalf("first=%#v err=%v", first, err)
	}
	if len(first.Graph.Nodes) != 2 || len(first.Graph.Edges) != 1 || first.Rows[0].Fields["tree"].Tree == nil {
		t.Fatalf("tree/graph result = %#v", first)
	}
	query.Page.Cursor = first.NextCursor
	second, err := engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx, Budget: queryengine.WorkBudget{BatchSize: 2}}, prepareIndexed(t, engine, tx, query))
	if err != nil || fmt.Sprint(resultNames(second)) != "[root-b]" || second.NextCursor != "" {
		t.Fatalf("second=%#v err=%v", second, err)
	}
	if len(second.Graph.Nodes) != 1 || len(second.Graph.Edges) != 0 || len(second.Rows[0].Fields["tree"].Tree.Roots) != 0 {
		t.Fatalf("filtered subtree = %#v", second)
	}
	if access.adjacencyCalls < 2 {
		t.Fatalf("subtree adjacency calls = %d", access.adjacencyCalls)
	}
	for _, scan := range access.adjacencyScans {
		if scan.Limit <= 0 {
			t.Fatalf("unbounded subtree adjacency scan: %#v", scan)
		}
	}
}

func indexedEdge(index int, from graph.Node, to graph.Node, label string) graph.Edge {
	return graph.Edge{ID: uuid.MustParse(fmt.Sprintf("10000000-0000-0000-0000-%012d", index)), FromID: from.ID, ToID: to.ID, Labels: []string{label}, Properties: map[string]any{"order": index}}
}

func newIndexedTestAccess(nodes []graph.Node) *indexedTestAccess {
	access := &indexedTestAccess{nodes: map[string]graph.Node{}, labels: map[string][]graph.Node{"Document": nodes}, tags: map[string][]graph.Node{}, indexes: map[string][]graph.Node{}}
	for _, node := range nodes {
		access.nodes[node.ID.String()] = node
	}
	return access
}

func indexedNodes(names ...string) []graph.Node {
	nodes := make([]graph.Node, 0, len(names))
	for index, name := range names {
		nodes = append(nodes, graph.Node{
			ID:     uuid.MustParse(fmt.Sprintf("00000000-0000-0000-0000-%012d", index+1)),
			Labels: []string{"Document"}, Properties: map[string]any{"name": name, "rank": index + 1},
		})
	}
	return nodes
}

func indexedTransaction() sessionmodel.GraphTransaction {
	return sessionmodel.GraphTransaction{
		ID: "tx-indexed", SessionID: "session-1", PrincipalID: uuid.NewString(), SpaceID: uuid.NewString(), DomainID: uuid.NewString(),
		Mode: sessionmodel.TransactionModeReadOnly, State: sessionmodel.TransactionStateActive, BaseRevision: 77,
	}
}

func indexedBaseQuery() querylogical.Query {
	field := querylogical.FieldRef{Alias: "n", Namespace: querylogical.NamespaceProperties, Name: "name"}
	return querylogical.Query{
		Source: querylogical.SourceStructured, AccessMode: querylogical.AccessModeReadOnly,
		Start:   querylogical.NodePattern{Alias: "n", Labels: []string{"Document"}},
		Returns: []querylogical.Projection{{Kind: querylogical.ProjectionScalar, Alias: "n", Field: &field, OutputName: "name"}},
		Page:    querylogical.PageSpec{Size: 2},
	}
}

func indexedSchema() schemamodel.DomainSchema {
	return schemamodel.DomainSchema{
		Version: "qrx7", Mode: schemamodel.SchemaModeStrict,
		NodeTypes: []schemamodel.NodeType{{Name: "Document", Labels: []string{"Document"}, Properties: []schemamodel.FieldSpec{{Name: "name", Type: schemamodel.FieldTypeString}, {Name: "rank", Type: schemamodel.FieldTypeInt}, {Name: "kind", Type: schemamodel.FieldTypeString}, {Name: "tags", Type: schemamodel.FieldTypeJSON}}}},
		EdgeTypes: []schemamodel.EdgeType{
			{Name: "LINKS", Labels: []string{"LINKS"}, From: schemamodel.EndpointSpec{NodeTypes: []string{"Document"}}, To: schemamodel.EndpointSpec{NodeTypes: []string{"Document"}}},
			{Name: "contains", Labels: []string{"contains"}, From: schemamodel.EndpointSpec{NodeTypes: []string{"Document"}}, To: schemamodel.EndpointSpec{NodeTypes: []string{"Document"}}, Properties: []schemamodel.FieldSpec{{Name: "keep", Type: schemamodel.FieldTypeBool}}},
		},
		Indexes: []schemamodel.IndexDefinition{
			{Name: "document_name", TargetKind: schemamodel.IndexTargetNode, TargetType: "Document", Field: schemamodel.FieldPath{Namespace: "properties", Name: "name"}, Kind: schemamodel.IndexKindOrdered},
			{Name: "document_rank", TargetKind: schemamodel.IndexTargetNode, TargetType: "Document", Field: schemamodel.FieldPath{Namespace: "properties", Name: "rank"}, Kind: schemamodel.IndexKindOrdered},
		},
	}
}

func prepareIndexed(t *testing.T, engine *queryengine.Engine, tx sessionmodel.GraphTransaction, query querylogical.Query) queryengine.PreparedQuery {
	t.Helper()
	schema := indexedSchema()
	prepared, err := engine.Prepare(context.Background(), queryengine.PlanningContext{
		Transaction: tx, Schema: &schema,
		Domain: queryengine.DomainPolicy{BroadSearchable: true, ExplicitSearchable: true, SemanticSearchable: true, SemanticCandidatesAvailable: true},
	}, querylogical.ReadStatement(query))
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	return prepared
}

func resultNames(result querylogical.Result) []string {
	out := make([]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		if cell := row.Fields["name"]; cell.Scalar != nil {
			out = append(out, cell.Scalar.String)
		}
	}
	return out
}

func indexedComparison(alias, field, operator string, value querylogical.Value) *querylogical.Predicate {
	left := querylogical.FieldValue(alias, field)
	return querylogical.LeafPredicate(querylogical.Leaf{Kind: querylogical.LeafComparison, Left: &left, Right: &value, Operator: operator})
}

func tamperIndexedCursor(t *testing.T, token string, field string, value any) string {
	t.Helper()
	payload, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(token, "qrx7:"))
	if err != nil {
		t.Fatalf("decode cursor: %v", err)
	}
	var envelope map[string]any
	if err := json.Unmarshal(payload, &envelope); err != nil {
		t.Fatalf("unmarshal cursor: %v", err)
	}
	envelope[field] = value
	payload, err = json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal cursor: %v", err)
	}
	return "qrx7:" + base64.RawURLEncoding.EncodeToString(payload)
}

func assertEngineQueryCode(t *testing.T, err error, want querylogical.ErrorCode) {
	t.Helper()
	var queryErr *querylogical.QueryError
	if !errors.As(err, &queryErr) || queryErr.Code != want {
		t.Fatalf("error = %#v, want %q", err, want)
	}
}
