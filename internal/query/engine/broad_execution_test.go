package engine_test

import (
	"context"
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
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
)

type cancelingBroadAccess struct {
	*broadTestAccess
	cancel context.CancelFunc
}

type changingViewBroadAccess struct {
	*broadTestAccess
}

func (a *cancelingBroadAccess) ScanNodes(ctx context.Context, tx sessionmodel.GraphTransaction, scan graphquery.NodeScan) ([]graph.Node, string, graphquery.IndexedReadStats, error) {
	nodes, next, stats, err := a.broadTestAccess.ScanNodes(ctx, tx, scan)
	a.cancel()
	return nodes, next, stats, err
}

func (a *changingViewBroadAccess) ScanNodes(ctx context.Context, tx sessionmodel.GraphTransaction, scan graphquery.NodeScan) ([]graph.Node, string, graphquery.IndexedReadStats, error) {
	nodes, next, stats, err := a.broadTestAccess.ScanNodes(ctx, tx, scan)
	a.viewIdentity = "1"
	return nodes, next, stats, err
}

type emptyProgressBroadAccess struct {
	*broadTestAccess
	scanCalls int
}

func (a *emptyProgressBroadAccess) ScanNodes(_ context.Context, _ sessionmodel.GraphTransaction, scan graphquery.NodeScan) ([]graph.Node, string, graphquery.IndexedReadStats, error) {
	a.scanCalls++
	if scan.Cursor == "" {
		return nil, "progress", graphquery.IndexedReadStats{IndexEntriesScanned: 1}, nil
	}
	if scan.Cursor != "progress" {
		return nil, "", graphquery.IndexedReadStats{}, errors.New("unexpected node scan cursor")
	}
	return []graph.Node{a.nodes[0]}, "", graphquery.IndexedReadStats{IndexEntriesScanned: 1, NodesLoaded: 1}, nil
}

type broadTestAccess struct {
	nodes          []graph.Node
	byID           map[string]graph.Node
	edges          map[string][]graph.Edge
	listNodeCalls  int
	listEdgeCalls  int
	adjacencyCalls int
	getNodeCalls   int
	viewIdentity   string
	limits         []int
	cursors        []string
	txs            []sessionmodel.GraphTransaction
}

func (a *broadTestAccess) ReadViewIdentity(ctx context.Context, tx sessionmodel.GraphTransaction) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	a.txs = append(a.txs, tx)
	return a.viewIdentity, nil
}

func (a *broadTestAccess) GetNode(_ context.Context, tx sessionmodel.GraphTransaction, id string) (graph.Node, error) {
	a.getNodeCalls++
	a.txs = append(a.txs, tx)
	node, ok := a.byID[id]
	if !ok {
		return graph.Node{}, fmt.Errorf("node %s not found", id)
	}
	return node, nil
}

func (a *broadTestAccess) GetEdge(context.Context, sessionmodel.GraphTransaction, string) (graph.Edge, error) {
	return graph.Edge{}, errors.New("unexpected edge point read")
}

func (a *broadTestAccess) ListNodes(_ context.Context, tx sessionmodel.GraphTransaction, limit int, cursor string) ([]graph.Node, string, error) {
	a.listNodeCalls++
	a.txs = append(a.txs, tx)
	a.limits = append(a.limits, limit)
	a.cursors = append(a.cursors, cursor)
	if limit <= 0 {
		return nil, "", fmt.Errorf("unbounded node limit %d", limit)
	}
	start, err := broadOffset(cursor, len(a.nodes))
	if err != nil {
		return nil, "", err
	}
	end := start + limit
	if end > len(a.nodes) {
		end = len(a.nodes)
	}
	next := ""
	if end < len(a.nodes) {
		next = strconv.Itoa(end)
	}
	return append([]graph.Node(nil), a.nodes[start:end]...), next, nil
}

func (a *broadTestAccess) ScanNodes(ctx context.Context, tx sessionmodel.GraphTransaction, scan graphquery.NodeScan) ([]graph.Node, string, graphquery.IndexedReadStats, error) {
	nodes, next, err := a.ListNodes(ctx, tx, scan.Limit, scan.Cursor)
	return nodes, next, graphquery.IndexedReadStats{IndexEntriesScanned: len(nodes), NodesLoaded: len(nodes)}, err
}

func (a *broadTestAccess) ListEdges(context.Context, sessionmodel.GraphTransaction, int, string) ([]graph.Edge, string, error) {
	a.listEdgeCalls++
	return nil, "", errors.New("whole-domain edge listing is forbidden")
}

func (a *broadTestAccess) ScanAdjacency(_ context.Context, tx sessionmodel.GraphTransaction, scan graphquery.AdjacencyScan) ([]graph.Edge, string, graphquery.IndexedReadStats, error) {
	a.adjacencyCalls++
	a.txs = append(a.txs, tx)
	a.limits = append(a.limits, scan.Limit)
	if scan.Limit <= 0 {
		return nil, "", graphquery.IndexedReadStats{}, fmt.Errorf("unbounded adjacency limit %d", scan.Limit)
	}
	edges := a.edges[scan.NodeID]
	start, err := broadOffset(scan.Cursor, len(edges))
	if err != nil {
		return nil, "", graphquery.IndexedReadStats{}, err
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
	return page, next, graphquery.IndexedReadStats{IndexEntriesScanned: len(page), EdgesLoaded: len(page), AdjacencyScanCalls: 1}, nil
}

func broadOffset(cursor string, length int) (int, error) {
	if cursor == "" {
		return 0, nil
	}
	offset, err := strconv.Atoi(cursor)
	if err != nil || offset < 0 || offset > length {
		return 0, fmt.Errorf("bad cursor %q", cursor)
	}
	return offset, nil
}

func TestEngineBroadNodeScanAcceptsBoundedEmptyProgressPages(t *testing.T) {
	node := broadNode(90, "Document", 1)
	access := &emptyProgressBroadAccess{broadTestAccess: newBroadTestAccess([]graph.Node{node})}
	engine := queryengine.New(queryengine.Ports{Entities: access})
	tx := sessionmodel.GraphTransaction{ID: "tx-empty-progress", BaseRevision: 90}
	prepared := prepareBroad(t, engine, tx, broadNodeStatement(10))
	result, err := engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx}, prepared)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if access.scanCalls != 2 || len(result.Rows) != 1 || result.Rows[0].Fields["node"].Node == nil || result.Rows[0].Fields["node"].Node.ID != node.ID.String() {
		t.Fatalf("scan calls=%d rows=%+v", access.scanCalls, result.Rows)
	}
	if result.Diagnostics.IndexEntriesScanned != 2 || result.Diagnostics.NodesLoaded != 1 {
		t.Fatalf("diagnostics = %+v", result.Diagnostics)
	}
}

func TestEngineBroadNodeScanUsesFinitePagesAndPlanBoundContinuation(t *testing.T) {
	nodes := []graph.Node{broadNode(1, "Document", 1), broadNode(2, "Document", 2), broadNode(3, "Document", 3)}
	access := newBroadTestAccess(nodes)
	engine := queryengine.New(queryengine.Ports{Entities: access})
	tx := sessionmodel.GraphTransaction{ID: "tx-broad-pages", BaseRevision: 41}
	statement := broadNodeStatement(1)

	prepared := prepareBroad(t, engine, tx, statement)
	if prepared.Physical.Family != queryphysical.FamilyBoundedFullScan {
		t.Fatalf("prepared plan = %#v", prepared.Physical)
	}
	first, err := engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx}, prepared)
	if err != nil {
		t.Fatalf("execute first page: %v", err)
	}
	if got := broadResultNodeIDs(first); len(got) != 1 || got[0] != nodes[0].ID.String() {
		t.Fatalf("first page IDs = %v", got)
	}
	if !strings.HasPrefix(first.NextCursor, "qrx7:") {
		t.Fatalf("next cursor = %q, want protected native cursor", first.NextCursor)
	}
	if access.listNodeCalls != 1 || access.listEdgeCalls != 0 || len(access.limits) != 1 || access.limits[0] != 2 {
		t.Fatalf("first-page access: node calls=%d edge calls=%d limits=%v", access.listNodeCalls, access.listEdgeCalls, access.limits)
	}
	if first.Diagnostics.FullScan != true || first.Diagnostics.NodesLoaded != 2 || first.Diagnostics.RowsReturned != 1 || first.Diagnostics.Plan != "StreamingNodeScan" {
		t.Fatalf("first-page diagnostics = %#v", first.Diagnostics)
	}

	statement.Read.Page.Cursor = first.NextCursor
	prepared = prepareBroad(t, engine, tx, statement)
	second, err := engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx}, prepared)
	if err != nil {
		t.Fatalf("execute second page: %v", err)
	}
	if got := broadResultNodeIDs(second); len(got) != 1 || got[0] != nodes[1].ID.String() {
		t.Fatalf("second page IDs = %v", got)
	}
	if access.listEdgeCalls != 0 {
		t.Fatalf("broad node scan listed edges %d times", access.listEdgeCalls)
	}
	for _, limit := range access.limits {
		if limit <= 0 {
			t.Fatalf("unbounded request limits = %v", access.limits)
		}
	}
	for _, seen := range access.txs {
		if seen != tx {
			t.Fatalf("access transaction = %#v, want %#v", seen, tx)
		}
	}
}

func TestEngineBroadSourceContinuationPreservesPageCheckpointUntilQueryLimit(t *testing.T) {
	nodes := []graph.Node{
		broadNode(4, "Document", 1), broadNode(5, "Document", 2), broadNode(6, "Document", 3),
		broadNode(7, "Document", 4), broadNode(8, "Document", 5), broadNode(9, "Document", 6),
	}
	access := newBroadTestAccess(nodes)
	engine := queryengine.New(queryengine.Ports{Entities: access})
	tx := sessionmodel.GraphTransaction{ID: "tx-broad-query-limit", BaseRevision: 43}
	statement := broadNodeStatement(2)
	statement.Read.Page.Limit = 5

	var got []string
	for {
		prepared := prepareBroad(t, engine, tx, statement)
		result, err := engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx}, prepared)
		if err != nil {
			t.Fatalf("execute page %d: %v", len(got)/2+1, err)
		}
		got = append(got, broadResultNodeIDs(result)...)
		if result.NextCursor == "" {
			break
		}
		statement.Read.Page.Cursor = result.NextCursor
	}
	want := []string{nodes[0].ID.String(), nodes[1].ID.String(), nodes[2].ID.String(), nodes[3].ID.String(), nodes[4].ID.String()}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("continued IDs = %v, want %v", got, want)
	}
}

func TestQRX9StructuredEdgeRowsPreserveSelfLoops(t *testing.T) {
	node := broadNode(91, "Document", 1)
	node.Properties["title"] = "A"
	loop := broadEdge(91, node, node, "REFERENCES")
	loop.Properties = map[string]any{"kind": "loop"}
	access := newBroadTestAccess([]graph.Node{node})
	access.edges[node.ID.String()] = []graph.Edge{loop}
	engine := queryengine.New(queryengine.Ports{Entities: access, Traversals: access})
	tx := sessionmodel.GraphTransaction{ID: "tx-self-loop", BaseRevision: 91}
	statement := querylogical.ReadStatement(querylogical.Query{
		Source: querylogical.SourceStructured, AccessMode: querylogical.AccessModeReadOnly,
		Start: querylogical.NodePattern{Alias: "a", Labels: []string{"Document"}},
		Steps: []querylogical.Step{{
			Edge:  querylogical.EdgePattern{Alias: "e", Labels: []string{"REFERENCES"}, Properties: map[string]querylogical.Value{"kind": querylogical.NativeValue("loop")}, Direction: querylogical.DirectionOutgoing},
			Depth: querylogical.DepthBounds{Min: 1, Max: 1}, Target: querylogical.NodePattern{Alias: "b", Labels: []string{"Document"}, Properties: map[string]querylogical.Value{"title": querylogical.NativeValue("A")}},
		}},
		Returns: []querylogical.Projection{{Kind: querylogical.ProjectionEdge, Alias: "e", OutputName: "edge"}, {Kind: querylogical.ProjectionNode, Alias: "b", OutputName: "target"}, {Kind: querylogical.ProjectionTree, Alias: "b", OutputName: "tree"}},
		Page:    querylogical.PageSpec{Size: 10},
	})
	prepared := prepareBroad(t, engine, tx, statement)
	result, err := engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx}, prepared)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0].Fields["edge"].Edge == nil || result.Rows[0].Fields["edge"].Edge.ID != loop.ID.String() || result.Rows[0].Fields["target"].Node == nil || result.Rows[0].Fields["target"].Node.ID != node.ID.String() {
		t.Fatalf("self-loop rows = %+v", result.Rows)
	}
	if tree := result.Rows[0].Fields["tree"].Tree; tree == nil || len(tree.Roots) != 1 || tree.Roots[0].Node.ID != node.ID.String() {
		t.Fatalf("self-loop tree = %+v", tree)
	}
}

func TestQRX9StructuredEdgeRowsPreserveMultiStepCycles(t *testing.T) {
	root := broadNode(92, "Document", 1)
	root.Properties["title"] = "A"
	middle := broadNode(93, "Section", 1)
	middle.Properties["title"] = "B"
	first := broadEdge(92, root, middle, "REFERENCES")
	first.Properties = map[string]any{"kind": "out"}
	second := broadEdge(93, middle, root, "REFERENCES")
	second.Properties = map[string]any{"kind": "back"}
	access := newBroadTestAccess([]graph.Node{root})
	access.byID[middle.ID.String()] = middle
	access.edges[root.ID.String()] = []graph.Edge{first}
	access.edges[middle.ID.String()] = []graph.Edge{second}
	engine := queryengine.New(queryengine.Ports{Entities: access, Traversals: access})
	tx := sessionmodel.GraphTransaction{ID: "tx-cycle", BaseRevision: 92}
	statement := querylogical.ReadStatement(querylogical.Query{
		Source: querylogical.SourceStructured, AccessMode: querylogical.AccessModeReadOnly,
		Start: querylogical.NodePattern{Alias: "a", Labels: []string{"Document"}},
		Steps: []querylogical.Step{
			{Edge: querylogical.EdgePattern{Alias: "e1", Labels: []string{"REFERENCES"}, Properties: map[string]querylogical.Value{"kind": querylogical.NativeValue("out")}, Direction: querylogical.DirectionOutgoing}, Depth: querylogical.DepthBounds{Min: 1, Max: 1}, Target: querylogical.NodePattern{Alias: "b", Labels: []string{"Section"}, Properties: map[string]querylogical.Value{"title": querylogical.NativeValue("B")}}},
			{Edge: querylogical.EdgePattern{Alias: "e2", Labels: []string{"REFERENCES"}, Properties: map[string]querylogical.Value{"kind": querylogical.NativeValue("back")}, Direction: querylogical.DirectionOutgoing}, Depth: querylogical.DepthBounds{Min: 1, Max: 1}, Target: querylogical.NodePattern{Alias: "c", Labels: []string{"Document"}, Properties: map[string]querylogical.Value{"title": querylogical.NativeValue("A")}}},
		},
		Returns: []querylogical.Projection{{Kind: querylogical.ProjectionEdge, Alias: "e1", OutputName: "out"}, {Kind: querylogical.ProjectionEdge, Alias: "e2", OutputName: "back"}, {Kind: querylogical.ProjectionNode, Alias: "c", OutputName: "target"}, {Kind: querylogical.ProjectionTree, Alias: "c", OutputName: "tree"}},
		Page:    querylogical.PageSpec{Size: 10},
	})
	prepared := prepareBroad(t, engine, tx, statement)
	result, err := engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx}, prepared)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0].Fields["out"].Edge == nil || result.Rows[0].Fields["out"].Edge.ID != first.ID.String() || result.Rows[0].Fields["back"].Edge == nil || result.Rows[0].Fields["back"].Edge.ID != second.ID.String() || result.Rows[0].Fields["target"].Node == nil || result.Rows[0].Fields["target"].Node.ID != root.ID.String() {
		t.Fatalf("cycle rows = %+v", result.Rows)
	}
	if tree := result.Rows[0].Fields["tree"].Tree; tree == nil || len(tree.Roots) != 1 || tree.Roots[0].Node.ID != root.ID.String() {
		t.Fatalf("cycle tree = %+v", tree)
	}
}

func TestEngineBroadTraversalUsesPagedAdjacencyWithoutWholeEdgeListing(t *testing.T) {
	root := broadNode(10, "Document", 1)
	target := broadNode(11, "Section", 1)
	access := newBroadTestAccess([]graph.Node{root})
	access.byID[target.ID.String()] = target
	access.edges[root.ID.String()] = []graph.Edge{
		broadEdge(21, root, target, "contains"),
		broadEdge(22, root, target, "contains"),
	}
	engine := queryengine.New(queryengine.Ports{Entities: access, Traversals: access})
	tx := sessionmodel.GraphTransaction{ID: "tx-broad-traversal", BaseRevision: 57}
	statement := querylogical.ReadStatement(querylogical.Query{
		Source: querylogical.SourceStructured, AccessMode: querylogical.AccessModeReadOnly,
		Start: querylogical.NodePattern{Alias: "d", Labels: []string{"Document"}},
		Steps: []querylogical.Step{{
			Edge:  querylogical.EdgePattern{Alias: "edge", Labels: []string{"contains"}, Direction: querylogical.DirectionOutgoing},
			Depth: querylogical.DepthBounds{Min: 1, Max: 1}, Target: querylogical.NodePattern{Alias: "s", Labels: []string{"Section"}},
		}},
		Returns: []querylogical.Projection{
			{Kind: querylogical.ProjectionEdge, Alias: "edge", OutputName: "edge"},
			{Kind: querylogical.ProjectionTree, Alias: "s", OutputName: "tree"},
		},
		Page: querylogical.PageSpec{Size: 10},
	})
	prepared := prepareBroad(t, engine, tx, statement)
	if operatorTreeContains(prepared.Physical.Root, queryphysical.OperatorCollectTraversal) || !operatorTreeContains(prepared.Physical.Root, queryphysical.OperatorIndexedPath) {
		t.Fatalf("physical operators = %#v", prepared.Physical.Root)
	}
	result, err := engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx}, prepared)
	if err != nil {
		t.Fatalf("execute traversal: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("structured edge rows = %#v", result.Rows)
	}
	for i, row := range result.Rows {
		if row.Fields["edge"].Edge == nil || row.Fields["edge"].Edge.ID != access.edges[root.ID.String()][i].ID.String() {
			t.Fatalf("structured edge row %d = %#v", i, row)
		}
		tree := row.Fields["tree"].Tree
		if tree == nil || len(tree.Roots) != 1 {
			t.Fatalf("edge row tree %d = %#v", i, tree)
		}
	}
	if access.listEdgeCalls != 0 || access.adjacencyCalls != 1 || access.getNodeCalls != 2 {
		t.Fatalf("traversal calls: listEdges=%d adjacency=%d getNode=%d limits=%v", access.listEdgeCalls, access.adjacencyCalls, access.getNodeCalls, access.limits)
	}
	if result.Diagnostics.AdjacencyScanCalls != 1 || result.Diagnostics.EdgesLoaded != 2 || result.Diagnostics.NodeReadCalls != 2 {
		t.Fatalf("traversal diagnostics = %#v", result.Diagnostics)
	}
}

func TestEngineBroadOrderedFallbackUsesBoundedTopNAndOutputReplay(t *testing.T) {
	nodes := []graph.Node{broadNode(31, "Document", 9), broadNode(32, "Document", 1), broadNode(33, "Document", 4), broadNode(34, "Document", 2)}
	access := newBroadTestAccess(nodes)
	engine := queryengine.New(queryengine.Ports{Entities: access})
	tx := sessionmodel.GraphTransaction{ID: "tx-broad-topn", BaseRevision: 63}
	left := querylogical.FieldValue("n", "score")
	right := querylogical.Value{Kind: querylogical.ValueInteger, Integer: 0}
	statement := querylogical.ReadStatement(querylogical.Query{
		Source: querylogical.SourceGQL, AccessMode: querylogical.AccessModeReadOnly,
		Start:     querylogical.NodePattern{Alias: "n", Labels: []string{"Document"}},
		Predicate: querylogical.LeafPredicate(querylogical.Leaf{Kind: querylogical.LeafComparison, Left: &left, Right: &right, Operator: ">"}),
		Returns:   []querylogical.Projection{{Kind: querylogical.ProjectionNode, Alias: "n", OutputName: "node"}},
		OrderBy:   []querylogical.Order{{Value: querylogical.FieldValue("n", "score"), Direction: querylogical.SortAscending}},
		Page:      querylogical.PageSpec{Size: 1, Limit: 2},
	})
	prepared := prepareBroad(t, engine, tx, statement)
	if !operatorTreeContains(prepared.Physical.Root, queryphysical.OperatorTopN) || prepared.Physical.Cursor.Kind != "output_offset_replay" {
		t.Fatalf("ordered broad plan = %#v", prepared.Physical)
	}
	result, err := engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx}, prepared)
	if err != nil {
		t.Fatalf("execute ordered fallback: %v", err)
	}
	if got := broadResultNodeIDs(result); len(got) != 1 || got[0] != nodes[1].ID.String() {
		t.Fatalf("ordered page IDs = %v", got)
	}
	if !strings.HasPrefix(result.NextCursor, "qrx7:") || result.Diagnostics.NextCursorKind != "output_offset_replay" {
		t.Fatalf("ordered continuation=%q diagnostics=%#v", result.NextCursor, result.Diagnostics)
	}
	if access.listNodeCalls < 2 || access.listEdgeCalls != 0 {
		t.Fatalf("ordered access calls: nodes=%d edges=%d limits=%v", access.listNodeCalls, access.listEdgeCalls, access.limits)
	}
	firstScanCalls := access.listNodeCalls
	statement.Read.Page.Cursor = result.NextCursor
	prepared = prepareBroad(t, engine, tx, statement)
	second, err := engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx}, prepared)
	if err != nil {
		t.Fatalf("execute ordered continuation: %v", err)
	}
	if got := broadResultNodeIDs(second); len(got) != 1 || got[0] != nodes[3].ID.String() || second.NextCursor != "" {
		t.Fatalf("ordered continuation IDs=%v cursor=%q", got, second.NextCursor)
	}
	if access.listNodeCalls <= firstScanCalls {
		t.Fatalf("ordered output replay did not rescan: before=%d after=%d", firstScanCalls, access.listNodeCalls)
	}
}

func TestEngineBroadScanHonorsCancellationAfterPageRead(t *testing.T) {
	base := newBroadTestAccess([]graph.Node{broadNode(35, "Document", 1)})
	ctx, cancel := context.WithCancel(context.Background())
	access := &cancelingBroadAccess{broadTestAccess: base, cancel: cancel}
	engine := queryengine.New(queryengine.Ports{Entities: access})
	tx := sessionmodel.GraphTransaction{ID: "tx-broad-cancel", BaseRevision: 67}
	prepared := prepareBroad(t, engine, tx, broadNodeStatement(1))
	_, err := engine.Execute(ctx, queryengine.ExecutionContext{Transaction: tx}, prepared)
	assertQueryErrorCode(t, err, querylogical.ErrorCanceled)
	if base.listNodeCalls != 1 || base.listEdgeCalls != 0 {
		t.Fatalf("canceled broad access: node calls=%d edge calls=%d", base.listNodeCalls, base.listEdgeCalls)
	}
}

func TestEngineBroadExecutionRejectsOverlayMutationDuringRead(t *testing.T) {
	base := newBroadTestAccess([]graph.Node{broadNode(37, "Document", 1)})
	access := &changingViewBroadAccess{broadTestAccess: base}
	engine := queryengine.New(queryengine.Ports{Entities: access})
	tx := sessionmodel.GraphTransaction{ID: "tx-broad-changing-overlay", Mode: sessionmodel.TransactionModeReadWrite, BaseRevision: 68}
	prepared := prepareBroad(t, engine, tx, broadNodeStatement(1))
	_, err := engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx}, prepared)
	assertQueryErrorCode(t, err, querylogical.ErrorStaleCursor)
}

func TestEngineBroadCursorRejectsChangedReadWriteOverlay(t *testing.T) {
	nodes := []graph.Node{broadNode(38, "Document", 1), broadNode(39, "Document", 2)}
	access := newBroadTestAccess(nodes)
	engine := queryengine.New(queryengine.Ports{Entities: access})
	tx := sessionmodel.GraphTransaction{ID: "tx-broad-overlay", Mode: sessionmodel.TransactionModeReadWrite, BaseRevision: 69}
	statement := broadNodeStatement(1)
	prepared := prepareBroad(t, engine, tx, statement)
	first, err := engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx}, prepared)
	if err != nil {
		t.Fatalf("execute first overlay page: %v", err)
	}
	if first.NextCursor == "" {
		t.Fatal("first overlay page did not return a cursor")
	}
	calls := access.listNodeCalls
	access.viewIdentity = "1"
	statement.Read.Page.Cursor = first.NextCursor
	prepared = prepareBroad(t, engine, tx, statement)
	_, err = engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx}, prepared)
	assertQueryErrorCode(t, err, querylogical.ErrorStaleCursor)
	if access.listNodeCalls != calls {
		t.Fatalf("stale overlay cursor reached node scan: calls=%d, want %d", access.listNodeCalls, calls)
	}
}

func TestEngineBroadScanRejectsTamperedCursorAndMemoryExhaustion(t *testing.T) {
	nodes := []graph.Node{broadNode(41, "Document", 3), broadNode(42, "Document", 2), broadNode(43, "Document", 1)}
	access := newBroadTestAccess(nodes)
	engine := queryengine.New(queryengine.Ports{Entities: access})
	tx := sessionmodel.GraphTransaction{ID: "tx-broad-budgets", BaseRevision: 71}
	statement := broadNodeStatement(1)
	prepared := prepareBroad(t, engine, tx, statement)
	first, err := engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx}, prepared)
	if err != nil {
		t.Fatalf("execute first page: %v", err)
	}
	statement.Read.Page.Cursor = first.NextCursor + "tampered"
	prepared = prepareBroad(t, engine, tx, statement)
	_, err = engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx}, prepared)
	assertQueryErrorCode(t, err, querylogical.ErrorStaleCursor)

	ordered := broadNodeStatement(10)
	ordered.Read.Source = querylogical.SourceGQL
	ordered.Read.Page.Limit = 2
	ordered.Read.OrderBy = []querylogical.Order{{Value: querylogical.FieldValue("n", "score"), Direction: querylogical.SortAscending}}
	left := querylogical.FieldValue("n", "score")
	right := querylogical.Value{Kind: querylogical.ValueInteger, Integer: 0}
	ordered.Read.Predicate = querylogical.LeafPredicate(querylogical.Leaf{Kind: querylogical.LeafComparison, Left: &left, Right: &right, Operator: ">"})
	ordered = querylogical.ReadStatement(*ordered.Read)
	prepared = prepareBroad(t, engine, tx, ordered)
	_, err = engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx, Budget: queryengine.WorkBudget{MaxMemoryBytes: 1}}, prepared)
	assertQueryErrorCode(t, err, querylogical.ErrorResourceExhausted)
}

func newBroadTestAccess(nodes []graph.Node) *broadTestAccess {
	access := &broadTestAccess{nodes: append([]graph.Node(nil), nodes...), byID: map[string]graph.Node{}, edges: map[string][]graph.Edge{}, viewIdentity: "0"}
	for _, node := range nodes {
		access.byID[node.ID.String()] = node
	}
	return access
}

func broadNode(index int, label string, score int64) graph.Node {
	return graph.Node{
		ID:       uuid.MustParse(fmt.Sprintf("00000000-0000-0000-0000-%012d", index)),
		DomainID: uuid.MustParse("10000000-0000-0000-0000-000000000001"),
		Labels:   []string{label}, Properties: map[string]any{"score": score},
	}
}

func broadEdge(index int, from graph.Node, to graph.Node, label string) graph.Edge {
	return graph.Edge{
		ID:       uuid.MustParse(fmt.Sprintf("20000000-0000-0000-0000-%012d", index)),
		DomainID: from.DomainID, FromID: from.ID, ToID: to.ID, Labels: []string{label},
	}
}

func broadNodeStatement(pageSize int32) querylogical.Statement {
	return querylogical.ReadStatement(querylogical.Query{
		Source: querylogical.SourceStructured, AccessMode: querylogical.AccessModeReadOnly,
		Start:   querylogical.NodePattern{Alias: "n", Labels: []string{"Document"}},
		Returns: []querylogical.Projection{{Kind: querylogical.ProjectionNode, Alias: "n", OutputName: "node"}},
		Page:    querylogical.PageSpec{Size: pageSize},
	})
}

func prepareBroad(t *testing.T, engine *queryengine.Engine, tx sessionmodel.GraphTransaction, statement querylogical.Statement) queryengine.PreparedQuery {
	t.Helper()
	prepared, err := engine.Prepare(context.Background(), queryengine.PlanningContext{Transaction: tx, Domain: queryengine.DomainPolicy{BroadSearchable: true}}, statement)
	if err != nil {
		t.Fatalf("prepare broad query: %v", err)
	}
	return prepared
}

func broadResultNodeIDs(result querylogical.Result) []string {
	ids := make([]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		if node := row.Fields["node"].Node; node != nil {
			ids = append(ids, node.ID)
		}
	}
	return ids
}
