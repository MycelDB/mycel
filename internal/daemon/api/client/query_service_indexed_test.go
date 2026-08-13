package client

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	graphmodel "github.com/myceldb/mycel/internal/graph/model"
	schemaservice "github.com/myceldb/mycel/internal/schema/service"
	"github.com/myceldb/mycel/internal/schema/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestQueryServiceExecuteQueryUsesOrderedNodePropertyIndex(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	manager := journalSchemaManagerForQueryTest(t, fixture.domainID, true)
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	for _, item := range []struct{ title, date string }{{"latest", "2026-07-20"}, {"oldest", "2026-07-18"}, {"middle", "2026-07-19"}} {
		if _, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx, Node: &clientv1.NodeCreate{Labels: []string{"JournalEntry"}, Properties: mustStruct(t, map[string]any{"title": item.title, "date": item.date})}}); err != nil {
			t.Fatalf("CreateNode(%s) error = %v", item.title, err)
		}
	}
	if _, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx, Node: &clientv1.NodeCreate{Labels: []string{"Note"}, Properties: mustStruct(t, map[string]any{"title": "unrelated", "date": "2026-07-17"})}}); err != nil {
		t.Fatalf("CreateNode(unrelated) error = %v", err)
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}

	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	querySvc := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithSchemaManager(manager)
	res, err := querySvc.ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: journalOrderedQuery(clientv1.SortDirection_SORT_DIRECTION_ASC), PageSize: 10})
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}
	if got := journalTitles(res.GetRows()); !reflect.DeepEqual(got, []string{"oldest", "middle", "latest"}) {
		t.Fatalf("unexpected order: %+v", got)
	}
	diag := res.GetDiagnostics()
	if diag.GetPlan() != "OrderedNodePropertyIndexScan" || diag.GetFullScan() || diag.GetEdgesLoaded() != 0 || diag.GetIndexes()[0] != "journal_entries_by_date" {
		t.Fatalf("unexpected diagnostics: %+v", diag)
	}
}

func TestQueryServiceExecuteQueryFindsDottedLabelNumericBetweenNodeCreatedDirectly(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	manager := dottedJournalSchemaManagerForQueryTest(t, fixture.domainID)
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	if _, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx, Node: &clientv1.NodeCreate{Labels: []string{"pkm.journal"}, Properties: mustStruct(t, map[string]any{"journal_date": "2026-08-09", "journal_day": 20260809})}}); err != nil {
		t.Fatalf("CreateNode(journal) error = %v", err)
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}

	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	query := &clientv1.GraphQuery{
		Match:   &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "j", Labels: []string{"pkm.journal"}}},
		Returns: []*clientv1.ReturnProjection{{Alias: "j", OutputName: "node", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_NODE}},
		Where: &clientv1.Expr{Expr: &clientv1.Expr_Between{Between: &clientv1.BetweenExpr{
			Value: &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Prop{Prop: &clientv1.PropExpr{Alias: "j", Name: "journal_day"}}},
			Low:   &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Literal{Literal: &clientv1.LiteralExpr{Value: structpb.NewNumberValue(20260809)}}},
			High:  &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Literal{Literal: &clientv1.LiteralExpr{Value: structpb.NewNumberValue(20260809)}}},
		}}},
		Limit: 2,
	}
	res, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithSchemaManager(manager).ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: query, PageSize: 2})
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}
	if len(res.GetRows()) != 1 {
		t.Fatalf("rows = %d, want 1; diagnostics=%+v", len(res.GetRows()), res.GetDiagnostics())
	}
}

func TestQueryServiceExecuteQueryIndexedStrictBeforeBoundDescendingLimit(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	manager := journalSchemaManagerForQueryTest(t, fixture.domainID, true)
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	for _, item := range []struct{ title, date string }{{"too-new", "2026-07-21"}, {"cutoff", "2026-07-20"}, {"middle", "2026-07-19"}, {"oldest", "2026-07-18"}} {
		if _, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx, Node: &clientv1.NodeCreate{Labels: []string{"JournalEntry"}, Properties: mustStruct(t, map[string]any{"title": item.title, "date": item.date})}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatal(err)
	}
	query := journalOrderedQuery(clientv1.SortDirection_SORT_DIRECTION_DESC)
	query.Limit = 2
	query.Where = &clientv1.Expr{Expr: &clientv1.Expr_LessThan{LessThan: &clientv1.LessThanExpr{Left: &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Prop{Prop: &clientv1.PropExpr{Alias: "j", Name: "date"}}}, Right: &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Literal{Literal: &clientv1.LiteralExpr{Value: structpb.NewStringValue("2026-07-20")}}}}}}
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	res, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithSchemaManager(manager).ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: query, PageSize: 10})
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}
	if got := journalTitles(res.GetRows()); !reflect.DeepEqual(got, []string{"middle", "oldest"}) {
		t.Fatalf("unexpected bounded order: %+v", got)
	}
	if res.GetDiagnostics().GetPlan() != "OrderedNodePropertyIndexScan" || res.GetDiagnostics().GetFullScan() || res.GetDiagnostics().GetEdgesLoaded() != 0 {
		t.Fatalf("unexpected diagnostics: %+v", res.GetDiagnostics())
	}
}

func TestQueryServiceExecuteQueryIndexedPagination(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	manager := journalSchemaManagerForQueryTest(t, fixture.domainID, true)
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	for _, item := range []struct{ title, date string }{{"a", "2026-07-18"}, {"b", "2026-07-19"}} {
		if _, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx, Node: &clientv1.NodeCreate{Labels: []string{"JournalEntry"}, Properties: mustStruct(t, map[string]any{"title": item.title, "date": item.date})}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatal(err)
	}
	querySvc := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithSchemaManager(manager)
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	page1, err := querySvc.ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: journalOrderedQuery(clientv1.SortDirection_SORT_DIRECTION_ASC), PageSize: 1})
	if err != nil || page1.GetNextPageToken() == "" || !reflect.DeepEqual(journalTitles(page1.GetRows()), []string{"a"}) {
		t.Fatalf("page1 rows=%+v next=%q err=%v", journalTitles(page1.GetRows()), page1.GetNextPageToken(), err)
	}
	page2, err := querySvc.ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: journalOrderedQuery(clientv1.SortDirection_SORT_DIRECTION_ASC), PageSize: 1, PageToken: page1.GetNextPageToken()})
	if err != nil || page2.GetNextPageToken() != "" || !reflect.DeepEqual(journalTitles(page2.GetRows()), []string{"b"}) {
		t.Fatalf("page2 rows=%+v next=%q err=%v", journalTitles(page2.GetRows()), page2.GetNextPageToken(), err)
	}
}

func TestQueryServiceExecuteGQLUsesOrderedNodePropertyIndex(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	manager := journalSchemaManagerForQueryTest(t, fixture.domainID, true)
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	for _, item := range []struct{ title, date string }{{"latest", "2026-07-20"}, {"oldest", "2026-07-18"}} {
		if _, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx, Node: &clientv1.NodeCreate{Labels: []string{"JournalEntry"}, Properties: mustStruct(t, map[string]any{"title": item.title, "date": item.date})}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatal(err)
	}
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	res, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithSchemaManager(manager).ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: readTx, Query: "MATCH (j:JournalEntry) RETURN j ORDER BY j.date FETCH FIRST 10 ROWS ONLY", PageSize: 10})
	if err != nil {
		t.Fatalf("ExecuteGQL() error = %v", err)
	}
	if got := journalTitles(res.GetResult().GetRows()); !reflect.DeepEqual(got, []string{"oldest", "latest"}) {
		t.Fatalf("unexpected GQL order: %+v", got)
	}
	if res.GetDiagnostics().GetPlan() != "OrderedNodePropertyIndexScan" || res.GetDiagnostics().GetFullScan() || res.GetDiagnostics().GetEdgesLoaded() != 0 {
		t.Fatalf("unexpected GQL diagnostics: %+v", res.GetDiagnostics())
	}
}

func TestQueryServiceExecuteQueryIndexedRootSubtreeGraph(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	manager := pkmJournalSubtreeSchemaManagerForQueryTest(t, fixture.domainID)
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	createJournalSubtreeFixture(t, fixture, graphSvc, writeTx, 10)
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatal(err)
	}
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	res, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithSchemaManager(manager).ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: pkmJournalSubtreeQuery(20260701, 20260710, 7, 0, 0), PageSize: 7})
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}
	assertJournalSubtreeResult(t, res.GetRows(), res.GetResult().GetGraph(), []int{20260710, 20260709, 20260708, 20260707, 20260706, 20260705, 20260704})
	if diag := res.GetDiagnostics(); diag.GetPlan() != indexedSubtreePlanName || diag.GetFullScan() || diag.GetRootCount() != 7 || diag.GetTruncated() || diag.GetEdgesLoaded() != 21 || diag.GetNodesLoaded() < 28 || diag.GetAdjacencyScanCalls() != 21 || diag.GetNodeReadCalls() != 21 || !containsString(diag.GetIndexes(), "pkm_journals_by_day") || !containsString(diag.GetIndexes(), "out:contains") {
		t.Fatalf("unexpected diagnostics: %+v", diag)
	}
}

func TestQueryServiceExecuteGQLIndexedRootSubtreeGraph(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	manager := pkmJournalSubtreeSchemaManagerForQueryTest(t, fixture.domainID)
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	createJournalSubtreeFixture(t, fixture, graphSvc, writeTx, 10)
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatal(err)
	}
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	query := "MATCH (d:pkm.journal)-[:contains*0..2]->(n) WHERE d.journal_day BETWEEN 20260701 AND 20260710 RETURN GRAPH d,n ORDER BY d.journal_day DESC FETCH FIRST 7 ROWS ONLY"
	res, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithSchemaManager(manager).ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: readTx, Query: query, PageSize: 7})
	if err != nil {
		t.Fatalf("ExecuteGQL() error = %v", err)
	}
	assertJournalSubtreeResult(t, res.GetResult().GetRows(), res.GetResult().GetGraph(), []int{20260710, 20260709, 20260708, 20260707, 20260706, 20260705, 20260704})
	if diag := res.GetDiagnostics(); diag.GetPlan() != indexedSubtreePlanName || diag.GetFullScan() || diag.GetRootCount() != 7 || diag.GetTruncated() || !containsString(diag.GetIndexes(), "pkm_journals_by_day") || !containsString(diag.GetIndexes(), "out:contains") {
		t.Fatalf("unexpected diagnostics: %+v", diag)
	}
}

func TestQueryServiceExecuteQueryIndexedRootSubtreePagination(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	manager := pkmJournalSubtreeSchemaManagerForQueryTest(t, fixture.domainID)
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	createJournalSubtreeFixture(t, fixture, graphSvc, writeTx, 14)
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatal(err)
	}
	querySvc := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithSchemaManager(manager)
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	query := pkmJournalSubtreeQuery(20260701, 20260714, 7, 0, 0)
	page1, err := querySvc.ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: query, PageSize: 7})
	if err != nil || page1.GetNextPageToken() == "" {
		t.Fatalf("page1 next=%q err=%v", page1.GetNextPageToken(), err)
	}
	page2, err := querySvc.ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: query, PageSize: 7, PageToken: page1.GetNextPageToken()})
	if err != nil {
		t.Fatalf("page2 error = %v", err)
	}
	got := append(journalDaysFromRows(page1.GetRows()), journalDaysFromRows(page2.GetRows())...)
	want := []int{20260714, 20260713, 20260712, 20260711, 20260710, 20260709, 20260708, 20260707, 20260706, 20260705, 20260704, 20260703, 20260702, 20260701}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("paged root days = %+v, want %+v", got, want)
	}
}

func TestQueryServiceExecuteQueryIndexedRootSubtreeTruncation(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	manager := pkmJournalSubtreeSchemaManagerForQueryTest(t, fixture.domainID)
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	createJournalSubtreeFixture(t, fixture, graphSvc, writeTx, 1)
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatal(err)
	}
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	res, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithSchemaManager(manager).ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: pkmJournalSubtreeQuery(20260701, 20260701, 1, 2, 10), PageSize: 1})
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}
	if !res.GetDiagnostics().GetTruncated() || res.GetDiagnostics().GetTruncationReason() == "" || res.GetNextPageToken() != "" || len(res.GetResult().GetGraph().GetNodes()) != 2 {
		t.Fatalf("expected visible node truncation, diagnostics=%+v graph nodes=%d", res.GetDiagnostics(), len(res.GetResult().GetGraph().GetNodes()))
	}
}

func pkmJournalSubtreeSchemaManagerForQueryTest(t *testing.T, domainID string) schemaservice.Manager {
	t.Helper()
	manager := schemaservice.NewManager(storage.NewMemoryStore())
	source := `schema "Knot PKM" version "1" mode warn
node pkm.journal {
  journal_day: int required
  title: string
}
node pkm.block {
  title: string
}
edge contains from pkm.journal to pkm.block hierarchy
index pkm_journals_by_day on node pkm.journal field properties.journal_day ordered asc`
	if err := manager.PutDomainSchemaGWL(context.Background(), mustDomainUUID(t, domainID), source); err != nil {
		t.Fatalf("PutDomainSchemaGWL() error = %v", err)
	}
	return manager
}

func createJournalSubtreeFixture(t *testing.T, fixture domainPolicyClientAPIFixture, graphSvc *GraphService, tx string, days int) {
	t.Helper()
	for day := 1; day <= days; day++ {
		journalDay := 20260700 + day
		root := createQueryTestNode(t, fixture, graphSvc, tx, []string{"pkm.journal"}, map[string]any{"title": fmt.Sprintf("journal-%d", journalDay), "journal_day": journalDay})
		childA := createQueryTestNode(t, fixture, graphSvc, tx, []string{"pkm.block"}, map[string]any{"title": fmt.Sprintf("%d-a", journalDay)})
		childB := createQueryTestNode(t, fixture, graphSvc, tx, []string{"pkm.block"}, map[string]any{"title": fmt.Sprintf("%d-b", journalDay)})
		grandchild := createQueryTestNode(t, fixture, graphSvc, tx, []string{"pkm.block"}, map[string]any{"title": fmt.Sprintf("%d-a-1", journalDay)})
		createQueryTestEdge(t, fixture, graphSvc, tx, root, childA, []string{"contains"}, map[string]any{"order": 1})
		createQueryTestEdge(t, fixture, graphSvc, tx, root, childB, []string{"contains"}, map[string]any{"order": 2})
		createQueryTestEdge(t, fixture, graphSvc, tx, childA, grandchild, []string{"contains"}, map[string]any{"order": 1})
	}
}

func pkmJournalSubtreeQuery(low int, high int, limit int32, maxNodes int32, maxEdges int32) *clientv1.GraphQuery {
	return &clientv1.GraphQuery{Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "d", Labels: []string{"pkm.journal"}}, Steps: []*clientv1.TraversalStep{{Direction: clientv1.TraversalDirection_TRAVERSAL_DIRECTION_OUT, EdgeKind: "contains", Depth: &clientv1.DepthSpec{MinDepth: 0, MaxDepth: 2}, Target: &clientv1.NodePattern{Alias: "n"}}}}, Where: &clientv1.Expr{Expr: &clientv1.Expr_Between{Between: &clientv1.BetweenExpr{Value: &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Prop{Prop: &clientv1.PropExpr{Alias: "d", Name: "journal_day"}}}, Low: &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Literal{Literal: &clientv1.LiteralExpr{Value: structpb.NewNumberValue(float64(low))}}}, High: &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Literal{Literal: &clientv1.LiteralExpr{Value: structpb.NewNumberValue(float64(high))}}}}}}, Returns: []*clientv1.ReturnProjection{{Alias: "d", OutputName: "journal", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_NODE}, {Alias: "n", OutputName: "graph", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_TREE}}, OrderBy: []*clientv1.OrderSpec{{Value: &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Prop{Prop: &clientv1.PropExpr{Alias: "d", Name: "journal_day"}}}, Direction: clientv1.SortDirection_SORT_DIRECTION_DESC}}, Limit: limit, MaxNodes: maxNodes, MaxEdges: maxEdges}
}

func assertJournalSubtreeResult(t *testing.T, rows []*clientv1.QueryRow, graph *clientv1.ResultGraph, wantDays []int) {
	t.Helper()
	if got := journalDaysFromRows(rows); !reflect.DeepEqual(got, wantDays) {
		t.Fatalf("root days = %+v, want %+v", got, wantDays)
	}
	if graph == nil {
		t.Fatal("result graph is nil")
	}
	if len(graph.GetNodes()) != len(wantDays)*4 || len(graph.GetEdges()) != len(wantDays)*3 {
		t.Fatalf("graph nodes=%d edges=%d, want nodes=%d edges=%d", len(graph.GetNodes()), len(graph.GetEdges()), len(wantDays)*4, len(wantDays)*3)
	}
	allowed := map[int]bool{}
	for _, day := range wantDays {
		allowed[day] = true
	}
	for _, node := range graph.GetNodes() {
		if nodeHasLabelProto(node, "pkm.journal") {
			day := int(node.GetProperties().GetFields()["journal_day"].GetNumberValue())
			if !allowed[day] {
				t.Fatalf("result graph contains unselected journal day %d", day)
			}
		}
	}
	firstTree := rows[0].GetFields()["graph"].GetTree()
	if firstTree == nil || len(firstTree.GetRoots()) != 1 || len(firstTree.GetRoots()[0].GetChildren()) != 2 {
		t.Fatalf("unexpected first tree: %+v", firstTree)
	}
	children := firstTree.GetRoots()[0].GetChildren()
	if !strings.HasSuffix(children[0].GetNode().GetProperties().GetFields()["title"].GetStringValue(), "-a") || !strings.HasSuffix(children[1].GetNode().GetProperties().GetFields()["title"].GetStringValue(), "-b") {
		t.Fatalf("children not ordered by contains.order: %+v", children)
	}
}

func journalDaysFromRows(rows []*clientv1.QueryRow) []int {
	out := make([]int, 0, len(rows))
	for _, row := range rows {
		node := row.GetFields()["journal"].GetNode()
		if node == nil {
			node = row.GetFields()["d"].GetNode()
		}
		out = append(out, int(node.GetProperties().GetFields()["journal_day"].GetNumberValue()))
	}
	return out
}

func nodeHasLabelProto(node *clientv1.Node, label string) bool {
	for _, got := range node.GetLabels() {
		if got == label {
			return true
		}
	}
	return false
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func TestQueryServiceExecuteQueryUnsupportedOrderByFailsClosed(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	manager := journalSchemaManagerForQueryTest(t, fixture.domainID, true)
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	query := journalOrderedQuery(clientv1.SortDirection_SORT_DIRECTION_ASC)
	query.Where = &clientv1.Expr{Expr: &clientv1.Expr_PropertyExists{PropertyExists: &clientv1.PropertyExistsExpr{Alias: "j", Name: "date"}}}
	_, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithSchemaManager(manager).ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: query, PageSize: 10})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ExecuteQuery() code=%v err=%v, want FailedPrecondition", status.Code(err), err)
	}
}

func TestQueryServiceExecuteQueryMissingOrderedIndexFailsClosed(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	manager := journalSchemaManagerForQueryTest(t, fixture.domainID, false)
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	_, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithSchemaManager(manager).ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: journalOrderedQuery(clientv1.SortDirection_SORT_DIRECTION_ASC), PageSize: 10})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ExecuteQuery() code=%v err=%v, want FailedPrecondition", status.Code(err), err)
	}
}

func journalSchemaManagerForQueryTest(t *testing.T, domainID string, withIndex bool) schemaservice.Manager {
	t.Helper()
	manager := schemaservice.NewManager(storage.NewMemoryStore())
	source := `schema "Journal" version "1" mode strict
node JournalEntry {
  title: string
  date: string required
}
node Note {
  title: string
}`
	if withIndex {
		source += "\nindex journal_entries_by_date on node JournalEntry field properties.date ordered asc"
	}
	if err := manager.PutDomainSchemaGWL(context.Background(), mustDomainUUID(t, domainID), source); err != nil {
		t.Fatalf("PutDomainSchemaGWL() error = %v", err)
	}
	return manager
}

func dottedJournalSchemaManagerForQueryTest(t *testing.T, domainID string) schemaservice.Manager {
	t.Helper()
	manager := schemaservice.NewManager(storage.NewMemoryStore())
	source := `schema "Knot PKM Personal" version "1" mode strict
node pkm.journal {
  journal_date: date required
  journal_day: int required
}
index pkm_journals_by_day on node pkm.journal field properties.journal_day ordered asc`
	if err := manager.PutDomainSchemaGWL(context.Background(), mustDomainUUID(t, domainID), source); err != nil {
		t.Fatalf("PutDomainSchemaGWL() error = %v", err)
	}
	return manager
}

func mustDomainUUID(t *testing.T, value string) graphmodel.DomainID {
	t.Helper()
	id, err := uuid.Parse(value)
	if err != nil {
		t.Fatalf("parse domain id: %v", err)
	}
	return graphmodel.DomainID(id)
}

func journalOrderedQuery(direction clientv1.SortDirection) *clientv1.GraphQuery {
	return &clientv1.GraphQuery{Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "j", Labels: []string{"JournalEntry"}}}, Returns: []*clientv1.ReturnProjection{{Alias: "j", OutputName: "journal", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_NODE}}, OrderBy: []*clientv1.OrderSpec{{Value: &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Prop{Prop: &clientv1.PropExpr{Alias: "j", Name: "date"}}}, Direction: direction}}}
}

func journalTitles(rows []*clientv1.QueryRow) []string {
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		node := row.GetFields()["journal"].GetNode()
		if node == nil {
			node = row.GetFields()["j"].GetNode()
		}
		out = append(out, node.GetProperties().GetFields()["title"].GetStringValue())
	}
	return out
}

func TestQueryServiceExecuteQueryUsesAdjacencyIndexForOutgoingEdgeProjection(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	source := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Note"}, map[string]any{"title": "source"})
	targetA := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Page"}, map[string]any{"title": "A"})
	targetB := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Page"}, map[string]any{"title": "B"})
	otherSource := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Note"}, map[string]any{"title": "other"})
	createQueryTestEdge(t, fixture, graphSvc, writeTx, source, targetA, []string{"references"}, map[string]any{"ref_type": "page", "normalized_target": "a"})
	createQueryTestEdge(t, fixture, graphSvc, writeTx, source, targetB, []string{"references"}, map[string]any{"ref_type": "page", "normalized_target": "b"})
	createQueryTestEdge(t, fixture, graphSvc, writeTx, otherSource, targetA, []string{"references"}, map[string]any{"ref_type": "page", "normalized_target": "a"})
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}

	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	res, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: referenceQuery(source, clientv1.TraversalDirection_TRAVERSAL_DIRECTION_OUT, ""), PageSize: 10})
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}
	if len(res.GetRows()) != 2 {
		t.Fatalf("rows = %d, want 2", len(res.GetRows()))
	}
	if got := referenceTargetTitles(res.GetRows()); !reflect.DeepEqual(got, []string{"A", "B"}) {
		t.Fatalf("target titles = %+v, want [A B]", got)
	}
	for _, row := range res.GetRows() {
		if row.GetFields()["ref"].GetEdge() == nil || row.GetFields()["source"].GetNode().GetNodeId() != source {
			t.Fatalf("row did not project source/ref/target: %+v", row.GetFields())
		}
	}
	if diag := res.GetDiagnostics(); diag.GetPlan() != "EdgeAdjacencyIndexScan" || diag.GetFullScan() || diag.GetEdgesLoaded() != 2 || diag.GetNodesLoaded() < 3 {
		t.Fatalf("unexpected diagnostics: %+v", diag)
	}
}

func TestQueryServiceExecuteQueryUsesAdjacencyIndexForIncomingEdgeProjection(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	target := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Page"}, map[string]any{"title": "target"})
	sourceA := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Note"}, map[string]any{"title": "source A"})
	sourceB := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Note"}, map[string]any{"title": "source B"})
	unrelated := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Page"}, map[string]any{"title": "unrelated"})
	createQueryTestEdge(t, fixture, graphSvc, writeTx, sourceA, target, []string{"references"}, map[string]any{"ref_type": "page"})
	createQueryTestEdge(t, fixture, graphSvc, writeTx, sourceB, target, []string{"references"}, map[string]any{"ref_type": "page"})
	createQueryTestEdge(t, fixture, graphSvc, writeTx, sourceA, unrelated, []string{"references"}, map[string]any{"ref_type": "page"})
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}

	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	res, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: referenceQuery(target, clientv1.TraversalDirection_TRAVERSAL_DIRECTION_IN, ""), PageSize: 10})
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}
	if got := referenceSourceTitles(res.GetRows()); !reflect.DeepEqual(got, []string{"source A", "source B"}) {
		t.Fatalf("source titles = %+v, want [source A source B]", got)
	}
	if diag := res.GetDiagnostics(); diag.GetPlan() != "EdgeAdjacencyIndexScan" || diag.GetFullScan() || diag.GetEdgesLoaded() != 2 {
		t.Fatalf("unexpected diagnostics: %+v", diag)
	}
}

func TestQueryServiceExecuteQueryAdjacencyEdgePropertyFilterAndPagination(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	source := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Note"}, map[string]any{"title": "source"})
	targetA := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Page"}, map[string]any{"title": "A"})
	targetB := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Page"}, map[string]any{"title": "B"})
	targetC := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Page"}, map[string]any{"title": "C"})
	createQueryTestEdge(t, fixture, graphSvc, writeTx, source, targetA, []string{"references"}, map[string]any{"normalized_target": "keep"})
	createQueryTestEdge(t, fixture, graphSvc, writeTx, source, targetB, []string{"references"}, map[string]any{"normalized_target": "drop"})
	createQueryTestEdge(t, fixture, graphSvc, writeTx, source, targetC, []string{"references"}, map[string]any{"normalized_target": "keep"})
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}

	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	query := referenceQuery(source, clientv1.TraversalDirection_TRAVERSAL_DIRECTION_OUT, "keep")
	page1, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: query, PageSize: 1})
	if err != nil {
		t.Fatalf("ExecuteQuery(page1) error = %v", err)
	}
	if len(page1.GetRows()) != 1 || page1.GetNextPageToken() == "" {
		t.Fatalf("page1 rows=%d next=%q, want one row and cursor", len(page1.GetRows()), page1.GetNextPageToken())
	}
	page2, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: query, PageSize: 10, PageToken: page1.GetNextPageToken()})
	if err != nil {
		t.Fatalf("ExecuteQuery(page2) error = %v", err)
	}
	all := append(referenceTargetTitles(page1.GetRows()), referenceTargetTitles(page2.GetRows())...)
	if !reflect.DeepEqual(all, []string{"A", "C"}) {
		t.Fatalf("filtered/paged titles = %+v, want [A C]", all)
	}
}

func referenceQuery(startID string, direction clientv1.TraversalDirection, normalizedTarget string) *clientv1.GraphQuery {
	startAlias, targetAlias := "source", "target"
	if direction == clientv1.TraversalDirection_TRAVERSAL_DIRECTION_IN {
		startAlias, targetAlias = "target", "source"
	}
	query := &clientv1.GraphQuery{
		Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: startAlias, NodeIds: []string{startID}}, Steps: []*clientv1.TraversalStep{{Direction: direction, EdgeKind: "references", EdgeAlias: "ref", Target: &clientv1.NodePattern{Alias: targetAlias}}}},
		Returns: []*clientv1.ReturnProjection{
			{Alias: "source", OutputName: "source", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_NODE},
			{Alias: "ref", OutputName: "ref", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_EDGE},
			{Alias: "target", OutputName: "target", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_NODE},
		},
	}
	if normalizedTarget != "" {
		query.Where = &clientv1.Expr{Expr: &clientv1.Expr_PropertyEquals{PropertyEquals: &clientv1.PropertyEqualsExpr{Alias: "ref", Name: "normalized_target", Value: structpb.NewStringValue(normalizedTarget)}}}
	}
	return query
}

func createQueryTestNode(t *testing.T, fixture domainPolicyClientAPIFixture, graphSvc *GraphService, tx string, labels []string, properties map[string]any) string {
	t.Helper()
	res, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: tx, Node: &clientv1.NodeCreate{Labels: labels, Properties: mustStruct(t, properties)}})
	if err != nil {
		t.Fatalf("CreateNode() error = %v", err)
	}
	return res.GetNode().GetNodeId()
}

func createQueryTestEdge(t *testing.T, fixture domainPolicyClientAPIFixture, graphSvc *GraphService, tx string, from string, to string, labels []string, properties map[string]any) string {
	t.Helper()
	res, err := graphSvc.CreateEdge(fixture.ctx, &clientv1.CreateEdgeRequest{TransactionId: tx, Edge: &clientv1.EdgeCreate{FromNodeId: from, ToNodeId: to, Labels: labels, Properties: mustStruct(t, properties)}})
	if err != nil {
		t.Fatalf("CreateEdge() error = %v", err)
	}
	return res.GetEdge().GetEdgeId()
}

func referenceTargetTitles(rows []*clientv1.QueryRow) []string {
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.GetFields()["target"].GetNode().GetProperties().GetFields()["title"].GetStringValue())
	}
	return out
}

func referenceSourceTitles(rows []*clientv1.QueryRow) []string {
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.GetFields()["source"].GetNode().GetProperties().GetFields()["title"].GetStringValue())
	}
	return out
}

func TestQueryServiceExecuteQueryAdjacencyMultiStartPagination(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	sourceA := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Note"}, map[string]any{"title": "source A"})
	sourceB := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Note"}, map[string]any{"title": "source B"})
	targetA := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Page"}, map[string]any{"title": "A"})
	targetB := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Page"}, map[string]any{"title": "B"})
	createQueryTestEdge(t, fixture, graphSvc, writeTx, sourceA, targetA, []string{"references"}, map[string]any{"normalized_target": "a"})
	createQueryTestEdge(t, fixture, graphSvc, writeTx, sourceB, targetB, []string{"references"}, map[string]any{"normalized_target": "b"})
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}

	query := referenceQuery(sourceA, clientv1.TraversalDirection_TRAVERSAL_DIRECTION_OUT, "")
	query.GetMatch().GetStart().NodeIds = []string{sourceA, sourceB}
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	querySvc := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces)
	page1, err := querySvc.ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: query, PageSize: 1})
	if err != nil {
		t.Fatalf("ExecuteQuery(page1) error = %v", err)
	}
	if got := referenceTargetTitles(page1.GetRows()); !reflect.DeepEqual(got, []string{"A"}) || page1.GetNextPageToken() == "" {
		t.Fatalf("page1 titles=%+v next=%q, want [A] and cursor", got, page1.GetNextPageToken())
	}
	page2, err := querySvc.ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: query, PageSize: 1, PageToken: page1.GetNextPageToken()})
	if err != nil {
		t.Fatalf("ExecuteQuery(page2) error = %v", err)
	}
	if got := referenceTargetTitles(page2.GetRows()); !reflect.DeepEqual(got, []string{"B"}) || page2.GetNextPageToken() != "" {
		t.Fatalf("page2 titles=%+v next=%q, want [B] and no cursor", got, page2.GetNextPageToken())
	}
}

func TestQueryServiceExecuteQueryStrictSchemaAllowsIncomingEdgeAliasTraversal(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	manager := referenceSchemaManagerForQueryTest(t, fixture.domainID)
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	source := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Note"}, map[string]any{"title": "source"})
	target := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Page"}, map[string]any{"title": "target"})
	createQueryTestEdge(t, fixture, graphSvc, writeTx, source, target, []string{"references"}, map[string]any{"normalized_target": "target"})
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}

	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	res, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithSchemaManager(manager).ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: referenceQuery(target, clientv1.TraversalDirection_TRAVERSAL_DIRECTION_IN, "target"), PageSize: 10})
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}
	if got := referenceSourceTitles(res.GetRows()); !reflect.DeepEqual(got, []string{"source"}) {
		t.Fatalf("source titles=%+v, want [source]", got)
	}
}

func referenceSchemaManagerForQueryTest(t *testing.T, domainID string) schemaservice.Manager {
	t.Helper()
	manager := schemaservice.NewManager(storage.NewMemoryStore())
	source := `schema "References" version "1" mode strict
node Note {
  title: string
}
node Page {
  title: string
}
edge references from Note to Page {
  normalized_target: string
}`
	if err := manager.PutDomainSchemaGWL(context.Background(), mustDomainUUID(t, domainID), source); err != nil {
		t.Fatalf("PutDomainSchemaGWL() error = %v", err)
	}
	return manager
}

func TestQueryServiceExecuteQueryAdjacencyHonorsQueryLimit(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	source := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Note"}, map[string]any{"title": "source"})
	targetA := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Page"}, map[string]any{"title": "A"})
	targetB := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Page"}, map[string]any{"title": "B"})
	createQueryTestEdge(t, fixture, graphSvc, writeTx, source, targetA, []string{"references"}, map[string]any{"normalized_target": "a"})
	createQueryTestEdge(t, fixture, graphSvc, writeTx, source, targetB, []string{"references"}, map[string]any{"normalized_target": "b"})
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}

	query := referenceQuery(source, clientv1.TraversalDirection_TRAVERSAL_DIRECTION_OUT, "")
	query.Limit = 1
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	res, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: query, PageSize: 10})
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}
	if len(res.GetRows()) != 1 || res.GetNextPageToken() != "" {
		t.Fatalf("rows=%d next=%q, want one row and no cursor due query limit", len(res.GetRows()), res.GetNextPageToken())
	}
}

func TestQueryServiceExecuteQueryAdjacencyPaginatesWriteOverlay(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	source := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Note"}, map[string]any{"title": "source"})
	targetA := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Page"}, map[string]any{"title": "A"})
	targetB := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Page"}, map[string]any{"title": "B"})
	createQueryTestEdge(t, fixture, graphSvc, writeTx, source, targetA, []string{"references"}, map[string]any{"order": 1000})
	createQueryTestEdge(t, fixture, graphSvc, writeTx, source, targetB, []string{"references"}, map[string]any{"order": 2000})

	query := referenceQuery(source, clientv1.TraversalDirection_TRAVERSAL_DIRECTION_OUT, "")
	querySvc := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces)
	page1, err := querySvc.ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: writeTx, Query: query, PageSize: 1})
	if err != nil {
		t.Fatalf("ExecuteQuery(page1) error = %v", err)
	}
	if got := referenceTargetTitles(page1.GetRows()); !reflect.DeepEqual(got, []string{"A"}) || page1.GetNextPageToken() == "" {
		t.Fatalf("page1 titles=%+v next=%q, want [A] and cursor", got, page1.GetNextPageToken())
	}
	page2, err := querySvc.ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: writeTx, Query: query, PageSize: 1, PageToken: page1.GetNextPageToken()})
	if err != nil {
		t.Fatalf("ExecuteQuery(page2) error = %v", err)
	}
	if got := referenceTargetTitles(page2.GetRows()); !reflect.DeepEqual(got, []string{"B"}) || page2.GetNextPageToken() != "" {
		t.Fatalf("page2 titles=%+v next=%q, want [B] and no cursor", got, page2.GetNextPageToken())
	}
}
