package client

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	graphmodel "github.com/myceldb/mycel/internal/graph/model"
	schemaservice "github.com/myceldb/mycel/internal/schema/service"
	"github.com/myceldb/mycel/internal/schema/storage"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	semanticsearch "github.com/myceldb/mycel/internal/semantic/search"
	daemonsemantic "github.com/myceldb/mycel/internal/semantic/service"
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

func TestQueryServiceExecuteQueryUsesIndexedEqualityNodeStart(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{SearchMode: graphmodel.DomainSearchModeDisabled})
	manager := journalSchemaManagerForQueryTest(t, fixture.domainID, true)
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	for _, item := range []struct{ title, date string }{{"target", "2026-07-20"}, {"other", "2026-07-19"}} {
		if _, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx, Node: &clientv1.NodeCreate{Labels: []string{"JournalEntry"}, Properties: mustStruct(t, map[string]any{"title": item.title, "date": item.date})}}); err != nil {
			t.Fatalf("CreateNode(%s) error = %v", item.title, err)
		}
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}
	query := &clientv1.GraphQuery{
		Match:   &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "j", Labels: []string{"JournalEntry"}}},
		Where:   &clientv1.Expr{Expr: &clientv1.Expr_PropertyEquals{PropertyEquals: &clientv1.PropertyEqualsExpr{Alias: "j", Name: "date", Value: structpb.NewStringValue("2026-07-20")}}},
		Returns: []*clientv1.ReturnProjection{{Alias: "j.title", OutputName: "title", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_SCALAR}},
	}
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	res, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithSchemaManager(manager).ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: query, PageSize: 10})
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}
	if len(res.GetRows()) != 1 || res.GetRows()[0].GetFields()["title"].GetScalar().GetStringValue() != "target" {
		t.Fatalf("unexpected rows: %+v", res.GetRows())
	}
	if res.GetDiagnostics().GetPlan() != "OrderedNodePropertyEqualityIndexScan" || res.GetDiagnostics().GetFullScan() || res.GetDiagnostics().GetEdgesLoaded() != 0 {
		t.Fatalf("diagnostics = %+v", res.GetDiagnostics())
	}
}

func TestQueryServiceExecuteQueryStructuredScalarFieldProjection(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	manager := journalSchemaManagerForQueryTest(t, fixture.domainID, true)
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	if _, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx, Node: &clientv1.NodeCreate{Labels: []string{"JournalEntry"}, Properties: mustStruct(t, map[string]any{"title": "latest", "date": "2026-07-20"})}}); err != nil {
		t.Fatalf("CreateNode() error = %v", err)
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	query := journalOrderedQuery(clientv1.SortDirection_SORT_DIRECTION_ASC)
	query.Returns = []*clientv1.ReturnProjection{{Alias: "j.title", OutputName: "title", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_SCALAR}}
	res, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithSchemaManager(manager).ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: query, PageSize: 10})
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}
	if got := res.GetRows()[0].GetFields()["title"].GetScalar().GetStringValue(); got != "latest" {
		t.Fatalf("scalar title = %q", got)
	}
	if res.GetDiagnostics().GetFullScan() {
		t.Fatalf("diagnostics = %+v", res.GetDiagnostics())
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

func TestQueryServiceExplainQueryReportsIndexedPlanWithoutExecutingReads(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	manager := journalSchemaManagerForQueryTest(t, fixture.domainID, true)
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	res, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithSchemaManager(manager).ExplainQuery(fixture.ctx, &clientv1.ExplainQueryRequest{TransactionId: readTx, Query: journalOrderedQuery(clientv1.SortDirection_SORT_DIRECTION_ASC)})
	if err != nil {
		t.Fatalf("ExplainQuery() error = %v", err)
	}
	diag := res.GetDiagnostics()
	if !diag.GetExplainOnly() || diag.GetPlanner() == "" || diag.GetPlannerVersion() == "" || diag.GetPlan() != "OrderedNodePropertyIndexScan" || diag.GetPlanKind() != "indexed_order" || diag.GetFullScan() || !containsString(diag.GetIndexes(), "journal_entries_by_date") {
		t.Fatalf("unexpected diagnostics: %+v", diag)
	}
	if diag.GetNodesLoaded() != 0 || diag.GetEdgesLoaded() != 0 || diag.GetRowsReturned() != 0 {
		t.Fatalf("explain loaded/executed rows: %+v", diag)
	}
}

func TestQueryServiceExplainQueryReportsRejectedMissingIndex(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{SearchMode: graphmodel.DomainSearchModeDisabled})
	manager := journalSchemaManagerForQueryTest(t, fixture.domainID, false)
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	res, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithSchemaManager(manager).ExplainQuery(fixture.ctx, &clientv1.ExplainQueryRequest{TransactionId: readTx, Query: journalOrderedQuery(clientv1.SortDirection_SORT_DIRECTION_ASC)})
	if err != nil {
		t.Fatalf("ExplainQuery() error = %v", err)
	}
	diag := res.GetDiagnostics()
	if !diag.GetExplainOnly() || diag.GetPlanKind() != "rejected" || diag.GetRejectedReason() == "" || diag.GetRequiredIndex() != "JournalEntry.properties.date" || diag.GetFullScan() {
		t.Fatalf("unexpected rejection diagnostics: %+v", diag)
	}
}

func TestQueryServiceExplainGQLReportsIndexedAndFallbackPlans(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	manager := journalSchemaManagerForQueryTest(t, fixture.domainID, true)
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	querySvc := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithSchemaManager(manager)
	indexed, err := querySvc.ExplainGQL(fixture.ctx, &clientv1.ExplainGQLRequest{TransactionId: readTx, Query: "MATCH (j:JournalEntry) RETURN j ORDER BY j.date"})
	if err != nil {
		t.Fatalf("ExplainGQL(indexed) error = %v", err)
	}
	if diag := indexed.GetDiagnostics(); diag.GetPlan() != "OrderedNodePropertyIndexScan" || diag.GetPlanKind() != "indexed_order" || diag.GetFullScan() || !diag.GetExplainOnly() {
		t.Fatalf("indexed diagnostics = %+v", diag)
	}
	fallback, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).ExplainGQL(fixture.ctx, &clientv1.ExplainGQLRequest{TransactionId: readTx, Query: "MATCH (p:Person) RETURN p"})
	if err != nil {
		t.Fatalf("ExplainGQL(fallback) error = %v", err)
	}
	if diag := fallback.GetDiagnostics(); diag.GetPlanKind() != "fallback" || !diag.GetFullScan() || diag.GetFallbackMode() == "" || !diag.GetExplainOnly() {
		t.Fatalf("fallback diagnostics = %+v", diag)
	}
}

func TestQueryServiceExecuteQueryIndexedOrderOffsetFetchCursorShapesRowsAndGraph(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	manager := journalSchemaManagerForQueryTest(t, fixture.domainID, true)
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	for _, item := range []struct{ title, date string }{{"a", "2026-07-18"}, {"b", "2026-07-19"}, {"c", "2026-07-20"}, {"d", "2026-07-21"}, {"e", "2026-07-22"}} {
		if _, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx, Node: &clientv1.NodeCreate{Labels: []string{"JournalEntry"}, Properties: mustStruct(t, map[string]any{"title": item.title, "date": item.date})}}); err != nil {
			t.Fatalf("CreateNode(%s) error = %v", item.title, err)
		}
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatal(err)
	}
	query := journalOrderedQuery(clientv1.SortDirection_SORT_DIRECTION_ASC)
	query.Offset = 1
	query.Limit = 3
	querySvc := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithSchemaManager(manager)
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	page1, err := querySvc.ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: query, PageSize: 2})
	if err != nil {
		t.Fatalf("ExecuteQuery(page1) error = %v", err)
	}
	if got := journalTitles(page1.GetRows()); !reflect.DeepEqual(got, []string{"b", "c"}) || page1.GetNextPageToken() == "" {
		t.Fatalf("page1 titles=%+v next=%q, want [b c] and cursor", got, page1.GetNextPageToken())
	}
	if graph := page1.GetResult().GetGraph(); len(graph.GetNodes()) != 2 || len(graph.GetEdges()) != 0 {
		t.Fatalf("page1 graph=%+v, want exactly two shaped row nodes", graph)
	}
	page2, err := querySvc.ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: query, PageSize: 2, PageToken: page1.GetNextPageToken()})
	if err != nil {
		t.Fatalf("ExecuteQuery(page2) error = %v", err)
	}
	if got := journalTitles(page2.GetRows()); !reflect.DeepEqual(got, []string{"d"}) || page2.GetNextPageToken() != "" {
		t.Fatalf("page2 titles=%+v next=%q, want [d] and no cursor", got, page2.GetNextPageToken())
	}
	if graph := page2.GetResult().GetGraph(); len(graph.GetNodes()) != 1 || len(graph.GetEdges()) != 0 {
		t.Fatalf("page2 graph=%+v, want exactly one shaped row node", graph)
	}
}

func TestQueryServiceExecuteGQLOrderDistinctOffsetFetchAndGraphEnvelope(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	for _, item := range []struct {
		name string
		rank int
		role string
	}{
		{"Ada", 3, "writer"},
		{"Bea", 1, "reader"},
		{"Cal", 2, "reader"},
		{"Dee", 4, "admin"},
	} {
		if _, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx, Node: &clientv1.NodeCreate{Labels: []string{"Person"}, Properties: mustStruct(t, map[string]any{"name": item.name, "rank": item.rank, "role": item.role})}}); err != nil {
			t.Fatalf("CreateNode(%s) error = %v", item.name, err)
		}
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatal(err)
	}
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	querySvc := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces)
	nodeRes, err := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: readTx, Query: "MATCH (p:Person) RETURN p ORDER BY p.rank OFFSET 1 FETCH FIRST 2 ROWS ONLY", PageSize: 10})
	if err != nil {
		t.Fatalf("ExecuteGQL(node order) error = %v", err)
	}
	if got := personNames(nodeRes.GetResult().GetRows(), "p"); !reflect.DeepEqual(got, []string{"Cal", "Ada"}) {
		t.Fatalf("ordered names=%+v, want [Cal Ada]", got)
	}
	if graph := nodeRes.GetResult().GetGraph(); len(graph.GetNodes()) != 2 || len(graph.GetEdges()) != 0 {
		t.Fatalf("graph=%+v, want exactly shaped returned nodes", graph)
	}
	distinctRes, err := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: readTx, Query: "MATCH (p:Person) RETURN DISTINCT p.role AS role ORDER BY p.role OFFSET 1 FETCH FIRST 2 ROWS ONLY", PageSize: 10})
	if err != nil {
		t.Fatalf("ExecuteGQL(distinct order) error = %v", err)
	}
	if got := scalarStrings(distinctRes.GetResult().GetRows(), "role"); !reflect.DeepEqual(got, []string{"reader", "writer"}) {
		t.Fatalf("distinct roles=%+v, want [reader writer]", got)
	}
}

func TestQueryServiceExecuteGQLIndexedOrderPreservesScalarProjection(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	manager := journalSchemaManagerForQueryTest(t, fixture.domainID, true)
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	if _, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx, Node: &clientv1.NodeCreate{Labels: []string{"JournalEntry"}, Properties: mustStruct(t, map[string]any{"title": "latest", "date": "2026-07-20"})}}); err != nil {
		t.Fatalf("CreateNode() error = %v", err)
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	res, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithSchemaManager(manager).ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: readTx, Query: "MATCH (j:JournalEntry) RETURN j.date AS date ORDER BY j.date", PageSize: 10})
	if err != nil {
		t.Fatalf("ExecuteGQL() error = %v", err)
	}
	if got := res.GetResult().GetRows()[0].GetFields()["date"].GetScalar().GetStringValue(); got != "2026-07-20" {
		t.Fatalf("date scalar = %q", got)
	}
	if res.GetDiagnostics().GetPlan() != "OrderedNodePropertyIndexScan" {
		t.Fatalf("diagnostics = %+v", res.GetDiagnostics())
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

func TestQueryServiceExecuteQueryIndexedStartPathTraversal(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{SearchMode: graphmodel.DomainSearchModeDisabled})
	manager := documentPathSchemaManagerForQueryTest(t, fixture.domainID)
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	doc1 := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Document"}, map[string]any{"title": "doc-1", "rank": 1})
	doc2 := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Document"}, map[string]any{"title": "doc-2", "rank": 2})
	doc3 := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Document"}, map[string]any{"title": "doc-3", "rank": 3})
	sec2 := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Section"}, map[string]any{"title": "doc-2-section"})
	leaf2 := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Section"}, map[string]any{"title": "doc-2-leaf"})
	sec3 := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Section"}, map[string]any{"title": "doc-3-section"})
	_ = doc1
	createQueryTestEdge(t, fixture, graphSvc, writeTx, doc2, sec2, []string{"contains"}, nil)
	createQueryTestEdge(t, fixture, graphSvc, writeTx, sec2, leaf2, []string{"contains"}, nil)
	createQueryTestEdge(t, fixture, graphSvc, writeTx, doc3, sec3, []string{"contains"}, nil)
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatal(err)
	}
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	query := &clientv1.GraphQuery{
		Match: &clientv1.GraphPattern{
			Start: &clientv1.NodePattern{Alias: "d", Labels: []string{"Document"}},
			Steps: []*clientv1.TraversalStep{{
				Direction: clientv1.TraversalDirection_TRAVERSAL_DIRECTION_OUT,
				EdgeKind:  "contains",
				Depth:     &clientv1.DepthSpec{MinDepth: 1, MaxDepth: 2},
				Target:    &clientv1.NodePattern{Alias: "s", Labels: []string{"Section"}},
			}},
		},
		PathAlias: "path",
		Where: &clientv1.Expr{Expr: &clientv1.Expr_Between{Between: &clientv1.BetweenExpr{
			Value: &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Prop{Prop: &clientv1.PropExpr{Alias: "d", Name: "rank"}}},
			Low:   &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Literal{Literal: &clientv1.LiteralExpr{Value: protoValue(2)}}},
			High:  &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Literal{Literal: &clientv1.LiteralExpr{Value: protoValue(3)}}},
		}}},
		Returns: []*clientv1.ReturnProjection{{Alias: "path", OutputName: "path", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_PATH}},
		Limit:   10,
	}
	res, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithSchemaManager(manager).ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: query, PageSize: 10})
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}
	if len(res.GetRows()) != 3 || len(res.GetResult().GetGraph().GetNodes()) != 5 || len(res.GetResult().GetGraph().GetEdges()) != 3 {
		t.Fatalf("rows=%d graph=%+v", len(res.GetRows()), res.GetResult().GetGraph())
	}
	if diag := res.GetDiagnostics(); diag.GetPlan() != "IndexedMultiHopAdjacencyPathScan" || diag.GetFullScan() || !containsString(diag.GetIndexes(), "documents_by_rank") || !containsString(diag.GetIndexes(), "out:contains") || diag.GetAdjacencyScanCalls() == 0 {
		t.Fatalf("unexpected diagnostics: %+v", diag)
	}
}

func TestQueryServiceExecuteQueryLabelIndexedStartPathTraversal(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{SearchMode: graphmodel.DomainSearchModeDisabled})
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	doc1 := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Document"}, map[string]any{"title": "doc-1"})
	doc2 := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Document"}, map[string]any{"title": "doc-2"})
	sec := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Section"}, map[string]any{"title": "section"})
	leaf := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Section"}, map[string]any{"title": "leaf"})
	_ = doc1
	createQueryTestEdge(t, fixture, graphSvc, writeTx, doc2, sec, []string{"contains"}, nil)
	createQueryTestEdge(t, fixture, graphSvc, writeTx, sec, leaf, []string{"contains"}, nil)
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatal(err)
	}
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	query := &clientv1.GraphQuery{
		Match: &clientv1.GraphPattern{
			Start: &clientv1.NodePattern{Alias: "d", Labels: []string{"Document"}},
			Steps: []*clientv1.TraversalStep{{
				Direction: clientv1.TraversalDirection_TRAVERSAL_DIRECTION_OUT,
				EdgeKind:  "contains",
				Depth:     &clientv1.DepthSpec{MinDepth: 1, MaxDepth: 2},
				Target:    &clientv1.NodePattern{Alias: "s", Labels: []string{"Section"}},
			}},
		},
		PathAlias: "path",
		Returns:   []*clientv1.ReturnProjection{{Alias: "path", OutputName: "path", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_PATH}},
		Limit:     10,
	}
	res, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: query, PageSize: 10})
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}
	if len(res.GetRows()) != 2 || len(res.GetResult().GetGraph().GetNodes()) != 3 || len(res.GetResult().GetGraph().GetEdges()) != 2 {
		t.Fatalf("rows=%d graph=%+v", len(res.GetRows()), res.GetResult().GetGraph())
	}
	if diag := res.GetDiagnostics(); diag.GetPlan() != "IndexedMultiHopAdjacencyPathScan" || diag.GetFullScan() || !containsString(diag.GetIndexes(), "label:Document") || !containsString(diag.GetIndexes(), "out:contains") {
		t.Fatalf("unexpected diagnostics: %+v", diag)
	}
}

func TestQueryServiceExecuteGQLLabelIndexedStartPathTraversal(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{SearchMode: graphmodel.DomainSearchModeDisabled})
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	doc := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Document"}, map[string]any{"title": "doc"})
	sec := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Section"}, map[string]any{"title": "section"})
	createQueryTestEdge(t, fixture, graphSvc, writeTx, doc, sec, []string{"contains"}, nil)
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatal(err)
	}
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	res, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: readTx, Query: "MATCH path = (d:Document)-[:contains]->(s:Section) RETURN path", PageSize: 10})
	if err != nil {
		t.Fatalf("ExecuteGQL() error = %v", err)
	}
	if len(res.GetResult().GetRows()) != 1 || len(res.GetResult().GetGraph().GetNodes()) != 2 || len(res.GetResult().GetGraph().GetEdges()) != 1 {
		t.Fatalf("rows=%d graph=%+v", len(res.GetResult().GetRows()), res.GetResult().GetGraph())
	}
	if diag := res.GetDiagnostics(); diag.GetPlan() != "IndexedMultiHopAdjacencyPathScan" || diag.GetFullScan() || !containsString(diag.GetIndexes(), "label:Document") || !containsString(diag.GetIndexes(), "out:contains") {
		t.Fatalf("unexpected diagnostics: %+v", diag)
	}
}

func TestQueryServiceExecuteQueryTagIndexedStartPathTraversal(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{SearchMode: graphmodel.DomainSearchModeDisabled})
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	doc1 := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Document"}, map[string]any{"title": "doc-1", "tags": []any{"project"}})
	doc2 := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Document"}, map[string]any{"title": "doc-2", "tags": []any{"archive"}})
	sec := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Section"}, map[string]any{"title": "section"})
	_ = doc2
	createQueryTestEdge(t, fixture, graphSvc, writeTx, doc1, sec, []string{"contains"}, nil)
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatal(err)
	}
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	query := &clientv1.GraphQuery{
		Match: &clientv1.GraphPattern{
			Start: &clientv1.NodePattern{Alias: "d", Labels: []string{"Document"}},
			Steps: []*clientv1.TraversalStep{{
				Direction: clientv1.TraversalDirection_TRAVERSAL_DIRECTION_OUT,
				EdgeKind:  "contains",
				Depth:     &clientv1.DepthSpec{MinDepth: 1, MaxDepth: 1},
				Target:    &clientv1.NodePattern{Alias: "s", Labels: []string{"Section"}},
			}},
		},
		PathAlias: "path",
		Where:     &clientv1.Expr{Expr: &clientv1.Expr_HasTag{HasTag: &clientv1.HasTagExpr{Alias: "d", Tag: "Project"}}},
		Returns:   []*clientv1.ReturnProjection{{Alias: "path", OutputName: "path", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_PATH}},
	}
	res, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: query, PageSize: 10})
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}
	if len(res.GetRows()) != 1 || len(res.GetResult().GetGraph().GetNodes()) != 2 || len(res.GetResult().GetGraph().GetEdges()) != 1 {
		t.Fatalf("rows=%d graph=%+v", len(res.GetRows()), res.GetResult().GetGraph())
	}
	if path := res.GetRows()[0].GetFields()["path"].GetPath(); path.GetNodes()[0].GetNodeId() != doc1 {
		t.Fatalf("path=%+v, want tagged document start", path)
	}
	if diag := res.GetDiagnostics(); diag.GetPlan() != "IndexedMultiHopAdjacencyPathScan" || diag.GetFullScan() || !containsString(diag.GetIndexes(), "tag:project") || !containsString(diag.GetIndexes(), "out:contains") {
		t.Fatalf("unexpected diagnostics: %+v", diag)
	}
}

func TestQueryServiceExecuteQueryLabelIndexedStartPathCapFailsClosed(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{SearchMode: graphmodel.DomainSearchModeDisabled})
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	for i := 0; i <= indexedPathMaxStartNodes; i++ {
		createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Document"}, map[string]any{"title": fmt.Sprintf("doc-%04d", i)})
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatal(err)
	}
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	query := &clientv1.GraphQuery{
		Match: &clientv1.GraphPattern{
			Start: &clientv1.NodePattern{Alias: "d", Labels: []string{"Document"}},
			Steps: []*clientv1.TraversalStep{{
				Direction: clientv1.TraversalDirection_TRAVERSAL_DIRECTION_OUT,
				EdgeKind:  "contains",
				Depth:     &clientv1.DepthSpec{MinDepth: 1, MaxDepth: 1},
				Target:    &clientv1.NodePattern{Alias: "s"},
			}},
		},
		PathAlias: "path",
		Returns:   []*clientv1.ReturnProjection{{Alias: "path", OutputName: "path", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_PATH}},
	}
	_, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: query, PageSize: 10})
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "indexed label path start matched more than") {
		t.Fatalf("ExecuteQuery() err=%v code=%v, want label start cap FailedPrecondition", err, status.Code(err))
	}
}

func TestQueryServiceExecuteQueryIndexedPathSkipsCycles(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{SearchMode: graphmodel.DomainSearchModeDisabled})
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	doc := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Document"}, map[string]any{"title": "doc"})
	section := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Section"}, map[string]any{"title": "section"})
	createQueryTestEdge(t, fixture, graphSvc, writeTx, doc, section, []string{"links"}, map[string]any{})
	createQueryTestEdge(t, fixture, graphSvc, writeTx, section, doc, []string{"links"}, map[string]any{})
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatal(err)
	}
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	query := &clientv1.GraphQuery{
		Match: &clientv1.GraphPattern{
			Start: &clientv1.NodePattern{Alias: "d", Labels: []string{"Document"}},
			Steps: []*clientv1.TraversalStep{{
				Direction: clientv1.TraversalDirection_TRAVERSAL_DIRECTION_OUT,
				EdgeKind:  "links",
				Depth:     &clientv1.DepthSpec{MinDepth: 1, MaxDepth: 2},
				Target:    &clientv1.NodePattern{Alias: "n"},
			}},
		},
		PathAlias: "path",
		Returns:   []*clientv1.ReturnProjection{{Alias: "path", OutputName: "path", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_PATH}},
	}
	res, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: query, PageSize: 10})
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}
	if len(res.GetRows()) != 1 {
		t.Fatalf("rows=%d, want 1 cycle-free path", len(res.GetRows()))
	}
	path := res.GetRows()[0].GetFields()["path"].GetPath()
	if len(path.GetNodes()) != 2 || path.GetNodes()[0].GetNodeId() != doc || path.GetNodes()[1].GetNodeId() != section || len(path.GetEdges()) != 1 {
		t.Fatalf("path=%+v, want doc -> section only", path)
	}
}

func TestQueryServiceExecuteQueryIndexedPathKeepsEdgeDistinctDuplicates(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{SearchMode: graphmodel.DomainSearchModeDisabled})
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	doc := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Document"}, map[string]any{"title": "doc"})
	section := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Section"}, map[string]any{"title": "section"})
	edgeA := createQueryTestEdge(t, fixture, graphSvc, writeTx, doc, section, []string{"links"}, map[string]any{"rank": 1})
	edgeB := createQueryTestEdge(t, fixture, graphSvc, writeTx, doc, section, []string{"links"}, map[string]any{"rank": 2})
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatal(err)
	}
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	query := &clientv1.GraphQuery{
		Match: &clientv1.GraphPattern{
			Start: &clientv1.NodePattern{Alias: "d", Labels: []string{"Document"}},
			Steps: []*clientv1.TraversalStep{{
				Direction: clientv1.TraversalDirection_TRAVERSAL_DIRECTION_OUT,
				EdgeKind:  "links",
				Depth:     &clientv1.DepthSpec{MinDepth: 1, MaxDepth: 1},
				Target:    &clientv1.NodePattern{Alias: "s", Labels: []string{"Section"}},
			}},
		},
		PathAlias: "path",
		Returns:   []*clientv1.ReturnProjection{{Alias: "path", OutputName: "path", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_PATH}},
	}
	res, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: query, PageSize: 10})
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}
	if len(res.GetRows()) != 2 || len(res.GetResult().GetGraph().GetEdges()) != 2 {
		t.Fatalf("rows=%d graph=%+v, want two edge-distinct paths", len(res.GetRows()), res.GetResult().GetGraph())
	}
	gotEdges := map[string]bool{}
	for _, row := range res.GetRows() {
		path := row.GetFields()["path"].GetPath()
		if len(path.GetEdges()) != 1 {
			t.Fatalf("path=%+v, want one edge", path)
		}
		gotEdges[path.GetEdges()[0].GetEdgeId()] = true
	}
	if !gotEdges[edgeA] || !gotEdges[edgeB] {
		t.Fatalf("path edge ids=%+v, want %s and %s", gotEdges, edgeA, edgeB)
	}
}

func TestQueryServiceExecuteGQLIndexedStartPathTraversal(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{SearchMode: graphmodel.DomainSearchModeDisabled})
	manager := documentPathSchemaManagerForQueryTest(t, fixture.domainID)
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	doc := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Document"}, map[string]any{"title": "doc-2", "rank": 2})
	sec := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Section"}, map[string]any{"title": "section"})
	leaf := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Section"}, map[string]any{"title": "leaf"})
	createQueryTestEdge(t, fixture, graphSvc, writeTx, doc, sec, []string{"contains"}, nil)
	createQueryTestEdge(t, fixture, graphSvc, writeTx, sec, leaf, []string{"contains"}, nil)
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatal(err)
	}
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	res, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithSchemaManager(manager).ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: readTx, Query: "MATCH path = (d:Document {rank: 2})-[:contains*1..2]->(s:Section) RETURN path", PageSize: 10})
	if err != nil {
		t.Fatalf("ExecuteGQL() error = %v", err)
	}
	if len(res.GetResult().GetRows()) != 2 || len(res.GetResult().GetGraph().GetNodes()) != 3 || len(res.GetResult().GetGraph().GetEdges()) != 2 {
		t.Fatalf("rows=%d graph=%+v", len(res.GetResult().GetRows()), res.GetResult().GetGraph())
	}
	if diag := res.GetDiagnostics(); diag.GetPlan() != "IndexedMultiHopAdjacencyPathScan" || diag.GetFullScan() || !containsString(diag.GetIndexes(), "documents_by_rank") || !containsString(diag.GetIndexes(), "out:contains") {
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

func documentPathSchemaManagerForQueryTest(t *testing.T, domainID string) schemaservice.Manager {
	t.Helper()
	manager := schemaservice.NewManager(storage.NewMemoryStore())
	source := `schema "Query Path Test" version "1" mode warn
node Document {
  rank: int required
  title: string
}
node Section {
  title: string
}
edge contains from Document to Section hierarchy
index documents_by_rank on node Document field properties.rank ordered asc`
	if err := manager.PutDomainSchemaGWL(context.Background(), mustDomainUUID(t, domainID), source); err != nil {
		t.Fatalf("PutDomainSchemaGWL() error = %v", err)
	}
	return manager
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

func TestQueryServiceExecuteQueryIndexedPredicateAndIntersection(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{SearchMode: graphmodel.DomainSearchModeDisabled})
	manager := journalSchemaManagerForQueryTest(t, fixture.domainID, true)
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	for _, item := range []struct{ title, date string }{{"target", "2026-07-20"}, {"wrong-title", "2026-07-20"}, {"target", "2026-07-19"}} {
		if _, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx, Node: &clientv1.NodeCreate{Labels: []string{"JournalEntry"}, Properties: mustStruct(t, map[string]any{"title": item.title, "date": item.date})}}); err != nil {
			t.Fatalf("CreateNode(%s/%s) error = %v", item.title, item.date, err)
		}
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}
	query := &clientv1.GraphQuery{
		Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "j", Labels: []string{"JournalEntry"}}},
		Where: &clientv1.Expr{Expr: &clientv1.Expr_And{And: &clientv1.AndExpr{Exprs: []*clientv1.Expr{
			{Expr: &clientv1.Expr_PropertyEquals{PropertyEquals: &clientv1.PropertyEqualsExpr{Alias: "j", Name: "date", Value: structpb.NewStringValue("2026-07-20")}}},
			{Expr: &clientv1.Expr_PropertyEquals{PropertyEquals: &clientv1.PropertyEqualsExpr{Alias: "j", Name: "title", Value: structpb.NewStringValue("target")}}},
		}}}},
		Returns: []*clientv1.ReturnProjection{{Alias: "j.title", OutputName: "title", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_SCALAR}},
	}
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	res, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithSchemaManager(manager).ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: query, PageSize: 10})
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}
	if len(res.GetRows()) != 1 || res.GetRows()[0].GetFields()["title"].GetScalar().GetStringValue() != "target" {
		t.Fatalf("unexpected rows: %+v", res.GetRows())
	}
	if diag := res.GetDiagnostics(); diag.GetPlan() != "OrderedNodePropertyPredicateIndexScan" || diag.GetFullScan() || !containsString(diag.GetIndexes(), "journal_entries_by_date") || !containsString(diag.GetIndexes(), "journal_entries_by_title") {
		t.Fatalf("unexpected diagnostics: %+v", diag)
	}
}

func TestQueryServiceExecuteQueryIndexedPredicateOrUnion(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{SearchMode: graphmodel.DomainSearchModeDisabled})
	manager := journalSchemaManagerForQueryTest(t, fixture.domainID, true)
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	for _, item := range []struct{ title, date string }{{"first", "2026-07-20"}, {"second", "2026-07-19"}, {"other", "2026-07-18"}} {
		if _, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx, Node: &clientv1.NodeCreate{Labels: []string{"JournalEntry"}, Properties: mustStruct(t, map[string]any{"title": item.title, "date": item.date})}}); err != nil {
			t.Fatalf("CreateNode(%s) error = %v", item.title, err)
		}
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}
	query := &clientv1.GraphQuery{
		Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "j", Labels: []string{"JournalEntry"}}},
		Where: &clientv1.Expr{Expr: &clientv1.Expr_Or{Or: &clientv1.OrExpr{Exprs: []*clientv1.Expr{
			{Expr: &clientv1.Expr_PropertyEquals{PropertyEquals: &clientv1.PropertyEqualsExpr{Alias: "j", Name: "date", Value: structpb.NewStringValue("2026-07-20")}}},
			{Expr: &clientv1.Expr_PropertyEquals{PropertyEquals: &clientv1.PropertyEqualsExpr{Alias: "j", Name: "date", Value: structpb.NewStringValue("2026-07-19")}}},
		}}}},
		Returns: []*clientv1.ReturnProjection{{Alias: "j.title", OutputName: "title", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_SCALAR}},
	}
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	res, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithSchemaManager(manager).ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: query, PageSize: 10})
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}
	got := scalarStrings(res.GetRows(), "title")
	sort.Strings(got)
	if !reflect.DeepEqual(got, []string{"first", "second"}) {
		t.Fatalf("titles = %+v", got)
	}
	if diag := res.GetDiagnostics(); diag.GetPlan() != "OrderedNodePropertyPredicateIndexScan" || diag.GetFullScan() || !containsString(diag.GetIndexes(), "journal_entries_by_date") {
		t.Fatalf("unexpected diagnostics: %+v", diag)
	}
}

func TestQueryServiceExecuteQueryIndexedPropertyExists(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{SearchMode: graphmodel.DomainSearchModeDisabled})
	manager := journalSchemaManagerForQueryTest(t, fixture.domainID, true)
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	for _, props := range []map[string]any{{"title": "has-title", "date": "2026-07-20"}, {"date": "2026-07-19"}} {
		if _, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx, Node: &clientv1.NodeCreate{Labels: []string{"JournalEntry"}, Properties: mustStruct(t, props)}}); err != nil {
			t.Fatalf("CreateNode() error = %v", err)
		}
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}
	query := &clientv1.GraphQuery{Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "j", Labels: []string{"JournalEntry"}}}, Where: &clientv1.Expr{Expr: &clientv1.Expr_PropertyExists{PropertyExists: &clientv1.PropertyExistsExpr{Alias: "j", Name: "title"}}}, Returns: []*clientv1.ReturnProjection{{Alias: "j.title", OutputName: "title", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_SCALAR}}}
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	res, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithSchemaManager(manager).ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: query, PageSize: 10})
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}
	if got := scalarStrings(res.GetRows(), "title"); !reflect.DeepEqual(got, []string{"has-title"}) {
		t.Fatalf("titles = %+v", got)
	}
}

func TestQueryServiceExecuteGQLUsesIndexedPredicateInSearchDisabledDomain(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{SearchMode: graphmodel.DomainSearchModeDisabled})
	manager := journalSchemaManagerForQueryTest(t, fixture.domainID, true)
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	for _, item := range []struct{ title, date string }{{"target", "2026-07-20"}, {"other", "2026-07-19"}} {
		if _, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx, Node: &clientv1.NodeCreate{Labels: []string{"JournalEntry"}, Properties: mustStruct(t, map[string]any{"title": item.title, "date": item.date})}}); err != nil {
			t.Fatalf("CreateNode(%s) error = %v", item.title, err)
		}
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	res, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithSchemaManager(manager).ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: readTx, Query: "MATCH (j:JournalEntry {date: '2026-07-20'}) RETURN j.title AS title", PageSize: 10})
	if err != nil {
		t.Fatalf("ExecuteGQL() error = %v", err)
	}
	if got := res.GetResult().GetRows()[0].GetFields()["title"].GetScalar().GetStringValue(); got != "target" {
		t.Fatalf("title = %q", got)
	}
	if diag := res.GetDiagnostics(); diag.GetPlan() != "OrderedNodePropertyEqualityIndexScan" && diag.GetPlan() != "OrderedNodePropertyPredicateIndexScan" {
		t.Fatalf("unexpected diagnostics: %+v", diag)
	}
}

func TestQueryServiceExecuteQueryIndexedStringContainsBoundedByPropertyIndex(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{SearchMode: graphmodel.DomainSearchModeDisabled})
	manager := journalSchemaManagerForQueryTest(t, fixture.domainID, true)
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	for _, title := range []string{"Alpha Note", "beta", "alphabet soup"} {
		if _, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx, Node: &clientv1.NodeCreate{Labels: []string{"JournalEntry"}, Properties: mustStruct(t, map[string]any{"title": title, "date": "2026-07-20"})}}); err != nil {
			t.Fatalf("CreateNode(%s) error = %v", title, err)
		}
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}
	query := &clientv1.GraphQuery{Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "j", Labels: []string{"JournalEntry"}}}, Where: &clientv1.Expr{Expr: &clientv1.Expr_StringPredicate{StringPredicate: &clientv1.StringPredicateExpr{Value: &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Prop{Prop: &clientv1.PropExpr{Alias: "j", Name: "title"}}}, Query: "ALPHA", Mode: clientv1.StringPredicateMode_STRING_PREDICATE_MODE_CONTAINS}}}, Returns: []*clientv1.ReturnProjection{{Alias: "j.title", OutputName: "title", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_SCALAR}}}
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	res, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithSchemaManager(manager).ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: query, PageSize: 10})
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}
	got := scalarStrings(res.GetRows(), "title")
	sort.Strings(got)
	if !reflect.DeepEqual(got, []string{"Alpha Note", "alphabet soup"}) {
		t.Fatalf("titles = %+v", got)
	}
	if diag := res.GetDiagnostics(); diag.GetPlan() != "OrderedNodePropertyPredicateIndexScan" || diag.GetFullScan() || !containsString(diag.GetIndexes(), "journal_entries_by_title") {
		t.Fatalf("unexpected diagnostics: %+v", diag)
	}
}

func TestQueryServiceExecuteGQLIndexedTextContainsProperty(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{SearchMode: graphmodel.DomainSearchModeDisabled})
	manager := journalSchemaManagerForQueryTest(t, fixture.domainID, true)
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	for _, title := range []string{"Graph Memory", "Other"} {
		if _, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx, Node: &clientv1.NodeCreate{Labels: []string{"JournalEntry"}, Properties: mustStruct(t, map[string]any{"title": title, "date": "2026-07-20"})}}); err != nil {
			t.Fatalf("CreateNode(%s) error = %v", title, err)
		}
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	res, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithSchemaManager(manager).ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: readTx, Query: "MATCH (j:JournalEntry) WHERE TEXT_CONTAINS(j.title, 'memory') RETURN j.title AS title", PageSize: 10})
	if err != nil {
		t.Fatalf("ExecuteGQL() error = %v", err)
	}
	if got := res.GetResult().GetRows()[0].GetFields()["title"].GetScalar().GetStringValue(); got != "Graph Memory" {
		t.Fatalf("title = %q", got)
	}
	if diag := res.GetDiagnostics(); diag.GetPlan() != "OrderedNodePropertyPredicateIndexScan" || diag.GetFullScan() || !containsString(diag.GetIndexes(), "journal_entries_by_title") {
		t.Fatalf("unexpected diagnostics: %+v", diag)
	}
}

func TestQueryServiceExecuteGQLUnindexedTextPropertyFailsClosed(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{SearchMode: graphmodel.DomainSearchModeDisabled})
	manager := journalSchemaManagerWithSummaryForQueryTest(t, fixture.domainID)
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	_, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithSchemaManager(manager).ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: readTx, Query: "MATCH (j:JournalEntry) WHERE TEXT_CONTAINS(j.summary, 'memory') RETURN j", PageSize: 10})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ExecuteGQL() code=%v err=%v, want FailedPrecondition", status.Code(err), err)
	}
}

func TestQueryServiceExecuteQueryUsesSemanticVectorSearch(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{SearchMode: graphmodel.DomainSearchModeDisabled})
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	created, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx, Node: &clientv1.NodeCreate{Labels: []string{"Note"}, Properties: mustStruct(t, map[string]any{"title": "not a textual match"})}})
	if err != nil {
		t.Fatalf("CreateNode() error = %v", err)
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	indexID := domainsemantic.SemanticIndexID(uuid.New())
	searcher := &fakeSemanticQuerySearcher{results: []semanticsearch.SearchResult{{SemanticIndexID: indexID, NodeID: graphmodel.NodeID(uuid.MustParse(created.GetNode().GetNodeId())), Score: 0.93}}}
	query := &clientv1.GraphQuery{Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "n", Labels: []string{"Note"}}}, Where: &clientv1.Expr{Expr: &clientv1.Expr_Semantic{Semantic: &clientv1.SemanticSearchExpr{Alias: "n", Query: "semantic-only needle", Limit: 10}}}, Returns: []*clientv1.ReturnProjection{{Alias: "n.title", OutputName: "title", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_SCALAR}}}
	res, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithSemanticManager(searcher).ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: query, PageSize: 10})
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}
	if got := scalarStrings(res.GetRows(), "title"); !reflect.DeepEqual(got, []string{"not a textual match"}) {
		t.Fatalf("titles = %+v", got)
	}
	if diag := res.GetDiagnostics(); diag.GetPlan() != "SemanticVectorSearch" || diag.GetFullScan() || !containsString(diag.GetIndexes(), indexID.String()) {
		t.Fatalf("unexpected diagnostics: %+v", diag)
	}
	if searcher.last.Text != "semantic-only needle" || searcher.last.Limit != 10 {
		t.Fatalf("semantic search input = %+v", searcher.last)
	}
	gqlRes, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithSemanticManager(searcher).ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: readTx, Query: "MATCH (n:Note) WHERE SEMANTIC_SIMILAR(n, 'semantic-only needle', TOP 10) RETURN n.title AS title", PageSize: 10})
	if err != nil {
		t.Fatalf("ExecuteGQL() error = %v", err)
	}
	if got := scalarStrings(gqlRes.GetResult().GetRows(), "title"); !reflect.DeepEqual(got, []string{"not a textual match"}) {
		t.Fatalf("GQL titles = %+v", got)
	}
	if diag := gqlRes.GetDiagnostics(); diag.GetPlan() != "SemanticVectorSearch" || diag.GetFullScan() {
		t.Fatalf("unexpected GQL diagnostics: %+v", diag)
	}
}

func TestQueryServiceExecuteQuerySemanticPredicateFailsClosedWithoutSearcher(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{SearchMode: graphmodel.DomainSearchModeDisabled})
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	query := &clientv1.GraphQuery{Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "n", Labels: []string{"Note"}}}, Where: &clientv1.Expr{Expr: &clientv1.Expr_Semantic{Semantic: &clientv1.SemanticSearchExpr{Alias: "n", Query: "needle", Limit: 10}}}}
	_, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: query, PageSize: 10})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ExecuteQuery() code=%v err=%v, want FailedPrecondition", status.Code(err), err)
	}
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
		source += "\nindex journal_entries_by_title on node JournalEntry field properties.title ordered asc"
	}
	if err := manager.PutDomainSchemaGWL(context.Background(), mustDomainUUID(t, domainID), source); err != nil {
		t.Fatalf("PutDomainSchemaGWL() error = %v", err)
	}
	return manager
}

func journalSchemaManagerWithSummaryForQueryTest(t *testing.T, domainID string) schemaservice.Manager {
	t.Helper()
	manager := schemaservice.NewManager(storage.NewMemoryStore())
	source := `schema "Journal" version "1" mode strict
node JournalEntry {
  title: string
  date: string required
  summary: string
}
index journal_entries_by_date on node JournalEntry field properties.date ordered asc
index journal_entries_by_title on node JournalEntry field properties.title ordered asc`
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

func scalarStrings(rows []*clientv1.QueryRow, field string) []string {
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		if value := row.GetFields()[field].GetScalar(); value != nil {
			out = append(out, value.GetStringValue())
		}
	}
	return out
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

func personNames(rows []*clientv1.QueryRow, field string) []string {
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		if node := row.GetFields()[field].GetNode(); node != nil {
			out = append(out, node.GetProperties().GetFields()["name"].GetStringValue())
		}
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
func TestQueryServiceExecuteGQLAggregationDistinctOffsetAndPredicates(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	for _, item := range []struct {
		name string
		role string
	}{
		{"Alice", "admin"},
		{"Alicia", "admin"},
		{"Bob", "reader"},
	} {
		if _, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx, Node: &clientv1.NodeCreate{Labels: []string{"Person"}, Properties: mustStruct(t, map[string]any{"name": item.name, "role": item.role})}}); err != nil {
			t.Fatalf("CreateNode(%s) error = %v", item.name, err)
		}
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	querySvc := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces)

	countRes, err := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: readTx, Query: "MATCH (p:Person) WHERE (p.name STARTS WITH 'Ali' OR p.role = 'reader') AND p.name IS NOT NULL RETURN COUNT(*) AS total"})
	if err != nil {
		t.Fatalf("ExecuteGQL(count) error = %v", err)
	}
	if got := countRes.GetResult().GetRows()[0].GetFields()["total"].GetScalar().GetNumberValue(); got != 3 {
		t.Fatalf("count = %v, want 3", got)
	}

	distinctRes, err := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: readTx, Query: "MATCH (p:Person) RETURN DISTINCT p.role AS role OFFSET 1 FETCH FIRST 1 ROW ONLY"})
	if err != nil {
		t.Fatalf("ExecuteGQL(distinct) error = %v", err)
	}
	rows := distinctRes.GetResult().GetRows()
	if len(rows) != 1 || rows[0].GetFields()["role"].GetScalar().GetStringValue() != "reader" {
		t.Fatalf("distinct rows = %+v", rows)
	}
}

func TestQueryServiceExecuteQueryAggregateFunctionsAndGrouping(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	for _, item := range []map[string]any{
		{"role": "reader", "score": 10},
		{"role": "reader", "score": 30},
		{"role": "reader"},
		{"role": "admin", "score": 5},
	} {
		if _, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx, Node: &clientv1.NodeCreate{Labels: []string{"Person"}, Properties: mustStruct(t, item)}}); err != nil {
			t.Fatalf("CreateNode() error = %v", err)
		}
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatal(err)
	}
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	score := &clientv1.AggregateArgument{Argument: &clientv1.AggregateArgument_Value{Value: &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Prop{Prop: &clientv1.PropExpr{Alias: "p", Name: "score"}}}}}
	query := &clientv1.GraphQuery{
		Match:   &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "p", Labels: []string{"Person"}}},
		Returns: []*clientv1.ReturnProjection{{Alias: "p.role", OutputName: "role", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_SCALAR}},
		AggregateReturns: []*clientv1.AggregateProjection{
			{OutputName: "total", Function: clientv1.AggregateFunction_AGGREGATE_FUNCTION_COUNT, Argument: &clientv1.AggregateArgument{Argument: &clientv1.AggregateArgument_Star{Star: true}}},
			{OutputName: "scored", Function: clientv1.AggregateFunction_AGGREGATE_FUNCTION_COUNT, Argument: score},
			{OutputName: "sum", Function: clientv1.AggregateFunction_AGGREGATE_FUNCTION_SUM, Argument: score},
			{OutputName: "avg", Function: clientv1.AggregateFunction_AGGREGATE_FUNCTION_AVG, Argument: score},
			{OutputName: "min", Function: clientv1.AggregateFunction_AGGREGATE_FUNCTION_MIN, Argument: score},
			{OutputName: "max", Function: clientv1.AggregateFunction_AGGREGATE_FUNCTION_MAX, Argument: score},
		},
	}
	res, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: query, PageSize: 10})
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}
	assertAggregateRows(t, res.GetRows())
}

func TestQueryServiceExecuteGQLAggregateFunctionsAndGrouping(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	for _, item := range []map[string]any{
		{"role": "reader", "score": 10},
		{"role": "reader", "score": 30},
		{"role": "reader"},
		{"role": "admin", "score": 5},
	} {
		if _, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx, Node: &clientv1.NodeCreate{Labels: []string{"Person"}, Properties: mustStruct(t, item)}}); err != nil {
			t.Fatalf("CreateNode() error = %v", err)
		}
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatal(err)
	}
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	res, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: readTx, Query: "MATCH (p:Person) RETURN p.role AS role, COUNT(*) AS total, COUNT(p.score) AS scored, SUM(p.score) AS sum, AVG(p.score) AS avg, MIN(p.score) AS min, MAX(p.score) AS max", PageSize: 10})
	if err != nil {
		t.Fatalf("ExecuteGQL() error = %v", err)
	}
	assertAggregateRows(t, res.GetResult().GetRows())
}

func assertAggregateRows(t *testing.T, rows []*clientv1.QueryRow) {
	t.Helper()
	byRole := map[string]map[string]float64{}
	for _, row := range rows {
		fields := row.GetFields()
		role := fields["role"].GetScalar().GetStringValue()
		byRole[role] = map[string]float64{}
		for _, name := range []string{"total", "scored", "sum", "avg", "min", "max"} {
			byRole[role][name] = fields[name].GetScalar().GetNumberValue()
		}
	}
	want := map[string]map[string]float64{
		"admin":  {"total": 1, "scored": 1, "sum": 5, "avg": 5, "min": 5, "max": 5},
		"reader": {"total": 3, "scored": 2, "sum": 40, "avg": 20, "min": 10, "max": 30},
	}
	if !reflect.DeepEqual(byRole, want) {
		t.Fatalf("aggregate rows = %+v, want %+v", byRole, want)
	}
}

func TestQueryServiceExecuteQueryStructuredAggregationAndPathValue(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	aID, bID, cID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	for _, node := range []struct{ id, name string }{{aID, "A"}, {bID, "B"}, {cID, "C"}} {
		if _, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx, Node: &clientv1.NodeCreate{NodeId: &node.id, Labels: []string{"Person"}, Properties: mustStruct(t, map[string]any{"name": node.name})}}); err != nil {
			t.Fatalf("CreateNode(%s) error = %v", node.name, err)
		}
	}
	if _, err := graphSvc.CreateEdge(fixture.ctx, &clientv1.CreateEdgeRequest{TransactionId: writeTx, Edge: &clientv1.EdgeCreate{FromNodeId: aID, ToNodeId: bID, Labels: []string{"FRIEND_OF"}}}); err != nil {
		t.Fatalf("CreateEdge(a-b) error = %v", err)
	}
	if _, err := graphSvc.CreateEdge(fixture.ctx, &clientv1.CreateEdgeRequest{TransactionId: writeTx, Edge: &clientv1.EdgeCreate{FromNodeId: bID, ToNodeId: cID, Labels: []string{"FRIEND_OF"}}}); err != nil {
		t.Fatalf("CreateEdge(b-c) error = %v", err)
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	querySvc := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces)

	countRes, err := querySvc.ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: &clientv1.GraphQuery{Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "p", Labels: []string{"Person"}}}, AggregateReturns: []*clientv1.AggregateProjection{{OutputName: "total", Function: clientv1.AggregateFunction_AGGREGATE_FUNCTION_COUNT, Argument: &clientv1.AggregateArgument{Argument: &clientv1.AggregateArgument_Star{Star: true}}}}}})
	if err != nil {
		t.Fatalf("ExecuteQuery(count) error = %v", err)
	}
	if got := countRes.GetRows()[0].GetFields()["total"].GetScalar().GetNumberValue(); got != 3 {
		t.Fatalf("structured count = %v, want 3", got)
	}

	pathRes, err := querySvc.ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{
		TransactionId: readTx,
		Query: &clientv1.GraphQuery{
			Match: &clientv1.GraphPattern{
				Start: &clientv1.NodePattern{Alias: "a", NodeIds: []string{aID}},
				Steps: []*clientv1.TraversalStep{{
					Direction: clientv1.TraversalDirection_TRAVERSAL_DIRECTION_OUT,
					EdgeKind:  "FRIEND_OF",
					Depth:     &clientv1.DepthSpec{MinDepth: 2, MaxDepth: 2},
					Target:    &clientv1.NodePattern{Alias: "c", Labels: []string{"Person"}},
				}},
			},
			PathAlias: "path",
			Returns: []*clientv1.ReturnProjection{{
				Alias: "path", OutputName: "path", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_PATH,
			}},
		},
		PageSize: 10,
	})
	if err != nil {
		t.Fatalf("ExecuteQuery(path) error = %v", err)
	}
	path := pathRes.GetRows()[0].GetFields()["path"].GetPath()
	if path == nil || len(path.GetNodes()) != 3 || len(path.GetEdges()) != 2 || pathRes.GetDiagnostics().GetPlan() != "IndexedMultiHopAdjacencyPathScan" {
		t.Fatalf("path=%+v diagnostics=%+v", path, pathRes.GetDiagnostics())
	}
}

type fakeSemanticQuerySearcher struct {
	results []semanticsearch.SearchResult
	last    daemonsemantic.SearchInput
}

func (f *fakeSemanticQuerySearcher) Search(ctx context.Context, in daemonsemantic.SearchInput) (semanticsearch.Result, error) {
	f.last = in
	return semanticsearch.Result{Results: append([]semanticsearch.SearchResult(nil), f.results...)}, nil
}
