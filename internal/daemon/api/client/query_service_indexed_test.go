package client

import (
	"context"
	"reflect"
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
