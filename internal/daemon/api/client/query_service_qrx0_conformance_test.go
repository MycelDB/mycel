package client

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"testing"
	"time"

	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	graphmodel "github.com/myceldb/mycel/internal/graph/model"
	graphquery "github.com/myceldb/mycel/internal/graph/query"
	graphservice "github.com/myceldb/mycel/internal/graph/service"
	queryengine "github.com/myceldb/mycel/internal/query/engine"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
	schemaservice "github.com/myceldb/mycel/internal/schema/service"
	semanticsearch "github.com/myceldb/mycel/internal/semantic/search"
	semanticservice "github.com/myceldb/mycel/internal/semantic/service"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestQRX0StructuredAndGQLReadConformance(t *testing.T) {
	fixture, manager, readTx := newQRX0JournalFixture(t)
	querySvc := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithSchemaManager(manager)

	countStar := &clientv1.AggregateArgument{Argument: &clientv1.AggregateArgument_Star{Star: true}}
	tests := []struct {
		name           string
		structured     *clientv1.GraphQuery
		gql            string
		field          string
		sortValues     bool
		want           []string
		structuredPlan string
		gqlPlan        string
		wantPlanKind   string
	}{
		{
			name: "broad label scan",
			structured: &clientv1.GraphQuery{
				Match:   &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "j", Labels: []string{"JournalEntry"}}},
				Returns: []*clientv1.ReturnProjection{{Alias: "j.title", OutputName: "title", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_SCALAR}},
			},
			gql:            "MATCH (j:JournalEntry) RETURN j.title AS title",
			field:          "title",
			sortValues:     true,
			want:           []string{"latest", "middle", "oldest"},
			structuredPlan: "BroadGraphFallback",
			gqlPlan:        "BroadGQLFallback",
			wantPlanKind:   "fallback",
		},
		{
			name: "indexed equality",
			structured: &clientv1.GraphQuery{
				Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "j", Labels: []string{"JournalEntry"}}},
				Where: &clientv1.Expr{Expr: &clientv1.Expr_PropertyEquals{PropertyEquals: &clientv1.PropertyEqualsExpr{
					Alias: "j", Name: "date", Value: structpb.NewStringValue("2026-07-20"),
				}}},
				Returns: []*clientv1.ReturnProjection{{Alias: "j.title", OutputName: "title", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_SCALAR}},
			},
			gql:            "MATCH (j:JournalEntry {date: '2026-07-20'}) RETURN j.title AS title",
			field:          "title",
			want:           []string{"latest"},
			structuredPlan: "OrderedNodePropertyEqualityIndexScan",
			gqlPlan:        "OrderedNodePropertyEqualityIndexScan",
			wantPlanKind:   "indexed",
		},
		{
			name: "indexed order",
			structured: &clientv1.GraphQuery{
				Match:   &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "j", Labels: []string{"JournalEntry"}}},
				Returns: []*clientv1.ReturnProjection{{Alias: "j.title", OutputName: "title", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_SCALAR}},
				OrderBy: []*clientv1.OrderSpec{{Value: &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Prop{Prop: &clientv1.PropExpr{Alias: "j", Name: "date"}}}, Direction: clientv1.SortDirection_SORT_DIRECTION_ASC}},
			},
			gql:            "MATCH (j:JournalEntry) RETURN j.title AS title ORDER BY j.date",
			field:          "title",
			want:           []string{"oldest", "middle", "latest"},
			structuredPlan: "OrderedNodePropertyIndexScan",
			gqlPlan:        "OrderedNodePropertyIndexScan",
			wantPlanKind:   "indexed",
		},
		{
			name: "aggregate count",
			structured: &clientv1.GraphQuery{
				Match:            &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "j", Labels: []string{"JournalEntry"}}},
				AggregateReturns: []*clientv1.AggregateProjection{{OutputName: "total", Function: clientv1.AggregateFunction_AGGREGATE_FUNCTION_COUNT, Argument: countStar}},
			},
			gql:            "MATCH (j:JournalEntry) RETURN COUNT(*) AS total",
			field:          "total",
			want:           []string{"3"},
			structuredPlan: "BroadGraphFallback",
			gqlPlan:        "BroadGQLFallback",
			wantPlanKind:   "fallback",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			structured, err := querySvc.ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: tt.structured, PageSize: 10})
			if err != nil {
				t.Fatalf("ExecuteQuery() error = %v", err)
			}
			gql, err := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: readTx, Query: tt.gql, PageSize: 10})
			if err != nil {
				t.Fatalf("ExecuteGQL() error = %v", err)
			}

			structuredValues := qrx0ScalarValues(structured.GetRows(), tt.field)
			gqlValues := qrx0ScalarValues(gql.GetResult().GetRows(), tt.field)
			if tt.sortValues {
				sort.Strings(structuredValues)
				sort.Strings(gqlValues)
			}
			if !reflect.DeepEqual(structuredValues, gqlValues) {
				t.Fatalf("structured values = %v, GQL values = %v", structuredValues, gqlValues)
			}
			if !reflect.DeepEqual(structuredValues, tt.want) {
				t.Fatalf("values = %v, want %v", structuredValues, tt.want)
			}
			assertQRX0ResponseMetadataParity(t, structured, gql, tt.sortValues)
			if got := structured.GetDiagnostics().GetPlan(); got != tt.structuredPlan {
				t.Fatalf("structured plan = %q, want %q", got, tt.structuredPlan)
			}
			if got := gql.GetDiagnostics().GetPlan(); got != tt.gqlPlan {
				t.Fatalf("GQL plan = %q, want %q", got, tt.gqlPlan)
			}
			if got := structured.GetDiagnostics().GetPlanKind(); got != tt.wantPlanKind {
				t.Fatalf("structured plan kind = %q, want %q", got, tt.wantPlanKind)
			}
			if got := gql.GetDiagnostics().GetPlanKind(); got != tt.wantPlanKind {
				t.Fatalf("GQL plan kind = %q, want %q", got, tt.wantPlanKind)
			}
		})
	}
}

func TestQRX0StructuredAndGQLPreserveReadOnlySnapshot(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	querySvc := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces)

	seedTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	created, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: seedTx, Node: &clientv1.NodeCreate{Labels: []string{"Note"}, Properties: mustStruct(t, map[string]any{"title": "revision 1"})}})
	if err != nil {
		t.Fatalf("CreateNode() error = %v", err)
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: seedTx}); err != nil {
		t.Fatalf("CommitTransaction(seed) error = %v", err)
	}

	snapshotTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	mutateTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	if _, err := graphSvc.UpdateNode(fixture.ctx, &clientv1.UpdateNodeRequest{
		TransactionId: mutateTx,
		Node:          &clientv1.Node{NodeId: created.GetNode().GetNodeId(), Properties: mustStruct(t, map[string]any{"title": "revision 2"})},
		UpdateMask:    &fieldmaskpb.FieldMask{Paths: []string{"properties"}},
	}); err != nil {
		t.Fatalf("UpdateNode() error = %v", err)
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: mutateTx}); err != nil {
		t.Fatalf("CommitTransaction(mutate) error = %v", err)
	}

	assertQRX0NoteTitles(t, fixture, querySvc, snapshotTx, []string{"revision 1"})
	currentTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	assertQRX0NoteTitles(t, fixture, querySvc, currentTx, []string{"revision 2"})
}

func TestQRX0StructuredAndGQLPreserveReadWriteOverlay(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	querySvc := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	if _, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx, Node: &clientv1.NodeCreate{Labels: []string{"Note"}, Properties: mustStruct(t, map[string]any{"title": "staged"})}}); err != nil {
		t.Fatalf("CreateNode() error = %v", err)
	}

	structured, gql := executeQRX0NoteTitlePair(t, fixture, querySvc, writeTx)
	if got := qrx0ScalarValues(structured.GetRows(), "title"); !reflect.DeepEqual(got, []string{"staged"}) {
		t.Fatalf("structured overlay titles = %v, want [staged]", got)
	}
	if got := qrx0ScalarValues(gql.GetResult().GetRows(), "title"); !reflect.DeepEqual(got, []string{"staged"}) {
		t.Fatalf("GQL overlay titles = %v, want [staged]", got)
	}
	if structured.GetReadMetadata().GetConsistency() != "overlay" || gql.GetReadMetadata().GetConsistency() != "overlay" {
		t.Fatalf("overlay metadata: structured=%+v GQL=%+v", structured.GetReadMetadata(), gql.GetReadMetadata())
	}
}

func TestQRX0LegacyGraphAccessProfiles(t *testing.T) {
	fixture, manager, readTx := newQRX0JournalFixture(t)

	t.Run("ordered index uses finite property scan", func(t *testing.T) {
		instrumented, profile := instrumentQRX0Graph(fixture.graphs)
		querySvc := NewQueryService(fixture.sessions, instrumented, fixture.spaces).WithSchemaManager(manager)
		res, err := querySvc.ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: journalOrderedQuery(clientv1.SortDirection_SORT_DIRECTION_ASC), PageSize: 1})
		if err != nil {
			t.Fatalf("ExecuteQuery() error = %v", err)
		}
		profile.recordDiagnostics(res.GetDiagnostics())
		if res.GetDiagnostics().GetFullScan() || profile.callCount("ScanNodePropertyOrdered") == 0 {
			t.Fatalf("diagnostics=%+v profile=%+v", res.GetDiagnostics(), profile.callsSnapshot())
		}
		if profile.callCount("ListNodes") != 0 || profile.callCount("ListEdges") != 0 {
			t.Fatalf("indexed query used broad reads: calls=%+v", profile.callsSnapshot())
		}
		limits := profile.requestedLimits("ScanNodePropertyOrdered")
		if len(limits) == 0 || limits[0] <= 0 {
			t.Fatalf("ordered scan limits = %v, want finite positive limit", limits)
		}
		work := profile.workSnapshot()
		if work.IndexEntriesScanned == 0 || work.CandidatesExamined == 0 || work.NodesLoaded == 0 || work.ReturnedRows != 1 {
			t.Fatalf("ordered scan work = %+v, want index/candidate/node work and one returned row", work)
		}
	})

	t.Run("structured broad page materializes nodes and edges", func(t *testing.T) {
		instrumented, profile := instrumentQRX0Graph(fixture.graphs)
		querySvc := NewQueryService(fixture.sessions, instrumented, fixture.spaces).WithSchemaManager(manager)
		query := &clientv1.GraphQuery{Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "j", Labels: []string{"JournalEntry"}}}}
		res, err := querySvc.ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: query, PageSize: 1})
		if err != nil {
			t.Fatalf("ExecuteQuery() error = %v", err)
		}
		profile.recordDiagnostics(res.GetDiagnostics())
		if !res.GetDiagnostics().GetFullScan() || profile.callCount("ListNodes") == 0 || profile.callCount("ListEdges") == 0 {
			t.Fatalf("legacy structured fallback baseline changed: diagnostics=%+v calls=%+v", res.GetDiagnostics(), profile.callsSnapshot())
		}
		if profile.returnedCount("ListNodes") != 3 {
			t.Fatalf("ListNodes returned %d entities for a one-row page, want baseline 3", profile.returnedCount("ListNodes"))
		}
	})

	t.Run("GQL broad node page materializes nodes but not edges", func(t *testing.T) {
		instrumented, profile := instrumentQRX0Graph(fixture.graphs)
		querySvc := NewQueryService(fixture.sessions, instrumented, fixture.spaces).WithSchemaManager(manager)
		res, err := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: readTx, Query: "MATCH (j:JournalEntry) RETURN j", PageSize: 1})
		if err != nil {
			t.Fatalf("ExecuteGQL() error = %v", err)
		}
		profile.recordDiagnostics(res.GetDiagnostics())
		if !res.GetDiagnostics().GetFullScan() || profile.callCount("ListNodes") == 0 || profile.callCount("ListEdges") != 0 {
			t.Fatalf("legacy GQL fallback baseline changed: diagnostics=%+v calls=%+v", res.GetDiagnostics(), profile.callsSnapshot())
		}
		if profile.returnedCount("ListNodes") != 3 {
			t.Fatalf("ListNodes returned %d entities for a one-row page, want baseline 3", profile.returnedCount("ListNodes"))
		}
	})

	t.Run("GQL script Explain stays on explicit bridge without graph access", func(t *testing.T) {
		instrumented, profile := instrumentQRX0Graph(fixture.graphs)
		querySvc := NewQueryService(fixture.sessions, instrumented, fixture.spaces).WithSchemaManager(manager).WithQueryEngine(queryEngineForExplainTest(instrumented, manager))
		res, err := querySvc.ExplainGQL(fixture.ctx, &clientv1.ExplainGQLRequest{
			TransactionId: readTx,
			Query:         "MATCH (j:JournalEntry) RETURN j; MATCH (j:JournalEntry) RETURN j",
		})
		if err != nil {
			t.Fatalf("ExplainGQL(script) error = %v", err)
		}
		if diagnostics := res.GetDiagnostics(); diagnostics.GetPlan() != "GQLScriptPlan" || diagnostics.GetPlanKind() != "gql_script" || diagnostics.GetFallbackMode() != "legacy_script_bridge" || !diagnostics.GetExplainOnly() {
			t.Fatalf("script diagnostics = %+v", diagnostics)
		}
		if profile.dataReadCalls() != 0 || profile.callCount("ConfigureIndexes") != 0 {
			t.Fatalf("script Explain calls = %+v", profile.callsSnapshot())
		}
	})

	t.Run("Explain performs no graph data reads", func(t *testing.T) {
		instrumented, profile := instrumentQRX0Graph(fixture.graphs)
		querySvc := NewQueryService(fixture.sessions, instrumented, fixture.spaces).WithSchemaManager(manager).WithQueryEngine(queryEngineForExplainTest(instrumented, manager))
		res, err := querySvc.ExplainQuery(fixture.ctx, &clientv1.ExplainQueryRequest{TransactionId: readTx, Query: journalOrderedQuery(clientv1.SortDirection_SORT_DIRECTION_ASC)})
		if err != nil {
			t.Fatalf("ExplainQuery() error = %v", err)
		}
		if !res.GetDiagnostics().GetExplainOnly() || profile.dataReadCalls() != 0 || profile.callCount("ConfigureIndexes") != 0 {
			t.Fatalf("Explain diagnostics=%+v calls=%+v", res.GetDiagnostics(), profile.callsSnapshot())
		}
	})
}

func TestQRX7StructuredAndGQLIndexedReadsBypassLegacyQueryExecution(t *testing.T) {
	fixture, manager, readTx := newQRX0JournalFixture(t)

	for _, test := range []struct {
		name string
		run  func(*QueryService) ([]string, *clientv1.QueryDiagnostics, error)
	}{
		{name: "structured", run: func(service *QueryService) ([]string, *clientv1.QueryDiagnostics, error) {
			response, err := service.ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{
				TransactionId: readTx,
				Query: &clientv1.GraphQuery{
					Match:   &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "j", Labels: []string{"JournalEntry"}}},
					Returns: []*clientv1.ReturnProjection{{Alias: "j.title", OutputName: "title", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_SCALAR}},
					OrderBy: []*clientv1.OrderSpec{{Value: &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Prop{Prop: &clientv1.PropExpr{Alias: "j", Name: "date"}}}, Direction: clientv1.SortDirection_SORT_DIRECTION_ASC}},
				},
				PageSize: 2,
			})
			if err != nil {
				return nil, nil, err
			}
			return qrx0ScalarValues(response.GetRows(), "title"), response.GetDiagnostics(), nil
		}},
		{name: "GQL", run: func(service *QueryService) ([]string, *clientv1.QueryDiagnostics, error) {
			response, err := service.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{
				TransactionId: readTx, Query: "MATCH (j:JournalEntry) RETURN j.title AS title ORDER BY j.date ASC", PageSize: 2,
			})
			if err != nil {
				return nil, nil, err
			}
			return qrx0ScalarValues(response.GetResult().GetRows(), "title"), response.GetDiagnostics(), nil
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			instrumented, profile := instrumentQRX0Graph(fixture.graphs)
			service := NewQueryService(fixture.sessions, instrumented, fixture.spaces).
				WithSchemaManager(manager).
				WithQueryEngine(queryEngineForExplainTest(instrumented, manager))
			titles, diagnostics, err := test.run(service)
			if err != nil {
				t.Fatalf("execute: %v", err)
			}
			if !reflect.DeepEqual(titles, []string{"oldest", "middle"}) {
				t.Fatalf("titles = %v", titles)
			}
			if diagnostics.GetPlanKind() != "indexed_order" || diagnostics.GetPlannerVersion() != "qrx7" || diagnostics.GetFullScan() {
				t.Fatalf("diagnostics = %+v", diagnostics)
			}
			if profile.callCount("ScanNodePropertyOrdered") == 0 || profile.callCount("ListNodes") != 0 || profile.callCount("ListEdges") != 0 {
				t.Fatalf("migrated query used legacy queryExecution access: %+v", profile.callsSnapshot())
			}
			for _, limit := range profile.requestedLimits("ScanNodePropertyOrdered") {
				if limit <= 0 {
					t.Fatalf("ordered scan used unbounded limit: %v", profile.requestedLimits("ScanNodePropertyOrdered"))
				}
			}
		})
	}
}

func TestQRX7StructuredCursorContinuesThroughEquivalentGQLLabelPlan(t *testing.T) {
	fixture, manager, readTx := newQRX0JournalFixture(t)
	instrumented, profile := instrumentQRX0Graph(fixture.graphs)
	service := NewQueryService(fixture.sessions, instrumented, fixture.spaces).
		WithSchemaManager(manager).
		WithQueryEngine(queryEngineForExplainTest(instrumented, manager))
	structured, err := service.ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{
		TransactionId: readTx,
		Query: &clientv1.GraphQuery{
			Match:   &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "j", Labels: []string{"JournalEntry"}}},
			Returns: []*clientv1.ReturnProjection{{Alias: "j.title", OutputName: "title", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_SCALAR}},
		},
		PageSize: 1,
	})
	if err != nil {
		t.Fatalf("structured execute: %v", err)
	}
	if structured.GetDiagnostics().GetPlanKind() != "label_index" || structured.GetNextPageToken() == "" {
		t.Fatalf("structured response = %+v", structured)
	}
	gql, err := service.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{
		TransactionId: readTx, Query: "MATCH (j:JournalEntry) RETURN j.title AS title",
		PageSize: 1, PageToken: structured.GetNextPageToken(),
	})
	if err != nil {
		t.Fatalf("GQL continuation: %v", err)
	}
	first := qrx0ScalarValues(structured.GetRows(), "title")
	second := qrx0ScalarValues(gql.GetResult().GetRows(), "title")
	if len(first) != 1 || len(second) != 1 || first[0] == second[0] || gql.GetDiagnostics().GetPlanKind() != "label_index" {
		t.Fatalf("structured=%v GQL=%v diagnostics=%+v", first, second, gql.GetDiagnostics())
	}
	if profile.callCount("ScanLabel") < 2 || profile.callCount("ListNodes") != 0 || profile.callCount("ListEdges") != 0 {
		t.Fatalf("label continuation access = %+v", profile.callsSnapshot())
	}
	for _, limit := range profile.requestedLimits("ScanLabel") {
		if limit <= 0 || limit > 2 {
			t.Fatalf("label scan limits = %v", profile.requestedLimits("ScanLabel"))
		}
	}
}

type qrx7BlockingLabelGraph struct {
	graphservice.Manager
	started chan struct{}
}

func (g *qrx7BlockingLabelGraph) ScanLabel(ctx context.Context, _ sessionmodel.GraphTransaction, _ graphquery.LabelScan) ([]graphmodel.Node, string, graphquery.IndexedReadStats, error) {
	close(g.started)
	<-ctx.Done()
	return nil, "", graphquery.IndexedReadStats{}, ctx.Err()
}

type qrx7BlockingSemanticSearcher struct {
	started chan struct{}
}

func (s *qrx7BlockingSemanticSearcher) Search(ctx context.Context, _ semanticservice.SearchInput) (semanticsearch.Result, error) {
	close(s.started)
	<-ctx.Done()
	return semanticsearch.Result{}, ctx.Err()
}

type qrx7StatusLabelGraph struct {
	graphservice.Manager
	err error
}

func (g *qrx7StatusLabelGraph) ScanLabel(context.Context, sessionmodel.GraphTransaction, graphquery.LabelScan) ([]graphmodel.Node, string, graphquery.IndexedReadStats, error) {
	return nil, "", graphquery.IndexedReadStats{}, g.err
}

type qrx7StatusConfigureGraph struct {
	graphservice.Manager
	err error
}

func (g *qrx7StatusConfigureGraph) ConfigureIndexes(context.Context, sessionmodel.GraphTransaction, string, []schemamodel.IndexDefinition) error {
	return g.err
}

type qrx7StatusSemanticSearcher struct {
	err error
}

func (s *qrx7StatusSemanticSearcher) Search(context.Context, semanticservice.SearchInput) (semanticsearch.Result, error) {
	return semanticsearch.Result{}, s.err
}

func TestQRX7CancellationDuringIndexedDependencyCallIsPreserved(t *testing.T) {
	t.Run("label index", func(t *testing.T) {
		fixture, manager, readTx := newQRX0JournalFixture(t)
		blocking := &qrx7BlockingLabelGraph{Manager: fixture.graphs, started: make(chan struct{})}
		service := NewQueryService(fixture.sessions, blocking, fixture.spaces).
			WithSchemaManager(manager).
			WithQueryEngine(queryEngineForExplainTest(blocking, manager))
		ctx, cancel := context.WithCancel(fixture.ctx)
		result := make(chan error, 1)
		go func() {
			_, err := service.ExecuteQuery(ctx, &clientv1.ExecuteQueryRequest{
				TransactionId: readTx,
				Query:         &clientv1.GraphQuery{Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "j", Labels: []string{"JournalEntry"}}}},
			})
			result <- err
		}()
		select {
		case <-blocking.started:
		case <-time.After(5 * time.Second):
			t.Fatal("label scan did not start")
		}
		cancel()
		select {
		case err := <-result:
			if status.Code(err) != codes.Canceled {
				t.Fatalf("error = %v, want Canceled", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("label cancellation did not return")
		}
	})

	t.Run("semantic search", func(t *testing.T) {
		fixture, manager, readTx := newQRX0JournalFixture(t)
		blocking := &qrx7BlockingSemanticSearcher{started: make(chan struct{})}
		engine := queryengine.New(queryengine.Ports{
			Entities: fixture.graphs, Traversals: fixture.graphs, Indexes: fixture.graphs,
			Catalog: manager, SemanticCandidates: NewQuerySemanticCandidateReader(blocking), Mutations: fixture.graphs,
		})
		service := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).
			WithSchemaManager(manager).
			WithSemanticManager(blocking).
			WithQueryEngine(engine)
		ctx, cancel := context.WithCancel(fixture.ctx)
		result := make(chan error, 1)
		go func() {
			_, err := service.ExecuteQuery(ctx, &clientv1.ExecuteQueryRequest{
				TransactionId: readTx,
				Query: &clientv1.GraphQuery{
					Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "j", Labels: []string{"JournalEntry"}}},
					Where: &clientv1.Expr{Expr: &clientv1.Expr_Semantic{Semantic: &clientv1.SemanticSearchExpr{Alias: "j", Query: "needle", Limit: 5}}},
				},
			})
			result <- err
		}()
		select {
		case <-blocking.started:
		case <-time.After(5 * time.Second):
			t.Fatal("semantic search did not start")
		}
		cancel()
		select {
		case err := <-result:
			if status.Code(err) != codes.Canceled {
				t.Fatalf("error = %v, want Canceled", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("semantic cancellation did not return")
		}
	})
}

func TestQRX7StatusCodedDependencyCancellationIsPreserved(t *testing.T) {
	t.Run("label deadline", func(t *testing.T) {
		fixture, manager, readTx := newQRX0JournalFixture(t)
		failing := &qrx7StatusLabelGraph{Manager: fixture.graphs, err: status.Error(codes.DeadlineExceeded, "injected label deadline")}
		service := NewQueryService(fixture.sessions, failing, fixture.spaces).
			WithSchemaManager(manager).
			WithQueryEngine(queryEngineForExplainTest(failing, manager))
		_, err := service.ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{
			TransactionId: readTx,
			Query:         &clientv1.GraphQuery{Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "j", Labels: []string{"JournalEntry"}}}},
		})
		if status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("error = %v, want DeadlineExceeded", err)
		}
	})

	t.Run("index configuration canceled", func(t *testing.T) {
		fixture, manager, readTx := newQRX0JournalFixture(t)
		failing := &qrx7StatusConfigureGraph{Manager: fixture.graphs, err: status.Error(codes.Canceled, "injected configure cancellation")}
		service := NewQueryService(fixture.sessions, failing, fixture.spaces).
			WithSchemaManager(manager).
			WithQueryEngine(queryEngineForExplainTest(failing, manager))
		_, err := service.ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{
			TransactionId: readTx, Query: journalOrderedQuery(clientv1.SortDirection_SORT_DIRECTION_ASC), PageSize: 1,
		})
		if status.Code(err) != codes.Canceled {
			t.Fatalf("error = %v, want Canceled", err)
		}
	})

	t.Run("semantic deadline", func(t *testing.T) {
		fixture, manager, readTx := newQRX0JournalFixture(t)
		failing := &qrx7StatusSemanticSearcher{err: status.Error(codes.DeadlineExceeded, "injected semantic deadline")}
		engine := queryengine.New(queryengine.Ports{
			Entities: fixture.graphs, Traversals: fixture.graphs, Indexes: fixture.graphs,
			Catalog: manager, SemanticCandidates: NewQuerySemanticCandidateReader(failing), Mutations: fixture.graphs,
		})
		service := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).
			WithSchemaManager(manager).
			WithSemanticManager(failing).
			WithQueryEngine(engine)
		_, err := service.ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{
			TransactionId: readTx,
			Query: &clientv1.GraphQuery{
				Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "j", Labels: []string{"JournalEntry"}}},
				Where: &clientv1.Expr{Expr: &clientv1.Expr_Semantic{Semantic: &clientv1.SemanticSearchExpr{Alias: "j", Query: "needle", Limit: 5}}},
			},
		})
		if status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("error = %v, want DeadlineExceeded", err)
		}
	})
}

func TestQRX0StructuredAndGQLGraphAndSearchErrorsFailClosed(t *testing.T) {
	t.Run("graph read error", func(t *testing.T) {
		fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
		failingGraph := &qrx0ListNodesErrorGraph{Manager: fixture.graphs, err: errors.New("injected graph read failure")}
		querySvc := NewQueryService(fixture.sessions, failingGraph, fixture.spaces)
		readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
		structuredQuery := &clientv1.GraphQuery{Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "n", Labels: []string{"Note"}}}}
		_, structuredErr := querySvc.ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: structuredQuery})
		_, gqlErr := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: readTx, Query: "MATCH (n:Note) RETURN n"})
		if status.Code(structuredErr) != codes.Internal || status.Code(gqlErr) != codes.InvalidArgument {
			t.Fatalf("legacy graph error mismatch changed: structured=%v (%v) GQL=%v (%v)", status.Code(structuredErr), structuredErr, status.Code(gqlErr), gqlErr)
		}
	})

	t.Run("semantic search error", func(t *testing.T) {
		fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{SearchMode: graphmodel.DomainSearchModeDisabled})
		querySvc := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithSemanticManager(&qrx0SemanticErrorSearcher{err: errors.New("injected semantic failure")})
		readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
		structuredQuery := &clientv1.GraphQuery{
			Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "n", Labels: []string{"Note"}}},
			Where: &clientv1.Expr{Expr: &clientv1.Expr_Semantic{Semantic: &clientv1.SemanticSearchExpr{Alias: "n", Query: "needle", Limit: 5}}},
		}
		_, structuredErr := querySvc.ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: structuredQuery})
		_, gqlErr := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: readTx, Query: "MATCH (n:Note) WHERE SEMANTIC_SIMILAR(n, 'needle', TOP 5) RETURN n"})
		if status.Code(structuredErr) == codes.OK || status.Code(structuredErr) != status.Code(gqlErr) {
			t.Fatalf("semantic error codes: structured=%v (%v) GQL=%v (%v)", status.Code(structuredErr), structuredErr, status.Code(gqlErr), gqlErr)
		}
	})
}

func TestQRX0CanceledRequestsStopBeforeGraphAccess(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	instrumented, profile := instrumentQRX0Graph(fixture.graphs)
	querySvc := NewQueryService(fixture.sessions, instrumented, fixture.spaces)
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	ctx, cancel := context.WithCancel(fixture.ctx)
	cancel()

	query := &clientv1.GraphQuery{Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "n"}}}
	if _, err := querySvc.ExecuteQuery(ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: query}); err == nil {
		t.Fatal("ExecuteQuery() error = nil, want cancellation failure")
	}
	if _, err := querySvc.ExecuteGQL(ctx, &clientv1.ExecuteGQLRequest{TransactionId: readTx, Query: "MATCH (n) RETURN n"}); err == nil {
		t.Fatal("ExecuteGQL() error = nil, want cancellation failure")
	}
	if got := profile.dataReadCalls(); got != 0 {
		t.Fatalf("canceled requests performed %d graph data reads", got)
	}
}

func TestQRX0AdjacencyAccessProfile(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{SearchMode: graphmodel.DomainSearchModeDisabled})
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	source := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Note"}, map[string]any{"title": "source"})
	target := createQueryTestNode(t, fixture, graphSvc, writeTx, []string{"Page"}, map[string]any{"title": "target"})
	createQueryTestEdge(t, fixture, graphSvc, writeTx, source, target, []string{"references"}, nil)
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}

	instrumented, profile := instrumentQRX0Graph(fixture.graphs)
	querySvc := NewQueryService(fixture.sessions, instrumented, fixture.spaces)
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	query := referenceQuery(source, clientv1.TraversalDirection_TRAVERSAL_DIRECTION_OUT, "")
	res, err := querySvc.ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: query, PageSize: 1})
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}
	profile.recordDiagnostics(res.GetDiagnostics())
	if profile.callCount("ScanAdjacency") == 0 || profile.callCount("GetNode") < 2 {
		t.Fatalf("adjacency profile calls = %+v", profile.callsSnapshot())
	}
	if profile.callCount("ListNodes") != 0 || profile.callCount("ListEdges") != 0 {
		t.Fatalf("adjacency query used broad reads: %+v", profile.callsSnapshot())
	}
	limits := profile.requestedLimits("ScanAdjacency")
	if len(limits) == 0 || limits[0] != 1 {
		t.Fatalf("adjacency scan limits = %v, want first limit 1", limits)
	}
	work := profile.workSnapshot()
	if work.IndexEntriesScanned == 0 || work.CandidatesExamined == 0 || work.NodesLoaded < 2 || work.EdgesLoaded == 0 || work.ReturnedRows != 1 {
		t.Fatalf("adjacency work = %+v, want candidate/node/edge work and one returned row", work)
	}
}

func newQRX0JournalFixture(t *testing.T) (domainPolicyClientAPIFixture, schemaservice.Manager, string) {
	t.Helper()
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	manager := journalSchemaManagerForQueryTest(t, fixture.domainID, true)
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	for _, item := range []struct {
		title string
		date  string
	}{
		{title: "oldest", date: "2026-07-18"},
		{title: "middle", date: "2026-07-19"},
		{title: "latest", date: "2026-07-20"},
	} {
		if _, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx, Node: &clientv1.NodeCreate{Labels: []string{"JournalEntry"}, Properties: mustStruct(t, map[string]any{"title": item.title, "date": item.date})}}); err != nil {
			t.Fatalf("CreateNode(%s) error = %v", item.title, err)
		}
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}
	return fixture, manager, fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
}

func assertQRX0NoteTitles(t *testing.T, fixture domainPolicyClientAPIFixture, querySvc *QueryService, txID string, want []string) {
	t.Helper()
	structured, gql := executeQRX0NoteTitlePair(t, fixture, querySvc, txID)
	structuredTitles := qrx0ScalarValues(structured.GetRows(), "title")
	gqlTitles := qrx0ScalarValues(gql.GetResult().GetRows(), "title")
	if !reflect.DeepEqual(structuredTitles, want) || !reflect.DeepEqual(gqlTitles, want) {
		t.Fatalf("snapshot titles: structured=%v GQL=%v want=%v", structuredTitles, gqlTitles, want)
	}
}

func executeQRX0NoteTitlePair(t *testing.T, fixture domainPolicyClientAPIFixture, querySvc *QueryService, txID string) (*clientv1.ExecuteQueryResponse, *clientv1.ExecuteGQLResponse) {
	t.Helper()
	structured, err := querySvc.ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{
		TransactionId: txID,
		Query: &clientv1.GraphQuery{
			Match:   &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "n", Labels: []string{"Note"}}},
			Returns: []*clientv1.ReturnProjection{{Alias: "n.title", OutputName: "title", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_SCALAR}},
		},
	})
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}
	gql, err := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: txID, Query: "MATCH (n:Note) RETURN n.title AS title"})
	if err != nil {
		t.Fatalf("ExecuteGQL() error = %v", err)
	}
	return structured, gql
}

func assertQRX0ResponseMetadataParity(t *testing.T, structured *clientv1.ExecuteQueryResponse, gql *clientv1.ExecuteGQLResponse, unorderedRows bool) {
	t.Helper()
	structuredResult, gqlResult := structured.GetResult(), gql.GetResult()
	assertQRX0TypedResultParity(t, structuredResult, gqlResult, unorderedRows, false)
	if !proto.Equal(structured.GetReadMetadata(), gql.GetReadMetadata()) {
		t.Fatalf("read metadata differs: structured=%+v GQL=%+v", structured.GetReadMetadata(), gql.GetReadMetadata())
	}
	if !proto.Equal(structuredResult.GetCounters(), gqlResult.GetCounters()) {
		t.Fatalf("counters differ: structured=%+v GQL=%+v", structuredResult.GetCounters(), gqlResult.GetCounters())
	}
	if structuredResult.GetNextPageToken() != gqlResult.GetNextPageToken() {
		t.Fatalf("next tokens differ: structured=%q GQL=%q", structuredResult.GetNextPageToken(), gqlResult.GetNextPageToken())
	}
	structuredDiagnostics, gqlDiagnostics := structured.GetDiagnostics(), gql.GetDiagnostics()
	if structuredDiagnostics.GetFullScan() != gqlDiagnostics.GetFullScan() || !slices.Equal(structuredDiagnostics.GetIndexes(), gqlDiagnostics.GetIndexes()) {
		t.Fatalf("access diagnostics differ: structured=%+v GQL=%+v", structuredDiagnostics, gqlDiagnostics)
	}
}

func qrx0ScalarValues(rows []*clientv1.QueryRow, field string) []string {
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		value := row.GetFields()[field].GetScalar()
		if value == nil {
			out = append(out, "<nil>")
			continue
		}
		out = append(out, fmt.Sprint(value.AsInterface()))
	}
	return out
}
