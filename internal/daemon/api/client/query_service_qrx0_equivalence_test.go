package client

import (
	"bytes"
	"context"
	"reflect"
	"sort"
	"testing"

	"github.com/google/uuid"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	graphmodel "github.com/myceldb/mycel/internal/graph/model"
	schemaservice "github.com/myceldb/mycel/internal/schema/service"
	"github.com/myceldb/mycel/internal/schema/storage"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	semanticsearch "github.com/myceldb/mycel/internal/semantic/search"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

type qrx0EquivalentRead struct {
	name                          string
	structured                    *clientv1.GraphQuery
	gql                           string
	pageSize                      int32
	unordered                     bool
	knownTemporalMetadataMismatch bool
}

func TestQRX0StructuredAndGQLPredicateShapingEquivalence(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	people := []struct {
		id    string
		props map[string]any
	}{
		{id: "00000000-0000-0000-0000-000000000001", props: map[string]any{"name": "Alpha", "role": "reader", "age": 10, "bio": "Graph memory", "nickname": nil}},
		{id: "00000000-0000-0000-0000-000000000002", props: map[string]any{"name": "Beta", "role": "writer", "age": 20, "bio": "Other", "nickname": "B"}},
		{id: "00000000-0000-0000-0000-000000000003", props: map[string]any{"name": "Alphabet", "role": "reader", "age": 30, "bio": "Memory graph"}},
	}
	for _, person := range people {
		id := person.id
		if _, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx, Node: &clientv1.NodeCreate{NodeId: &id, Labels: []string{"Person"}, Properties: mustStruct(t, person.props)}}); err != nil {
			t.Fatalf("CreateNode(%s) error = %v", person.props["name"], err)
		}
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}

	property := func(name string) *clientv1.ValueExpr {
		return &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Prop{Prop: &clientv1.PropExpr{Alias: "p", Name: name}}}
	}
	literal := func(value *structpb.Value) *clientv1.ValueExpr {
		return &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Literal{Literal: &clientv1.LiteralExpr{Value: value}}}
	}
	scalarReturn := func(name string) []*clientv1.ReturnProjection {
		return []*clientv1.ReturnProjection{{Alias: "p." + name, OutputName: name, Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_SCALAR}}
	}
	baseMatch := func() *clientv1.GraphPattern {
		return &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "p", Labels: []string{"Person"}}}
	}
	score := &clientv1.AggregateArgument{Argument: &clientv1.AggregateArgument_Value{Value: property("age")}}

	cases := []qrx0EquivalentRead{
		{
			name:       "label and scalar projection",
			structured: &clientv1.GraphQuery{Match: baseMatch(), Returns: scalarReturn("name")},
			gql:        "MATCH (p:Person) RETURN p.name AS name",
			unordered:  true,
		},
		{
			name: "equality predicate",
			structured: &clientv1.GraphQuery{Match: baseMatch(), Where: &clientv1.Expr{Expr: &clientv1.Expr_PropertyEquals{PropertyEquals: &clientv1.PropertyEqualsExpr{
				Alias: "p", Name: "role", Value: structpb.NewStringValue("reader"),
			}}}, Returns: scalarReturn("name")},
			gql:       "MATCH (p:Person) WHERE p.role = 'reader' RETURN p.name AS name",
			unordered: true,
		},
		{
			name: "range predicate",
			structured: &clientv1.GraphQuery{Match: baseMatch(), Where: &clientv1.Expr{Expr: &clientv1.Expr_Between{Between: &clientv1.BetweenExpr{
				Value: property("age"), Low: literal(structpb.NewNumberValue(15)), High: literal(structpb.NewNumberValue(30)),
			}}}, Returns: scalarReturn("name")},
			gql:       "MATCH (p:Person) WHERE p.age BETWEEN 15 AND 30 RETURN p.name AS name",
			unordered: true,
		},
		{
			name:       "null predicate",
			structured: &clientv1.GraphQuery{Match: baseMatch(), Where: &clientv1.Expr{Expr: &clientv1.Expr_Null{Null: &clientv1.NullExpr{Alias: "p", Name: "nickname", IsNull: true}}}, Returns: scalarReturn("name")},
			gql:        "MATCH (p:Person) WHERE p.nickname IS NULL RETURN p.name AS name",
			unordered:  true,
		},
		{
			name: "string predicate",
			structured: &clientv1.GraphQuery{Match: baseMatch(), Where: &clientv1.Expr{Expr: &clientv1.Expr_StringPredicate{StringPredicate: &clientv1.StringPredicateExpr{
				Value: property("name"), Query: "alpha", Mode: clientv1.StringPredicateMode_STRING_PREDICATE_MODE_STARTS_WITH,
			}}}, Returns: scalarReturn("name")},
			gql:       "MATCH (p:Person) WHERE p.name STARTS WITH 'alpha' RETURN p.name AS name",
			unordered: true,
		},
		{
			name:       "text predicate",
			structured: &clientv1.GraphQuery{Match: baseMatch(), Where: &clientv1.Expr{Expr: &clientv1.Expr_Text{Text: &clientv1.TextSearchExpr{Alias: "p", Field: "bio", Query: "memory"}}}, Returns: scalarReturn("name")},
			gql:        "MATCH (p:Person) WHERE TEXT_CONTAINS(p.bio, 'memory') RETURN p.name AS name",
			unordered:  true,
		},
		{
			name:       "distinct projection",
			structured: &clientv1.GraphQuery{Match: baseMatch(), Returns: scalarReturn("role"), Distinct: true},
			gql:        "MATCH (p:Person) RETURN DISTINCT p.role AS role",
			unordered:  true,
		},
		{
			name: "grouping and aggregates",
			structured: &clientv1.GraphQuery{Match: baseMatch(), Returns: scalarReturn("role"), AggregateReturns: []*clientv1.AggregateProjection{
				{OutputName: "total", Function: clientv1.AggregateFunction_AGGREGATE_FUNCTION_COUNT, Argument: &clientv1.AggregateArgument{Argument: &clientv1.AggregateArgument_Star{Star: true}}},
				{OutputName: "sum", Function: clientv1.AggregateFunction_AGGREGATE_FUNCTION_SUM, Argument: score},
			}},
			gql:       "MATCH (p:Person) RETURN p.role AS role, COUNT(*) AS total, SUM(p.age) AS sum",
			unordered: true,
		},
	}

	querySvc := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithSchemaManager(personQRX0SchemaManager(t, fixture.domainID))
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			assertQRX0EquivalentRead(t, fixture, querySvc, readTx, tt, "")
		})
	}
}

func TestQRX0IndexedPaginationEquivalence(t *testing.T) {
	fixture, manager, readTx := newQRX0JournalFixture(t)
	querySvc := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithSchemaManager(manager)
	query := journalOrderedQuery(clientv1.SortDirection_SORT_DIRECTION_ASC)
	query.Returns = []*clientv1.ReturnProjection{{Alias: "j.title", OutputName: "title", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_SCALAR}}
	shaped := proto.Clone(query).(*clientv1.GraphQuery)
	shaped.Offset = 1
	shaped.Limit = 1
	structuredShaped, err := querySvc.ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: shaped, PageSize: 10})
	if err != nil {
		t.Fatalf("ExecuteQuery(offset) error = %v", err)
	}
	gqlShaped, err := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: readTx, Query: "MATCH (j:JournalEntry) RETURN j.title AS title ORDER BY j.date OFFSET 1 FETCH FIRST 1 ROW ONLY", PageSize: 10})
	if err != nil {
		t.Fatalf("ExecuteGQL(offset) error = %v", err)
	}
	if structuredTitles, gqlTitles := qrx0ScalarValues(structuredShaped.GetRows(), "title"), qrx0ScalarValues(gqlShaped.GetResult().GetRows(), "title"); !reflect.DeepEqual(structuredTitles, []string{"middle"}) || !reflect.DeepEqual(gqlTitles, []string{"oldest"}) {
		t.Fatalf("legacy indexed OFFSET mismatch changed: structured=%v GQL=%v", structuredTitles, gqlTitles)
	}

	pair := qrx0EquivalentRead{name: "indexed page", structured: query, gql: "MATCH (j:JournalEntry) RETURN j.title AS title ORDER BY j.date", pageSize: 1}
	structuredFirst, _ := assertQRX0EquivalentRead(t, fixture, querySvc, readTx, pair, "")
	if structuredFirst.GetResult().GetNextPageToken() == "" {
		t.Fatal("first indexed page token is empty")
	}
	structuredSecond, gqlSecond := assertQRX0EquivalentRead(t, fixture, querySvc, readTx, pair, structuredFirst.GetResult().GetNextPageToken())
	if structuredSecond.GetResult().GetNextPageToken() == "" {
		t.Fatal("second indexed page token is empty; fixture must exercise three pages")
	}
	structuredThird, _ := assertQRX0EquivalentRead(t, fixture, querySvc, readTx, pair, gqlSecond.GetResult().GetNextPageToken())
	if structuredThird.GetResult().GetNextPageToken() != "" {
		t.Fatalf("third indexed page token = %q, want empty", structuredThird.GetResult().GetNextPageToken())
	}
}

func personQRX0SchemaManager(t *testing.T, domainID string) schemaservice.Manager {
	t.Helper()
	manager := schemaservice.NewManager(storage.NewMemoryStore())
	source := `schema "QRX0 Person" version "1" mode warn
node Person {
  name: string required
  role: string required
  age: int required
  bio: string required
  nickname: string
}
index qrx0_people_by_name on node Person field properties.name ordered asc
index qrx0_people_by_role on node Person field properties.role ordered asc
index qrx0_people_by_age on node Person field properties.age ordered asc
index qrx0_people_by_bio on node Person field properties.bio ordered asc
index qrx0_people_by_nickname on node Person field properties.nickname ordered asc`
	if err := manager.PutDomainSchemaGWL(context.Background(), mustDomainUUID(t, domainID), source); err != nil {
		t.Fatalf("PutDomainSchemaGWL() error = %v", err)
	}
	return manager
}

func TestQRX0TraversalAndProjectionEquivalence(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	manager := documentPathSchemaManagerForQueryTest(t, fixture.domainID)
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	documentID := "00000000-0000-0000-0000-000000000061"
	sectionID := "00000000-0000-0000-0000-000000000062"
	edgeID := "00000000-0000-0000-0000-000000000071"
	createNode := func(id string, labels []string, properties map[string]any) {
		t.Helper()
		if _, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx, Node: &clientv1.NodeCreate{NodeId: &id, Labels: labels, Properties: mustStruct(t, properties)}}); err != nil {
			t.Fatalf("CreateNode(%s) error = %v", id, err)
		}
	}
	createNode(documentID, []string{"Document"}, map[string]any{"rank": 1, "title": "Document"})
	createNode(sectionID, []string{"Section"}, map[string]any{"title": "Section"})
	if _, err := graphSvc.CreateEdge(fixture.ctx, &clientv1.CreateEdgeRequest{TransactionId: writeTx, Edge: &clientv1.EdgeCreate{EdgeId: &edgeID, FromNodeId: documentID, ToNodeId: sectionID, Labels: []string{"contains"}}}); err != nil {
		t.Fatalf("CreateEdge() error = %v", err)
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	instrumented, profile := instrumentQRX0Graph(fixture.graphs)
	querySvc := NewQueryService(fixture.sessions, instrumented, fixture.spaces).
		WithSchemaManager(manager).
		WithQueryEngine(queryEngineForExplainTest(instrumented, manager))

	broadSyntheticQuery := &clientv1.GraphQuery{
		Match: &clientv1.GraphPattern{
			Start: &clientv1.NodePattern{Alias: "d", Labels: []string{"Document"}},
			Steps: []*clientv1.TraversalStep{{Direction: clientv1.TraversalDirection_TRAVERSAL_DIRECTION_OUT, EdgeKind: "contains", EdgeAlias: "edge", Target: &clientv1.NodePattern{Alias: "s", Labels: []string{"Section"}}}},
		},
		Returns: []*clientv1.ReturnProjection{
			{Alias: "d", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_SCALAR},
			{Alias: "d.node_id", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_SCALAR},
			{Alias: "d.properties.node_id", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_SCALAR},
			{Alias: "edge.edge_id", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_SCALAR},
			{Alias: "edge.properties.from_node_id", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_SCALAR},
			{Alias: "edge.properties.to_node_id", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_SCALAR},
		},
	}
	broad, err := querySvc.ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: broadSyntheticQuery, PageSize: 10})
	if err != nil {
		t.Fatalf("ExecuteQuery(broad synthetic fields) error = %v", err)
	}
	if len(broad.GetRows()) != 1 || !broad.GetDiagnostics().GetFullScan() {
		t.Fatalf("broad synthetic result = %+v, diagnostics = %+v", broad.GetRows(), broad.GetDiagnostics())
	}
	if profile.callCount("ListNodes") == 0 || profile.callCount("ScanAdjacency") == 0 || profile.callCount("ListEdges") != 0 {
		t.Fatalf("broad traversal access profile = %+v", profile.callsSnapshot())
	}
	for _, limit := range append(profile.requestedLimits("ListNodes"), profile.requestedLimits("ScanAdjacency")...) {
		if limit <= 0 {
			t.Fatalf("broad traversal used unbounded access limit: %+v", profile.callsSnapshot())
		}
	}
	fields := broad.GetRows()[0].GetFields()
	wantSynthetic := map[string]string{
		"d":                            documentID,
		"d.node_id":                    documentID,
		"d.properties.node_id":         documentID,
		"edge.edge_id":                 edgeID,
		"edge.properties.from_node_id": documentID,
		"edge.properties.to_node_id":   sectionID,
	}
	for name, expected := range wantSynthetic {
		if got := fields[name].GetScalar().GetStringValue(); got != expected {
			t.Fatalf("%s = %q, want %q; fields=%+v", name, got, expected, fields)
		}
	}

	pathQuery := &clientv1.GraphQuery{
		Match: &clientv1.GraphPattern{
			Start: &clientv1.NodePattern{Alias: "d", Labels: []string{"Document"}},
			Steps: []*clientv1.TraversalStep{{Direction: clientv1.TraversalDirection_TRAVERSAL_DIRECTION_OUT, EdgeKind: "contains", Depth: &clientv1.DepthSpec{MinDepth: 1, MaxDepth: 1}, Target: &clientv1.NodePattern{Alias: "s", Labels: []string{"Section"}}}},
		},
		PathAlias: "path",
		Where:     &clientv1.Expr{Expr: &clientv1.Expr_PropertyEquals{PropertyEquals: &clientv1.PropertyEqualsExpr{Alias: "d", Name: "rank", Value: structpb.NewNumberValue(1)}}},
		Returns:   []*clientv1.ReturnProjection{{Alias: "path", OutputName: "path", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_PATH}},
	}
	assertQRX0EquivalentRead(t, fixture, querySvc, readTx, qrx0EquivalentRead{
		name: "bounded path value and graph projection", structured: pathQuery,
		gql: "MATCH path = (d:Document {rank: 1})-[:contains*1..1]->(s:Section) RETURN GRAPH path",
	}, "")

	adjacency := &clientv1.GraphQuery{
		Match: &clientv1.GraphPattern{
			Start: &clientv1.NodePattern{Alias: "d", NodeIds: []string{documentID}},
			Steps: []*clientv1.TraversalStep{{Direction: clientv1.TraversalDirection_TRAVERSAL_DIRECTION_OUT, EdgeKind: "contains", EdgeAlias: "edge", Target: &clientv1.NodePattern{Alias: "s", Labels: []string{"Section"}}}},
		},
		Returns: []*clientv1.ReturnProjection{
			{Alias: "d.title", OutputName: "document", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_SCALAR},
			{Alias: "edge", OutputName: "edge", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_EDGE},
			{Alias: "s.title", OutputName: "section", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_SCALAR},
		},
	}
	assertQRX0EquivalentRead(t, fixture, querySvc, readTx, qrx0EquivalentRead{
		name: "adjacency edge and scalar projections", structured: adjacency,
		gql: "MATCH (d:Document {rank: 1})-[edge:contains]->(s:Section) RETURN d.title AS document, edge, s.title AS section",
	}, "")
}

func TestQRX0SubtreeTreeAndGraphProjectionEquivalence(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	manager := pkmJournalSubtreeSchemaManagerForQueryTest(t, fixture.domainID)
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	createJournalSubtreeFixture(t, fixture, graphSvc, writeTx, 2)
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	querySvc := newQRX8QueryService(fixture, manager)
	structured := pkmJournalSubtreeQuery(20260701, 20260702, 2, 0, 0)
	structured.Returns[0].OutputName = "d"
	pair := qrx0EquivalentRead{
		name:       "subtree tree and graph projections",
		structured: structured,
		gql:        "MATCH (d:pkm.journal)-[:contains*0..2]->(n) WHERE d.journal_day <= 20260702 RETURN GRAPH d,n ORDER BY d.journal_day DESC FETCH FIRST 2 ROWS ONLY",
		pageSize:   2,
	}
	assertQRX0EquivalentRead(t, fixture, querySvc, readTx, pair, "")
}

func TestQRX0SemanticPredicateEquivalenceAndSearchProfile(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{SearchMode: graphmodel.DomainSearchModeDisabled})
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	nodeID := "00000000-0000-0000-0000-000000000041"
	if _, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx, Node: &clientv1.NodeCreate{NodeId: &nodeID, Labels: []string{"Note"}, Properties: mustStruct(t, map[string]any{"title": "semantic match"})}}); err != nil {
		t.Fatalf("CreateNode() error = %v", err)
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}

	indexID := domainsemantic.SemanticIndexID(uuid.MustParse("00000000-0000-0000-0000-000000000051"))
	delegate := &fakeSemanticQuerySearcher{results: []semanticsearch.SearchResult{{SemanticIndexID: indexID, NodeID: graphmodel.NodeID(uuid.MustParse(nodeID)), Score: 0.9}}}
	profile := newQRX0AccessProfile()
	querySvc := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithSemanticManager(instrumentQRX0Semantic(delegate, profile))
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	pair := qrx0EquivalentRead{
		name: "semantic",
		structured: &clientv1.GraphQuery{
			Match:   &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "n", Labels: []string{"Note"}}},
			Where:   &clientv1.Expr{Expr: &clientv1.Expr_Semantic{Semantic: &clientv1.SemanticSearchExpr{Alias: "n", Query: "needle", Limit: 5}}},
			Returns: []*clientv1.ReturnProjection{{Alias: "n.title", OutputName: "title", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_SCALAR}},
		},
		gql: "MATCH (n:Note) WHERE SEMANTIC_SIMILAR(n, 'needle', TOP 5) RETURN n.title AS title",
	}
	structured, gql := assertQRX0EquivalentRead(t, fixture, querySvc, readTx, pair, "")
	profile.recordDiagnostics(structured.GetDiagnostics())
	profile.recordDiagnostics(gql.GetDiagnostics())
	if profile.callCount("SemanticSearch") != 2 || profile.requestedLimits("SemanticSearch")[0] != 5 || profile.workSnapshot().CandidatesExamined != 2 {
		t.Fatalf("semantic profile calls=%+v limits=%v work=%+v", profile.callsSnapshot(), profile.requestedLimits("SemanticSearch"), profile.workSnapshot())
	}
}

func assertQRX0EquivalentRead(t *testing.T, fixture domainPolicyClientAPIFixture, querySvc *QueryService, txID string, pair qrx0EquivalentRead, pageToken string) (*clientv1.ExecuteQueryResponse, *clientv1.ExecuteGQLResponse) {
	t.Helper()
	pageSize := pair.pageSize
	if pageSize == 0 {
		pageSize = 10
	}
	structured, err := querySvc.ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: txID, Query: pair.structured, PageSize: pageSize, PageToken: pageToken})
	if err != nil {
		t.Fatalf("ExecuteQuery(%s) error = %v", pair.name, err)
	}
	gql, err := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: txID, Query: pair.gql, PageSize: pageSize, PageToken: pageToken})
	if err != nil {
		t.Fatalf("ExecuteGQL(%s) error = %v", pair.name, err)
	}
	assertQRX0TypedResultParity(t, structured.GetResult(), gql.GetResult(), pair.unordered, pair.knownTemporalMetadataMismatch)
	if !proto.Equal(structured.GetReadMetadata(), gql.GetReadMetadata()) {
		t.Fatalf("%s read metadata differs: structured=%+v GQL=%+v", pair.name, structured.GetReadMetadata(), gql.GetReadMetadata())
	}
	return structured, gql
}

func assertQRX0TypedResultParity(t *testing.T, structured *clientv1.QueryResult, gql *clientv1.QueryResult, unorderedRows bool, knownTemporalMetadataMismatch bool) {
	t.Helper()
	left := canonicalQRX0Result(structured, unorderedRows, false)
	right := canonicalQRX0Result(gql, unorderedRows, false)
	if knownTemporalMetadataMismatch {
		if proto.Equal(left, right) {
			t.Fatal("known structured/GQL temporal metadata mismatch was fixed; remove the compatibility normalization")
		}
		left = canonicalQRX0Result(structured, unorderedRows, true)
		right = canonicalQRX0Result(gql, unorderedRows, true)
	}
	if !proto.Equal(left, right) {
		t.Fatalf("typed query results differ:\nstructured: %s\nGQL: %s", left, right)
	}
}

func canonicalQRX0Result(in *clientv1.QueryResult, unorderedRows bool, clearTemporalMetadata bool) *clientv1.QueryResult {
	out := proto.Clone(in).(*clientv1.QueryResult)
	if clearTemporalMetadata {
		clearQRX0TemporalMetadata(out)
	}
	if unorderedRows {
		sort.Slice(out.Rows, func(i, j int) bool {
			return bytes.Compare(deterministicQRX0Proto(out.Rows[i]), deterministicQRX0Proto(out.Rows[j])) < 0
		})
	}
	if out.Graph != nil {
		sort.Slice(out.Graph.Nodes, func(i, j int) bool { return out.Graph.Nodes[i].GetNodeId() < out.Graph.Nodes[j].GetNodeId() })
		sort.Slice(out.Graph.Edges, func(i, j int) bool { return out.Graph.Edges[i].GetEdgeId() < out.Graph.Edges[j].GetEdgeId() })
	}
	return out
}

func clearQRX0TemporalMetadata(result *clientv1.QueryResult) {
	clearNode := func(node *clientv1.Node) {
		if node != nil {
			node.CreateTime = nil
			node.UpdateTime = nil
		}
	}
	clearEdge := func(edge *clientv1.Edge) {
		if edge != nil {
			edge.CreateTime = nil
			edge.UpdateTime = nil
		}
	}
	var clearTreeNode func(*clientv1.TreeNode)
	clearTreeNode = func(treeNode *clientv1.TreeNode) {
		if treeNode == nil {
			return
		}
		clearNode(treeNode.Node)
		for _, child := range treeNode.Children {
			clearTreeNode(child)
		}
	}
	for _, row := range result.GetRows() {
		for _, value := range row.GetFields() {
			clearNode(value.GetNode())
			clearEdge(value.GetEdge())
			if path := value.GetPath(); path != nil {
				for _, node := range path.GetNodes() {
					clearNode(node)
				}
				for _, edge := range path.GetEdges() {
					clearEdge(edge)
				}
			}
			if tree := value.GetTree(); tree != nil {
				for _, root := range tree.GetRoots() {
					clearTreeNode(root)
				}
			}
		}
	}
	for _, node := range result.GetGraph().GetNodes() {
		clearNode(node)
	}
	for _, edge := range result.GetGraph().GetEdges() {
		clearEdge(edge)
	}
}

func deterministicQRX0Proto(message proto.Message) []byte {
	out, err := (proto.MarshalOptions{Deterministic: true}).Marshal(message)
	if err != nil {
		panic(err)
	}
	return out
}
