package client

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	graphmodel "github.com/myceldb/mycel/internal/graph/model"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
	schemaservice "github.com/myceldb/mycel/internal/schema/service"
	"github.com/myceldb/mycel/internal/schema/storage"
)

func TestQueryServiceExecuteQueryUsesStrictDomainSchema(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	manager := schemaManagerForQueryTest(t, fixture.domainID, schemamodel.SchemaModeStrict)
	querySvc := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithSchemaManager(manager)
	tx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)

	cases := []struct {
		name  string
		query *clientv1.GraphQuery
		want  string
	}{
		{name: "node label", query: &clientv1.GraphQuery{Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "n", Labels: []string{"Missing"}}}}, want: "unknown node label"},
		{name: "edge label", query: &clientv1.GraphQuery{Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "a", Labels: []string{"Person"}}, Steps: []*clientv1.TraversalStep{{Direction: clientv1.TraversalDirection_TRAVERSAL_DIRECTION_OUT, EdgeKind: "MISSING", Target: &clientv1.NodePattern{Alias: "b", Labels: []string{"Person"}}}}}}, want: "unknown edge label"},
		{name: "property", query: &clientv1.GraphQuery{Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "p", Labels: []string{"Person"}}}, Where: &clientv1.Expr{Expr: &clientv1.Expr_PropertyExists{PropertyExists: &clientv1.PropertyExistsExpr{Alias: "p", Name: "missing"}}}}, want: "unknown properties field"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := querySvc.ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: tx, Query: tc.query})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ExecuteQuery() error = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestQueryServiceExecuteQueryAllowsUnknownsWithoutSchemaOrInPermissiveMode(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	tx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	query := &clientv1.GraphQuery{Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "n", Labels: []string{"Missing"}}}}

	if _, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: tx, Query: query}); err != nil {
		t.Fatalf("schema-free ExecuteQuery() error = %v", err)
	}

	manager := schemaManagerForQueryTest(t, fixture.domainID, schemamodel.SchemaModePermissive)
	if _, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithSchemaManager(manager).ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: tx, Query: query}); err != nil {
		t.Fatalf("permissive ExecuteQuery() error = %v", err)
	}
}

func TestQueryServiceExecuteGQLUsesStrictDomainSchema(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	manager := schemaManagerForQueryTest(t, fixture.domainID, schemamodel.SchemaModeStrict)
	querySvc := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithSchemaManager(manager)
	tx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)

	cases := []struct {
		name  string
		query string
		want  string
	}{
		{name: "node label", query: "MATCH (n:Missing) RETURN n", want: "unknown node label"},
		{name: "edge label", query: "MATCH (a:Person)-[:MISSING]->(b:Person) RETURN a", want: "unknown edge label"},
		{name: "property", query: "MATCH (p:Person) RETURN p.missing", want: "unknown properties field"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: tx, Query: tc.query})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ExecuteGQL() error = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestQueryServiceExecuteGQLScriptUsesStrictDomainSchema(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	manager := schemaManagerForQueryTest(t, fixture.domainID, schemamodel.SchemaModeStrict)
	querySvc := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithSchemaManager(manager)
	tx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)

	_, err := querySvc.ExecuteGQLScript(fixture.ctx, &clientv1.ExecuteGQLScriptRequest{TransactionId: tx, Script: "MATCH (n:Missing) RETURN n"})
	if err == nil || !strings.Contains(err.Error(), "unknown node label") {
		t.Fatalf("ExecuteGQLScript() error = %v, want unknown node label", err)
	}
}

func TestQueryServiceExecuteGQLAllowsUnknownsWithoutSchemaOrInPermissiveMode(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	tx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	query := "MATCH (n:Missing) RETURN n.missing"

	if _, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: tx, Query: query}); err != nil {
		t.Fatalf("schema-free ExecuteGQL() error = %v", err)
	}

	manager := schemaManagerForQueryTest(t, fixture.domainID, schemamodel.SchemaModePermissive)
	if _, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithSchemaManager(manager).ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: tx, Query: query}); err != nil {
		t.Fatalf("permissive ExecuteGQL() error = %v", err)
	}
}

func schemaManagerForQueryTest(t *testing.T, domainID string, mode schemamodel.SchemaMode) schemaservice.Manager {
	t.Helper()
	parsed, err := uuid.Parse(domainID)
	if err != nil {
		t.Fatalf("parse domain id: %v", err)
	}
	manager := schemaservice.NewManager(storage.NewMemoryStore())
	doc := schemamodel.DomainSchema{
		DomainID: graphmodel.DomainID(parsed),
		Name:     "query-test",
		Version:  "v1",
		Mode:     mode,
		NodeTypes: []schemamodel.NodeType{
			{Name: "Person", Labels: []string{"Person"}, Properties: []schemamodel.FieldSpec{{Name: "firstName", Type: schemamodel.FieldTypeString}}, Payload: []schemamodel.FieldSpec{{Name: "text", Type: schemamodel.FieldTypeString}}},
		},
		EdgeTypes: []schemamodel.EdgeType{
			{Name: "Knows", Labels: []string{"KNOWS"}, From: schemamodel.EndpointSpec{NodeTypes: []string{"Person"}}, To: schemamodel.EndpointSpec{NodeTypes: []string{"Person"}}},
		},
	}
	if err := manager.PutDomainSchema(context.Background(), doc); err != nil {
		t.Fatalf("PutDomainSchema() error = %v", err)
	}
	return manager
}
