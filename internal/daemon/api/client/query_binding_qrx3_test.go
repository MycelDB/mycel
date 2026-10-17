package client

import (
	"errors"
	"strings"
	"testing"

	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	querybinding "github.com/myceldb/mycel/internal/query/binding"
	"github.com/myceldb/mycel/internal/query/gql"
	"github.com/myceldb/mycel/internal/query/gql/analysis"
	querylogical "github.com/myceldb/mycel/internal/query/logical"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
)

func TestQRX3StructuredLegacyAndCommonSchemaValidationParity(t *testing.T) {
	schema := qrx3BindingSchema()
	schemaCtx := analysis.SchemaContext{Schema: &schema}
	tests := []struct {
		name  string
		query *clientv1.GraphQuery
		want  string
	}{
		{name: "valid", query: qrx3StructuredPersonQuery()},
		{name: "node label", query: &clientv1.GraphQuery{Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "p", Labels: []string{"Missing"}}}}, want: "unknown node label"},
		{name: "edge label", query: &clientv1.GraphQuery{Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "p", Labels: []string{"Person"}}, Steps: []*clientv1.TraversalStep{{Direction: clientv1.TraversalDirection_TRAVERSAL_DIRECTION_OUT, EdgeKind: "MISSING", Target: &clientv1.NodePattern{Alias: "c", Labels: []string{"Company"}}}}}}, want: "unknown edge label"},
		{name: "property", query: &clientv1.GraphQuery{Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "p", Labels: []string{"Person"}}}, Where: &clientv1.Expr{Expr: &clientv1.Expr_PropertyExists{PropertyExists: &clientv1.PropertyExistsExpr{Alias: "p", Name: "missing"}}}}, want: "unknown properties field"},
		{name: "endpoint", query: &clientv1.GraphQuery{Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "c", Labels: []string{"Company"}}, Steps: []*clientv1.TraversalStep{{Direction: clientv1.TraversalDirection_TRAVERSAL_DIRECTION_OUT, EdgeKind: "WORKS_AT", Target: &clientv1.NodePattern{Alias: "p", Labels: []string{"Person"}}}}}}, want: "endpoint constraints"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			legacyErr := validateStructuredGraphQueryWithSchema(test.query, schemaCtx)
			statement := querylogical.ReadStatement(logicalQueryFromStructured(test.query, 0, ""))
			_, bindErr := querybinding.Bind(statement, qrx3BindingEnvironment(&schema))
			if test.want == "" {
				if legacyErr != nil || bindErr != nil {
					t.Fatalf("legacy error = %v, binder error = %v", legacyErr, bindErr)
				}
				return
			}
			if legacyErr == nil || !strings.Contains(legacyErr.Error(), test.want) {
				t.Fatalf("legacy error = %v, want %q", legacyErr, test.want)
			}
			var queryErr *querylogical.QueryError
			if !errors.As(bindErr, &queryErr) || queryErr.Code != querylogical.ErrorInvalidQuery || !strings.Contains(bindErr.Error(), test.want) {
				t.Fatalf("binder error = %#v, want invalid_query containing %q", bindErr, test.want)
			}
		})
	}
}

func TestQRX3StructuredAndGQLReadsUseEquivalentCommonBindings(t *testing.T) {
	schema := qrx3BindingSchema()
	environment := qrx3BindingEnvironment(&schema)

	structured := logicalQueryFromStructured(qrx3StructuredPersonQuery(), 25, "")
	structuredBound, err := querybinding.Bind(querylogical.ReadStatement(structured), environment)
	if err != nil {
		t.Fatalf("bind structured: %v", err)
	}
	plan, err := gql.CompileWithSchemaAndParams(
		"MATCH (p:Person) WHERE p.firstName = 'Ada' RETURN p.firstName FETCH FIRST 10 ROWS ONLY",
		analysis.SchemaContext{Schema: &schema}, nil,
	)
	if err != nil {
		t.Fatalf("compile GQL: %v", err)
	}
	statement, ok := querylogical.StatementFromGQLPlan(plan, querylogical.NormalizeOptions{PageSize: 25})
	if !ok {
		t.Fatal("normalize GQL statement")
	}
	gqlBound, err := querybinding.Bind(statement, environment)
	if err != nil {
		t.Fatalf("bind GQL: %v", err)
	}
	if len(structuredBound.Variables) != 1 || len(gqlBound.Variables) != 1 || structuredBound.Variables[0].Name != "p" || gqlBound.Variables[0].Name != "p" {
		t.Fatalf("structured variables = %#v, GQL variables = %#v", structuredBound.Variables, gqlBound.Variables)
	}
	if structuredBound.Variables[0].Kind != gqlBound.Variables[0].Kind || len(structuredBound.Variables[0].NodeTypes) != 1 || len(gqlBound.Variables[0].NodeTypes) != 1 || structuredBound.Variables[0].NodeTypes[0] != gqlBound.Variables[0].NodeTypes[0] {
		t.Fatalf("structured binding = %#v, GQL binding = %#v", structuredBound.Variables[0], gqlBound.Variables[0])
	}
}

func qrx3StructuredPersonQuery() *clientv1.GraphQuery {
	return &clientv1.GraphQuery{
		Match:   &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "p", Labels: []string{"Person"}}},
		Where:   &clientv1.Expr{Expr: &clientv1.Expr_PropertyEquals{PropertyEquals: &clientv1.PropertyEqualsExpr{Alias: "p", Name: "firstName"}}},
		Returns: []*clientv1.ReturnProjection{{Alias: "p.firstName", OutputName: "firstName", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_SCALAR}},
		Limit:   10,
	}
}

func qrx3BindingEnvironment(schema *schemamodel.DomainSchema) querybinding.Environment {
	return querybinding.Environment{
		Transaction: sessionmodel.GraphTransaction{Mode: sessionmodel.TransactionModeReadOnly, State: sessionmodel.TransactionStateActive},
		Schema:      schema,
		Domain: querybinding.DomainPolicy{
			BroadSearchable: true, ExplicitSearchable: true,
			SemanticSearchable: true, SemanticCandidatesAvailable: true,
		},
	}
}

func qrx3BindingSchema() schemamodel.DomainSchema {
	return schemamodel.DomainSchema{
		Version: "qrx3", Mode: schemamodel.SchemaModeStrict,
		NodeTypes: []schemamodel.NodeType{
			{Name: "Person", Labels: []string{"Person"}, Properties: []schemamodel.FieldSpec{{Name: "firstName", Type: schemamodel.FieldTypeString}}},
			{Name: "Company", Labels: []string{"Company"}, Properties: []schemamodel.FieldSpec{{Name: "name", Type: schemamodel.FieldTypeString}}},
		},
		EdgeTypes: []schemamodel.EdgeType{{
			Name: "WorksAt", Labels: []string{"WORKS_AT"},
			From: schemamodel.EndpointSpec{NodeTypes: []string{"Person"}}, To: schemamodel.EndpointSpec{NodeTypes: []string{"Company"}},
		}},
	}
}
