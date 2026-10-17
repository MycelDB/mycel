package client

import (
	"testing"

	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	querybinding "github.com/myceldb/mycel/internal/query/binding"
	"github.com/myceldb/mycel/internal/query/gql"
	"github.com/myceldb/mycel/internal/query/gql/analysis"
	querylogical "github.com/myceldb/mycel/internal/query/logical"
	queryphysical "github.com/myceldb/mycel/internal/query/physical"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestQRX4StructuredAndGQLEqualitySelectSamePhysicalPlan(t *testing.T) {
	schema := qrx3BindingSchema()
	schema.Indexes = append(schema.Indexes, orderedNodeIndex("person_first_name", "Person", "firstName"))
	environment := qrx4PhysicalEnvironment(&schema)

	structuredQuery := &clientv1.GraphQuery{
		Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "p", Labels: []string{"Person"}}},
		Where: &clientv1.Expr{Expr: &clientv1.Expr_PropertyEquals{PropertyEquals: &clientv1.PropertyEqualsExpr{
			Alias: "p", Name: "firstName", Value: structpb.NewStringValue("Ada"),
		}}},
		Returns: []*clientv1.ReturnProjection{{Alias: "p", OutputName: "person", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_NODE}},
	}
	structured := querylogical.ReadStatement(logicalQueryFromStructured(structuredQuery, 25, ""))
	structuredPlan := bindAndPlanQRX4(t, structured, &schema, environment)

	gqlPlan, err := gql.CompileWithSchemaAndParams(
		"MATCH (p:Person) WHERE p.firstName = 'Ada' RETURN p",
		analysis.SchemaContext{Schema: &schema}, nil,
	)
	if err != nil {
		t.Fatalf("compile GQL: %v", err)
	}
	gqlStatement, ok := querylogical.StatementFromGQLPlan(gqlPlan, querylogical.NormalizeOptions{PageSize: 25})
	if !ok {
		t.Fatal("normalize GQL statement")
	}
	textPlan := bindAndPlanQRX4(t, gqlStatement, &schema, environment)

	for name, plan := range map[string]queryphysical.Plan{"structured": structuredPlan, "gql": textPlan} {
		if plan.Family != queryphysical.FamilyIndexedEquality || plan.Name != "OrderedNodePropertyEqualityIndexScan" {
			t.Fatalf("%s plan = %#v", name, plan)
		}
		if plan.Access.Field == nil || plan.Access.Field.Alias != "p" || plan.Access.Field.Name != "firstName" || plan.Access.Index != "person_first_name" {
			t.Fatalf("%s access = %#v", name, plan.Access)
		}
		if len(plan.Indexes) != 1 || plan.Indexes[0].Name != "person_first_name" || plan.Cursor.Kind != "index_key" || !plan.RequiresLegacy() {
			t.Fatalf("%s metadata = %#v", name, plan)
		}
	}
}

func TestQRX4StructuredLegacyShapePrecedenceSelectsPathBeforeAdjacency(t *testing.T) {
	schema := qrx3BindingSchema()
	query := &clientv1.GraphQuery{
		Match: &clientv1.GraphPattern{
			Start: &clientv1.NodePattern{Alias: "p", NodeIds: []string{"node-1"}, Labels: []string{"Person"}},
			Steps: []*clientv1.TraversalStep{{
				Direction: clientv1.TraversalDirection_TRAVERSAL_DIRECTION_OUT,
				EdgeKind:  "WORKS_AT", EdgeAlias: "e",
				Target: &clientv1.NodePattern{Alias: "c", Labels: []string{"Company"}},
			}},
		},
		PathAlias: "path",
		Returns:   []*clientv1.ReturnProjection{{Alias: "path", OutputName: "path", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_PATH}},
	}
	statement := querylogical.ReadStatement(logicalQueryFromStructured(query, 25, ""))
	plan := bindAndPlanQRX4(t, statement, &schema, qrx4PhysicalEnvironment(&schema))
	if plan.Family != queryphysical.FamilyIndexedPath || plan.Access.Kind != queryphysical.AccessNodeIDs {
		t.Fatalf("plan = %#v", plan)
	}
}

func bindAndPlanQRX4(t *testing.T, statement querylogical.Statement, schema *schemamodel.DomainSchema, environment queryphysical.Environment) queryphysical.Plan {
	t.Helper()
	bound, err := querybinding.Bind(statement, qrx3BindingEnvironment(schema))
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	plan, err := queryphysical.Build(bound, environment)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	return plan
}

func qrx4PhysicalEnvironment(schema *schemamodel.DomainSchema) queryphysical.Environment {
	return queryphysical.Environment{
		Schema: schema,
		Domain: querybinding.DomainPolicy{
			BroadSearchable: true, ExplicitSearchable: true,
			SemanticSearchable: true, SemanticCandidatesAvailable: true,
		},
		Capabilities: queryphysical.Capabilities{
			EntityReads: true, Traversals: true, Indexes: true,
			SemanticCandidates: true, Mutations: true,
		},
	}
}

func orderedNodeIndex(name string, target string, field string) schemamodel.IndexDefinition {
	return schemamodel.IndexDefinition{
		Name: name, TargetKind: schemamodel.IndexTargetNode, TargetType: target,
		Field: schemamodel.FieldPath{Namespace: "properties", Name: field}, Kind: schemamodel.IndexKindOrdered,
	}
}
