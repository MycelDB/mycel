package analysis

import (
	"testing"

	ast "github.com/myceldb/mycel/internal/query/gql/ast/model"
	schema "github.com/myceldb/mycel/internal/schema/model"
)

func TestStrictSchemaRejectsUnknownNodeLabel(t *testing.T) {
	_, err := AnalyzeWithSchema(ast.Query{Statement: ast.MatchStatement{MatchPattern: ast.MatchPattern{Start: ast.NodePattern{Variable: "n", Labels: []string{"Missing"}}}, Returns: []ast.ReturnItem{{Variable: "n"}}}}, strictSchemaContext())
	if err == nil {
		t.Fatalf("expected unknown node label error")
	}
}

func TestStrictSchemaRejectsUnknownEdgeLabel(t *testing.T) {
	_, err := AnalyzeWithSchema(matchPathQuery([]string{"UNKNOWN"}, ast.ReturnItem{Variable: "a"}), strictSchemaContext())
	if err == nil {
		t.Fatalf("expected unknown edge label error")
	}
}

func TestStrictSchemaRejectsUnknownPropertyProjection(t *testing.T) {
	_, err := AnalyzeWithSchema(ast.Query{Statement: ast.MatchStatement{MatchPattern: ast.MatchPattern{Start: ast.NodePattern{Variable: "p", Labels: []string{"Person"}}}, Returns: []ast.ReturnItem{{Kind: ast.ReturnProperty, Variable: "p", Property: "missing"}}}}, strictSchemaContext())
	if err == nil {
		t.Fatalf("expected unknown property projection error")
	}
}

func TestStrictSchemaRejectsUnknownPayloadProjection(t *testing.T) {
	_, err := AnalyzeWithSchema(ast.Query{Statement: ast.MatchStatement{MatchPattern: ast.MatchPattern{Start: ast.NodePattern{Variable: "p", Labels: []string{"Person"}}}, Returns: []ast.ReturnItem{{Kind: ast.ReturnProperty, Variable: "p", Namespace: "payload", Property: "missing"}}}}, strictSchemaContext())
	if err == nil {
		t.Fatalf("expected unknown payload projection error")
	}
}

func TestStrictSchemaValidatesRelationshipEndpointConstraints(t *testing.T) {
	query := ast.Query{Statement: ast.MatchCreateStatement{
		Matches: []ast.NodePattern{{Variable: "p", Labels: []string{"Person"}}, {Variable: "d", Labels: []string{"Document"}}},
		Create:  ast.CreateRelationshipPattern{FromVariable: "p", ToVariable: "d", Relationship: ast.RelationshipPattern{Labels: []string{"KNOWS"}}},
	}}
	_, err := AnalyzeWithSchema(query, strictSchemaContext())
	if err == nil {
		t.Fatalf("expected endpoint constraint error")
	}
}

func TestPermissiveAndNoSchemaAllowUnknownLabelsAndProperties(t *testing.T) {
	query := ast.Query{Statement: ast.MatchStatement{MatchPattern: ast.MatchPattern{Start: ast.NodePattern{Variable: "n", Labels: []string{"Missing"}}}, Returns: []ast.ReturnItem{{Kind: ast.ReturnProperty, Variable: "n", Property: "anything"}}}}
	if _, err := AnalyzeWithSchema(query, permissiveSchemaContext()); err != nil {
		t.Fatalf("permissive schema AnalyzeWithSchema() error = %v", err)
	}
	if _, err := Analyze(query); err != nil {
		t.Fatalf("schema-free Analyze() error = %v", err)
	}
}

func TestStrictSchemaAcceptsKnownPropertyAndPayloadProjection(t *testing.T) {
	query := ast.Query{Statement: ast.MatchStatement{MatchPattern: ast.MatchPattern{Start: ast.NodePattern{Variable: "p", Labels: []string{"Person"}}}, Returns: []ast.ReturnItem{{Kind: ast.ReturnProperty, Variable: "p", Property: "firstName"}, {Kind: ast.ReturnProperty, Variable: "p", Namespace: "payload", Property: "text"}}}}
	if _, err := AnalyzeWithSchema(query, strictSchemaContext()); err != nil {
		t.Fatalf("AnalyzeWithSchema() error = %v", err)
	}
}

func matchPathQuery(edgeLabels []string, returns ast.ReturnItem) ast.Query {
	return ast.Query{Statement: ast.MatchStatement{MatchPattern: ast.MatchPattern{Start: ast.NodePattern{Variable: "a", Labels: []string{"Person"}}, Relationship: &ast.RelationshipPattern{Labels: edgeLabels}, End: &ast.NodePattern{Variable: "b", Labels: []string{"Person"}}}, Returns: []ast.ReturnItem{returns}}}
}

func strictSchemaContext() SchemaContext {
	doc := schemaForAnalysis(schema.SchemaModeStrict)
	return SchemaContext{Schema: &doc}
}

func permissiveSchemaContext() SchemaContext {
	doc := schemaForAnalysis(schema.SchemaModePermissive)
	return SchemaContext{Schema: &doc}
}

func schemaForAnalysis(mode schema.SchemaMode) schema.DomainSchema {
	return schema.DomainSchema{
		Mode: mode,
		NodeTypes: []schema.NodeType{
			{Name: "Person", Labels: []string{"Person"}, Properties: []schema.FieldSpec{{Name: "firstName", Type: schema.FieldTypeString}}, Payload: []schema.FieldSpec{{Name: "text", Type: schema.FieldTypeString}}},
			{Name: "Document", Labels: []string{"Document"}, Properties: []schema.FieldSpec{{Name: "title", Type: schema.FieldTypeString}}},
		},
		EdgeTypes: []schema.EdgeType{
			{Name: "Knows", Labels: []string{"KNOWS"}, From: schema.EndpointSpec{NodeTypes: []string{"Person"}}, To: schema.EndpointSpec{NodeTypes: []string{"Person"}}, Properties: []schema.FieldSpec{{Name: "since", Type: schema.FieldTypeInt}}},
		},
	}
}
