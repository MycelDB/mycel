package schema_test

import (
	"testing"

	queryschema "github.com/myceldb/mycel/internal/query/schema"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
)

func TestCatalogResolvesLabelsFieldsAndEndpoints(t *testing.T) {
	document := schemamodel.DomainSchema{
		Version: "v3", Mode: schemamodel.SchemaModeStrict,
		NodeTypes: []schemamodel.NodeType{
			{Name: "person", Labels: []string{"Person", "Actor"}, Properties: []schemamodel.FieldSpec{{Name: "name"}}, Payload: []schemamodel.FieldSpec{{Name: "bio"}}},
			{Name: "company", Labels: []string{"Company"}, Properties: []schemamodel.FieldSpec{{Name: "name"}}},
		},
		EdgeTypes: []schemamodel.EdgeType{{
			Name: "works_at", Labels: []string{"WORKS_AT"},
			From: schemamodel.EndpointSpec{NodeTypes: []string{"person"}}, To: schemamodel.EndpointSpec{Labels: []string{"Company"}},
			Properties: []schemamodel.FieldSpec{{Name: "since"}},
		}},
	}
	catalog := queryschema.NewCatalog(&document)
	people, unknown := catalog.ResolveNodeLabels([]string{"Person", "Actor"})
	if unknown != "" || len(people) != 1 || people[0].Name != "person" {
		t.Fatalf("people = %#v, unknown = %q", people, unknown)
	}
	companies, unknown := catalog.ResolveNodeLabels([]string{"Company"})
	if unknown != "" || len(companies) != 1 {
		t.Fatalf("companies = %#v, unknown = %q", companies, unknown)
	}
	edges, unknown := catalog.ResolveEdgeLabels([]string{"WORKS_AT"})
	if unknown != "" || len(edges) != 1 {
		t.Fatalf("edges = %#v, unknown = %q", edges, unknown)
	}
	if !catalog.NodeFieldKnown(people, "payload", "bio") || catalog.NodeFieldKnown(people, "properties", "missing") {
		t.Fatal("unexpected node field lookup result")
	}
	if !catalog.EdgeFieldKnown(edges, "properties", "since") || catalog.EdgeFieldKnown(edges, "payload", "since") {
		t.Fatal("unexpected edge field lookup result")
	}
	if !catalog.EndpointsAllowed(edges, people, companies) || catalog.EndpointsAllowed(edges, companies, people) {
		t.Fatal("unexpected endpoint lookup result")
	}
	if _, unknown := catalog.ResolveNodeLabels([]string{"Missing"}); unknown != "Missing" {
		t.Fatalf("unknown node label = %q", unknown)
	}
}

func TestCatalogNormalizesWithoutMutatingInput(t *testing.T) {
	document := schemamodel.DomainSchema{
		Mode: schemamodel.SchemaModeStrict,
		NodeTypes: []schemamodel.NodeType{{
			Name: " person ", Labels: []string{" Person "}, Properties: []schemamodel.FieldSpec{{Name: " name "}},
		}},
	}
	catalog := queryschema.NewCatalog(&document)
	types, unknown := catalog.ResolveNodeLabels([]string{"Person"})
	if unknown != "" || len(types) != 1 || !catalog.NodeFieldKnown(types, "properties", "name") {
		t.Fatalf("normalized lookup = %#v, unknown = %q", types, unknown)
	}
	if document.NodeTypes[0].Name != " person " || document.NodeTypes[0].Labels[0] != " Person " || document.NodeTypes[0].Properties[0].Name != " name " {
		t.Fatalf("catalog construction mutated input: %#v", document.NodeTypes[0])
	}
}

func TestCatalogWithoutStrictSchemaIsPermissive(t *testing.T) {
	catalog := queryschema.NewCatalog(nil)
	if catalog.Enabled() || catalog.Strict() {
		t.Fatalf("catalog = %#v", catalog)
	}
	if !catalog.NodeFieldKnown(nil, "properties", "anything") || !catalog.EdgeFieldKnown(nil, "payload", "anything") || !catalog.EndpointsAllowed(nil, nil, nil) {
		t.Fatal("schema-free catalog must be permissive")
	}
}
