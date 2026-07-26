package dsl

import (
	"testing"

	schema "github.com/myceldb/mycel/internal/schema/model"
)

func TestParseSchemaDSL(t *testing.T) {
	got, err := Parse(`
# Human-authored schema
schema "Knot PKM" version "1" mode strict domain 00000000-0000-0000-0000-000000000001

node Note {
  title: string required
  tags?: string[]
  kind: enum page,journal,block
}
node Task labels Task,Todo {
  done: bool
}
edge contains from Note to Note hierarchy
edge hasTask from Note to Task {
  confidence?: float
}
`)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if got.Name != "Knot PKM" || got.Version != "1" || got.Mode != schema.SchemaModeStrict {
		t.Fatalf("unexpected schema header: %+v", got)
	}
	if len(got.NodeTypes) != 2 || got.NodeTypes[0].Name != "Note" || got.NodeTypes[1].Labels[1] != "Todo" {
		t.Fatalf("unexpected node types: %+v", got.NodeTypes)
	}
	if len(got.NodeTypes[0].Properties) != 3 {
		t.Fatalf("unexpected fields: %+v", got.NodeTypes[0].Properties)
	}
	if !got.NodeTypes[0].Properties[1].Repeated || got.NodeTypes[0].Properties[1].Required {
		t.Fatalf("optional repeated field not parsed: %+v", got.NodeTypes[0].Properties[1])
	}
	if got.NodeTypes[0].Properties[2].Type != schema.FieldTypeEnum || len(got.NodeTypes[0].Properties[2].EnumValues) != 3 {
		t.Fatalf("enum not parsed: %+v", got.NodeTypes[0].Properties[2])
	}
	if len(got.EdgeTypes) != 2 || got.EdgeTypes[0].Hierarchy == nil || !got.EdgeTypes[0].Hierarchy.Enabled {
		t.Fatalf("unexpected edge types: %+v", got.EdgeTypes)
	}
	if err := schema.Validate(got); err != nil {
		t.Fatalf("schema.Validate() error = %v", err)
	}
}

func TestParseExtendedFieldTypes(t *testing.T) {
	got, err := Parse(`schema "PKM" version "1" mode strict domain 00000000-0000-0000-0000-000000000001
node Journal {
  journal_date: date required
  properties: object required
  metadata?: json optional
}`)
	if err != nil {
		t.Fatal(err)
	}
	fields := got.NodeTypes[0].Properties
	if fields[0].Type != schema.FieldTypeDate || fields[1].Type != schema.FieldTypeObject || fields[2].Type != schema.FieldTypeJSON {
		t.Fatalf("unexpected field types: %+v", fields)
	}
	if err := schema.Validate(got); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestParseSchemaDSLErrorIncludesLine(t *testing.T) {
	_, err := Parse("node Note {\n  broken\n}")
	if err == nil {
		t.Fatal("expected parse error")
	}
	if got := err.Error(); got != "line 2: field requires NAME: TYPE" {
		t.Fatalf("unexpected error: %s", got)
	}
}
