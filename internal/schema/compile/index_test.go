package compile

import (
	"testing"

	"github.com/google/uuid"
	schema "github.com/myceldb/mycel/internal/schema/model"
)

func TestCompileIndexesByName(t *testing.T) {
	compiled, err := Compile(schema.DomainSchema{
		DomainID:  uuid.New(),
		NodeTypes: []schema.NodeType{{Name: "JournalEntry", Labels: []string{"JournalEntry"}, Properties: []schema.FieldSpec{{Name: "date", Type: schema.FieldTypeDate}}}},
		Indexes:   []schema.IndexDefinition{{Name: "journal_entries_by_date", TargetKind: schema.IndexTargetNode, TargetType: "JournalEntry", Field: schema.FieldPath{Namespace: "properties", Name: "date"}, Kind: schema.IndexKindOrdered}},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	idx := compiled.IndexesByName["journal_entries_by_date"]
	if idx == nil || idx.Direction != schema.IndexSortDirectionAsc || len(idx.Labels) != 1 || idx.Labels[0] != "JournalEntry" {
		t.Fatalf("compiled index not normalized: %+v", idx)
	}
}
