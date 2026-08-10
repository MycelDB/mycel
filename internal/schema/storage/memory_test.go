package storage

import (
	"context"
	"testing"

	"github.com/google/uuid"
	schema "github.com/myceldb/mycel/internal/schema/model"
)

func TestMemoryStoreClonesIndexes(t *testing.T) {
	ctx := context.Background()
	domainID := uuid.New()
	store := NewMemoryStore()
	input := schema.DomainSchema{DomainID: domainID, NodeTypes: []schema.NodeType{{Name: "JournalEntry", Labels: []string{"JournalEntry"}, Properties: []schema.FieldSpec{{Name: "date", Type: schema.FieldTypeDate}}}}, Indexes: []schema.IndexDefinition{{Name: "journal_entries_by_date", TargetKind: schema.IndexTargetNode, TargetType: "JournalEntry", Field: schema.FieldPath{Namespace: "properties", Name: "date"}, Kind: schema.IndexKindOrdered, Labels: []string{"JournalEntry"}}}}
	if err := store.PutDomainSchema(ctx, input); err != nil {
		t.Fatalf("PutDomainSchema() error = %v", err)
	}
	got, err := store.GetDomainSchema(ctx, domainID)
	if err != nil {
		t.Fatalf("GetDomainSchema() error = %v", err)
	}
	got.Indexes[0].Labels[0] = "mutated"
	again, err := store.GetDomainSchema(ctx, domainID)
	if err != nil {
		t.Fatalf("GetDomainSchema() error = %v", err)
	}
	if again.Indexes[0].Labels[0] != "JournalEntry" {
		t.Fatalf("stored index labels were mutated: %+v", again.Indexes)
	}
}
