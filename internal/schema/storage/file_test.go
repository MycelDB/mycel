package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	schema "github.com/myceldb/mycel/internal/schema/model"
)

func TestFileStorePutGetDomainSchema(t *testing.T) {
	ctx := context.Background()
	domainID := uuid.New()
	store := NewFileStore(t.TempDir())
	input := schema.DomainSchema{DomainID: domainID, Name: "test", Version: "v1", Mode: schema.SchemaModeStrict, NodeTypes: []schema.NodeType{{Name: "Person", Labels: []string{"Person"}, Properties: []schema.FieldSpec{{Name: "name", Type: schema.FieldTypeString}}}}, Indexes: []schema.IndexDefinition{{Name: "people_by_name", TargetKind: schema.IndexTargetNode, TargetType: "Person", Field: schema.FieldPath{Namespace: "properties", Name: "name"}, Kind: schema.IndexKindOrdered}}}
	if err := store.PutDomainSchema(ctx, input); err != nil {
		t.Fatalf("PutDomainSchema() error = %v", err)
	}
	got, err := store.GetDomainSchema(ctx, domainID)
	if err != nil {
		t.Fatalf("GetDomainSchema() error = %v", err)
	}
	if got.DomainID != domainID || got.Name != "test" || got.Mode != schema.SchemaModeStrict {
		t.Fatalf("unexpected schema: %+v", got)
	}
	if len(got.Indexes) != 1 || got.Indexes[0].Name != "people_by_name" || got.Indexes[0].Direction != schema.IndexSortDirectionAsc {
		t.Fatalf("indexes not persisted: %+v", got.Indexes)
	}
}

func TestFileStoreMissingSchema(t *testing.T) {
	_, err := NewFileStore(t.TempDir()).GetDomainSchema(context.Background(), uuid.New())
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}
