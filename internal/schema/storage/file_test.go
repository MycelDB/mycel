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
	input := schema.DomainSchema{DomainID: domainID, Name: "test", Version: "v1", Mode: schema.SchemaModeStrict, NodeTypes: []schema.NodeType{{Name: "Person", Labels: []string{"Person"}}}}
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
}

func TestFileStoreMissingSchema(t *testing.T) {
	_, err := NewFileStore(t.TempDir()).GetDomainSchema(context.Background(), uuid.New())
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}
