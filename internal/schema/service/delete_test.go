package service

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	graph "github.com/myceldb/mycel/internal/graph/model"
	schema "github.com/myceldb/mycel/internal/schema/model"
	"github.com/myceldb/mycel/internal/schema/storage"
	"github.com/myceldb/mycel/internal/wal"
)

func TestDeleteDomainSchemaRemovesStoreAndCache(t *testing.T) {
	ctx := context.Background()
	domainID := graph.DomainID(uuid.New())
	mgr := NewManager(storage.NewMemoryStore())
	source := `schema "Test" version "1" mode strict
node Person {
  record_type: enum person required
}`
	if err := mgr.PutDomainSchemaGWL(ctx, domainID, source); err != nil {
		t.Fatal(err)
	}
	if err := mgr.DeleteDomainSchema(ctx, domainID); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.GetDomainSchema(ctx, domainID); err != ErrSchemaNotFound {
		t.Fatalf("GetDomainSchema() error = %v, want ErrSchemaNotFound", err)
	}
	res, err := mgr.ValidateNode(ctx, domainID, graph.Node{Properties: map[string]any{"record_type": "person", "missing": true}})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Valid() || res.Mode != schema.SchemaModePermissive {
		t.Fatalf("ValidateNode() after delete = %+v, want permissive valid result", res)
	}
}

func TestDeleteDomainSchemaAppendsWALAndApplies(t *testing.T) {
	ctx := context.Background()
	wm, err := wal.Open(ctx, wal.Options{Dir: t.TempDir(), SegmentBytes: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	progress := wal.NewFileProgressStore(t.TempDir() + "/progress.json")
	waiter := wal.NewApplyWaiter()
	mgr := NewManager(storage.NewMemoryStore()).WithWAL(wm, progress, waiter)
	domainID := graph.DomainID(uuid.New())
	source := `schema "Test" version "1" mode strict
node Person {
  record_type: enum person required
}`
	if err := mgr.PutDomainSchemaGWL(ctx, domainID, source); err != nil {
		t.Fatal(err)
	}
	if err := mgr.DeleteDomainSchema(ctx, domainID); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.GetDomainSchema(ctx, domainID); err != ErrSchemaNotFound {
		t.Fatalf("GetDomainSchema() error = %v, want ErrSchemaNotFound", err)
	}
	lsn, err := progress.AppliedLSN(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if lsn == 0 {
		t.Fatal("expected delete WAL LSN to be marked applied")
	}
}

func TestApplySchemaDeleteWALRemovesSchema(t *testing.T) {
	ctx := context.Background()
	domainID := graph.DomainID(uuid.New())
	mgr := NewManager(storage.NewMemoryStore())
	source := `schema "Test" version "1" mode strict
node Person {
  record_type: enum person required
}`
	if err := mgr.PutDomainSchemaGWL(ctx, domainID, source); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(schemaDeleteRecord{DomainID: domainID})
	if err != nil {
		t.Fatal(err)
	}
	if err := mgr.applySchemaDelete(ctx, wal.Record{Payload: payload}); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.GetDomainSchema(ctx, domainID); err != ErrSchemaNotFound {
		t.Fatalf("GetDomainSchema() error = %v, want ErrSchemaNotFound", err)
	}
}
