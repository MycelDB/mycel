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

func makeSchemaPutPayload(value schema.DomainSchema) ([]byte, error) {
	return json.Marshal(schemaPutRecord{Schema: value})
}

func TestPutDomainSchemaGWLAppendsWALAndApplies(t *testing.T) {
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
  name: string required
}`
	if err := mgr.PutDomainSchemaGWL(ctx, domainID, source); err != nil {
		t.Fatal(err)
	}
	got, err := mgr.GetDomainSchema(ctx, domainID)
	if err != nil {
		t.Fatal(err)
	}
	if got.SourceGWL != source || got.SourceHash == "" {
		t.Fatalf("schema source not stored: %+v", got)
	}
	lsn, err := progress.AppliedLSN(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if lsn == 0 {
		t.Fatal("expected WAL LSN to be marked applied")
	}
}

func TestApplySchemaPutWALWarmsValidationCache(t *testing.T) {
	ctx := context.Background()
	domainID := graph.DomainID(uuid.New())
	mgr := NewManager(storage.NewMemoryStore())
	source := `schema "Test" version "1" mode strict
node Journal {
  record_type: enum pkm.journal required
  journal_date: date required
}`
	if err := mgr.PutDomainSchemaGWL(ctx, domainID, source); err != nil {
		t.Fatal(err)
	}
	stored, err := mgr.GetDomainSchema(ctx, domainID)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := makeSchemaPutPayload(stored)
	if err != nil {
		t.Fatal(err)
	}
	replay := NewManager(storage.NewMemoryStore())
	if err := replay.applySchemaPut(ctx, wal.Record{Payload: payload}); err != nil {
		t.Fatal(err)
	}
	res, err := replay.ValidateNode(ctx, domainID, graph.Node{Properties: map[string]any{"record_type": "pkm.journal", "journal_date": "bad"}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Valid() {
		t.Fatalf("expected replayed schema cache to reject invalid date: %+v", res)
	}
}
