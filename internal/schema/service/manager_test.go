package service

import (
	"context"
	"testing"

	"github.com/google/uuid"
	graph "github.com/myceldb/mycel/internal/graph/model"
	schema "github.com/myceldb/mycel/internal/schema/model"
	"github.com/myceldb/mycel/internal/schema/storage"
)

func TestManagerPutGetDomainSchema(t *testing.T) {
	ctx := context.Background()
	domainID := uuid.New()
	mgr := NewManager(storage.NewMemoryStore())
	input := sampleSchema(domainID, schema.SchemaModeStrict)
	if err := mgr.PutDomainSchema(ctx, input); err != nil {
		t.Fatalf("PutDomainSchema() error = %v", err)
	}
	got, err := mgr.GetDomainSchema(ctx, domainID)
	if err != nil {
		t.Fatalf("GetDomainSchema() error = %v", err)
	}
	if got.ID == uuid.Nil {
		t.Fatalf("expected schema id to be assigned")
	}
	if got.Mode != schema.SchemaModeStrict || got.NodeTypes[0].Name != "Person" {
		t.Fatalf("unexpected schema: %+v", got)
	}
}

func TestPutDomainSchemaGWLAndValidateExtendedTypes(t *testing.T) {
	ctx := context.Background()
	domainID := graph.DomainID(uuid.New())
	mgr := NewManager(storage.NewMemoryStore())
	source := `schema "PKM" version "1" mode strict
node Journal {
  record_type: enum pkm.journal required
  journal_date: date required
  properties: object required
}`
	if err := mgr.PutDomainSchemaGWL(ctx, domainID, source); err != nil {
		t.Fatal(err)
	}
	got, err := mgr.GetDomainSchema(ctx, domainID)
	if err != nil {
		t.Fatal(err)
	}
	if got.SourceGWL != source || got.SourceHash == "" {
		t.Fatalf("source not persisted: %+v", got)
	}
	valid := graph.Node{Properties: map[string]any{"record_type": "pkm.journal", "journal_date": "2026-07-26", "properties": map[string]any{}}}
	res, err := mgr.ValidateNode(ctx, domainID, valid)
	if err != nil || !res.Valid() {
		t.Fatalf("valid node rejected: res=%+v err=%v", res, err)
	}
	invalid := graph.Node{Properties: map[string]any{"record_type": "pkm.journal", "journal_date": "2026-7-26", "properties": "bad"}}
	res, err = mgr.ValidateNode(ctx, domainID, invalid)
	if err != nil {
		t.Fatal(err)
	}
	if res.Valid() {
		t.Fatalf("invalid node accepted: %+v", res)
	}
}

func TestSchemaValidationRejectsDuplicateLabels(t *testing.T) {
	domainID := uuid.New()
	mgr := NewManager(storage.NewMemoryStore())
	value := schema.DomainSchema{DomainID: domainID, Mode: schema.SchemaModeStrict, NodeTypes: []schema.NodeType{
		{Name: "Person", Labels: []string{"Thing"}},
		{Name: "Place", Labels: []string{"Thing"}},
	}}
	if err := mgr.PutDomainSchema(context.Background(), value); err == nil {
		t.Fatalf("expected duplicate label validation error")
	}
}

func TestValidateNodeModes(t *testing.T) {
	ctx := context.Background()
	domainID := uuid.New()
	for _, tc := range []struct {
		mode       schema.SchemaMode
		wantIssues int
		wantValid  bool
	}{
		{mode: schema.SchemaModePermissive, wantIssues: 0, wantValid: true},
		{mode: schema.SchemaModeWarn, wantIssues: 1, wantValid: true},
		{mode: schema.SchemaModeStrict, wantIssues: 1, wantValid: false},
	} {
		mgr := NewManager(storage.NewMemoryStore())
		if err := mgr.PutDomainSchema(ctx, sampleSchema(domainID, tc.mode)); err != nil {
			t.Fatalf("PutDomainSchema(%s) error = %v", tc.mode, err)
		}
		result, err := mgr.ValidateNode(ctx, domainID, graph.Node{DomainID: domainID, Labels: []string{"Unknown"}})
		if err != nil {
			t.Fatalf("ValidateNode(%s) error = %v", tc.mode, err)
		}
		if len(result.Issues) != tc.wantIssues || result.Valid() != tc.wantValid {
			t.Fatalf("ValidateNode(%s) issues=%+v valid=%t", tc.mode, result.Issues, result.Valid())
		}
	}
}

func TestValidateNodeFields(t *testing.T) {
	ctx := context.Background()
	domainID := uuid.New()
	mgr := NewManager(storage.NewMemoryStore())
	if err := mgr.PutDomainSchema(ctx, sampleSchema(domainID, schema.SchemaModeStrict)); err != nil {
		t.Fatalf("PutDomainSchema() error = %v", err)
	}
	result, err := mgr.ValidateNode(ctx, domainID, graph.Node{DomainID: domainID, Labels: []string{"Person"}, Properties: map[string]any{"firstName": "Ada", "age": "not-int"}})
	if err != nil {
		t.Fatalf("ValidateNode() error = %v", err)
	}
	if result.Valid() || len(result.Issues) != 1 {
		t.Fatalf("expected one type issue, got valid=%t issues=%+v", result.Valid(), result.Issues)
	}
}

func TestValidateEdgeEndpointConstraints(t *testing.T) {
	ctx := context.Background()
	domainID := uuid.New()
	mgr := NewManager(storage.NewMemoryStore())
	if err := mgr.PutDomainSchema(ctx, sampleSchema(domainID, schema.SchemaModeStrict)); err != nil {
		t.Fatalf("PutDomainSchema() error = %v", err)
	}
	valid, err := mgr.ValidateEdge(ctx, domainID, graph.Edge{DomainID: domainID, Labels: []string{"KNOWS"}}, graph.Node{Labels: []string{"Person"}}, graph.Node{Labels: []string{"Person"}})
	if err != nil || !valid.Valid() {
		t.Fatalf("expected valid edge, result=%+v err=%v", valid, err)
	}
	invalid, err := mgr.ValidateEdge(ctx, domainID, graph.Edge{DomainID: domainID, Labels: []string{"KNOWS"}}, graph.Node{Labels: []string{"Person"}}, graph.Node{Labels: []string{"Place"}})
	if err != nil {
		t.Fatalf("ValidateEdge() error = %v", err)
	}
	if invalid.Valid() {
		t.Fatalf("expected invalid endpoint, got %+v", invalid)
	}
}

func sampleSchema(domainID graph.DomainID, mode schema.SchemaMode) schema.DomainSchema {
	return schema.DomainSchema{
		DomainID: domainID,
		Name:     "test",
		Version:  "v1",
		Mode:     mode,
		NodeTypes: []schema.NodeType{
			{Name: "Person", Labels: []string{"Person"}, Properties: []schema.FieldSpec{{Name: "firstName", Type: schema.FieldTypeString, Required: true}, {Name: "age", Type: schema.FieldTypeInt}}},
			{Name: "Place", Labels: []string{"Place"}},
		},
		EdgeTypes: []schema.EdgeType{
			{Name: "Knows", Labels: []string{"KNOWS"}, From: schema.EndpointSpec{NodeTypes: []string{"Person"}}, To: schema.EndpointSpec{NodeTypes: []string{"Person"}}},
		},
	}
}
