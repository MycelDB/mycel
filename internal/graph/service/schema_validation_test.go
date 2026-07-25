package service

import (
	"context"
	"testing"

	"github.com/google/uuid"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
	schemaservice "github.com/myceldb/mycel/internal/schema/service"
	"github.com/myceldb/mycel/internal/schema/storage"
	daemonsession "github.com/myceldb/mycel/internal/session/service"
)

func TestModuleSchemaValidationRejectsInvalidNode(t *testing.T) {
	ctx := context.Background()
	m, tx := newSchemaValidatedModule(t, schemamodel.SchemaModeStrict)
	if _, err := m.CreateNode(ctx, tx, NodeInput{Labels: []string{"Person"}, Properties: map[string]any{"firstName": 42}}); err == nil {
		t.Fatalf("expected schema validation error")
	}
}

func TestModuleSchemaValidationRejectsInvalidEdgeEndpoint(t *testing.T) {
	ctx := context.Background()
	m, tx := newSchemaValidatedModule(t, schemamodel.SchemaModeStrict)
	person, err := m.CreateNode(ctx, tx, NodeInput{Labels: []string{"Person"}, Properties: map[string]any{"firstName": "Ada"}})
	if err != nil {
		t.Fatalf("CreateNode(person) error = %v", err)
	}
	place, err := m.CreateNode(ctx, tx, NodeInput{Labels: []string{"Place"}})
	if err != nil {
		t.Fatalf("CreateNode(place) error = %v", err)
	}
	if _, err := m.CreateEdge(ctx, tx, EdgeInput{FromNodeID: person.ID.String(), ToNodeID: place.ID.String(), Labels: []string{"KNOWS"}}); err == nil {
		t.Fatalf("expected schema endpoint validation error")
	}
}

func TestModuleSchemaValidationWarnModeDoesNotReject(t *testing.T) {
	ctx := context.Background()
	m, tx := newSchemaValidatedModule(t, schemamodel.SchemaModeWarn)
	if _, err := m.CreateNode(ctx, tx, NodeInput{Labels: []string{"Unknown"}}); err != nil {
		t.Fatalf("warn mode CreateNode() error = %v", err)
	}
}

func newSchemaValidatedModule(t *testing.T, mode schemamodel.SchemaMode) (*Module, daemonsession.GraphTransaction) {
	t.Helper()
	ctx := context.Background()
	domainID := uuid.NewString()
	manager := schemaservice.NewManager(storage.NewMemoryStore())
	if err := manager.PutDomainSchema(ctx, schemaForGraphService(uuid.MustParse(domainID), mode)); err != nil {
		t.Fatalf("PutDomainSchema() error = %v", err)
	}
	m := NewModule()
	m.SetSchemaManager(manager)
	return m, graphTx(uuid.NewString(), domainID, 0)
}

func schemaForGraphService(domainID uuid.UUID, mode schemamodel.SchemaMode) schemamodel.DomainSchema {
	return schemamodel.DomainSchema{
		DomainID: domainID,
		Name:     "graph-service-test",
		Version:  "v1",
		Mode:     mode,
		NodeTypes: []schemamodel.NodeType{
			{Name: "Person", Labels: []string{"Person"}, Properties: []schemamodel.FieldSpec{{Name: "firstName", Type: schemamodel.FieldTypeString, Required: true}}},
			{Name: "Place", Labels: []string{"Place"}},
		},
		EdgeTypes: []schemamodel.EdgeType{
			{Name: "Knows", Labels: []string{"KNOWS"}, From: schemamodel.EndpointSpec{NodeTypes: []string{"Person"}}, To: schemamodel.EndpointSpec{NodeTypes: []string{"Person"}}},
		},
	}
}
