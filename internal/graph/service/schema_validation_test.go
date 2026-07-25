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

func TestModuleSchemaHierarchyDisabledDoesNotEnforceContainsSingleParent(t *testing.T) {
	ctx := context.Background()
	m, tx := newSchemaValidatedModule(t, schemamodel.SchemaModeStrict)
	rootA, err := m.CreateNode(ctx, tx, NodeInput{Labels: []string{"Person"}, Properties: map[string]any{"firstName": "A"}})
	if err != nil {
		t.Fatalf("CreateNode(rootA) error = %v", err)
	}
	rootB, err := m.CreateNode(ctx, tx, NodeInput{Labels: []string{"Person"}, Properties: map[string]any{"firstName": "B"}})
	if err != nil {
		t.Fatalf("CreateNode(rootB) error = %v", err)
	}
	child, err := m.CreateNode(ctx, tx, NodeInput{Labels: []string{"Person"}, Properties: map[string]any{"firstName": "child"}})
	if err != nil {
		t.Fatalf("CreateNode(child) error = %v", err)
	}
	if _, err := m.CreateEdge(ctx, tx, EdgeInput{FromNodeID: rootA.ID.String(), ToNodeID: child.ID.String(), Labels: []string{"contains"}}); err != nil {
		t.Fatalf("CreateEdge(first contains) error = %v", err)
	}
	if _, err := m.CreateEdge(ctx, tx, EdgeInput{FromNodeID: rootB.ID.String(), ToNodeID: child.ID.String(), Labels: []string{"contains"}}); err != nil {
		t.Fatalf("schema-disabled contains should not enforce single-parent, got %v", err)
	}
}

func TestModuleSchemaHierarchyEnabledEnforcesContainsSingleParent(t *testing.T) {
	ctx := context.Background()
	m, tx := newSchemaValidatedModule(t, schemamodel.SchemaModeStrict)
	manager := schemaservice.NewManager(storage.NewMemoryStore())
	if err := manager.PutDomainSchema(ctx, schemaForGraphServiceWithContains(uuid.MustParse(tx.DomainID), schemamodel.SchemaModeStrict, true)); err != nil {
		t.Fatalf("PutDomainSchema() error = %v", err)
	}
	m.SetSchemaManager(manager)
	rootA, _ := m.CreateNode(ctx, tx, NodeInput{Labels: []string{"Person"}, Properties: map[string]any{"firstName": "A"}})
	rootB, _ := m.CreateNode(ctx, tx, NodeInput{Labels: []string{"Person"}, Properties: map[string]any{"firstName": "B"}})
	child, _ := m.CreateNode(ctx, tx, NodeInput{Labels: []string{"Person"}, Properties: map[string]any{"firstName": "child"}})
	if _, err := m.CreateEdge(ctx, tx, EdgeInput{FromNodeID: rootA.ID.String(), ToNodeID: child.ID.String(), Labels: []string{"contains"}}); err != nil {
		t.Fatalf("CreateEdge(first contains) error = %v", err)
	}
	if _, err := m.CreateEdge(ctx, tx, EdgeInput{FromNodeID: rootB.ID.String(), ToNodeID: child.ID.String(), Labels: []string{"contains"}}); err == nil {
		t.Fatalf("expected hierarchy single-parent validation error")
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
	return schemaForGraphServiceWithContains(domainID, mode, false)
}

func schemaForGraphServiceWithContains(domainID uuid.UUID, mode schemamodel.SchemaMode, containsHierarchy bool) schemamodel.DomainSchema {
	edgeTypes := []schemamodel.EdgeType{
		{Name: "Knows", Labels: []string{"KNOWS"}, From: schemamodel.EndpointSpec{NodeTypes: []string{"Person"}}, To: schemamodel.EndpointSpec{NodeTypes: []string{"Person"}}},
		{Name: "Contains", Labels: []string{"contains"}, From: schemamodel.EndpointSpec{NodeTypes: []string{"Person"}}, To: schemamodel.EndpointSpec{NodeTypes: []string{"Person"}}},
	}
	if containsHierarchy {
		edgeTypes[1].Hierarchy = &schemamodel.HierarchyPolicy{Enabled: true, Acyclic: true, SingleParent: true, SameDomain: true}
	}
	return schemamodel.DomainSchema{
		DomainID: domainID,
		Name:     "graph-service-test",
		Version:  "v1",
		Mode:     mode,
		NodeTypes: []schemamodel.NodeType{
			{Name: "Person", Labels: []string{"Person"}, Properties: []schemamodel.FieldSpec{{Name: "firstName", Type: schemamodel.FieldTypeString, Required: true}}},
			{Name: "Place", Labels: []string{"Place"}},
		},
		EdgeTypes: edgeTypes,
	}
}
