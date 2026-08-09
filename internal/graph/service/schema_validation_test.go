package service

import (
	"context"
	"log/slog"
	"testing"

	"github.com/google/uuid"
	config "github.com/myceldb/mycel/internal/runtime/runtimetest"
	daemonruntime "github.com/myceldb/mycel/internal/runtime/runtimetest"
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
	children, err := m.ListChildren(ctx, tx, rootA.ID.String())
	if err != nil {
		t.Fatalf("ListChildren() error = %v", err)
	}
	if len(children) != 0 {
		t.Fatalf("schema-disabled contains should not be treated as hierarchy children, got %+v", children)
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
	children, err := m.ListChildren(ctx, tx, rootA.ID.String())
	if err != nil {
		t.Fatalf("ListChildren() error = %v", err)
	}
	if len(children) != 1 || children[0].ToID != child.ID {
		t.Fatalf("schema-enabled contains should be treated as hierarchy child, got %+v", children)
	}
}

func TestModuleSchemaHierarchyValidationUsesStagedEdgesForCycleDetection(t *testing.T) {
	ctx := context.Background()
	m, tx := newSchemaValidatedModule(t, schemamodel.SchemaModeStrict)
	manager := schemaservice.NewManager(storage.NewMemoryStore())
	if err := manager.PutDomainSchema(ctx, schemaForGraphServiceWithContains(uuid.MustParse(tx.DomainID), schemamodel.SchemaModeStrict, true)); err != nil {
		t.Fatalf("PutDomainSchema() error = %v", err)
	}
	m.SetSchemaManager(manager)
	a, _ := m.CreateNode(ctx, tx, NodeInput{Labels: []string{"Person"}, Properties: map[string]any{"firstName": "A"}})
	b, _ := m.CreateNode(ctx, tx, NodeInput{Labels: []string{"Person"}, Properties: map[string]any{"firstName": "B"}})
	c, _ := m.CreateNode(ctx, tx, NodeInput{Labels: []string{"Person"}, Properties: map[string]any{"firstName": "C"}})
	if _, err := m.CreateEdge(ctx, tx, EdgeInput{FromNodeID: a.ID.String(), ToNodeID: b.ID.String(), Labels: []string{"contains"}}); err != nil {
		t.Fatalf("CreateEdge(a->b) error = %v", err)
	}
	if _, err := m.CreateEdge(ctx, tx, EdgeInput{FromNodeID: b.ID.String(), ToNodeID: c.ID.String(), Labels: []string{"contains"}}); err != nil {
		t.Fatalf("CreateEdge(b->c) error = %v", err)
	}
	if _, err := m.CreateEdge(ctx, tx, EdgeInput{FromNodeID: c.ID.String(), ToNodeID: a.ID.String(), Labels: []string{"contains"}}); err == nil {
		t.Fatalf("expected cycle validation error from staged hierarchy edges")
	}
}

func TestModuleSchemaHierarchyValidationIgnoresOverlayDeletedParent(t *testing.T) {
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
	edge, err := m.CreateEdge(ctx, tx, EdgeInput{FromNodeID: rootA.ID.String(), ToNodeID: child.ID.String(), Labels: []string{"contains"}})
	if err != nil {
		t.Fatalf("CreateEdge(rootA->child) error = %v", err)
	}
	commit, err := m.CommitTransactionGraph(ctx, tx)
	if err != nil {
		t.Fatalf("CommitTransactionGraph(seed) error = %v", err)
	}

	move := graphTx(tx.SpaceID, tx.DomainID, commit.CommittedRevision)
	if _, err := m.DeleteEdge(ctx, move, edge.ID.String()); err != nil {
		t.Fatalf("DeleteEdge(old parent) error = %v", err)
	}
	if _, err := m.CreateEdge(ctx, move, EdgeInput{FromNodeID: rootB.ID.String(), ToNodeID: child.ID.String(), Labels: []string{"contains"}}); err != nil {
		t.Fatalf("CreateEdge(new parent) should ignore overlay-deleted parent, got %v", err)
	}
	children, err := m.ListChildren(ctx, move, rootB.ID.String())
	if err != nil {
		t.Fatalf("ListChildren(new parent) error = %v", err)
	}
	if len(children) != 1 || children[0].ToID != child.ID {
		t.Fatalf("ListChildren(new parent) = %+v, want child", children)
	}
}

func TestModuleSchemaHierarchyMoveUsesSchemaLabel(t *testing.T) {
	ctx := context.Background()
	domainID := uuid.NewString()
	manager := schemaservice.NewManager(storage.NewMemoryStore())
	if err := manager.PutDomainSchema(ctx, schemaForGraphServiceWithHierarchyLabel(uuid.MustParse(domainID), schemamodel.SchemaModeStrict, "PARENT_OF")); err != nil {
		t.Fatalf("PutDomainSchema() error = %v", err)
	}
	m := newSchemaValidationGraphModule(t, ctx)
	m.SetSchemaManager(manager)
	tx := graphTx(uuid.NewString(), domainID, 0)
	root, _ := m.CreateNode(ctx, tx, NodeInput{Labels: []string{"Person"}, Properties: map[string]any{"firstName": "root"}})
	child, _ := m.CreateNode(ctx, tx, NodeInput{Labels: []string{"Person"}, Properties: map[string]any{"firstName": "child"}})
	edge, err := m.MoveSubtree(ctx, tx, child.ID.String(), root.ID.String(), nil)
	if err != nil {
		t.Fatalf("MoveSubtree() error = %v", err)
	}
	if len(edge.Labels) != 1 || edge.Labels[0] != "PARENT_OF" {
		t.Fatalf("MoveSubtree() labels = %v, want [PARENT_OF]", edge.Labels)
	}
	children, err := m.ListChildren(ctx, tx, root.ID.String())
	if err != nil {
		t.Fatalf("ListChildren() error = %v", err)
	}
	if len(children) != 1 || children[0].ID != edge.ID {
		t.Fatalf("ListChildren() = %+v, want moved hierarchy edge", children)
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
	m := newSchemaValidationGraphModule(t, ctx)
	m.SetSchemaManager(manager)
	return m, graphTx(uuid.NewString(), domainID, 0)
}

func newSchemaValidationGraphModule(t *testing.T, ctx context.Context) *Module {
	t.Helper()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init graph module failed: %v", result.Error)
	}
	return m
}

func schemaForGraphService(domainID uuid.UUID, mode schemamodel.SchemaMode) schemamodel.DomainSchema {
	return schemaForGraphServiceWithContains(domainID, mode, false)
}

func schemaForGraphServiceWithContains(domainID uuid.UUID, mode schemamodel.SchemaMode, containsHierarchy bool) schemamodel.DomainSchema {
	return schemaForGraphServiceWithHierarchyLabelAndFlag(domainID, mode, "contains", containsHierarchy)
}

func schemaForGraphServiceWithHierarchyLabel(domainID uuid.UUID, mode schemamodel.SchemaMode, hierarchyLabel string) schemamodel.DomainSchema {
	return schemaForGraphServiceWithHierarchyLabelAndFlag(domainID, mode, hierarchyLabel, true)
}

func schemaForGraphServiceWithHierarchyLabelAndFlag(domainID uuid.UUID, mode schemamodel.SchemaMode, hierarchyLabel string, containsHierarchy bool) schemamodel.DomainSchema {
	edgeTypes := []schemamodel.EdgeType{
		{Name: "Knows", Labels: []string{"KNOWS"}, From: schemamodel.EndpointSpec{NodeTypes: []string{"Person"}}, To: schemamodel.EndpointSpec{NodeTypes: []string{"Person"}}},
		{Name: "Hierarchy", Labels: []string{hierarchyLabel}, From: schemamodel.EndpointSpec{NodeTypes: []string{"Person"}}, To: schemamodel.EndpointSpec{NodeTypes: []string{"Person"}}},
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
