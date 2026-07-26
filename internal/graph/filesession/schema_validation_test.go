package filesession

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	graph "github.com/myceldb/mycel/internal/graph/model"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
	schemaservice "github.com/myceldb/mycel/internal/schema/service"
	"github.com/myceldb/mycel/internal/schema/storage"
	sessionapi "github.com/myceldb/mycel/internal/session/api"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestSchemaValidationRejectsInvalidNodeCreate(t *testing.T) {
	ctx := context.Background()
	sess, _ := newSchemaValidationSession(t, schemamodel.SchemaModeStrict)
	_, err := sess.AddNode(ctx, sessionapi.AddNodeInput{Labels: []string{"Person"}, Properties: map[string]any{"firstName": 42}})
	if err == nil {
		t.Fatalf("expected schema validation error")
	}
}

func TestSchemaValidationAcceptsValidNodeAndEdge(t *testing.T) {
	ctx := context.Background()
	sess, _ := newSchemaValidationSession(t, schemamodel.SchemaModeStrict)
	alice, err := sess.AddNode(ctx, sessionapi.AddNodeInput{Labels: []string{"Person"}, Properties: map[string]any{"firstName": "Ada"}})
	if err != nil {
		t.Fatalf("AddNode(alice) error = %v", err)
	}
	bob, err := sess.AddNode(ctx, sessionapi.AddNodeInput{Labels: []string{"Person"}, Properties: map[string]any{"firstName": "Bob"}})
	if err != nil {
		t.Fatalf("AddNode(bob) error = %v", err)
	}
	if _, err := sess.AddEdge(ctx, sessionapi.AddEdgeInput{FromID: alice.ID, ToID: bob.ID, Labels: []string{"KNOWS"}}); err != nil {
		t.Fatalf("AddEdge() error = %v", err)
	}
}

func TestSchemaValidationRejectsInvalidEdgeEndpoint(t *testing.T) {
	ctx := context.Background()
	sess, _ := newSchemaValidationSession(t, schemamodel.SchemaModeStrict)
	person, err := sess.AddNode(ctx, sessionapi.AddNodeInput{Labels: []string{"Person"}, Properties: map[string]any{"firstName": "Ada"}})
	if err != nil {
		t.Fatalf("AddNode(person) error = %v", err)
	}
	place, err := sess.AddNode(ctx, sessionapi.AddNodeInput{Labels: []string{"Place"}})
	if err != nil {
		t.Fatalf("AddNode(place) error = %v", err)
	}
	if _, err := sess.AddEdge(ctx, sessionapi.AddEdgeInput{FromID: person.ID, ToID: place.ID, Labels: []string{"KNOWS"}}); err == nil {
		t.Fatalf("expected schema endpoint validation error")
	}
}

func TestSchemaValidationWarnModeDoesNotReject(t *testing.T) {
	ctx := context.Background()
	sess, _ := newSchemaValidationSession(t, schemamodel.SchemaModeWarn)
	if _, err := sess.AddNode(ctx, sessionapi.AddNodeInput{Labels: []string{"Unknown"}}); err != nil {
		t.Fatalf("warn mode AddNode() error = %v", err)
	}
}

func newSchemaValidationSession(t *testing.T, mode schemamodel.SchemaMode) (sessionapi.Session, graph.DomainID) {
	t.Helper()
	ctx := context.Background()
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	graphsDir := t.TempDir()
	prepareSpaceDir(t, graphsDir, spaceID)
	store := storage.NewMemoryStore()
	manager := schemaservice.NewManager(store)
	if err := manager.PutDomainSchema(ctx, schemaForValidation(domainID, mode)); err != nil {
		t.Fatalf("PutDomainSchema() error = %v", err)
	}
	sess := NewConfig(graphsDir, t.TempDir(), spaceID, sessionapi.Permissions{Read: true, Write: true, Admin: true}, sessionapi.Errors{Closed: errors.New("closed"), NotFound: errors.New("not found"), Unauthorized: errors.New("unauthorized"), Conflict: errors.New("conflict")}, Config{DomainID: domainID, SchemaManager: manager})
	t.Cleanup(func() { _ = sess.Close() })
	return sess, domainID
}

func schemaForValidation(domainID graph.DomainID, mode schemamodel.SchemaMode) schemamodel.DomainSchema {
	return schemamodel.DomainSchema{
		DomainID: domainID,
		Name:     "filesession-test",
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
