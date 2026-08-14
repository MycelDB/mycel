package service

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/automation/storage"
	graph "github.com/myceldb/mycel/internal/graph/model"
)

func TestCreateAutomationAsIgnoresSubmittedOwnershipMetadata(t *testing.T) {
	ctx := context.Background()
	domainID := graph.DomainID(uuid.New())
	mgr := NewManager(storage.NewFileStore(t.TempDir()))

	def, err := mgr.CreateAutomationAs(ctx, domainID, automationDefinitionJSON("owner-forgery", `"owner_principal_id":"victim","created_by_principal_id":"victim","updated_by_principal_id":"victim",`), "creator")
	if err != nil {
		t.Fatalf("CreateAutomationAs() error = %v", err)
	}
	if def.OwnerPrincipalID != "creator" || def.CreatedByPrincipalID != "creator" || def.UpdatedByPrincipalID != "creator" {
		t.Fatalf("ownership metadata should come from authenticated actor, got %+v", def)
	}
}

func TestUpdateAutomationAsPreservesOwnerAndIgnoresSubmittedOwnershipMetadata(t *testing.T) {
	ctx := context.Background()
	domainID := graph.DomainID(uuid.New())
	mgr := NewManager(storage.NewFileStore(t.TempDir()))

	created, err := mgr.CreateAutomationAs(ctx, domainID, automationDefinitionJSON("owner-preserve", ""), "creator")
	if err != nil {
		t.Fatalf("CreateAutomationAs() error = %v", err)
	}
	updated, err := mgr.UpdateAutomationAs(ctx, domainID, created.ID, automationDefinitionJSON("owner-preserve", `"owner_principal_id":"victim","created_by_principal_id":"victim","updated_by_principal_id":"victim",`), "editor")
	if err != nil {
		t.Fatalf("UpdateAutomationAs() error = %v", err)
	}
	if updated.OwnerPrincipalID != "creator" || updated.CreatedByPrincipalID != "creator" || updated.UpdatedByPrincipalID != "editor" {
		t.Fatalf("update should preserve owner/creator and set updater from actor, got %+v", updated)
	}
}

func automationDefinitionJSON(id string, extraFields string) string {
	template := `{
		"id":"__ID__",
		"version":1,
		"status":"enabled",
		__EXTRA__
		"on":{"events":["node.updated"],"labels":["Page"]},
		"condition":{"gql":"MATCH (changed:Page) RETURN changed"},
		"input":{"target":"changed","fields":["properties.title"]},
		"inference":{"operation":"chat","profile":"summarize-page"},
		"prompt":"Summarize this page.",
		"output":{"mode":"text","actions":[{"update_node":{"target":"changed","set":{"payload.summary":"$result.text"}}}]}
	}`
	template = strings.ReplaceAll(template, "__ID__", id)
	return strings.ReplaceAll(template, "__EXTRA__", extraFields)
}
