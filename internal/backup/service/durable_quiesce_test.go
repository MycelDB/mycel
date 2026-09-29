package service

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	activitymodel "github.com/myceldb/mycel/internal/activity/model"
	activityservice "github.com/myceldb/mycel/internal/activity/service"
	automationservice "github.com/myceldb/mycel/internal/automation/service"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	graph "github.com/myceldb/mycel/internal/graph/model"
	graphnotification "github.com/myceldb/mycel/internal/graph/notification"
	inferencemodel "github.com/myceldb/mycel/internal/inference/model"
	inferenceservice "github.com/myceldb/mycel/internal/inference/service"
	"github.com/myceldb/mycel/internal/runtime/quiesce"
	"github.com/myceldb/mycel/internal/runtime/runtimetest"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
	schemaservice "github.com/myceldb/mycel/internal/schema/service"
	lexicalanalyzer "github.com/myceldb/mycel/internal/search/lexical/analyzer"
	lexicalindex "github.com/myceldb/mycel/internal/search/lexical/index"
	lexicalservice "github.com/myceldb/mycel/internal/search/lexical/service"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBackupQuiesceRejectsRepresentativeDurableWriters(t *testing.T) {
	ctx := context.Background()
	rt := runtimetest.New(t.TempDir(), nil)
	activityMod := activityservice.NewModule()
	schemaMod := schemaservice.NewModule("")
	notificationMod := graphnotification.NewModule()
	lexicalMod := lexicalservice.NewModule()
	inferenceMod := inferenceservice.NewModule()
	automationMod := automationservice.NewModule(filepath.Join(rt.DataDir(), "automation"))
	if err := rt.InitServices(ctx, []runtimetest.Service{activityMod, schemaMod, notificationMod, lexicalMod, inferenceMod, automationMod}); err != nil {
		t.Fatalf("InitServices() error=%v", err)
	}

	wantParticipants := map[string]bool{
		activityservice.ModuleName:   false,
		schemaservice.ModuleName:     false,
		graphnotification.ModuleName: false,
		lexicalservice.ModuleName:    false,
		inferenceservice.ModuleName:  false,
		automationservice.ModuleName: false,
	}
	for _, participant := range rt.QuiesceCoordinator().Participants() {
		if _, ok := wantParticipants[participant.Name()]; ok {
			wantParticipants[participant.Name()] = true
		}
	}
	for name, seen := range wantParticipants {
		if !seen {
			t.Fatalf("quiesce participant %q was not registered", name)
		}
	}

	lease, err := rt.QuiesceCoordinator().QuiesceAll(ctx, quiesce.Request{Reason: "backup regression", Mode: quiesce.ModeBackup, Source: "test"})
	if err != nil {
		t.Fatalf("QuiesceAll() error=%v", err)
	}
	defer lease.Release(context.Background())

	domainID := graph.DomainID(uuid.New())
	spaceID := domainspace.SpaceID(uuid.New())
	nodeID := graph.NodeID(uuid.New())

	mustUnavailable(t, "activity append", func() error {
		_, err := activityMod.Append(ctx, activitymodel.Event{Severity: activitymodel.SeverityInfo, Category: activitymodel.CategoryBackup, Type: "backup.quiesce.test", Message: "test"})
		return err
	})
	mustUnavailable(t, "schema put", func() error {
		return schemaMod.PutDomainSchema(ctx, schemamodel.DomainSchema{DomainID: domainID, Mode: schemamodel.SchemaModePermissive})
	})
	mustUnavailable(t, "graph notification publish", func() error {
		return notificationMod.OnGraphCommitted(ctx, graphchange.CommittedEvent{SpaceID: spaceID, DomainID: domainID, GraphRevision: 1, Revision: 1, Changes: []graphchange.Change{{Type: graphchange.ChangeTypeNodeCreated, NodeID: nodeID.String()}}})
	})
	mustUnavailable(t, "lexical rebuild", func() error {
		return lexicalMod.Rebuild(ctx, spaceID.String(), domainID.String(), []lexicalindex.IndexedDocument{{Document: lexicalanalyzer.Document{NodeID: nodeID.String(), DomainID: domainID.String(), Fields: []lexicalanalyzer.Field{{Path: "properties.title", Text: "backup quiesce"}}}, GraphRevision: 1}}, 1)
	})
	mustUnavailable(t, "inference endpoint", func() error {
		_, err := inferenceMod.GlobalManager().UpsertEndpoint(ctx, inferencemodel.Endpoint{Key: "openai"})
		return err
	})
	spaceInference, err := inferenceMod.SpaceManager(ctx, spaceID.String())
	if err != nil {
		t.Fatalf("SpaceManager() error=%v", err)
	}
	mustUnavailable(t, "inference profile", func() error {
		_, err := spaceInference.UpsertProfile(ctx, inferencemodel.Profile{Key: "chat", Operation: inferencemodel.OperationChat})
		return err
	})
	mustUnavailable(t, "inference usage", func() error {
		_, err := inferenceMod.UsageLedger().AppendUsageEvent(ctx, inferencemodel.UsageEvent{SpaceID: spaceID.String(), Operation: inferencemodel.OperationChat, Status: inferencemodel.UsageStatusSucceeded})
		return err
	})
	mustUnavailable(t, "automation scheduled", func() error {
		_, err := automationMod.ProcessScheduled(ctx, domainID, 10)
		return err
	})
	mustUnavailable(t, "automation config", func() error {
		_, err := automationMod.CreateAutomation(ctx, domainID, `{"id":"a","version":1,"status":"enabled","trigger":{"events":["node.created"]},"steps":[{"id":"s","type":"builtin"}]}`)
		return err
	})
}

func mustUnavailable(t *testing.T, name string, fn func() error) {
	t.Helper()
	if err := fn(); status.Code(err) != codes.Unavailable {
		t.Fatalf("%s error=%v, want Unavailable", name, err)
	}
}
