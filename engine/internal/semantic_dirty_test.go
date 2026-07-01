package internal

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/myceldb/mycel/domain/graph"
	"github.com/myceldb/mycel/domain/identity"
	sessionapi "github.com/myceldb/mycel/session/api"
	storesemantic "github.com/myceldb/mycel/store/semantic"
)

func TestRuntimeEngineAdvancedSemanticAppendsGraphDirtyEvent(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	eng, err := NewEngine(EngineConfig{DataDir: dataDir, Mode: EngineModeStandalone, CreateIfMissing: true, AdminUsername: "admin", AdminPassword: "pass", AdvancedSemanticEnabled: true}, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("engine init failed: %v", err)
	}
	auth, err := eng.Authenticate(ctx, AuthInput{UserRef: identity.UserRef("admin"), Password: "pass"})
	if err != nil {
		t.Fatalf("auth failed: %v", err)
	}
	space, err := eng.CreateSpace(ctx, CreateSpaceInput{AccessToken: auth.AccessToken, Name: "Semantic"})
	if err != nil {
		t.Fatalf("create space failed: %v", err)
	}
	sess, err := eng.OpenSession(ctx, OpenSessionInput{AccessToken: auth.AccessToken, SpaceID: space.SpaceID})
	if err != nil {
		t.Fatalf("open session failed: %v", err)
	}
	node, err := sess.AddNode(ctx, sessionAddNode("hello"))
	if err != nil {
		t.Fatalf("add node failed: %v", err)
	}
	if _, err := sess.UpdateNode(ctx, sessionUpdateNode(node.ID, "updated")); err != nil {
		t.Fatalf("update node failed: %v", err)
	}
	mgr := storesemantic.NewSpaceManager()
	if err := mgr.Init(ctx, filepath.Join(dataDir, "graphs", space.SpaceID.String(), "semantic"), space.SpaceID); err != nil {
		t.Fatalf("semantic init failed: %v", err)
	}
	events, err := mgr.ListGraphDirtyEvents(ctx)
	if err != nil {
		t.Fatalf("list dirty events failed: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected two graph dirty events, got %+v", events)
	}
	if events[0].TxnID == events[1].TxnID || events[0].GraphRevision == 0 || events[1].GraphRevision <= events[0].GraphRevision {
		t.Fatalf("unexpected txn/revision events: %+v", events)
	}
	if len(events[0].CreatedNodeIDs) != 1 || len(events[1].UpdatedNodeIDs) != 1 {
		t.Fatalf("unexpected node classifications: %+v", events)
	}
}

func TestRuntimeEngineAdvancedSemanticSearchRequiresGate(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	eng, err := NewEngine(EngineConfig{DataDir: dataDir, Mode: EngineModeStandalone, CreateIfMissing: true, AdminUsername: "admin", AdminPassword: "pass"}, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("engine init failed: %v", err)
	}
	auth, err := eng.Authenticate(ctx, AuthInput{UserRef: identity.UserRef("admin"), Password: "pass"})
	if err != nil {
		t.Fatalf("auth failed: %v", err)
	}
	space, err := eng.CreateSpace(ctx, CreateSpaceInput{AccessToken: auth.AccessToken, Name: "Semantic"})
	if err != nil {
		t.Fatalf("create space failed: %v", err)
	}
	sess, err := eng.OpenSession(ctx, OpenSessionInput{AccessToken: auth.AccessToken, SpaceID: space.SpaceID})
	if err != nil {
		t.Fatalf("open session failed: %v", err)
	}
	defer sess.Close()
	if _, err := sess.AdvancedSemanticSearch(ctx, sessionapi.AdvancedSemanticSearchInput{Text: "hello"}); err == nil {
		t.Fatal("expected disabled advanced semantic search to fail")
	}
}

func TestRuntimeEngineAdvancedSemanticSearchEmptyWhenNoIndexes(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	eng, err := NewEngine(EngineConfig{DataDir: dataDir, Mode: EngineModeStandalone, CreateIfMissing: true, AdminUsername: "admin", AdminPassword: "pass", AdvancedSemanticEnabled: true}, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("engine init failed: %v", err)
	}
	auth, err := eng.Authenticate(ctx, AuthInput{UserRef: identity.UserRef("admin"), Password: "pass"})
	if err != nil {
		t.Fatalf("auth failed: %v", err)
	}
	space, err := eng.CreateSpace(ctx, CreateSpaceInput{AccessToken: auth.AccessToken, Name: "Semantic"})
	if err != nil {
		t.Fatalf("create space failed: %v", err)
	}
	sess, err := eng.OpenSession(ctx, OpenSessionInput{AccessToken: auth.AccessToken, SpaceID: space.SpaceID})
	if err != nil {
		t.Fatalf("open session failed: %v", err)
	}
	defer sess.Close()
	out, err := sess.AdvancedSemanticSearch(ctx, sessionapi.AdvancedSemanticSearchInput{Text: "hello"})
	if err != nil {
		t.Fatalf("advanced search failed: %v", err)
	}
	if len(out.Results) != 0 || len(out.Warnings) != 0 || len(out.Groups) != 0 {
		t.Fatalf("expected empty search output, got %+v", out)
	}
}

func TestRuntimeEngineRunSemanticMaintenanceNoop(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	eng, err := NewEngine(EngineConfig{DataDir: dataDir, Mode: EngineModeStandalone, CreateIfMissing: true, AdminUsername: "admin", AdminPassword: "pass", AdvancedSemanticEnabled: true}, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("engine init failed: %v", err)
	}
	auth, err := eng.Authenticate(ctx, AuthInput{UserRef: identity.UserRef("admin"), Password: "pass"})
	if err != nil {
		t.Fatalf("auth failed: %v", err)
	}
	space, err := eng.CreateSpace(ctx, CreateSpaceInput{AccessToken: auth.AccessToken, Name: "Semantic"})
	if err != nil {
		t.Fatalf("create space failed: %v", err)
	}
	result, err := eng.RunSemanticMaintenance(ctx, RunSemanticMaintenanceInput{AccessToken: auth.AccessToken, SpaceID: space.SpaceID})
	if err != nil {
		t.Fatalf("semantic maintenance failed: %v", err)
	}
	if result.ProcessedEvents != 0 || result.EnqueuedItems != 0 || result.ProcessedItems != 0 || result.CompletedItems != 0 || result.FailedItems != 0 {
		t.Fatalf("expected no-op maintenance result, got %+v", result)
	}
}

func TestRuntimeEngineSemanticDirtyDisabledByDefault(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	eng, err := NewEngine(EngineConfig{DataDir: dataDir, Mode: EngineModeStandalone, CreateIfMissing: true, AdminUsername: "admin", AdminPassword: "pass"}, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("engine init failed: %v", err)
	}
	auth, err := eng.Authenticate(ctx, AuthInput{UserRef: identity.UserRef("admin"), Password: "pass"})
	if err != nil {
		t.Fatalf("auth failed: %v", err)
	}
	space, err := eng.CreateSpace(ctx, CreateSpaceInput{AccessToken: auth.AccessToken, Name: "Semantic"})
	if err != nil {
		t.Fatalf("create space failed: %v", err)
	}
	sess, err := eng.OpenSession(ctx, OpenSessionInput{AccessToken: auth.AccessToken, SpaceID: space.SpaceID})
	if err != nil {
		t.Fatalf("open session failed: %v", err)
	}
	if _, err := sess.AddNode(ctx, sessionAddNode("hello")); err != nil {
		t.Fatalf("add node failed: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(dataDir, "graphs", space.SpaceID.String(), "semantic", "events", "graph-dirty-000001.ksem"))
	if err == nil && strings.TrimSpace(string(raw)) != "" {
		t.Fatalf("expected no dirty event by default, got %s", string(raw))
	}
}

func sessionAddNode(content string) sessionapi.AddNodeInput {
	return sessionapi.AddNodeInput{Content: content}
}

func sessionUpdateNode(id graph.NodeID, content string) sessionapi.UpdateNodeInput {
	return sessionapi.UpdateNodeInput{ID: id, Content: content}
}
