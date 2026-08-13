package accounting

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/graph/model"
	"github.com/myceldb/mycel/internal/identity/model"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestAccountingAppendListSummarizeAndRebuild(t *testing.T) {
	ctx := context.Background()
	location := t.TempDir()
	mgr := NewManager()
	if err := mgr.Init(ctx, location); err != nil {
		t.Fatalf("init failed: %v", err)
	}
	for _, rel := range []string{manifestFile, activeLedger, "indexes", "rollups"} {
		if _, err := os.Stat(filepath.Join(location, rel)); err != nil {
			t.Fatalf("expected %s: %v", rel, err)
		}
	}
	principal := identity.PrincipalID(uuid.NewString())
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	nodeID := graph.NodeID(uuid.New())
	endpointID := domainsemantic.ModelEndpointID(uuid.New())
	modelID := domainsemantic.InferenceModelID(uuid.New())
	grantID := domainsemantic.CredentialGrantID(uuid.New())
	created := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	if _, err := mgr.Append(ctx, domainsemantic.InferenceUsageEvent{CreatedAt: created, Status: "success", Operation: "content_embedding", ActorPrincipalID: principal, EffectivePrincipalID: principal, SpaceID: spaceID, DomainID: domainID, TargetNodeID: nodeID, ModelEndpointID: endpointID, ModelID: modelID, CredentialGrantID: grantID, InputTokens: 10, TotalTokens: 10, TokenCountSource: "provider_reported"}); err != nil {
		t.Fatalf("append success failed: %v", err)
	}
	if _, err := mgr.Append(ctx, domainsemantic.InferenceUsageEvent{CreatedAt: created.Add(time.Hour), Status: "failed", Operation: "chat", ActorPrincipalID: principal, SpaceID: spaceID, DomainID: domainID, ModelEndpointID: endpointID, ModelID: modelID, InputTokens: 5, OutputTokens: 2, TokenCountSource: "estimated", ErrorCode: "boom"}); err != nil {
		t.Fatalf("append failed failed: %v", err)
	}
	events, err := mgr.List(ctx, Filter{PrincipalID: principal})
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected two events, got %+v", events)
	}
	rows, err := mgr.Summarize(ctx, Filter{SpaceID: spaceID}, []string{"operation"})
	if err != nil {
		t.Fatalf("summarize failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected two summary rows, got %+v", rows)
	}
	workerID := identity.PrincipalID(uuid.NewString())
	representedID := identity.PrincipalID(uuid.NewString())
	if _, err := mgr.Append(ctx, domainsemantic.InferenceUsageEvent{CreatedAt: created.Add(2 * time.Hour), Status: "success", Operation: "content_embedding", ActorPrincipalID: workerID, EffectivePrincipalID: workerID, OnBehalfOfPrincipalID: representedID, SpaceID: spaceID, InputTokens: 1, TokenCountSource: "unavailable"}); err != nil {
		t.Fatalf("append background event failed: %v", err)
	}
	byUser, err := mgr.Summarize(ctx, Filter{}, []string{"user"})
	if err != nil {
		t.Fatalf("summarize by user failed: %v", err)
	}
	foundRepresented := false
	for _, row := range byUser {
		if row.Group["user"] == representedID.String() {
			foundRepresented = true
		}
	}
	if !foundRepresented {
		t.Fatalf("expected background usage grouped by represented user, rows=%+v", byUser)
	}
	if err := mgr.RebuildIndexes(ctx); err != nil {
		t.Fatalf("rebuild indexes failed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(location, "indexes", "by_space", spaceID.String(), "2026-06.kidx")); err != nil {
		t.Fatalf("expected by_space index: %v", err)
	}
	principalIndexRaw, err := os.ReadFile(filepath.Join(location, "indexes", "by_principal", principal.String(), "2026-06.kidx"))
	if err != nil {
		t.Fatalf("read principal index failed: %v", err)
	}
	if bytes.Count(principalIndexRaw, []byte("event_id")) != 2 {
		t.Fatalf("expected two principal index entries without actor/effective duplication, got %s", string(principalIndexRaw))
	}
	if err := mgr.RebuildRollups(ctx); err != nil {
		t.Fatalf("rebuild rollups failed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(location, "rollups", "space-monthly.json")); err != nil {
		t.Fatalf("expected space rollup: %v", err)
	}
	var csv bytes.Buffer
	if err := WriteCSV(&csv, events); err != nil {
		t.Fatalf("csv failed: %v", err)
	}
	if !bytes.Contains(csv.Bytes(), []byte("content_embedding")) || !bytes.Contains(csv.Bytes(), []byte("chat")) {
		t.Fatalf("unexpected csv: %s", csv.String())
	}

	reloaded := NewManager()
	if err := reloaded.Init(ctx, location); err != nil {
		t.Fatalf("reload failed: %v", err)
	}
	reloadedEvents, err := reloaded.List(ctx, Filter{Operation: "chat"})
	if err != nil || len(reloadedEvents) != 1 {
		t.Fatalf("unexpected reloaded chat events: events=%+v err=%v", reloadedEvents, err)
	}
}

func TestAccountingRejectsInvalidEvent(t *testing.T) {
	mgr := NewManager()
	if err := mgr.Init(context.Background(), t.TempDir()); err != nil {
		t.Fatalf("init failed: %v", err)
	}
	if _, err := mgr.Append(context.Background(), domainsemantic.InferenceUsageEvent{Operation: "chat"}); err == nil {
		t.Fatal("expected missing status error")
	}
	if _, err := mgr.Append(context.Background(), domainsemantic.InferenceUsageEvent{Status: "success"}); err == nil {
		t.Fatal("expected missing operation error")
	}
}
