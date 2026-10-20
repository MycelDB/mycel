package service

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	graphmodel "github.com/myceldb/mycel/internal/graph/model"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestReplicatedMaintenanceWorkMutationsAreDeterministic(t *testing.T) {
	ctx := context.Background()
	spaceID := domainspace.SpaceID(uuid.New())
	managers := []storesemantic.MaintenanceManager{storesemantic.NewMaintenanceManager(), storesemantic.NewMaintenanceManager()}
	locations := []string{t.TempDir(), t.TempDir()}
	for i, manager := range managers {
		if err := manager.Init(ctx, locations[i], spaceID); err != nil {
			t.Fatalf("init manager %d: %v", i, err)
		}
	}
	at := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	item := prepareWorkItemMutation(domainsemantic.SemanticDirtyWorkItem{SpaceID: spaceID, DomainID: graphmodel.DomainID(uuid.New()), SemanticRuleID: domainsemantic.SemanticRuleID(uuid.New()), EmbeddingBindingKey: "search", TargetNodeID: graphmodel.NodeID(uuid.New()), FirstGraphRevision: 7, LastGraphRevision: 7}, at)
	applyAll := func(kind string, payload any) {
		t.Helper()
		raw, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		for i, manager := range managers {
			if err := applyMaintenanceMutation(ctx, manager, maintenanceMutationRecord{Kind: kind, SpaceID: spaceID, Payload: raw}); err != nil {
				t.Fatalf("apply %s to manager %d: %v", kind, i, err)
			}
		}
	}
	assertEqual := func(stage string) []domainsemantic.SemanticDirtyWorkItem {
		t.Helper()
		left, err := managers[0].ListDirtyWorkItems(ctx)
		if err != nil {
			t.Fatal(err)
		}
		right, err := managers[1].ListDirtyWorkItems(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(left, right) {
			t.Fatalf("%s diverged: left=%+v right=%+v", stage, left, right)
		}
		leftLog, err := os.ReadFile(filepath.Join(locations[0], "work", "work-000001.ksem"))
		if err != nil {
			t.Fatal(err)
		}
		rightLog, err := os.ReadFile(filepath.Join(locations[1], "work", "work-000001.ksem"))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(leftLog, rightLog) {
			t.Fatalf("%s durable work logs diverged", stage)
		}
		return left
	}
	applyAll("work.upsert", item)
	items := assertEqual("upsert")
	if len(items) != 1 || items[0].ID != item.ID || items[0].CreatedAt != at || items[0].UpdatedAt != at {
		t.Fatalf("proposal identity was not preserved: %+v", items)
	}
	claimAt := at.Add(time.Minute)
	applyAll("work.claim", storesemantic.ClaimReadyWorkInput{Now: claimAt, Limit: 1, LeaseDuration: time.Minute, ClaimedBy: "worker"})
	items = assertEqual("claim")
	if items[0].Status != domainsemantic.SemanticDirtyWorkStatusRunning || items[0].Attempts != 1 {
		t.Fatalf("claim was not replicated: %+v", items[0])
	}
	failedAt := claimAt.Add(time.Second)
	applyAll("work.fail", struct {
		ID      uuid.UUID                 `json:"id"`
		Failure storesemantic.WorkFailure `json:"failure"`
	}{item.ID, storesemantic.WorkFailure{FailedAt: failedAt, Retryable: true, Generation: items[0].Generation, Message: "retry"}})
	assertEqual("retryable failure")
	secondClaimAt := claimAt.Add(2 * time.Minute)
	applyAll("work.claim", storesemantic.ClaimReadyWorkInput{Now: secondClaimAt, Limit: 1, LeaseDuration: time.Minute, ClaimedBy: "worker"})
	items = assertEqual("second claim")
	if items[0].Attempts != 2 {
		t.Fatalf("replicated attempt count=%d, want 2", items[0].Attempts)
	}
	completedAt := secondClaimAt.Add(time.Second)
	applyAll("work.complete", struct {
		ID     uuid.UUID                `json:"id"`
		Result storesemantic.WorkResult `json:"result"`
	}{item.ID, storesemantic.WorkResult{CompletedAt: completedAt, Generation: items[0].Generation}})
	items = assertEqual("completion")
	if items[0].Status != domainsemantic.SemanticDirtyWorkStatusComplete || items[0].CompletedAt == nil || !items[0].CompletedAt.Equal(completedAt) {
		t.Fatalf("completion was not replicated: %+v", items[0])
	}
}
