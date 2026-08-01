package service

import (
	"context"
	"log/slog"
	"testing"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	config "github.com/myceldb/mycel/internal/runtime/runtimetest"
	daemonruntime "github.com/myceldb/mycel/internal/runtime/runtimetest"
	"github.com/myceldb/mycel/internal/space/access"
)

func TestSpaceRaftStateMachineSnapshotRestorePartition(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	ownerID := uuid.New()
	_, createCmd, err := m.buildCreateSpaceRaftCommand(CreateSpaceInput{Name: "snapshot-space", OwnerUserID: ownerID, DefaultDomainKey: "notes", DefaultDomainName: "Notes"}, 4, "create-space-snapshot")
	if err != nil {
		t.Fatalf("buildCreateSpaceRaftCommand() error = %v", err)
	}
	sm := RaftStateMachine{Module: m, PartitionID: createCmd.PartitionID, PartitionCount: 4}
	if err := sm.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, createCmd); err != nil {
		t.Fatalf("ApplyCommand(create) error = %v", err)
	}
	spaceID := uuid.MustParse(createCmd.SpaceID)
	userID := uuid.New()
	grant := grantSpaceUserRecord{Rule: access.SpaceAccessRule{ID: uuid.New(), SpaceID: spaceID, UserID: userID, Permissions: []access.SpacePermission{access.SpacePermissionRead}}}
	grantCmd, err := m.buildGrantSpaceUserRaftCommand(grant, 4, "grant-snapshot")
	if err != nil {
		t.Fatalf("buildGrantSpaceUserRaftCommand() error = %v", err)
	}
	if err := sm.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 2, RaftTerm: 1}, grantCmd); err != nil {
		t.Fatalf("ApplyCommand(grant) error = %v", err)
	}
	snapshot, err := sm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}

	restored := NewModule()
	if result := restored.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("restored init failed: %v", result.Error)
	}
	restoredSM := RaftStateMachine{Module: restored, PartitionID: createCmd.PartitionID, PartitionCount: 4}
	if err := restoredSM.RestoreSnapshot(snapshot); err != nil {
		t.Fatalf("RestoreSnapshot() error = %v", err)
	}
	sp, err := restored.GetSpace(ctx, spaceID.String())
	if err != nil {
		t.Fatalf("GetSpace() error = %v", err)
	}
	if sp.Name != "snapshot-space" {
		t.Fatalf("restored space name=%q", sp.Name)
	}
	if _, err := restored.GetDomainByRef(ctx, spaceID.String(), "notes"); err != nil {
		t.Fatalf("GetDomainByRef() error = %v", err)
	}
	access, err := restored.DomainEffectiveAccess(ctx, userID.String(), spaceID.String())
	if err != nil || len(access.Capabilities) == 0 {
		t.Fatalf("DomainEffectiveAccess() = %+v, %v; want capabilities", access, err)
	}
	if err := restoredSM.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 3, RaftTerm: 1}, createCmd); err != nil {
		t.Fatalf("reapplying restored create command should be idempotent: %v", err)
	}
}
