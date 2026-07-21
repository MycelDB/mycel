package service

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	graph "github.com/myceldb/mycel/internal/graph/model"
	config "github.com/myceldb/mycel/internal/runtime/runtimetest"
	daemonruntime "github.com/myceldb/mycel/internal/runtime/runtimetest"
	"github.com/myceldb/mycel/internal/space/access"
	"github.com/myceldb/mycel/internal/wal"
)

func TestBuildCreateSpaceRaftCommand(t *testing.T) {
	m := NewModule()
	ownerID := uuid.New()
	record, cmd, err := m.buildCreateSpaceRaftCommand(CreateSpaceInput{Name: "raft-space", OwnerUserID: ownerID}, 64, "create-space-1")
	if err != nil {
		t.Fatalf("buildCreateSpaceRaftCommand() error = %v", err)
	}
	if record.Space.SpaceID == uuid.Nil || record.Space.OwnerID != ownerID || record.Space.Name != "raft-space" {
		t.Fatalf("unexpected record space: %+v", record.Space)
	}
	if cmd.Scope != consensus.CommandScopeSpacePartition || cmd.RecordType != recordTypeCreateSpaceWithDefaultDomain || cmd.SpaceID != record.Space.SpaceID.String() || cmd.CommandID != "create-space-1" {
		t.Fatalf("unexpected command: %+v", cmd)
	}
	if err := cmd.Validate(64); err != nil {
		t.Fatalf("command Validate() error = %v", err)
	}
}

func TestApplyCreateSpaceRaftCommand(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	m := NewModule()
	rt := &daemonruntime.Runtime{Config: config.Config{DataDir: dataDir}, LoggerValue: slog.Default()}
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	_, cmd, err := m.buildCreateSpaceRaftCommand(CreateSpaceInput{Name: "raft-space", OwnerUserID: uuid.New()}, 64, "create-space-1")
	if err != nil {
		t.Fatalf("buildCreateSpaceRaftCommand() error = %v", err)
	}
	applied, err := m.applyCreateSpaceRaftCommand(ctx, consensus.ApplyContext{RaftIndex: 5, RaftTerm: 2}, cmd, 64)
	if err != nil {
		t.Fatalf("applyCreateSpaceRaftCommand() error = %v", err)
	}
	if applied.Space.Name != "raft-space" || applied.Domain.SpaceID != applied.Space.SpaceID {
		t.Fatalf("unexpected applied result: %+v", applied)
	}
	loaded, err := m.GetSpace(ctx, applied.Space.SpaceID.String())
	if err != nil {
		t.Fatalf("GetSpace() error = %v", err)
	}
	if loaded.SpaceID != applied.Space.SpaceID {
		t.Fatalf("loaded space id=%s want %s", loaded.SpaceID, applied.Space.SpaceID)
	}
}

func TestRaftStateMachineAppliesSpaceMetadataCommands(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	rt := &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	_, createCmd, err := m.buildCreateSpaceRaftCommand(CreateSpaceInput{Name: "raft-space", OwnerUserID: uuid.New()}, 64, "create-space-1")
	if err != nil {
		t.Fatalf("buildCreateSpaceRaftCommand() error = %v", err)
	}
	sm := RaftStateMachine{Module: m, PartitionCount: 64}
	if err := sm.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, createCmd); err != nil {
		t.Fatalf("apply create space command: %v", err)
	}
	spaceID := uuid.MustParse(createCmd.SpaceID)

	domainRecord := m.buildCreateDomainRecord(spaceID, CreateDomainInput{Key: "docs", Name: "Docs"})
	domainCmd := mustSpaceRaftCommand(t, spaceID, recordTypeCreateDomain, domainRecord, "create-domain-1")
	if err := sm.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 2, RaftTerm: 1}, domainCmd); err != nil {
		t.Fatalf("apply create domain command: %v", err)
	}
	if _, err := m.GetDomainByRef(ctx, spaceID.String(), "docs"); err != nil {
		t.Fatalf("GetDomainByRef() after raft apply error = %v", err)
	}

	userID := uuid.New()
	grantRecord := grantSpaceUserRecord{Rule: access.SpaceAccessRule{ID: uuid.New(), SpaceID: spaceID, UserID: userID, Permissions: []access.SpacePermission{access.SpacePermissionRead}}}
	grantCmd := mustSpaceRaftCommand(t, spaceID, recordTypeGrantSpaceUser, grantRecord, "grant-1")
	if err := sm.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 3, RaftTerm: 1}, grantCmd); err != nil {
		t.Fatalf("apply grant command: %v", err)
	}
	if access, err := m.DomainEffectiveAccess(ctx, userID.String(), spaceID.String()); err != nil || len(access.Capabilities) == 0 {
		t.Fatalf("DomainEffectiveAccess() = %+v, %v; want capabilities", access, err)
	}

	template := graph.Template{ID: uuid.New(), SpaceID: spaceID, Key: "note", Version: "1.0.0", DisplayName: "Note", State: graph.TemplateStateActive}
	putTemplateCmd := mustSpaceRaftCommand(t, spaceID, recordTypePutTemplate, putTemplateRecord{Template: template}, "put-template-1")
	if err := sm.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 4, RaftTerm: 1}, putTemplateCmd); err != nil {
		t.Fatalf("apply put template command: %v", err)
	}
	if _, err := m.GetTemplate(ctx, spaceID.String(), template.ID.String()); err != nil {
		t.Fatalf("GetTemplate() after raft apply error = %v", err)
	}
	deleteTemplateCmd := mustSpaceRaftCommand(t, spaceID, recordTypeDeleteTemplate, deleteTemplateRecord{TemplateID: template.ID, SpaceID: spaceID}, "delete-template-1")
	if err := sm.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 5, RaftTerm: 1}, deleteTemplateCmd); err != nil {
		t.Fatalf("apply delete template command: %v", err)
	}
	if _, err := m.GetTemplate(ctx, spaceID.String(), template.ID.String()); err == nil {
		t.Fatal("expected template to be deleted")
	}
}

func TestBuildPhase8SpaceMetadataRaftCommands(t *testing.T) {
	m := NewModule()
	spaceID := uuid.New()
	userID := uuid.New()
	domainRecord := m.buildCreateDomainRecord(spaceID, CreateDomainInput{Key: "docs", Name: "Docs"})
	commands := []struct {
		name       string
		recordType string
		build      func() (consensus.RaftCommand, error)
	}{
		{"grant", string(recordTypeGrantSpaceUser), func() (consensus.RaftCommand, error) {
			return m.buildGrantSpaceUserRaftCommand(grantSpaceUserRecord{Rule: access.SpaceAccessRule{ID: uuid.New(), SpaceID: spaceID, UserID: userID, Permissions: []access.SpacePermission{access.SpacePermissionRead}}}, 64, "grant-1")
		}},
		{"create-domain", string(recordTypeCreateDomain), func() (consensus.RaftCommand, error) {
			return m.buildCreateDomainRaftCommand(domainRecord, 64, "domain-create-1")
		}},
		{"update-domain", string(recordTypeUpdateDomain), func() (consensus.RaftCommand, error) {
			return m.buildUpdateDomainRaftCommand(updateDomainRecord{Domain: domainRecord.Domain}, 64, "domain-update-1")
		}},
		{"delete-domain", string(recordTypeDeleteDomain), func() (consensus.RaftCommand, error) {
			return m.buildDeleteDomainRaftCommand(deleteDomainRecord{DomainID: domainRecord.Domain.ID, SpaceID: spaceID}, 64, "domain-delete-1")
		}},
		{"put-template", string(recordTypePutTemplate), func() (consensus.RaftCommand, error) {
			return m.buildPutTemplateRaftCommand(putTemplateRecord{Template: graph.Template{ID: uuid.New(), SpaceID: spaceID, Key: "note", Version: "1.0.0", State: graph.TemplateStateActive}}, 64, "template-put-1")
		}},
		{"delete-template", string(recordTypeDeleteTemplate), func() (consensus.RaftCommand, error) {
			return m.buildDeleteTemplateRaftCommand(deleteTemplateRecord{TemplateID: uuid.New(), SpaceID: spaceID}, 64, "template-delete-1")
		}},
	}
	for _, tc := range commands {
		t.Run(tc.name, func(t *testing.T) {
			cmd, err := tc.build()
			if err != nil {
				t.Fatalf("build command: %v", err)
			}
			if cmd.SpaceID != spaceID.String() || string(cmd.RecordType) != tc.recordType || cmd.CommandID == "" {
				t.Fatalf("unexpected command: %+v", cmd)
			}
			if err := cmd.Validate(64); err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
		})
	}
}

func TestBuildCreateSpaceRaftCommandRequiresCommandID(t *testing.T) {
	m := NewModule()
	if _, _, err := m.buildCreateSpaceRaftCommand(CreateSpaceInput{Name: "raft-space", OwnerUserID: uuid.New()}, 64, ""); err == nil {
		t.Fatal("expected missing command id to fail")
	}
}

func mustSpaceRaftCommand(t *testing.T, spaceID uuid.UUID, recordType wal.RecordType, payload any, commandID string) consensus.RaftCommand {
	t.Helper()
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	cmd, err := consensus.NewSpaceCommand(spaceID, 64, recordType, data, commandID)
	if err != nil {
		t.Fatalf("NewSpaceCommand(): %v", err)
	}
	return cmd
}
