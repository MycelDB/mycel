package service

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	config "github.com/myceldb/mycel/internal/runtime/runtimetest"
	daemonruntime "github.com/myceldb/mycel/internal/runtime/runtimetest"
	"github.com/myceldb/mycel/internal/space/access"
	"github.com/myceldb/mycel/internal/wal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBuildCreateSpaceRaftCommand(t *testing.T) {
	m := NewModule()
	ownerID := testPrincipalID(t)
	record, cmd, err := m.buildCreateSpaceRaftCommand(CreateSpaceInput{Name: "raft-space", OwnerPrincipalID: ownerID}, 64, "create-space-1")
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
	_, cmd, err := m.buildCreateSpaceRaftCommand(CreateSpaceInput{Name: "raft-space", OwnerPrincipalID: testPrincipalID(t)}, 64, "create-space-1")
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
	_, createCmd, err := m.buildCreateSpaceRaftCommand(CreateSpaceInput{Name: "raft-space", OwnerPrincipalID: testPrincipalID(t)}, 64, "create-space-1")
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

	principalID := testPrincipalID(t)
	grantRecord := grantSpacePrincipalRecord{Rule: access.SpaceAccessRule{ID: uuid.New(), SpaceID: spaceID, PrincipalID: principalID, Permissions: []access.SpacePermission{access.SpacePermissionRead}}}
	grantCmd := mustSpaceRaftCommand(t, spaceID, recordTypeGrantSpacePrincipal, grantRecord, "grant-1")
	if err := sm.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 3, RaftTerm: 1}, grantCmd); err != nil {
		t.Fatalf("apply grant command: %v", err)
	}
	if access, err := m.DomainEffectiveAccess(ctx, string(principalID), spaceID.String()); err != nil || len(access.Capabilities) == 0 {
		t.Fatalf("DomainEffectiveAccess() = %+v, %v; want capabilities", access, err)
	}

	deleteCmd := mustSpaceRaftCommand(t, spaceID, recordTypeDeleteSpace, deleteSpaceRecord{SpaceID: spaceID}, "space-delete-1")
	if err := sm.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 4, RaftTerm: 1}, deleteCmd); err != nil {
		t.Fatalf("apply delete space command: %v", err)
	}
	if _, err := m.GetSpace(ctx, spaceID.String()); !errors.Is(err, ErrSpaceNotFound) {
		t.Fatalf("GetSpace() after delete error = %v, want ErrSpaceNotFound", err)
	}
}

func TestBuildPhase8SpaceMetadataRaftCommands(t *testing.T) {
	m := NewModule()
	spaceID := uuid.New()
	principalID := testPrincipalID(t)
	domainRecord := m.buildCreateDomainRecord(spaceID, CreateDomainInput{Key: "docs", Name: "Docs"})
	commands := []struct {
		name       string
		recordType string
		build      func() (consensus.RaftCommand, error)
	}{
		{"grant", string(recordTypeGrantSpacePrincipal), func() (consensus.RaftCommand, error) {
			return m.buildGrantSpacePrincipalRaftCommand(grantSpacePrincipalRecord{Rule: access.SpaceAccessRule{ID: uuid.New(), SpaceID: spaceID, PrincipalID: principalID, Permissions: []access.SpacePermission{access.SpacePermissionRead}}}, 64, "grant-1")
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
		{"delete-space", string(recordTypeDeleteSpace), func() (consensus.RaftCommand, error) {
			return m.buildDeleteSpaceRaftCommand(deleteSpaceRecord{SpaceID: spaceID}, 64, "space-delete-1")
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

func TestSpaceMetadataRaftProposalFailsClosedWithoutLeader(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	rt := &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	transport := consensus.RoutedTransport{Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) { return nil, false })}
	groups, err := consensus.StartMultiGroup(ctx, consensus.MultiGroupOptions{NodeID: 1, PeerNodeIDs: []consensus.NodeID{1}, PartitionCount: 64, Transport: transport, StateMachines: consensus.StateMachineFactoryFunc{System: func() consensus.StateMachine { return consensus.NewSystemStateMachine() }, Partition: func(uint32) consensus.StateMachine { return RaftStateMachine{Module: m, PartitionCount: 64} }}, ElectionTick: 50, HeartbeatTick: 1})
	if err != nil {
		t.Fatalf("StartMultiGroup() error = %v", err)
	}
	defer groups.Stop()
	m.EnableExperimentalRaft(groups, 1, nil, "")
	spaceID := uuid.New()
	cmd, err := m.buildDeleteSpaceRaftCommand(deleteSpaceRecord{SpaceID: spaceID}, 64, "space-delete-no-leader")
	if err != nil {
		t.Fatalf("build delete space command: %v", err)
	}
	if err := m.proposeSpaceMetadataCommand(ctx, cmd); status.Code(err) != codes.Unavailable {
		t.Fatalf("proposeSpaceMetadataCommand() error = %v, want Unavailable", err)
	}
}

func TestBuildCreateSpaceRaftCommandRequiresCommandID(t *testing.T) {
	m := NewModule()
	if _, _, err := m.buildCreateSpaceRaftCommand(CreateSpaceInput{Name: "raft-space", OwnerPrincipalID: testPrincipalID(t)}, 64, ""); err == nil {
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
