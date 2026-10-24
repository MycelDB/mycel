package service

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/wal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const recordTypeInferenceGlobalAuthorizationAssert wal.RecordType = "inference.global.authorization-assert.v1"

type globalAuthorizationAssertion struct {
	ExpectedRevision uint64 `json:"expected_revision"`
}

// AuthorizationRaftStateMachine validates global Intelligence Access state in
// the system Raft order. It carries no independent state and therefore needs no
// snapshot payload.
type AuthorizationRaftStateMachine struct {
	Module *Module
}

func (AuthorizationRaftStateMachine) RaftStateMachineName() string { return "inference.authorization" }

func (AuthorizationRaftStateMachine) SupportsRaftCommandRecord(scope consensus.CommandScope, recordType wal.RecordType) bool {
	return scope == consensus.CommandScopeSystem && recordType == recordTypeInferenceGlobalAuthorizationAssert
}

func (s AuthorizationRaftStateMachine) ApplyCommand(ctx context.Context, _ consensus.ApplyContext, cmd consensus.RaftCommand) error {
	if s.Module == nil {
		return fmt.Errorf("inference authorization state machine is not configured")
	}
	var assertion globalAuthorizationAssertion
	if err := json.Unmarshal(cmd.Payload, &assertion); err != nil {
		return fmt.Errorf("decode global authorization assertion: %w", err)
	}
	if assertion.ExpectedRevision == 0 {
		return fmt.Errorf("global authorization assertion revision is required")
	}
	source := s.Module.authorizationRevisionSource()
	if source == nil {
		return fmt.Errorf("authoritative global authorization revision source is not configured")
	}
	current, err := source.GlobalAuthorizationRevision(ctx)
	if err != nil {
		return err
	}
	if current != assertion.ExpectedRevision {
		return status.Error(codes.FailedPrecondition, "global inference authorization revision changed")
	}
	return nil
}

func (m *Module) EnableExperimentalAuthorizationRaft(groups *consensus.MultiGroup) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.raftGroups = groups
}

func (m *Module) currentGlobalAuthorizationRevision(ctx context.Context) (uint64, error) {
	if source := m.authorizationRevisionSource(); source != nil {
		return source.GlobalAuthorizationRevision(ctx)
	}
	return globalAuthorizationStateRevision(ctx, m.GlobalManager())
}

func (m *Module) AssertGlobalAuthorizationRevision(ctx context.Context, expected uint64) error {
	if expected == 0 {
		return status.Error(codes.FailedPrecondition, "global inference authorization revision is required")
	}
	m.mu.Lock()
	groups := m.raftGroups
	m.mu.Unlock()
	if groups == nil {
		current, err := m.currentGlobalAuthorizationRevision(ctx)
		if err != nil {
			return err
		}
		if current != expected {
			return status.Error(codes.FailedPrecondition, "global inference authorization revision changed")
		}
		return nil
	}
	payload, err := json.Marshal(globalAuthorizationAssertion{ExpectedRevision: expected})
	if err != nil {
		return err
	}
	group, ok := groups.Group(consensus.SystemGroupID)
	if !ok || group == nil || group.Leader() == 0 {
		return status.Error(codes.Unavailable, "inference authorization system group is unavailable")
	}
	cmd := consensus.NewCommand(consensus.CommandScopeSystem, recordTypeInferenceGlobalAuthorizationAssert, payload, "inference-auth-assert-"+uuid.NewString())
	if _, err := group.Propose(ctx, cmd); err != nil {
		return err
	}
	return nil
}
