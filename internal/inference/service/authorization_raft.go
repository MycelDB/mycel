package service

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	model "github.com/myceldb/mycel/internal/inference/model"
	"github.com/myceldb/mycel/internal/wal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const recordTypeInferenceGlobalAuthorizationAssert wal.RecordType = "inference.global.authorization-assert.v1"

type globalAuthorizationAssertion struct {
	ExpectedRevision uint64 `json:"expected_revision"`
}

type inferenceSystemSnapshot struct {
	Version              uint32              `json:"version"`
	BudgetScopeHighWater uint64              `json:"budget_scope_high_water"`
	BudgetScopes         []model.BudgetScope `json:"budget_scopes"`
	UsageEvents          []model.UsageEvent  `json:"usage_events"`
}

// AuthorizationRaftStateMachine validates global Intelligence Access state and
// applies replicated inference budget/usage records in the system Raft order.
type AuthorizationRaftStateMachine struct {
	Module *Module
}

func (AuthorizationRaftStateMachine) RaftStateMachineName() string { return "inference.authorization" }

func (AuthorizationRaftStateMachine) SupportsRaftCommandRecord(scope consensus.CommandScope, recordType wal.RecordType) bool {
	return scope == consensus.CommandScopeSystem && (recordType == recordTypeInferenceGlobalAuthorizationAssert || recordType == recordTypeInferenceBudget || recordType == recordTypeInferenceUsage)
}

func (s AuthorizationRaftStateMachine) ApplyCommand(ctx context.Context, _ consensus.ApplyContext, cmd consensus.RaftCommand) error {
	if s.Module == nil {
		return consensus.NewFatalApplyError(fmt.Errorf("inference authorization state machine is not configured"))
	}
	switch cmd.RecordType {
	case recordTypeInferenceBudget, recordTypeInferenceUsage:
		s.Module.budgetApplyMu.Lock()
		defer s.Module.budgetApplyMu.Unlock()
		record := wal.Record{Type: cmd.RecordType, SchemaVersion: cmd.SchemaVersion, Encoding: cmd.Encoding, Payload: cmd.Payload}
		var err error
		if cmd.RecordType == recordTypeInferenceBudget {
			err = s.Module.applyInferenceBudget(ctx, record)
		} else {
			err = s.Module.applyInferenceUsage(ctx, record)
		}
		if err != nil && status.Code(err) != codes.FailedPrecondition {
			return consensus.NewFatalApplyError(err)
		}
		return err
	case recordTypeInferenceGlobalAuthorizationAssert:
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
	default:
		return fmt.Errorf("unsupported inference system record type %q", cmd.RecordType)
	}
}

func (s AuthorizationRaftStateMachine) Snapshot() ([]byte, error) {
	if s.Module == nil {
		return nil, fmt.Errorf("inference system state machine is not configured")
	}
	s.Module.budgetApplyMu.Lock()
	defer s.Module.budgetApplyMu.Unlock()
	s.Module.mu.Lock()
	ledger := s.Module.usageBase
	s.Module.mu.Unlock()
	if ledger == nil {
		return nil, fmt.Errorf("inference budget ledger is not configured")
	}
	ctx := context.Background()
	scopes, err := ledger.ListBudgetScopes(ctx)
	if err != nil {
		return nil, err
	}
	highWater, err := ledger.BudgetScopeHighWater(ctx)
	if err != nil {
		return nil, err
	}
	usageEvents, err := ledger.ListUsageEvents(ctx)
	if err != nil {
		return nil, err
	}
	return json.Marshal(inferenceSystemSnapshot{Version: 1, BudgetScopeHighWater: highWater, BudgetScopes: scopes, UsageEvents: usageEvents})
}

func (s AuthorizationRaftStateMachine) RestoreSnapshot(data []byte) error {
	if s.Module == nil {
		return fmt.Errorf("inference system state machine is not configured")
	}
	if len(data) == 0 {
		return nil
	}
	s.Module.budgetApplyMu.Lock()
	defer s.Module.budgetApplyMu.Unlock()
	var snapshot inferenceSystemSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return fmt.Errorf("decode inference system snapshot: %w", err)
	}
	if snapshot.Version != 1 {
		return fmt.Errorf("unsupported inference system snapshot version %d", snapshot.Version)
	}
	s.Module.mu.Lock()
	ledger := s.Module.usageBase
	s.Module.mu.Unlock()
	if ledger == nil {
		return fmt.Errorf("inference budget ledger is not configured")
	}
	return ledger.ReplaceInferenceSnapshot(context.Background(), snapshot.BudgetScopes, snapshot.BudgetScopeHighWater, snapshot.UsageEvents)
}

func (m *Module) EnableExperimentalAuthorizationRaft(groups *consensus.MultiGroup) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.raftGroups = groups
}

func (m *Module) EnsureLinearizableBudgetRead(ctx context.Context) error {
	m.mu.Lock()
	groups := m.raftGroups
	m.mu.Unlock()
	if groups == nil {
		return nil
	}
	group, ok := groups.Group(consensus.SystemGroupID)
	if !ok || group == nil || group.Leader() == 0 {
		return status.Error(codes.Unavailable, "inference budget system group is unavailable")
	}
	if _, err := group.LinearizableRead(ctx); err != nil {
		return status.Error(codes.Unavailable, "inference budget linearizable read is unavailable")
	}
	return nil
}

func (m *Module) proposeInferenceSystemMutation(ctx context.Context, recordType wal.RecordType, rec inferenceMutationRecord) (bool, error) {
	m.mu.Lock()
	groups := m.raftGroups
	m.mu.Unlock()
	if groups == nil {
		return false, nil
	}
	payload, err := json.Marshal(rec)
	if err != nil {
		return true, err
	}
	group, ok := groups.Group(consensus.SystemGroupID)
	if !ok || group == nil || group.Leader() == 0 {
		return true, status.Error(codes.Unavailable, "inference budget system group is unavailable")
	}
	commandPrefix := "inference-usage-"
	if recordType == recordTypeInferenceBudget {
		commandPrefix = "inference-budget-"
	}
	cmd := consensus.NewCommand(consensus.CommandScopeSystem, recordType, payload, commandPrefix+uuid.NewString())
	if _, err := group.Propose(ctx, cmd); err != nil {
		return true, err
	}
	return true, nil
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
