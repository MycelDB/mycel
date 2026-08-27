package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/storage"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	graph "github.com/myceldb/mycel/internal/graph/model"
	graphservice "github.com/myceldb/mycel/internal/graph/service"
	inferenceservice "github.com/myceldb/mycel/internal/inference/service"
	schemaservice "github.com/myceldb/mycel/internal/schema/service"
	sessionservice "github.com/myceldb/mycel/internal/session/service"
)

var ErrAutomationNotFound = errors.New("automation not found")

type Manager interface {
	ValidateAutomation(ctx context.Context, domainID graph.DomainID, rawJSON string) (automation.Definition, error)
	CreateAutomation(ctx context.Context, domainID graph.DomainID, rawJSON string) (automation.Definition, error)
	CreateAutomationAs(ctx context.Context, domainID graph.DomainID, rawJSON string, principalID string) (automation.Definition, error)
	UpdateAutomation(ctx context.Context, domainID graph.DomainID, id string, rawJSON string) (automation.Definition, error)
	UpdateAutomationAs(ctx context.Context, domainID graph.DomainID, id string, rawJSON string, principalID string) (automation.Definition, error)
	DeleteAutomation(ctx context.Context, domainID graph.DomainID, id string) error
	GetAutomation(ctx context.Context, domainID graph.DomainID, id string) (automation.Definition, error)
	ListAutomationDomains(ctx context.Context) ([]graph.DomainID, error)
	ListAutomations(ctx context.Context, domainID graph.DomainID, status string) ([]automation.Definition, error)
	SetAutomationStatus(ctx context.Context, domainID graph.DomainID, id string, status string) (automation.Definition, error)
	SetAutomationStatusAs(ctx context.Context, domainID graph.DomainID, id string, status string, principalID string) (automation.Definition, error)
	ValidateProcedure(ctx context.Context, domainID graph.DomainID, rawJSON string) (automation.Procedure, error)
	CreateProcedureAs(ctx context.Context, domainID graph.DomainID, rawJSON string, principalID string) (automation.Procedure, error)
	UpdateProcedureAs(ctx context.Context, domainID graph.DomainID, id string, rawJSON string, principalID string) (automation.Procedure, error)
	DeleteProcedure(ctx context.Context, domainID graph.DomainID, id string) error
	GetProcedure(ctx context.Context, domainID graph.DomainID, id string) (automation.Procedure, error)
	ListProcedures(ctx context.Context, domainID graph.DomainID, status string) ([]automation.Procedure, error)
	ValidateBinding(ctx context.Context, domainID graph.DomainID, rawJSON string) (automation.Binding, error)
	CreateBindingAs(ctx context.Context, domainID graph.DomainID, rawJSON string, principalID string) (automation.Binding, error)
	UpdateBindingAs(ctx context.Context, domainID graph.DomainID, id string, rawJSON string, principalID string) (automation.Binding, error)
	DeleteBinding(ctx context.Context, domainID graph.DomainID, id string) error
	GetBinding(ctx context.Context, domainID graph.DomainID, id string) (automation.Binding, error)
	ListBindings(ctx context.Context, domainID graph.DomainID, status string) ([]automation.Binding, error)
	SetBindingStatusAs(ctx context.Context, domainID graph.DomainID, id string, status string, principalID string) (automation.Binding, error)
	ListInvocations(ctx context.Context, domainID graph.DomainID, filter storage.InvocationFilter) ([]automation.Invocation, error)
	GetRun(ctx context.Context, domainID graph.DomainID, runID string) (automation.Run, error)
	RetryInvocation(ctx context.Context, domainID graph.DomainID, invocationID string) (automation.Invocation, error)
	CancelInvocation(ctx context.Context, domainID graph.DomainID, invocationID string) (automation.Invocation, error)
	HandleGraphChange(ctx context.Context, event graphchange.CommittedEvent) error
	HandleGraphChangeGap(ctx context.Context, gap graphchange.Gap) error
	ProcessPending(ctx context.Context, domainID graph.DomainID, limit int) (int, error)
}

type AutomationManager struct {
	store                                 storage.Store
	now                                   func() time.Time
	sessions                              sessionservice.Manager
	graphs                                graphservice.Manager
	schemas                               schemaservice.Manager
	inference                             inferenceservice.Manager
	raftGroups                            *consensus.MultiGroup
	raftPartitionCount                    uint32
	raftLocalNode                         consensus.NodeID
	raftNodeAddrs                         []string
	raftBackendAuthToken                  string
	maxInputTokens                        int64
	maxOutputTokens                       int64
	metricsProcessed                      int64
	metricsSucceeded                      int64
	metricsSkipped                        int64
	metricsFailed                         int64
	metricsRetryable                      int64
	metricsGraphReplayScopes              int64
	metricsGraphReplayFollowerSkips       int64
	metricsGraphReplayEvents              int64
	metricsGraphReplaySkippedEvents       int64
	metricsGraphReplayInvocationsCreated  int64
	metricsGraphReplayInvocationsExisting int64
	metricsGraphReplayCursorAdvances      int64
	metricsGraphReplayGaps                int64
	metricsClaimReclaims                  int64
	metricsClaimAbandoned                 int64
	writeAllowed                          func() error
}

func NewManager(store storage.Store) *AutomationManager {
	return &AutomationManager{store: store, now: func() time.Time { return time.Now().UTC() }}
}

func (m *AutomationManager) WithGraphRuntime(sessions sessionservice.Manager, graphs graphservice.Manager) *AutomationManager {
	m.sessions = sessions
	m.graphs = graphs
	return m
}

func (m *AutomationManager) WithInferenceManager(inference inferenceservice.Manager) *AutomationManager {
	m.inference = inference
	return m
}

func (m *AutomationManager) WithSchemaManager(schemas schemaservice.Manager) *AutomationManager {
	m.schemas = schemas
	return m
}

func (m *AutomationManager) WithRunCeilings(maxInputTokens int64, maxOutputTokens int64) *AutomationManager {
	m.maxInputTokens = maxInputTokens
	m.maxOutputTokens = maxOutputTokens
	return m
}

func (m *AutomationManager) WithWriteAllowed(fn func() error) *AutomationManager {
	m.writeAllowed = fn
	return m
}

func (m *AutomationManager) requireWriteAllowed() error {
	if m.writeAllowed == nil {
		return nil
	}
	return m.writeAllowed()
}

func (m *AutomationManager) raftEnabled() bool {
	return m.raftGroups != nil
}

func (m *AutomationManager) ValidateAutomation(ctx context.Context, domainID graph.DomainID, rawJSON string) (automation.Definition, error) {
	def, err := decodeDefinition(rawJSON)
	if err != nil {
		return def, err
	}
	def.DomainID = domainID
	def = def.Normalize()
	if err := automation.ValidateDefinition(def); err != nil {
		return def, err
	}
	if err := m.enforcePolicy(ctx, def); err != nil {
		return def, err
	}
	return def, ctx.Err()
}

func (m *AutomationManager) CreateAutomation(ctx context.Context, domainID graph.DomainID, rawJSON string) (automation.Definition, error) {
	return m.CreateAutomationAs(ctx, domainID, rawJSON, "")
}

func (m *AutomationManager) CreateAutomationAs(ctx context.Context, domainID graph.DomainID, rawJSON string, principalID string) (automation.Definition, error) {
	if err := m.requireWriteAllowed(); err != nil {
		return automation.Definition{}, err
	}
	def, err := m.ValidateAutomation(ctx, domainID, rawJSON)
	if err != nil {
		return def, err
	}
	if _, err := m.store.GetDefinition(ctx, domainID, def.ID); err == nil {
		return def, fmt.Errorf("automation %q already exists", def.ID)
	} else if !errors.Is(err, storage.ErrNotFound) {
		return def, mapStoreError(err)
	}
	now := m.now()
	principalID = strings.TrimSpace(principalID)
	def.OwnerPrincipalID = principalID
	def.CreatedByPrincipalID = principalID
	def.UpdatedByPrincipalID = principalID
	def.CreatedAt = now
	def.UpdatedAt = now
	if err := m.store.PutDefinition(ctx, def); err != nil {
		return def, mapStoreError(err)
	}
	return def, nil
}

func (m *AutomationManager) UpdateAutomation(ctx context.Context, domainID graph.DomainID, id string, rawJSON string) (automation.Definition, error) {
	return m.UpdateAutomationAs(ctx, domainID, id, rawJSON, "")
}

func (m *AutomationManager) UpdateAutomationAs(ctx context.Context, domainID graph.DomainID, id string, rawJSON string, principalID string) (automation.Definition, error) {
	if err := m.requireWriteAllowed(); err != nil {
		return automation.Definition{}, err
	}
	current, err := m.store.GetDefinition(ctx, domainID, id)
	if err != nil {
		return automation.Definition{}, mapStoreError(err)
	}
	def, err := decodeDefinition(rawJSON)
	if err != nil {
		return def, err
	}
	def.DomainID = domainID
	def.ID = strings.TrimSpace(id)
	def = def.Normalize()
	if err := automation.ValidateDefinition(def); err != nil {
		return def, err
	}
	principalID = strings.TrimSpace(principalID)
	def.OwnerPrincipalID = current.OwnerPrincipalID
	if def.OwnerPrincipalID == "" {
		def.OwnerPrincipalID = principalID
	}
	def.CreatedByPrincipalID = current.CreatedByPrincipalID
	if def.CreatedByPrincipalID == "" {
		def.CreatedByPrincipalID = principalID
	}
	def.UpdatedByPrincipalID = principalID
	def.CreatedAt = current.CreatedAt
	def.UpdatedAt = m.now()
	if err := m.store.PutDefinition(ctx, def); err != nil {
		return def, mapStoreError(err)
	}
	return def, nil
}

func (m *AutomationManager) DeleteAutomation(ctx context.Context, domainID graph.DomainID, id string) error {
	if err := m.requireWriteAllowed(); err != nil {
		return err
	}
	return mapStoreError(m.store.DeleteDefinition(ctx, domainID, strings.TrimSpace(id)))
}

func (m *AutomationManager) ValidateProcedure(ctx context.Context, domainID graph.DomainID, rawJSON string) (automation.Procedure, error) {
	procedure, err := decodeProcedure(rawJSON)
	if err != nil {
		return procedure, err
	}
	procedure.DomainID = domainID
	procedure = procedure.Normalize()
	if err := automation.ValidateProcedure(procedure); err != nil {
		return procedure, err
	}
	def := automation.ComposeDefinition(procedure, automation.Binding{ID: procedure.ID, Version: procedure.Version, DomainID: domainID, ProcedureID: procedure.ID, ProcedureVersion: procedure.Version, Status: automation.StatusDisabled, Scope: automation.BindingScope{DomainID: domainID}})
	if err := m.enforcePolicy(ctx, def); err != nil {
		return procedure, err
	}
	return procedure, ctx.Err()
}

func (m *AutomationManager) CreateProcedureAs(ctx context.Context, domainID graph.DomainID, rawJSON string, principalID string) (automation.Procedure, error) {
	procedure, err := m.ValidateProcedure(ctx, domainID, rawJSON)
	if err != nil {
		return procedure, err
	}
	if _, err := m.store.GetProcedure(ctx, domainID, procedure.ID); err == nil {
		return procedure, fmt.Errorf("graph procedure %q already exists", procedure.ID)
	} else if !errors.Is(err, storage.ErrNotFound) {
		return procedure, mapStoreError(err)
	}
	now := m.now()
	principalID = strings.TrimSpace(principalID)
	procedure.CreatedByPrincipalID = principalID
	procedure.UpdatedByPrincipalID = principalID
	procedure.CreatedAt = now
	procedure.UpdatedAt = now
	if err := m.commitAutomationMutation(ctx, automationMutationRecord{Kind: "procedure.create", DomainID: domainID, ID: procedure.ID, Payload: rawAutomation(procedure)}); err != nil {
		return procedure, mapStoreError(err)
	}
	return procedure, nil
}

func (m *AutomationManager) UpdateProcedureAs(ctx context.Context, domainID graph.DomainID, id string, rawJSON string, principalID string) (automation.Procedure, error) {
	current, err := m.store.GetProcedure(ctx, domainID, strings.TrimSpace(id))
	if err != nil {
		return automation.Procedure{}, mapStoreError(err)
	}
	procedure, err := m.ValidateProcedure(ctx, domainID, rawJSON)
	if err != nil {
		return procedure, err
	}
	procedure.ID = strings.TrimSpace(id)
	procedure.CreatedByPrincipalID = current.CreatedByPrincipalID
	procedure.CreatedAt = current.CreatedAt
	procedure.UpdatedByPrincipalID = strings.TrimSpace(principalID)
	procedure.UpdatedAt = m.now()
	if err := automation.ValidateProcedure(procedure); err != nil {
		return procedure, err
	}
	if err := m.commitAutomationMutation(ctx, automationMutationRecord{Kind: "procedure.update", DomainID: domainID, ID: procedure.ID, Payload: rawAutomation(procedure)}); err != nil {
		return procedure, mapStoreError(err)
	}
	return procedure, nil
}

func (m *AutomationManager) DeleteProcedure(ctx context.Context, domainID graph.DomainID, id string) error {
	return mapStoreError(m.commitAutomationMutation(ctx, automationMutationRecord{Kind: "procedure.delete", DomainID: domainID, ID: strings.TrimSpace(id)}))
}

func (m *AutomationManager) GetProcedure(ctx context.Context, domainID graph.DomainID, id string) (automation.Procedure, error) {
	procedure, err := m.store.GetProcedure(ctx, domainID, strings.TrimSpace(id))
	return procedure, mapStoreError(err)
}

func (m *AutomationManager) ListProcedures(ctx context.Context, domainID graph.DomainID, status string) ([]automation.Procedure, error) {
	procedures, err := m.store.ListProcedures(ctx, domainID)
	if err != nil {
		return nil, mapStoreError(err)
	}
	status = strings.TrimSpace(strings.ToLower(status))
	if status == "" {
		return procedures, nil
	}
	out := []automation.Procedure{}
	for _, procedure := range procedures {
		if procedure.Normalize().Status == status {
			out = append(out, procedure)
		}
	}
	return out, nil
}

func (m *AutomationManager) ValidateBinding(ctx context.Context, domainID graph.DomainID, rawJSON string) (automation.Binding, error) {
	binding, err := decodeBinding(rawJSON)
	if err != nil {
		return binding, err
	}
	binding.DomainID = domainID
	if binding.Scope.DomainID == (graph.DomainID{}) {
		binding.Scope.DomainID = domainID
	}
	binding = binding.Normalize()
	procedure, err := m.store.GetProcedure(ctx, domainID, binding.ProcedureID)
	if err != nil {
		return binding, mapStoreError(err)
	}
	procedure.DomainID = domainID
	procedure = procedure.Normalize()
	if err := automation.ValidateBinding(binding, &procedure); err != nil {
		return binding, err
	}
	return binding, ctx.Err()
}

func (m *AutomationManager) CreateBindingAs(ctx context.Context, domainID graph.DomainID, rawJSON string, principalID string) (automation.Binding, error) {
	binding, err := m.ValidateBinding(ctx, domainID, rawJSON)
	if err != nil {
		return binding, err
	}
	if _, err := m.store.GetBinding(ctx, domainID, binding.ID); err == nil {
		return binding, fmt.Errorf("graph automation binding %q already exists", binding.ID)
	} else if !errors.Is(err, storage.ErrNotFound) {
		return binding, mapStoreError(err)
	}
	now := m.now()
	principalID = strings.TrimSpace(principalID)
	binding.CreatedByPrincipalID = principalID
	binding.UpdatedByPrincipalID = principalID
	binding.CreatedAt = now
	binding.UpdatedAt = now
	if err := m.commitAutomationMutation(ctx, automationMutationRecord{Kind: "binding.create", DomainID: domainID, ID: binding.ID, Payload: rawAutomation(binding)}); err != nil {
		return binding, mapStoreError(err)
	}
	return binding, nil
}

func (m *AutomationManager) UpdateBindingAs(ctx context.Context, domainID graph.DomainID, id string, rawJSON string, principalID string) (automation.Binding, error) {
	current, err := m.store.GetBinding(ctx, domainID, strings.TrimSpace(id))
	if err != nil {
		return automation.Binding{}, mapStoreError(err)
	}
	binding, err := m.ValidateBinding(ctx, domainID, rawJSON)
	if err != nil {
		return binding, err
	}
	binding.ID = strings.TrimSpace(id)
	binding.CreatedByPrincipalID = current.CreatedByPrincipalID
	binding.CreatedAt = current.CreatedAt
	binding.UpdatedByPrincipalID = strings.TrimSpace(principalID)
	binding.UpdatedAt = m.now()
	procedure, err := m.store.GetProcedure(ctx, domainID, binding.ProcedureID)
	if err != nil {
		return binding, mapStoreError(err)
	}
	if err := automation.ValidateBinding(binding, &procedure); err != nil {
		return binding, err
	}
	if err := m.commitAutomationMutation(ctx, automationMutationRecord{Kind: "binding.update", DomainID: domainID, ID: binding.ID, Payload: rawAutomation(binding)}); err != nil {
		return binding, mapStoreError(err)
	}
	return binding, nil
}

func (m *AutomationManager) DeleteBinding(ctx context.Context, domainID graph.DomainID, id string) error {
	return mapStoreError(m.commitAutomationMutation(ctx, automationMutationRecord{Kind: "binding.delete", DomainID: domainID, ID: strings.TrimSpace(id)}))
}

func (m *AutomationManager) GetBinding(ctx context.Context, domainID graph.DomainID, id string) (automation.Binding, error) {
	binding, err := m.store.GetBinding(ctx, domainID, strings.TrimSpace(id))
	return binding, mapStoreError(err)
}

func (m *AutomationManager) ListBindings(ctx context.Context, domainID graph.DomainID, status string) ([]automation.Binding, error) {
	bindings, err := m.store.ListBindings(ctx, domainID)
	if err != nil {
		return nil, mapStoreError(err)
	}
	status = strings.TrimSpace(strings.ToLower(status))
	if status == "" {
		return bindings, nil
	}
	out := []automation.Binding{}
	for _, binding := range bindings {
		if binding.Normalize().Status == status {
			out = append(out, binding)
		}
	}
	return out, nil
}

func (m *AutomationManager) SetBindingStatusAs(ctx context.Context, domainID graph.DomainID, id string, status string, principalID string) (automation.Binding, error) {
	status = strings.TrimSpace(strings.ToLower(status))
	if status != automation.StatusEnabled && status != automation.StatusDisabled {
		return automation.Binding{}, fmt.Errorf("invalid graph automation binding status %q", status)
	}
	binding, err := m.store.GetBinding(ctx, domainID, strings.TrimSpace(id))
	if err != nil {
		return binding, mapStoreError(err)
	}
	binding.Status = status
	binding.UpdatedByPrincipalID = strings.TrimSpace(principalID)
	binding.UpdatedAt = m.now()
	if status == automation.StatusEnabled {
		procedure, err := m.store.GetProcedure(ctx, domainID, binding.ProcedureID)
		if err != nil {
			return binding, mapStoreError(err)
		}
		if err := automation.ValidateBinding(binding, &procedure); err != nil {
			return binding, err
		}
	}
	if err := m.commitAutomationMutation(ctx, automationMutationRecord{Kind: "binding.update", DomainID: domainID, ID: binding.ID, Payload: rawAutomation(binding)}); err != nil {
		return binding, mapStoreError(err)
	}
	return binding, nil
}

func (m *AutomationManager) GetAutomation(ctx context.Context, domainID graph.DomainID, id string) (automation.Definition, error) {
	def, err := m.store.GetDefinition(ctx, domainID, strings.TrimSpace(id))
	return def, mapStoreError(err)
}

func (m *AutomationManager) ListAutomationDomains(ctx context.Context) ([]graph.DomainID, error) {
	definitionIDs, err := m.store.ListDefinitionDomains(ctx)
	if err != nil {
		return nil, mapStoreError(err)
	}
	bindingIDs, err := m.store.ListBindingDomains(ctx)
	if err != nil {
		return nil, mapStoreError(err)
	}
	seen := map[string]graph.DomainID{}
	for _, id := range definitionIDs {
		seen[id.String()] = id
	}
	for _, id := range bindingIDs {
		seen[id.String()] = id
	}
	out := make([]graph.DomainID, 0, len(seen))
	for _, id := range seen {
		out = append(out, id)
	}
	return out, nil
}

func (m *AutomationManager) ListAutomations(ctx context.Context, domainID graph.DomainID, status string) ([]automation.Definition, error) {
	defs, err := m.store.ListDefinitions(ctx, domainID)
	if err != nil {
		return nil, mapStoreError(err)
	}
	status = strings.TrimSpace(strings.ToLower(status))
	if status == "" {
		return defs, nil
	}
	out := []automation.Definition{}
	for _, def := range defs {
		if def.Status == status {
			out = append(out, def)
		}
	}
	return out, nil
}

func (m *AutomationManager) SetAutomationStatus(ctx context.Context, domainID graph.DomainID, id string, status string) (automation.Definition, error) {
	return m.SetAutomationStatusAs(ctx, domainID, id, status, "")
}

func (m *AutomationManager) SetAutomationStatusAs(ctx context.Context, domainID graph.DomainID, id string, status string, principalID string) (automation.Definition, error) {
	if err := m.requireWriteAllowed(); err != nil {
		return automation.Definition{}, err
	}
	status = strings.TrimSpace(strings.ToLower(status))
	if status != automation.StatusEnabled && status != automation.StatusDisabled {
		return automation.Definition{}, fmt.Errorf("invalid automation status %q", status)
	}
	def, err := m.store.GetDefinition(ctx, domainID, strings.TrimSpace(id))
	if err != nil {
		return def, mapStoreError(err)
	}
	def.Status = status
	def.UpdatedByPrincipalID = strings.TrimSpace(principalID)
	def.UpdatedAt = m.now()
	if err := m.store.PutDefinition(ctx, def); err != nil {
		return def, mapStoreError(err)
	}
	return def, nil
}

func (m *AutomationManager) ListInvocations(ctx context.Context, domainID graph.DomainID, filter storage.InvocationFilter) ([]automation.Invocation, error) {
	if m.forwardedAutomationRuntimeReadsEnabled() {
		out, err := m.listInvocationsRaftForwarded(ctx, domainID, filter)
		return out, mapStoreError(err)
	}
	if err := m.ensureCommittedExecutionRead(ctx); err != nil {
		return nil, err
	}
	out, err := m.store.ListInvocations(ctx, domainID, filter)
	return out, mapStoreError(err)
}

func (m *AutomationManager) GetRun(ctx context.Context, domainID graph.DomainID, runID string) (automation.Run, error) {
	if m.forwardedAutomationRuntimeReadsEnabled() {
		run, err := m.getRunRaftForwarded(ctx, domainID, runID)
		return run, mapStoreError(err)
	}
	if err := m.ensureCommittedExecutionRead(ctx); err != nil {
		return automation.Run{}, err
	}
	run, err := m.store.GetRun(ctx, domainID, strings.TrimSpace(runID))
	return run, mapStoreError(err)
}

func (m *AutomationManager) RetryInvocation(ctx context.Context, domainID graph.DomainID, invocationID string) (automation.Invocation, error) {
	if err := m.ensureCommittedExecutionRead(ctx); err != nil {
		return automation.Invocation{}, err
	}
	inv, err := m.store.GetInvocation(ctx, domainID, strings.TrimSpace(invocationID))
	if err != nil {
		return inv, mapStoreError(err)
	}
	inv.Status = "pending"
	inv.SkipReason = ""
	inv.NextAttemptAt = time.Time{}
	inv.ClaimOwnerNodeID = 0
	inv.ClaimVersion++
	inv.ClaimToken = ""
	inv.ClaimExpiresAt = time.Time{}
	inv.UpdatedAt = m.now()
	if err := m.putInvocationRuntime(ctx, inv); err != nil {
		return inv, err
	}
	return inv, nil
}

func (m *AutomationManager) CancelInvocation(ctx context.Context, domainID graph.DomainID, invocationID string) (automation.Invocation, error) {
	if err := m.ensureCommittedExecutionRead(ctx); err != nil {
		return automation.Invocation{}, err
	}
	inv, err := m.store.GetInvocation(ctx, domainID, strings.TrimSpace(invocationID))
	if err != nil {
		return inv, mapStoreError(err)
	}
	if m.raftEnabled() && inv.Status == "running" {
		return inv, fmt.Errorf("cannot cancel a running automation invocation in clustered raft mode; wait for completion or retry after claim expiry")
	}
	inv.Status = "cancelled"
	inv.SkipReason = "cancelled"
	inv.NextAttemptAt = time.Time{}
	inv.ClaimOwnerNodeID = 0
	inv.ClaimVersion++
	inv.ClaimToken = ""
	inv.ClaimExpiresAt = time.Time{}
	inv.UpdatedAt = m.now()
	if err := m.putInvocationRuntime(ctx, inv); err != nil {
		return inv, err
	}
	return inv, nil
}

func (m *AutomationManager) HandleGraphChange(ctx context.Context, event graphchange.CommittedEvent) error {
	event.Normalize()
	if m.raftEnabled() {
		if err := m.requireLocalExecutionLeader(ctx, event.SpaceID.String()); err != nil {
			return err
		}
	} else if err := m.requireWriteAllowed(); err != nil {
		return err
	}
	if event.DomainID == uuid.Nil {
		return nil
	}
	domainID := graph.DomainID(event.DomainID)
	items, err := m.listRunnableAutomations(ctx, domainID, automation.StatusEnabled)
	if err != nil {
		return err
	}
	for _, change := range event.Changes {
		eventType := automationEventType(change.Type)
		if eventType == "" || change.Node == nil {
			continue
		}
		for _, item := range items {
			def := item.Definition
			if !item.Binding.CreatedAt.IsZero() && !event.CommittedAt.IsZero() && event.CommittedAt.Before(item.Binding.CreatedAt) {
				continue
			}
			if item.Binding.Scope.SpaceID != "" && item.Binding.Scope.SpaceID != event.SpaceID.String() {
				continue
			}
			if !matchesEvent(def, eventType) || !matchesLabels(def, change.Node.Labels) {
				continue
			}
			if generatedByAutomation(change.Node, def.ID) {
				continue
			}
			var oldNode *graph.Node
			if change.OldNode != nil {
				copy := *change.OldNode
				oldNode = &copy
			}
			invID := graphTriggeredInvocationID(event.SpaceID.String(), domainID, event.ID.String(), item.Binding.ID, change.NodeID)
			inv := invocationForRunnable(m.now, domainID, item, automation.Invocation{ID: invID, SpaceID: event.SpaceID.String(), EventID: event.ID.String(), ChangedElementID: change.NodeID, ChangedElementKind: "node", OldNode: oldNode, EventType: eventType}, event.Origin.PrincipalID)
			if err := m.putInvocationIdempotent(ctx, inv); err != nil {
				return err
			}
		}
	}
	return m.advanceGraphReplayCursor(ctx, event)
}

func (m *AutomationManager) putInvocationIdempotent(ctx context.Context, inv automation.Invocation) error {
	current, err := m.store.GetInvocation(ctx, inv.DomainID, inv.ID)
	if err == nil {
		if !sameInvocationTrigger(current, inv) {
			return fmt.Errorf("automation invocation %q already exists with different trigger metadata", inv.ID)
		}
		return nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return mapStoreError(err)
	}
	return m.putInvocationRuntime(ctx, inv)
}

func sameInvocationTrigger(a, b automation.Invocation) bool {
	return a.ID == b.ID &&
		a.DomainID == b.DomainID &&
		strings.TrimSpace(a.SpaceID) == strings.TrimSpace(b.SpaceID) &&
		strings.TrimSpace(a.AutomationID) == strings.TrimSpace(b.AutomationID) &&
		a.AutomationVersion == b.AutomationVersion &&
		strings.TrimSpace(a.BindingID) == strings.TrimSpace(b.BindingID) &&
		a.BindingVersion == b.BindingVersion &&
		strings.TrimSpace(a.ProcedureID) == strings.TrimSpace(b.ProcedureID) &&
		a.ProcedureVersion == b.ProcedureVersion &&
		strings.TrimSpace(a.EventID) == strings.TrimSpace(b.EventID) &&
		strings.TrimSpace(a.ChangedElementID) == strings.TrimSpace(b.ChangedElementID) &&
		strings.TrimSpace(a.ChangedElementKind) == strings.TrimSpace(b.ChangedElementKind) &&
		strings.TrimSpace(a.EventType) == strings.TrimSpace(b.EventType) &&
		strings.TrimSpace(a.ActorPrincipalID) == strings.TrimSpace(b.ActorPrincipalID) &&
		strings.TrimSpace(a.OwnerPrincipalID) == strings.TrimSpace(b.OwnerPrincipalID) &&
		strings.TrimSpace(a.OnBehalfOfPrincipalID) == strings.TrimSpace(b.OnBehalfOfPrincipalID) &&
		strings.TrimSpace(a.AutomationOwnerPrincipalID) == strings.TrimSpace(b.AutomationOwnerPrincipalID) &&
		strings.TrimSpace(a.EventOriginPrincipalID) == strings.TrimSpace(b.EventOriginPrincipalID)
}

func graphTriggeredInvocationID(spaceID string, domainID graph.DomainID, eventID string, bindingID string, targetNodeID string) string {
	key := strings.Join([]string{
		"mycel",
		"automation",
		"graph-invocation",
		"v1",
		strings.TrimSpace(spaceID),
		domainID.String(),
		strings.TrimSpace(eventID),
		strings.TrimSpace(bindingID),
		strings.TrimSpace(targetNodeID),
	}, "\x00")
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(key)).String()
}

func (m *AutomationManager) ProcessPending(ctx context.Context, domainID graph.DomainID, limit int) (int, error) {
	if !m.raftEnabled() {
		if err := m.requireWriteAllowed(); err != nil {
			return 0, err
		}
	}
	items, err := m.pendingInvocations(ctx, domainID, limit)
	if err != nil {
		return 0, mapStoreError(err)
	}
	processed := 0
	for _, inv := range items {
		item, resolveErr := m.resolveInvocationAutomation(ctx, domainID, inv)
		var debounce debounceDecision
		if resolveErr == nil && item.Definition.Status == automation.StatusEnabled {
			var err error
			debounce, err = m.debounceInvocation(ctx, domainID, item.Definition, inv)
			if err != nil {
				return processed, err
			}
			if debounce.Wait {
				continue
			}
		}
		claimed, ok, err := m.claimInvocation(ctx, inv)
		if err != nil {
			return processed, err
		}
		if !ok {
			continue
		}
		inv = claimed
		if resolveErr != nil {
			inv.Status = "failed"
			inv.SkipReason = resolveErr.Error()
			inv.UpdatedAt = m.now()
			_ = m.putInvocationRuntime(ctx, inv)
			continue
		}
		def := item.Definition
		if def.Status != automation.StatusEnabled {
			now := m.now()
			inv.Status = "skipped"
			inv.SkipReason = "automation_disabled"
			inv.UpdatedAt = now
			runID := newRunID()
			run := automation.Run{ID: runID, DomainID: domainID, InvocationID: inv.ID, BindingID: inv.BindingID, BindingVersion: inv.BindingVersion, ProcedureID: inv.ProcedureID, ProcedureVersion: inv.ProcedureVersion, AttemptNumber: inv.AttemptCount + 1, Status: "skipped", Error: inv.SkipReason, ActorPrincipalID: inv.ActorPrincipalID, OnBehalfOfPrincipalID: inv.OnBehalfOfPrincipalID, OwnerPrincipalID: inv.OwnerPrincipalID, AutomationOwnerPrincipalID: inv.AutomationOwnerPrincipalID, EventOriginPrincipalID: inv.EventOriginPrincipalID, ClaimOwnerNodeID: inv.ClaimOwnerNodeID, ClaimVersion: inv.ClaimVersion, ClaimToken: inv.ClaimToken, OutputIdempotencyKey: automationOutputIdempotencyKey(inv, runID), StartedAt: now, CompletedAt: now}
			if err := m.putInvocationRuntime(ctx, inv); err != nil {
				return processed, err
			}
			if err := m.putRunRuntime(ctx, inv.SpaceID, run); err != nil {
				return processed, err
			}
			m.recordMetric(inv.Status)
			processed++
			continue
		}
		if debounce.Coalesced {
			now := m.now()
			inv.Status = "skipped"
			inv.SkipReason = skipReasonCoalesced
			inv.UpdatedAt = now
			runID := newRunID()
			run := automation.Run{ID: runID, DomainID: domainID, InvocationID: inv.ID, BindingID: inv.BindingID, BindingVersion: inv.BindingVersion, ProcedureID: inv.ProcedureID, ProcedureVersion: inv.ProcedureVersion, AttemptNumber: inv.AttemptCount + 1, Status: "skipped", Error: skipReasonCoalesced, ActorPrincipalID: inv.ActorPrincipalID, OnBehalfOfPrincipalID: inv.OnBehalfOfPrincipalID, OwnerPrincipalID: inv.OwnerPrincipalID, AutomationOwnerPrincipalID: inv.AutomationOwnerPrincipalID, EventOriginPrincipalID: inv.EventOriginPrincipalID, ClaimOwnerNodeID: inv.ClaimOwnerNodeID, ClaimVersion: inv.ClaimVersion, ClaimToken: inv.ClaimToken, OutputIdempotencyKey: automationOutputIdempotencyKey(inv, runID), TargetAlias: debounce.TargetAlias, TargetNodeID: debounce.TargetNodeID, CoalescedInvocationIDs: debounce.CoalescedByIDs, StartedAt: now, CompletedAt: now}
			if err := m.putInvocationRuntime(ctx, inv); err != nil {
				return processed, err
			}
			if err := m.putRunRuntime(ctx, inv.SpaceID, run); err != nil {
				return processed, err
			}
			m.recordMetric(inv.Status)
			processed++
			continue
		}
		if def.Workflow != nil {
			_, err := m.startWorkflowInstance(ctx, def, inv)
			now := m.now()
			if err != nil {
				inv.Status = "failed"
				inv.SkipReason = err.Error()
				inv.UpdatedAt = now
			} else {
				inv.Status = "succeeded"
				inv.UpdatedAt = now
			}
			if err := m.putInvocationRuntime(ctx, inv); err != nil {
				return processed, err
			}
			m.recordMetric(inv.Status)
			processed++
			continue
		}
		run, err := m.executeInvocation(ctx, def, inv)
		if err != nil {
			now := m.now()
			inv.AttemptCount++
			inv.SkipReason = err.Error()
			inv.UpdatedAt = now
			if inv.AttemptCount < maxAttempts(def) && retryableAutomationError(err) {
				inv.Status = "retryable"
				inv.NextAttemptAt = now.Add(retryBackoff(inv.AttemptCount))
			} else {
				inv.Status = "failed"
			}
			if run.ID == "" {
				run.ID = uuid.NewString()
			}
			run.DomainID = domainID
			run.InvocationID = inv.ID
			run.AttemptNumber = inv.AttemptCount
			run.Status = inv.Status
			run.Error = err.Error()
			if run.StartedAt.IsZero() {
				run.StartedAt = now
			}
			run.CompletedAt = now
		} else {
			inv.AttemptCount++
			inv.NextAttemptAt = time.Time{}
			inv.InputHash = run.RenderedInputHash
			inv.Status = run.Status
			if run.Status == "skipped" && run.Error != "" {
				inv.SkipReason = run.Error
			}
			inv.UpdatedAt = run.CompletedAt
		}
		run.ClaimOwnerNodeID = inv.ClaimOwnerNodeID
		run.ClaimVersion = inv.ClaimVersion
		run.ClaimToken = inv.ClaimToken
		run.OutputIdempotencyKey = automationOutputIdempotencyKey(inv, run.ID)
		if err := m.putInvocationRuntime(ctx, inv); err != nil {
			return processed, err
		}
		if err := m.putRunRuntime(ctx, inv.SpaceID, run); err != nil {
			return processed, err
		}
		if err := m.recordSuccessfulInputHash(ctx, def, inv, run); err != nil {
			return processed, err
		}
		m.recordMetric(inv.Status)
		processed++
	}
	return processed, nil
}

func decodeDefinition(rawJSON string) (automation.Definition, error) {
	var def automation.Definition
	if strings.TrimSpace(rawJSON) == "" {
		return def, fmt.Errorf("automation definition JSON is required")
	}
	if err := rejectLegacyAutomationModelJSON([]byte(rawJSON)); err != nil {
		return def, err
	}
	if err := json.Unmarshal([]byte(rawJSON), &def); err != nil {
		return def, fmt.Errorf("invalid automation definition JSON: %w", err)
	}
	return def, nil
}

func decodeProcedure(rawJSON string) (automation.Procedure, error) {
	var procedure automation.Procedure
	if strings.TrimSpace(rawJSON) == "" {
		return procedure, fmt.Errorf("graph procedure JSON is required")
	}
	if err := rejectLegacyAutomationModelJSON([]byte(rawJSON)); err != nil {
		return procedure, err
	}
	if err := json.Unmarshal([]byte(rawJSON), &procedure); err != nil {
		return procedure, fmt.Errorf("invalid graph procedure JSON: %w", err)
	}
	return procedure, nil
}

func decodeBinding(rawJSON string) (automation.Binding, error) {
	var binding automation.Binding
	if strings.TrimSpace(rawJSON) == "" {
		return binding, fmt.Errorf("graph automation binding JSON is required")
	}
	if err := rejectLegacyAutomationModelJSON([]byte(rawJSON)); err != nil {
		return binding, err
	}
	if err := json.Unmarshal([]byte(rawJSON), &binding); err != nil {
		return binding, fmt.Errorf("invalid graph automation binding JSON: %w", err)
	}
	return binding, nil
}

func rejectLegacyAutomationModelJSON(raw []byte) error {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(raw, &root); err != nil {
		return nil
	}
	if _, ok := root["model"]; ok {
		return fmt.Errorf("automation model provider/model fields are not supported; use inference profile refs")
	}
	var workflow struct {
		Steps []map[string]json.RawMessage `json:"steps"`
	}
	if rawWorkflow, ok := root["workflow"]; ok {
		if err := json.Unmarshal(rawWorkflow, &workflow); err == nil {
			for _, step := range workflow.Steps {
				if _, ok := step["model"]; ok {
					return fmt.Errorf("workflow step model provider/model fields are not supported; use inference profile refs")
				}
			}
		}
	}
	return nil
}

func mapStoreError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, storage.ErrNotFound) {
		return ErrAutomationNotFound
	}
	return err
}
