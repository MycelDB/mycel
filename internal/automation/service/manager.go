package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/automation/accounting"
	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/provider"
	"github.com/myceldb/mycel/internal/automation/storage"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	graph "github.com/myceldb/mycel/internal/graph/model"
	graphservice "github.com/myceldb/mycel/internal/graph/service"
	schemaservice "github.com/myceldb/mycel/internal/schema/service"
	sessionservice "github.com/myceldb/mycel/internal/session/service"
)

var ErrAutomationNotFound = errors.New("automation not found")

type Manager interface {
	ValidateAutomation(ctx context.Context, domainID graph.DomainID, rawJSON string) (automation.Definition, error)
	CreateAutomation(ctx context.Context, domainID graph.DomainID, rawJSON string) (automation.Definition, error)
	UpdateAutomation(ctx context.Context, domainID graph.DomainID, id string, rawJSON string) (automation.Definition, error)
	DeleteAutomation(ctx context.Context, domainID graph.DomainID, id string) error
	GetAutomation(ctx context.Context, domainID graph.DomainID, id string) (automation.Definition, error)
	ListAutomationDomains(ctx context.Context) ([]graph.DomainID, error)
	ListAutomations(ctx context.Context, domainID graph.DomainID, status string) ([]automation.Definition, error)
	SetAutomationStatus(ctx context.Context, domainID graph.DomainID, id string, status string) (automation.Definition, error)
	ListInvocations(ctx context.Context, domainID graph.DomainID, filter storage.InvocationFilter) ([]automation.Invocation, error)
	GetRun(ctx context.Context, domainID graph.DomainID, runID string) (automation.Run, error)
	RetryInvocation(ctx context.Context, domainID graph.DomainID, invocationID string) (automation.Invocation, error)
	CancelInvocation(ctx context.Context, domainID graph.DomainID, invocationID string) (automation.Invocation, error)
	HandleGraphChange(ctx context.Context, event graphchange.CommittedEvent) error
	HandleGraphChangeGap(ctx context.Context, gap graphchange.Gap) error
	ProcessPending(ctx context.Context, domainID graph.DomainID, limit int) (int, error)
}

type AutomationManager struct {
	store            storage.Store
	now              func() time.Time
	sessions         sessionservice.Manager
	graphs           graphservice.Manager
	schemas          schemaservice.Manager
	provider         provider.Provider
	accounting       accounting.Estimator
	maxTokensPerRun  int64
	maxCostPerRun    float64
	metricsProcessed int64
	metricsSucceeded int64
	metricsSkipped   int64
	metricsFailed    int64
	metricsRetryable int64
	writeAllowed     func() error
}

func NewManager(store storage.Store) *AutomationManager {
	return &AutomationManager{store: store, now: func() time.Time { return time.Now().UTC() }, accounting: accounting.DefaultEstimator()}
}

func (m *AutomationManager) WithGraphRuntime(sessions sessionservice.Manager, graphs graphservice.Manager) *AutomationManager {
	m.sessions = sessions
	m.graphs = graphs
	return m
}

func (m *AutomationManager) WithProvider(provider provider.Provider) *AutomationManager {
	m.provider = provider
	return m
}

func (m *AutomationManager) WithSchemaManager(schemas schemaservice.Manager) *AutomationManager {
	m.schemas = schemas
	return m
}

func (m *AutomationManager) WithRunCeilings(maxTokens int64, maxCost float64) *AutomationManager {
	m.maxTokensPerRun = maxTokens
	m.maxCostPerRun = maxCost
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
	def.CreatedAt = now
	def.UpdatedAt = now
	if err := m.store.PutDefinition(ctx, def); err != nil {
		return def, mapStoreError(err)
	}
	return def, nil
}

func (m *AutomationManager) UpdateAutomation(ctx context.Context, domainID graph.DomainID, id string, rawJSON string) (automation.Definition, error) {
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

func (m *AutomationManager) GetAutomation(ctx context.Context, domainID graph.DomainID, id string) (automation.Definition, error) {
	def, err := m.store.GetDefinition(ctx, domainID, strings.TrimSpace(id))
	return def, mapStoreError(err)
}

func (m *AutomationManager) ListAutomationDomains(ctx context.Context) ([]graph.DomainID, error) {
	ids, err := m.store.ListDefinitionDomains(ctx)
	return ids, mapStoreError(err)
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
	def.UpdatedAt = m.now()
	if err := m.store.PutDefinition(ctx, def); err != nil {
		return def, mapStoreError(err)
	}
	return def, nil
}

func (m *AutomationManager) ListInvocations(ctx context.Context, domainID graph.DomainID, filter storage.InvocationFilter) ([]automation.Invocation, error) {
	out, err := m.store.ListInvocations(ctx, domainID, filter)
	return out, mapStoreError(err)
}

func (m *AutomationManager) GetRun(ctx context.Context, domainID graph.DomainID, runID string) (automation.Run, error) {
	run, err := m.store.GetRun(ctx, domainID, strings.TrimSpace(runID))
	return run, mapStoreError(err)
}

func (m *AutomationManager) RetryInvocation(ctx context.Context, domainID graph.DomainID, invocationID string) (automation.Invocation, error) {
	if err := m.requireWriteAllowed(); err != nil {
		return automation.Invocation{}, err
	}
	inv, err := m.store.GetInvocation(ctx, domainID, strings.TrimSpace(invocationID))
	if err != nil {
		return inv, mapStoreError(err)
	}
	inv.Status = "pending"
	inv.SkipReason = ""
	inv.NextAttemptAt = time.Time{}
	inv.UpdatedAt = m.now()
	if err := m.store.PutInvocation(ctx, inv); err != nil {
		return inv, mapStoreError(err)
	}
	return inv, nil
}

func (m *AutomationManager) CancelInvocation(ctx context.Context, domainID graph.DomainID, invocationID string) (automation.Invocation, error) {
	if err := m.requireWriteAllowed(); err != nil {
		return automation.Invocation{}, err
	}
	inv, err := m.store.GetInvocation(ctx, domainID, strings.TrimSpace(invocationID))
	if err != nil {
		return inv, mapStoreError(err)
	}
	inv.Status = "cancelled"
	inv.SkipReason = "cancelled"
	inv.NextAttemptAt = time.Time{}
	inv.UpdatedAt = m.now()
	if err := m.store.PutInvocation(ctx, inv); err != nil {
		return inv, mapStoreError(err)
	}
	return inv, nil
}

func (m *AutomationManager) HandleGraphChange(ctx context.Context, event graphchange.CommittedEvent) error {
	if err := m.requireWriteAllowed(); err != nil {
		return err
	}
	event.Normalize()
	if event.DomainID == uuid.Nil {
		return nil
	}
	domainID := graph.DomainID(event.DomainID)
	defs, err := m.ListAutomations(ctx, domainID, automation.StatusEnabled)
	if err != nil {
		return err
	}
	for _, change := range event.Changes {
		eventType := automationEventType(change.Type)
		if eventType == "" || change.Node == nil {
			continue
		}
		for _, def := range defs {
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
			inv := automation.Invocation{ID: uuid.NewString(), DomainID: domainID, SpaceID: event.SpaceID.String(), AutomationID: def.ID, AutomationVersion: def.Version, EventID: event.ID.String(), ChangedElementID: change.NodeID, ChangedElementKind: "node", OldNode: oldNode, EventType: eventType, Status: "pending", CreatedAt: m.now(), UpdatedAt: m.now()}
			_ = m.store.PutInvocation(ctx, inv)
		}
	}
	return nil
}

func (m *AutomationManager) HandleGraphChangeGap(ctx context.Context, gap graphchange.Gap) error {
	return nil
}

func (m *AutomationManager) ProcessPending(ctx context.Context, domainID graph.DomainID, limit int) (int, error) {
	if err := m.requireWriteAllowed(); err != nil {
		return 0, err
	}
	items, err := m.pendingInvocations(ctx, domainID, limit)
	if err != nil {
		return 0, mapStoreError(err)
	}
	processed := 0
	for _, inv := range items {
		def, err := m.GetAutomation(ctx, domainID, inv.AutomationID)
		if err != nil {
			inv.Status = "failed"
			inv.SkipReason = err.Error()
			inv.UpdatedAt = m.now()
			_ = m.store.PutInvocation(ctx, inv)
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
			if err := m.store.PutInvocation(ctx, inv); err != nil {
				return processed, mapStoreError(err)
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
			run = automation.Run{ID: uuid.NewString(), DomainID: domainID, InvocationID: inv.ID, AttemptNumber: inv.AttemptCount, Status: inv.Status, Error: err.Error(), StartedAt: now, CompletedAt: now}
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
		if err := m.store.PutInvocation(ctx, inv); err != nil {
			return processed, mapStoreError(err)
		}
		if err := m.store.PutRun(ctx, run); err != nil {
			return processed, mapStoreError(err)
		}
		if err := m.recordSuccessfulInputHash(ctx, inv, run); err != nil {
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
	if err := json.Unmarshal([]byte(rawJSON), &def); err != nil {
		return def, fmt.Errorf("invalid automation definition JSON: %w", err)
	}
	return def, nil
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
