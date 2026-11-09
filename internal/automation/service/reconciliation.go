package service

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/storage"
	graph "github.com/myceldb/mycel/internal/graph/model"
)

func (m *AutomationManager) StartReconciliation(ctx context.Context, principalID string, spaceID string, domainID graph.DomainID, bindingID string, idempotencyKey string, batchSize int, maxConcurrency int) (automation.ReconciliationOperation, error) {
	m.reconciliationMu.Lock()
	defer m.reconciliationMu.Unlock()
	if err := ctx.Err(); err != nil {
		return automation.ReconciliationOperation{}, err
	}
	spaceID = strings.TrimSpace(spaceID)
	bindingID = strings.TrimSpace(bindingID)
	idempotencyKey = strings.TrimSpace(idempotencyKey)
	if spaceID == "" || bindingID == "" || idempotencyKey == "" {
		return automation.ReconciliationOperation{}, fmt.Errorf("space_id, binding_id, and idempotency_key are required")
	}
	if batchSize == 0 {
		batchSize = automation.ReconciliationDefaultBatchSize
	}
	if batchSize < 1 || batchSize > automation.ReconciliationMaxBatchSize {
		return automation.ReconciliationOperation{}, fmt.Errorf("batch_size must be between 1 and %d", automation.ReconciliationMaxBatchSize)
	}
	if maxConcurrency < 0 || maxConcurrency > automation.ReconciliationMaxConcurrency {
		return automation.ReconciliationOperation{}, fmt.Errorf("max_concurrency must be between 0 and %d", automation.ReconciliationMaxConcurrency)
	}
	if existing, err := m.store.GetReconciliationOperationByIdempotency(ctx, spaceID, domainID, bindingID, idempotencyKey); err == nil {
		return existing, nil
	} else if !errors.Is(err, storage.ErrNotFound) {
		return automation.ReconciliationOperation{}, mapStoreError(err)
	}
	binding, err := m.store.GetBinding(ctx, domainID, bindingID)
	if err != nil {
		return automation.ReconciliationOperation{}, mapStoreError(err)
	}
	binding = binding.Normalize()
	if binding.Status != automation.StatusEnabled {
		return automation.ReconciliationOperation{}, fmt.Errorf("graph automation binding is not enabled")
	}
	if binding.Reconciliation == nil {
		return automation.ReconciliationOperation{}, fmt.Errorf("graph automation binding does not define reconciliation discovery")
	}
	procedure, err := m.store.GetProcedure(ctx, domainID, binding.ProcedureID)
	if err != nil {
		return automation.ReconciliationOperation{}, mapStoreError(err)
	}
	procedure = procedure.Normalize()
	if err := automation.ValidateBinding(binding, &procedure); err != nil {
		return automation.ReconciliationOperation{}, err
	}
	now := m.now()
	operation := automation.ReconciliationOperation{
		ID:               uuid.NewString(),
		SpaceID:          spaceID,
		DomainID:         domainID,
		BindingID:        binding.ID,
		BindingVersion:   binding.Version,
		ProcedureID:      procedure.ID,
		ProcedureVersion: procedure.Version,
		IdempotencyKey:   idempotencyKey,
		Status:           automation.ReconciliationStatusQueued,
		BatchSize:        batchSize,
		MaxConcurrency:   maxConcurrency,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	if err := m.putReconciliationOperationRuntime(ctx, operation); err != nil {
		return automation.ReconciliationOperation{}, err
	}
	_ = principalID
	return operation, nil
}

func (m *AutomationManager) GetReconciliation(ctx context.Context, spaceID string, operationID string) (automation.ReconciliationOperation, error) {
	spaceID = strings.TrimSpace(spaceID)
	operationID = strings.TrimSpace(operationID)
	if m.forwardedAutomationRuntimeReadsEnabled() {
		operation, err := m.getReconciliationRaftForwarded(ctx, spaceID, operationID)
		return operation, mapStoreError(err)
	}
	operation, err := m.store.GetReconciliationOperation(ctx, spaceID, operationID)
	return operation, mapStoreError(err)
}

func (m *AutomationManager) CancelReconciliation(ctx context.Context, spaceID string, operationID string) (automation.ReconciliationOperation, error) {
	m.reconciliationMu.Lock()
	defer m.reconciliationMu.Unlock()
	operation, err := m.GetReconciliation(ctx, spaceID, operationID)
	if err != nil {
		return operation, err
	}
	if operation.Terminal() {
		return operation, nil
	}
	operation.CancelRequested = true
	operation.Status = automation.ReconciliationStatusRunning
	operation.UpdatedAt = m.now()
	if err := m.putReconciliationOperationRuntime(ctx, operation); err != nil {
		return automation.ReconciliationOperation{}, err
	}
	return operation, nil
}
