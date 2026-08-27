package service

import (
	"context"
	"errors"

	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/storage"
)

const (
	skipReasonDuplicateInput  = "duplicate_input_hash"
	skipReasonDuplicateOutput = "duplicate_output_idempotency_key"
)

func (m *AutomationManager) hasSuccessfulInputHash(ctx context.Context, inv automation.Invocation, inputHash string, elementID string) (bool, error) {
	if inputHash == "" {
		return false, nil
	}
	if elementID == "" {
		elementID = inv.ChangedElementID
	}
	_, err := m.store.GetSuccessfulInputIndex(ctx, inv.DomainID, inv.AutomationID, inv.AutomationVersion, elementID, inputHash)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, storage.ErrNotFound) {
		return false, nil
	}
	return false, mapStoreError(err)
}

func (m *AutomationManager) recordSuccessfulInputHash(ctx context.Context, def automation.Definition, inv automation.Invocation, run automation.Run) error {
	if inv.InputHash == "" {
		return nil
	}
	if run.Status != "succeeded" && !(run.Status == "skipped" && run.Error == skipReasonDuplicateOutput) {
		return nil
	}
	elementID := idempotencyElementID(def, run, inv, run.IdempotencyTargetNodeID)
	return m.putSuccessfulInputRuntime(ctx, inv.SpaceID, storage.SuccessfulInputIndex{DomainID: inv.DomainID, AutomationID: inv.AutomationID, Version: inv.AutomationVersion, ChangedElementID: inv.ChangedElementID, TargetElementID: elementID, InputHash: inv.InputHash, InvocationID: inv.ID, RunID: run.ID})
}
