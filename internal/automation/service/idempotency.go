package service

import (
	"context"
	"errors"

	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/storage"
)

const skipReasonDuplicateInput = "duplicate_input_hash"

func (m *AutomationManager) hasSuccessfulInputHash(ctx context.Context, inv automation.Invocation, inputHash string) (bool, error) {
	if inputHash == "" {
		return false, nil
	}
	_, err := m.store.GetSuccessfulInputIndex(ctx, inv.DomainID, inv.AutomationID, inv.AutomationVersion, inv.ChangedElementID, inputHash)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, storage.ErrNotFound) {
		return false, nil
	}
	return false, mapStoreError(err)
}

func (m *AutomationManager) recordSuccessfulInputHash(ctx context.Context, inv automation.Invocation, run automation.Run) error {
	if inv.InputHash == "" || run.Status != "succeeded" {
		return nil
	}
	return mapStoreError(m.store.PutSuccessfulInputIndex(ctx, storage.SuccessfulInputIndex{DomainID: inv.DomainID, AutomationID: inv.AutomationID, Version: inv.AutomationVersion, ChangedElementID: inv.ChangedElementID, InputHash: inv.InputHash, InvocationID: inv.ID, RunID: run.ID}))
}
