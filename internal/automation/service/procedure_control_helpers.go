package service

import (
	"context"
	"fmt"

	"github.com/myceldb/mycel/internal/automation/storage"
	graph "github.com/myceldb/mycel/internal/graph/model"
)

func (m *AutomationManager) ensureNoPendingProcedureControl(ctx context.Context, domainID graph.DomainID, procedureID string) error {
	barrier, err := m.store.GetProcedureControlBarrier(ctx, domainID, procedureID)
	if err == storage.ErrNotFound {
		return nil
	}
	if err != nil {
		return mapStoreError(err)
	}
	if !barrier.Completed {
		return fmt.Errorf("procedure %q has an incomplete control operation at epoch %d", procedureID, barrier.ControlEpoch)
	}
	return nil
}

func uint32SliceContainsAll(have, required []uint32) bool {
	set := make(map[uint32]struct{}, len(have))
	for _, value := range have {
		set[value] = struct{}{}
	}
	for _, value := range required {
		if _, ok := set[value]; !ok {
			return false
		}
	}
	return true
}

func uint32SlicesEqual(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
