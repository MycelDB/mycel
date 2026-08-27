package service

import (
	"context"
	"errors"
	"strings"
	"time"

	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/storage"
	graph "github.com/myceldb/mycel/internal/graph/model"
)

func (m *AutomationManager) pendingInvocations(ctx context.Context, domainID graph.DomainID, limit int) ([]automation.Invocation, error) {
	pending, err := m.store.ListInvocations(ctx, domainID, storage.InvocationFilter{Status: "pending", Limit: limit})
	if err != nil {
		return nil, mapStoreError(err)
	}
	retries, err := m.store.ListInvocations(ctx, domainID, storage.InvocationFilter{Status: "retryable", Limit: limit})
	if err != nil {
		return nil, mapStoreError(err)
	}
	now := m.now()
	out := append([]automation.Invocation{}, pending...)
	for _, inv := range retries {
		if inv.NextAttemptAt.IsZero() || !inv.NextAttemptAt.After(now) {
			out = append(out, inv)
		}
	}
	if m.raftEnabled() {
		running, err := m.store.ListInvocations(ctx, domainID, storage.InvocationFilter{Status: "running", Limit: limit})
		if err != nil {
			return nil, mapStoreError(err)
		}
		for _, inv := range running {
			if inv.ClaimExpiresAt.IsZero() {
				m.recordClaimAbandoned()
				continue
			}
			if !inv.ClaimExpiresAt.After(now) {
				out = append(out, inv)
			}
		}
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func maxAttempts(def automation.Definition) int {
	if def.Safety.MaxAttempts > 0 {
		return def.Safety.MaxAttempts
	}
	return 3
}
func retryBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	return time.Duration(attempt*attempt) * time.Second
}
func retryableAutomationError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrInferenceUnavailable) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "timeout") || strings.Contains(msg, "temporary") || strings.Contains(msg, "rate") || strings.Contains(msg, " 5")
}
