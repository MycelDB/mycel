package service

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
)

func (m *Module) SetSemanticRuleEnabled(ctx context.Context, in SemanticRuleEnabledInput) error {
	if in.Rule.SpaceID == uuid.Nil || in.Rule.ID == uuid.Nil {
		return fmt.Errorf("%w: space_id and semantic_rule_id are required", storesemantic.ErrInvalidInput)
	}
	if in.ExpectedUpdatedAt.IsZero() || !in.Rule.UpdatedAt.Equal(in.ExpectedUpdatedAt) || in.UpdatedAt.IsZero() || !in.UpdatedAt.After(in.ExpectedUpdatedAt) {
		return fmt.Errorf("%w: semantic rule timestamps must advance from the expected version", storesemantic.ErrInvalidInput)
	}
	expectedUpdatedAt := in.ExpectedUpdatedAt.UTC()
	rule := in.Rule
	rule.Enabled = in.Enabled
	rule.UpdatedAt = in.UpdatedAt.UTC()
	record := semanticMutationRecord{
		Kind: "semantic_rule.upsert", SpaceID: rule.SpaceID, Payload: raw(rule),
		ExpectedSemanticRuleUpdatedAt: &expectedUpdatedAt,
	}
	if err := m.requireConditionalRuleMutationSupport(ctx, rule.SpaceID); err != nil {
		return err
	}
	if m.wal == nil && m.raftGroups == nil {
		manager, err := m.baseSpaceManager(ctx, rule.SpaceID)
		if err != nil {
			return err
		}
		return classifySemanticMutationError(applySemanticSpaceMutation(ctx, manager, record))
	}
	return m.commitSemanticMutation(ctx, recordTypeSemanticSpaceConditional, record)
}
