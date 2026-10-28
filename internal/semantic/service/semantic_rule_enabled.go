package service

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
)

func (m *Module) SetSemanticRuleEnabled(ctx context.Context, in SemanticRuleEnabledInput) error {
	if in.Rule.SpaceID == uuid.Nil || in.Rule.ID == uuid.Nil {
		return fmt.Errorf("%w: space_id and semantic_rule_id are required", storesemantic.ErrInvalidInput)
	}
	if in.ExpectedUpdatedAt.IsZero() || !in.Rule.UpdatedAt.Equal(in.ExpectedUpdatedAt) || in.UpdatedAt.IsZero() || !in.UpdatedAt.After(in.ExpectedUpdatedAt) {
		return fmt.Errorf("%w: semantic rule timestamps must advance from the expected version", storesemantic.ErrInvalidInput)
	}
	expectedRule := in.Rule
	expectedRule.UpdatedAt = in.ExpectedUpdatedAt.UTC()
	rule := expectedRule
	rule.Enabled = in.Enabled
	rule.ProviderBudgetTransition = in.ProviderBudgetTransition
	rule.ProviderBudgetTransitionID = in.ProviderBudgetTransitionID
	rule.ProviderBudgetTransitionScopeIDs = append([]string(nil), in.ProviderBudgetTransitionScopeIDs...)
	rule.ProviderBudgetTransitionRequiresRebuild = in.ProviderBudgetTransitionRequiresRebuild
	rule.UpdatedAt = in.UpdatedAt.UTC()
	return m.ConditionalUpsertSemanticRule(ctx, expectedRule, rule)
}

func (m *Module) ConditionalUpsertSemanticRule(ctx context.Context, expected, desired domainsemantic.SemanticGenerationRule) error {
	if expected.SpaceID == uuid.Nil || expected.ID == uuid.Nil || desired.SpaceID != expected.SpaceID || desired.ID != expected.ID {
		return fmt.Errorf("%w: matching semantic rule identities are required", storesemantic.ErrInvalidInput)
	}
	if expected.UpdatedAt.IsZero() || desired.UpdatedAt.IsZero() || !desired.UpdatedAt.After(expected.UpdatedAt) {
		return fmt.Errorf("%w: semantic rule timestamps must advance", storesemantic.ErrInvalidInput)
	}
	record := semanticMutationRecord{Kind: "semantic_rule.upsert", SpaceID: desired.SpaceID, Payload: raw(desired), ExpectedSemanticRule: &expected}
	if err := m.requireConditionalRuleMutationSupport(ctx, desired.SpaceID); err != nil {
		return err
	}
	if m.wal == nil && m.raftGroups == nil {
		manager, err := m.baseSpaceManager(ctx, desired.SpaceID)
		if err != nil {
			return err
		}
		return classifySemanticMutationError(applySemanticSpaceMutation(ctx, manager, record))
	}
	return m.commitSemanticMutation(ctx, recordTypeSemanticSpaceConditional, record)
}

func (m *Module) ConditionalDeleteSemanticRule(ctx context.Context, expected domainsemantic.SemanticGenerationRule, purgeDependents bool) error {
	if expected.SpaceID == uuid.Nil || expected.ID == uuid.Nil || expected.UpdatedAt.IsZero() {
		return fmt.Errorf("%w: semantic rule identity and version are required", storesemantic.ErrInvalidInput)
	}
	record := semanticMutationRecord{Kind: "semantic_rule.delete", SpaceID: expected.SpaceID, Payload: raw(expected.ID), Flag: purgeDependents, ExpectedSemanticRule: &expected}
	if err := m.requireConditionalRuleMutationSupport(ctx, expected.SpaceID); err != nil {
		return err
	}
	if m.wal == nil && m.raftGroups == nil {
		manager, err := m.baseSpaceManager(ctx, expected.SpaceID)
		if err != nil {
			return err
		}
		return classifySemanticMutationError(applySemanticSpaceMutation(ctx, manager, record))
	}
	return m.commitSemanticMutation(ctx, recordTypeSemanticSpaceConditional, record)
}
