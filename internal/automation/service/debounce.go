package service

import (
	"context"
	"strings"
	"time"

	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/storage"
	graph "github.com/myceldb/mycel/internal/graph/model"
	sessionservice "github.com/myceldb/mycel/internal/session/service"
)

const skipReasonCoalesced = "coalesced"

type debounceDecision struct {
	Wait           bool
	Coalesced      bool
	CoalescedByIDs []string
	TargetAlias    string
	TargetNodeID   string
}

func (m *AutomationManager) debounceInvocation(ctx context.Context, domainID graph.DomainID, def automation.Definition, inv automation.Invocation) (debounceDecision, error) {
	if def.Safety.Debounce == nil {
		return debounceDecision{}, nil
	}
	duration, err := time.ParseDuration(strings.TrimSpace(def.Safety.Debounce.Duration))
	if err != nil || duration <= 0 {
		return debounceDecision{}, nil
	}
	base := inv.UpdatedAt
	if base.IsZero() {
		base = inv.CreatedAt
	}
	if m.now().Before(base.Add(duration)) {
		return debounceDecision{Wait: true}, nil
	}
	alias := strings.TrimSpace(def.Safety.Debounce.CoalesceBy)
	if alias == "" {
		alias = firstNonEmptyString(def.Input.Target, "changed")
	}
	targetID, err := m.resolveInvocationAliasNodeID(ctx, domainID, def, inv, alias)
	if err != nil || targetID == "" {
		return debounceDecision{TargetAlias: alias}, err
	}
	peers, err := m.store.ListInvocations(ctx, domainID, storage.InvocationFilter{AutomationID: def.ID})
	if err != nil {
		return debounceDecision{}, mapStoreError(err)
	}
	var newer []string
	for _, peer := range peers {
		if peer.ID == inv.ID || peer.AutomationVersion != inv.AutomationVersion || !coalescingReplacementStatus(peer) {
			continue
		}
		if !peer.CreatedAt.After(inv.CreatedAt) && !(peer.CreatedAt.Equal(inv.CreatedAt) && peer.ID > inv.ID) {
			continue
		}
		peerTargetID, err := m.resolveInvocationAliasNodeID(ctx, domainID, def, peer, alias)
		if err != nil {
			return debounceDecision{}, err
		}
		if peerTargetID == targetID {
			newer = append(newer, peer.ID)
		}
	}
	if len(newer) > 0 {
		return debounceDecision{Coalesced: true, CoalescedByIDs: newer, TargetAlias: alias, TargetNodeID: targetID}, nil
	}
	return debounceDecision{TargetAlias: alias, TargetNodeID: targetID}, nil
}

func coalescingReplacementStatus(inv automation.Invocation) bool {
	if inv.Status == "pending" || inv.Status == "retryable" || inv.Status == "succeeded" {
		return true
	}
	return inv.Status == "skipped" && inv.SkipReason == skipReasonCoalesced
}

func (m *AutomationManager) resolveInvocationAliasNodeID(ctx context.Context, domainID graph.DomainID, def automation.Definition, inv automation.Invocation, alias string) (string, error) {
	alias = strings.TrimSpace(alias)
	if alias == "" || alias == "changed" {
		return inv.ChangedElementID, nil
	}
	if m.sessions == nil || m.graphs == nil {
		return inv.ChangedElementID, nil
	}
	sess, err := m.sessions.OpenSession(ctx, sessionservice.OpenSessionInput{PrincipalID: automationActor, SpaceID: inv.SpaceID, DomainID: domainID.String()})
	if err != nil {
		return "", err
	}
	tx, err := m.sessions.BeginTransaction(ctx, sessionservice.BeginTransactionInput{PrincipalID: automationActor, SessionID: sess.ID, Mode: sessionservice.TransactionModeReadWrite})
	if err != nil {
		return "", err
	}
	defer func() { _, _ = m.sessions.RollbackTransaction(ctx, automationActor, tx.ID) }()
	changed, err := m.graphs.GetNode(ctx, tx, inv.ChangedElementID)
	if err != nil {
		return "", err
	}
	condition, err := m.evaluateCondition(ctx, tx, def, changed, inv.OldNode)
	if err != nil || !condition.Matched {
		return "", err
	}
	return targetNodeID(alias, condition.Aliases, changed), nil
}
