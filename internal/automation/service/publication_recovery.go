package service

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/automation/actions"
	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/storage"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
)

const (
	publicationReconciliationRequired = "publication_reconciliation_required"
	publicationRecoveryFailure        = "publication_recovery_failed"
)

var ErrPublicationReconciliationRequired = errors.New("automation publication requires reconciliation")

func (m *AutomationManager) publicationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	timeout := m.publicationTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return context.WithTimeout(context.WithoutCancel(ctx), timeout)
}

func (m *AutomationManager) logPublicationStage(run automation.Run, stage, result, category string) {
	if m.logger == nil || lastString(run.InferenceAttemptIDs) == "" {
		return
	}
	m.logger.Info("procedure_inference_publication_stage",
		"stage", stage,
		"result", result,
		"category", category,
		"run_id", run.ID,
		"attempt_id", lastString(run.InferenceAttemptIDs),
	)
}

func publicationLogResult(err error) string {
	if err != nil {
		return "failed"
	}
	return "completed"
}

func publicationFailureCategory(err error) string {
	switch {
	case err == nil:
		return "none"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return "operation_failed"
	}
}

func lastString(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[len(values)-1]
}

func (m *AutomationManager) recoverableProviderAttempt(ctx context.Context, inv automation.Invocation) (automation.ProcedureInferenceAttempt, bool, error) {
	ledger, err := m.store.GetProcedureInferenceLedger(ctx, inv.SpaceID)
	if errors.Is(err, storage.ErrNotFound) {
		return automation.ProcedureInferenceAttempt{}, false, nil
	}
	if err != nil {
		return automation.ProcedureInferenceAttempt{}, false, mapStoreError(err)
	}
	var candidate automation.ProcedureInferenceAttempt
	for _, attempt := range ledger.Attempts {
		if attempt.InvocationID != inv.ID {
			continue
		}
		if candidate.ID == "" || attempt.RunAttemptNumber > candidate.RunAttemptNumber || (attempt.RunAttemptNumber == candidate.RunAttemptNumber && attempt.ProviderAttemptNumber > candidate.ProviderAttemptNumber) {
			candidate = attempt
		}
	}
	if candidate.ID == "" || candidate.State != automation.InferenceAttemptStateSucceeded {
		return automation.ProcedureInferenceAttempt{}, false, nil
	}
	disposition := strings.ToLower(strings.TrimSpace(candidate.PublicationDisposition))
	desiredStatus := ""
	switch disposition {
	case "published":
		desiredStatus = "succeeded"
	case "not_changed_or_duplicate":
		desiredStatus = "skipped"
	case "", "publication_pending":
		// Non-final publication requires an idempotency probe.
	default:
		desiredStatus = "failed"
	}
	if desiredStatus != "" && strings.EqualFold(inv.Status, desiredStatus) && inv.AttemptCount >= candidate.RunAttemptNumber {
		return automation.ProcedureInferenceAttempt{}, false, nil
	}
	return candidate, true, nil
}

// claimPublicationRecovery fences an interrupted worker before inspecting the
// stable output idempotency marker. A new partition leader may take over an
// unexpired claim because provider work is already durably terminal and must
// never be repeated.
func (m *AutomationManager) claimPublicationRecovery(ctx context.Context, inv automation.Invocation) (automation.Invocation, bool, error) {
	if m.raftGroups == nil {
		m.localExecutionMu.Lock()
		defer m.localExecutionMu.Unlock()
		current, err := m.store.GetInvocation(ctx, inv.DomainID, inv.ID)
		if err != nil {
			return inv, false, mapStoreError(err)
		}
		// Provider work is durably terminal. Take over even an unexpired local
		// claim: after restart there is no old worker to release it, and the new
		// claim token fences any concurrently unwinding publication path.
		current.Status = "running"
		current.ClaimVersion++
		current.ClaimToken = uuid.NewString()
		current.ClaimExpiresAt = m.now().Add(automationClaimLease)
		current.UpdatedAt = m.now()
		if err := m.putInvocationRuntime(ctx, current); err != nil {
			return current, false, err
		}
		return current, true, nil
	}
	leader, local, _, _, err := m.executionRoute(inv.SpaceID)
	if err != nil {
		return inv, false, err
	}
	if leader != local {
		return inv, false, nil
	}
	current, err := m.store.GetInvocation(ctx, inv.DomainID, inv.ID)
	if err != nil {
		return inv, false, mapStoreError(err)
	}
	// Recovery is safe to reclaim an unexpired claim even when this node ID
	// was also the former owner. Stable output idempotency plus the renewed
	// claim token prevent provider replay and fence the interrupted publisher.
	current.Status = "running"
	current.ClaimOwnerNodeID = uint64(local)
	current.ClaimVersion++
	current.ClaimToken = uuid.NewString()
	current.ClaimExpiresAt = m.now().Add(automationClaimLease)
	current.UpdatedAt = m.now()
	if err := m.putInvocationRuntime(ctx, current); err != nil {
		return current, false, err
	}
	claimed, err := m.store.GetInvocation(ctx, current.DomainID, current.ID)
	if err != nil {
		return current, false, mapStoreError(err)
	}
	if claimed.Status != "running" || claimed.ClaimOwnerNodeID != uint64(local) || claimed.ClaimVersion != current.ClaimVersion || claimed.ClaimToken != current.ClaimToken {
		return claimed, false, nil
	}
	return claimed, true, nil
}

func (m *AutomationManager) publicationOutputApplied(ctx context.Context, inv automation.Invocation, run automation.Run) (bool, error) {
	if m.sessions == nil || m.graphs == nil {
		return false, ErrPublicationReconciliationRequired
	}
	principal := firstNonEmptyString(run.ActorPrincipalID, inv.ActorPrincipalID, automationActor)
	sess, err := m.sessions.OpenSession(ctx, sessionmodel.OpenSessionInput{PrincipalID: principal, SpaceID: inv.SpaceID, DomainID: inv.DomainID.String()})
	if err != nil {
		return false, err
	}
	baseRevision, err := m.graphs.CurrentDomainRevision(ctx, inv.SpaceID, inv.DomainID.String())
	if err != nil {
		return false, err
	}
	tx, err := m.sessions.BeginTransaction(ctx, sessionmodel.BeginTransactionInput{PrincipalID: principal, SessionID: sess.ID, Mode: sessionmodel.TransactionModeReadOnly, BaseRevision: &baseRevision})
	if err != nil {
		return false, err
	}
	defer m.rollbackAutomationOutputTransaction(ctx, principal, tx.ID)
	return (actions.Engine{Graphs: m.graphs}).OutputAlreadyApplied(ctx, tx, automationOutputIdempotencyKey(inv, run.ID))
}

func (m *AutomationManager) reconcileProviderStartedPublication(ctx context.Context, inv *automation.Invocation) (bool, error) {
	if inv == nil {
		return false, nil
	}
	attempt, ok, err := m.recoverableProviderAttempt(ctx, *inv)
	if err != nil || !ok {
		return false, err
	}
	recoveryCtx, cancelRecovery := m.publicationContext(ctx)
	defer cancelRecovery()
	claimed, claimedOK, err := m.claimPublicationRecovery(recoveryCtx, *inv)
	if err != nil || !claimedOK {
		return false, err
	}
	*inv = claimed
	run, err := m.store.GetRun(recoveryCtx, inv.DomainID, attempt.RunID)
	if errors.Is(err, storage.ErrNotFound) {
		run = automation.Run{
			ID:                          attempt.RunID,
			DomainID:                    inv.DomainID,
			InvocationID:                inv.ID,
			BindingID:                   attempt.BindingID,
			BindingVersion:              attempt.BindingVersion,
			ProcedureID:                 attempt.ProcedureID,
			ProcedureVersion:            attempt.ProcedureVersion,
			AttemptNumber:               attempt.RunAttemptNumber,
			ActorPrincipalID:            attempt.ActorPrincipalID,
			OwnerPrincipalID:            attempt.OwnerPrincipalID,
			OnBehalfOfPrincipalID:       attempt.OnBehalfOfPrincipalID,
			EventOriginPrincipalID:      attempt.EventOriginPrincipalID,
			InferenceProfileID:          attempt.ProfileID,
			ModelEndpointID:             attempt.EndpointID,
			ModelID:                     attempt.ModelID,
			CapabilityID:                attempt.CapabilityID,
			CredentialGrantID:           attempt.CredentialGrantID,
			PolicyDecisionID:            attempt.PolicyDecisionID,
			AuthorizationRevision:       attempt.AuthorizationRevision,
			GlobalAuthorizationRevision: attempt.GlobalAuthorizationRevision,
			SpaceAuthorizationRevision:  attempt.SpaceAuthorizationRevision,
			InferenceAttemptIDs:         []string{attempt.ID},
			StartedAt:                   attempt.RequestedAt,
		}
	} else if err != nil {
		return false, mapStoreError(err)
	}
	run.InferenceAttemptIDs = appendUniqueString(run.InferenceAttemptIDs, attempt.ID)
	run.OutputIdempotencyKey = automationOutputIdempotencyKey(*inv, run.ID)
	run.ClaimOwnerNodeID = inv.ClaimOwnerNodeID
	run.ClaimVersion = inv.ClaimVersion
	run.ClaimToken = inv.ClaimToken

	disposition := strings.ToLower(strings.TrimSpace(attempt.PublicationDisposition))
	if disposition == "" || disposition == "publication_pending" {
		m.logPublicationStage(run, "recovery_output_probe", "started", "none")
		applied, probeErr := m.publicationOutputApplied(recoveryCtx, *inv, run)
		if probeErr != nil {
			m.logPublicationStage(run, "recovery_output_probe", "failed", publicationFailureCategory(probeErr))
			return false, probeErr
		}
		if applied {
			disposition = "published"
		} else {
			disposition = "publication_failed"
		}
		attempt.RecordVersion++
		attempt.PublicationDisposition = disposition
		if err := m.putProcedureInferenceAttemptRuntime(recoveryCtx, attempt); err != nil {
			return false, err
		}
		m.logPublicationStage(run, "recovery_output_probe", "completed", disposition)
	}

	now := m.now()
	run.CompletedAt = now
	inv.AttemptCount = max(inv.AttemptCount, attempt.RunAttemptNumber)
	inv.NextAttemptAt = time.Time{}
	inv.UpdatedAt = now
	switch disposition {
	case "published":
		run.Status = "succeeded"
		run.Error = ""
		inv.Status = "succeeded"
		inv.SkipReason = ""
	case "not_changed_or_duplicate":
		run.Status = "skipped"
		run.Error = disposition
		inv.Status = "skipped"
		inv.SkipReason = disposition
	default:
		run.Status = "failed"
		run.Error = publicationRecoveryFailure
		inv.Status = "failed"
		inv.SkipReason = publicationRecoveryFailure
	}
	if err := m.putRunRuntime(recoveryCtx, inv.SpaceID, run); err != nil {
		return false, err
	}
	if err := m.putInvocationRuntime(recoveryCtx, *inv); err != nil {
		return false, err
	}
	return true, nil
}

func (m *AutomationManager) hasRecoverableProviderPublication(ctx context.Context, inv automation.Invocation) bool {
	_, ok, err := m.recoverableProviderAttempt(ctx, inv)
	return err == nil && ok
}
