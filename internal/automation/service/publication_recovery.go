package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func boundedDetachedContext(ctx context.Context, maximum time.Duration) (context.Context, context.CancelFunc) {
	if maximum <= 0 {
		maximum = time.Second
	}
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining < maximum {
			maximum = remaining
		}
	}
	return context.WithTimeout(context.WithoutCancel(ctx), maximum)
}

func (m *AutomationManager) logPublicationStage(run automation.Run, stage, result, category string) {
	m.logPublicationStageAtRevision(run, stage, result, category, 0)
}

func (m *AutomationManager) logPublicationStageAtRevision(run automation.Run, stage, result, category string, graphRevision int64) {
	if m.logger == nil || lastString(run.InferenceAttemptIDs) == "" {
		return
	}
	attributes := []any{
		"stage", stage,
		"result", result,
		"category", category,
		"run_id", run.ID,
		"attempt_id", lastString(run.InferenceAttemptIDs),
		"claim_owner_node_id", run.ClaimOwnerNodeID,
		"claim_version", run.ClaimVersion,
		"authorization_revision", run.AuthorizationRevision,
		"global_authorization_revision", run.GlobalAuthorizationRevision,
		"space_authorization_revision", run.SpaceAuthorizationRevision,
		"output_key_hash", publicationOutputKeyHash(run.OutputIdempotencyKey),
	}
	if graphRevision > 0 {
		attributes = append(attributes, "graph_revision", graphRevision)
	}
	m.logger.Info("procedure_inference_publication_stage", attributes...)
}

func (m *AutomationManager) recordRunPublicationDispositionWithDiagnostics(ctx context.Context, run automation.Run, disposition string, receiptCommitID ...string) error {
	return m.runPublicationPersistenceStage(run, publicationDispositionStage(disposition), func() error {
		return m.recordRunPublicationDisposition(ctx, run, disposition, receiptCommitID...)
	})
}

func (m *AutomationManager) runPublicationPersistenceStage(run automation.Run, stage string, persist func() error) error {
	m.logPublicationStage(run, stage, "started", "none")
	err := persist()
	m.logPublicationStage(run, stage, publicationLogResult(err), publicationFailureCategory(err))
	return err
}

func publicationDispositionStage(disposition string) string {
	switch strings.ToLower(strings.TrimSpace(disposition)) {
	case "not_published_pre_provider_failure":
		return "disposition_not_published_pre_provider_failure"
	case "not_published_provider_failure":
		return "disposition_not_published_provider_failure"
	case "not_published_provider_ambiguous":
		return "disposition_not_published_provider_ambiguous"
	case "suppressed_procedure_control":
		return "disposition_suppressed_procedure_control"
	case "rejected_invalid_output":
		return "disposition_rejected_invalid_output"
	case "publication_precondition_failed":
		return "disposition_publication_precondition_failed"
	case "publication_pending":
		return "disposition_publication_pending"
	case "suppressed_authorization_or_control":
		return "disposition_suppressed_authorization_or_control"
	case "not_changed_or_duplicate":
		return "disposition_not_changed_or_duplicate"
	case "published":
		return "disposition_published"
	default:
		return "disposition_invalid"
	}
}

func publicationOutputKeyHash(key string) string {
	if strings.TrimSpace(key) == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
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
	case errors.Is(err, ErrProcedureDisabled):
		return "procedure_disabled"
	case errors.Is(err, ErrProcedureAuthorizationRevoked):
		return "authorization_revoked"
	case errors.Is(err, ErrAutomationNotFound):
		return "not_found"
	case isGraphConflict(err):
		return "graph_conflict"
	}
	switch status.Code(err) {
	case codes.FailedPrecondition:
		return "failed_precondition"
	case codes.Unavailable:
		return "unavailable"
	case codes.Aborted:
		return "conflict"
	case codes.NotFound:
		return "not_found"
	case codes.PermissionDenied:
		return "permission_denied"
	case codes.ResourceExhausted:
		return "resource_exhausted"
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
		if current.Status == "running" && current.ClaimProcessID == m.processID && !current.ClaimExpiresAt.IsZero() && current.ClaimExpiresAt.After(m.now()) {
			return current, false, nil
		}
		// A different process incarnation cannot have a live local worker. The
		// renewed token fences any concurrently unwinding obsolete publisher.
		current.Status = "running"
		current.ClaimVersion++
		current.ClaimToken = uuid.NewString()
		current.ClaimProcessID = m.processID
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
	if current.Status == "running" && current.ClaimProcessID == m.processID && !current.ClaimExpiresAt.IsZero() && current.ClaimExpiresAt.After(m.now()) {
		return current, false, nil
	}
	// A changed leader process incarnation proves the prior worker obsolete,
	// including restart with the same durable Raft node ID.
	current.Status = "running"
	current.ClaimOwnerNodeID = uint64(local)
	current.ClaimVersion++
	current.ClaimToken = uuid.NewString()
	current.ClaimProcessID = m.processID
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

func (m *AutomationManager) publicationOutputApplied(ctx context.Context, inv automation.Invocation, run automation.Run) (bool, string, error) {
	if m.graphs == nil {
		return false, "", ErrPublicationReconciliationRequired
	}
	principal := firstNonEmptyString(run.ActorPrincipalID, inv.ActorPrincipalID, automationActor)
	key := automationOutputIdempotencyKey(inv, run.ID)
	commit, found, err := m.automationOutputAlreadyApplied(ctx, principal, inv, key)
	if err != nil || !found {
		return found, "", err
	}
	// Promote legacy matching receipts before relying on them. A crash after
	// this point cannot let generic idempotency expiry erase the publication
	// proof before the terminal attempt disposition is durable.
	if err := m.graphs.RetainIdempotentCommitUntilAcknowledged(ctx, principal, inv.SpaceID, inv.DomainID.String(), key, commit.CommitID); err != nil {
		return false, "", err
	}
	return true, commit.CommitID, nil
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
	runExisted := err == nil
	existingRunStatus := strings.ToLower(strings.TrimSpace(run.Status))
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
	receiptCommitID := strings.TrimSpace(attempt.PublicationReceiptCommitID)
	if disposition == "" || disposition == "publication_pending" {
		m.logPublicationStage(run, "recovery_output_probe", "started", "none")
		applied, observedCommitID, probeErr := m.publicationOutputApplied(recoveryCtx, *inv, run)
		if probeErr != nil {
			m.logPublicationStage(run, "recovery_output_probe", "failed", publicationFailureCategory(probeErr))
			return false, probeErr
		}
		if applied {
			disposition = "published"
			receiptCommitID = observedCommitID
		} else {
			disposition = "publication_failed"
		}
		attempt.RecordVersion++
		attempt.PublicationDisposition = disposition
		attempt.PublicationReceiptCommitID = receiptCommitID
		if err := m.runPublicationPersistenceStage(run, "recovery_disposition_persistence", func() error {
			return m.putProcedureInferenceAttemptRuntime(recoveryCtx, attempt)
		}); err != nil {
			return false, err
		}
		m.logPublicationStage(run, "recovery_output_probe", "completed", disposition)
	}

	if disposition == "published" && receiptCommitID == "" {
		// Backfill generation identity for a legacy terminal disposition when its
		// retained receipt still exists. If it was already acknowledged and
		// expired, no further maintenance mutation is needed.
		m.logPublicationStage(run, "recovery_receipt_backfill", "started", "none")
		applied, observedCommitID, probeErr := m.publicationOutputApplied(recoveryCtx, *inv, run)
		if probeErr != nil {
			m.logPublicationStage(run, "recovery_receipt_backfill", "failed", publicationFailureCategory(probeErr))
			return false, probeErr
		}
		if applied {
			receiptCommitID = observedCommitID
			attempt.RecordVersion++
			attempt.PublicationReceiptCommitID = receiptCommitID
			if err := m.runPublicationPersistenceStage(run, "recovery_receipt_backfill_persistence", func() error {
				return m.putProcedureInferenceAttemptRuntime(recoveryCtx, attempt)
			}); err != nil {
				m.logPublicationStage(run, "recovery_receipt_backfill", "failed", publicationFailureCategory(err))
				return false, err
			}
		}
		m.logPublicationStage(run, "recovery_receipt_backfill", "completed", "none")
	}
	if disposition == "published" && receiptCommitID != "" {
		principal := firstNonEmptyString(run.ActorPrincipalID, inv.ActorPrincipalID, automationActor)
		m.logPublicationStage(run, "recovery_receipt_acknowledgement", "started", "none")
		if err := m.acknowledgePublicationReceipt(recoveryCtx, principal, *inv, run, receiptCommitID); err != nil {
			m.logPublicationStage(run, "recovery_receipt_acknowledgement", "failed", publicationFailureCategory(err))
			return false, err
		}
		m.logPublicationStage(run, "recovery_receipt_acknowledgement", "completed", "none")
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
	persistRun := true
	if runExisted && existingRunStatus != "" && existingRunStatus != "running" && existingRunStatus != "retryable" {
		if existingRunStatus != run.Status {
			return false, errors.New("automation publication recovery conflicts with terminal run")
		}
		// A prior apply completed the run before the invocation update. Preserve
		// the immutable terminal run and finish only the invocation.
		persistRun = false
	}
	if persistRun {
		if err := m.runPublicationPersistenceStage(run, "recovery_run_persistence", func() error {
			return m.putRunRuntime(recoveryCtx, inv.SpaceID, run)
		}); err != nil {
			return false, err
		}
	}
	if err := m.runPublicationPersistenceStage(run, "recovery_invocation_persistence", func() error {
		return m.putInvocationRuntime(recoveryCtx, *inv)
	}); err != nil {
		return false, err
	}
	return true, nil
}

func (m *AutomationManager) hasRecoverableProviderPublication(ctx context.Context, inv automation.Invocation) bool {
	_, ok, err := m.recoverableProviderAttempt(ctx, inv)
	return err == nil && ok
}
