package service

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/automation/actions"
	automation "github.com/myceldb/mycel/internal/automation/model"
	autooutput "github.com/myceldb/mycel/internal/automation/output"
	"github.com/myceldb/mycel/internal/automation/render"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	graph "github.com/myceldb/mycel/internal/graph/model"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	inferenceruntime "github.com/myceldb/mycel/internal/inference/runtime"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const automationActor = "automation"

var ErrInferenceUnavailable = errors.New("automation inference subsystem is not configured")

func (m *AutomationManager) executeInvocation(ctx context.Context, def automation.Definition, inv automation.Invocation) (automation.Run, error) {
	now := m.now()
	runID := automationRunID(inv, inv.AttemptCount+1)
	run := automation.Run{ID: runID, DomainID: inv.DomainID, InvocationID: inv.ID, BindingID: inv.BindingID, BindingVersion: inv.BindingVersion, ProcedureID: inv.ProcedureID, ProcedureVersion: inv.ProcedureVersion, AttemptNumber: inv.AttemptCount + 1, Status: "running", ClaimOwnerNodeID: inv.ClaimOwnerNodeID, ClaimVersion: inv.ClaimVersion, ClaimToken: inv.ClaimToken, OutputIdempotencyKey: automationOutputIdempotencyKey(inv, runID), ActorPrincipalID: firstNonEmptyString(inv.ActorPrincipalID, automationActor), OnBehalfOfPrincipalID: firstNonEmptyString(inv.OnBehalfOfPrincipalID, inv.OwnerPrincipalID, def.OwnerPrincipalID, automationActor), OwnerPrincipalID: firstNonEmptyString(inv.OwnerPrincipalID, inv.AutomationOwnerPrincipalID, def.OwnerPrincipalID), AutomationOwnerPrincipalID: firstNonEmptyString(inv.AutomationOwnerPrincipalID, inv.OwnerPrincipalID, def.OwnerPrincipalID), EventOriginPrincipalID: inv.EventOriginPrincipalID, InferenceProfile: strings.TrimSpace(def.Inference.Profile), InferenceProfileID: strings.TrimSpace(def.Inference.ProfileID), StartedAt: now}
	if m.sessions == nil || m.graphs == nil {
		result, err := render.Render(def.Input, render.Context{})
		if err != nil {
			return run, err
		}
		run.RenderedInputHash = result.Hash
		output, err := m.generateWithInference(ctx, def, inv, result.Text, &run)
		if err != nil {
			return run, err
		}
		run.Status = "succeeded"
		run.CompletedAt = now
		outSum := sha256.Sum256([]byte(output))
		run.OutputHash = hex.EncodeToString(outSum[:])
		return run, nil
	}
	executionPrincipal := firstNonEmptyString(run.ActorPrincipalID, automationActor)
	sess, err := m.sessions.OpenSession(ctx, sessionmodel.OpenSessionInput{PrincipalID: executionPrincipal, SpaceID: inv.SpaceID, DomainID: inv.DomainID.String()})
	if err != nil {
		return run, err
	}
	baseRevision, err := m.graphs.CurrentDomainRevision(ctx, inv.SpaceID, inv.DomainID.String())
	if err != nil {
		return run, err
	}
	tx, err := m.sessions.BeginTransaction(ctx, sessionmodel.BeginTransactionInput{PrincipalID: executionPrincipal, SessionID: sess.ID, Mode: sessionmodel.TransactionModeReadWrite, BaseRevision: &baseRevision})
	if err != nil {
		return run, err
	}
	node, err := m.graphs.GetNode(ctx, tx, inv.ChangedElementID)
	if err != nil {
		m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
		return run, err
	}
	condition, err := m.evaluateCondition(ctx, tx, def, node, inv.OldNode)
	if err != nil {
		m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
		return run, err
	} else if !condition.Matched {
		m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
		run.Status = "skipped"
		run.Error = conditionFalseReason
		run.CompletedAt = m.now()
		return run, nil
	}
	run.TargetAlias = firstNonEmptyString(strings.TrimSpace(def.Input.Target), "changed")
	run.TargetNodeID, err = requireNodeAliasID(run.TargetAlias, condition.Aliases, node)
	if err != nil {
		m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
		return run, err
	}
	idempotencyTargetID := run.TargetNodeID
	if strings.TrimSpace(def.Safety.Idempotency.Scope) == "target" {
		idempotencyAlias := firstNonEmptyString(def.Safety.Idempotency.Target, run.TargetAlias)
		idempotencyTargetID, err = requireNodeAliasID(idempotencyAlias, condition.Aliases, node)
		if err != nil {
			m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
			return run, err
		}
		run.IdempotencyTargetNodeID = idempotencyTargetID
	}
	if err := preflightActionTargets(def, condition.Aliases, node); err != nil {
		m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
		return run, err
	}
	inputContext, err := m.evaluateInputContext(ctx, tx, def, condition.Aliases)
	if err != nil {
		m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
		run.Context = inputContext.Summaries
		return run, err
	}
	run.Context = inputContext.Summaries
	rendered, err := render.Render(def.Input, render.Context{Changed: node, Old: inv.OldNode, Aliases: condition.Aliases, Collections: inputContext.Collections})
	if err != nil {
		m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
		return run, err
	}
	run.RenderedInputHash = rendered.Hash
	if def.Safety.Idempotency.SkipIfOutputUnchanged {
		duplicate, err := m.hasSuccessfulInputHash(ctx, inv, rendered.Hash, idempotencyElementID(def, run, inv, idempotencyTargetID))
		if err != nil {
			m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
			return run, err
		}
		if duplicate {
			m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
			run.Status = "skipped"
			run.Error = skipReasonDuplicateInput
			run.CompletedAt = m.now()
			return run, nil
		}
	}
	existingCommit, applied, err := m.automationOutputAlreadyApplied(ctx, executionPrincipal, inv, run.OutputIdempotencyKey)
	if err != nil {
		m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
		return run, err
	}
	if applied {
		m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
		run.Status = "skipped"
		run.Error = skipReasonDuplicateOutput
		run.CompletedAt = m.now()
		if err := m.recordRunPublicationDispositionWithDiagnostics(ctx, run, "published", existingCommit.CommitID); err != nil {
			if !errors.Is(err, ErrAutomationNotFound) {
				return run, err
			}
		} else if err := m.acknowledgePublicationReceipt(ctx, executionPrincipal, inv, run, existingCommit.CommitID); err != nil {
			return run, errors.Join(ErrPublicationReconciliationRequired, err)
		}
		return run, nil
	}
	if err := m.ensureClaimStillOwned(ctx, inv); err != nil {
		m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
		return run, err
	}
	imageInputs, err := m.imageInputsForProcedure(ctx, inv, def, run, node, condition.Aliases, inputContext.Collections)
	if err != nil {
		m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
		return run, err
	}
	imageInputHash := automationImageInputsHash(imageInputs)
	// Input collection is complete. Release the graph transaction before any
	// credential resolution or provider access; publication opens a fresh
	// transaction and revalidates the complete provider input, condition,
	// target, claim, and idempotency.
	m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
	output, err := m.generateWithInference(ctx, def, inv, rendered.Text, &run, imageInputs...)
	if err != nil {
		disposition := inferenceFailurePublicationDisposition(err)
		return run, errors.Join(err, m.recordRunPublicationDispositionWithDiagnostics(ctx, run, disposition))
	}
	// Every stage after durable provider success shares one detached, bounded
	// publication budget. No graph/session/store call can strand the worker.
	publicationCtx, cancelPublication := m.publicationContext(ctx)
	defer cancelPublication()
	m.logPublicationStage(run, "runtime_control", "started", "none")
	if err := m.requireProcedureEnabledRuntime(publicationCtx, inv); err != nil {
		m.logPublicationStage(run, "runtime_control", "failed", publicationFailureCategory(err))
		return run, errors.Join(err, m.recordRunPublicationDispositionWithDiagnostics(publicationCtx, run, "suppressed_procedure_control"))
	}
	m.logPublicationStage(run, "runtime_control", "completed", "none")
	m.logPublicationStage(run, "output_parse", "started", "none")
	parsed, err := autooutput.Parse(def.Output.Mode, def.Output.Schema, output)
	if err != nil {
		m.logPublicationStage(run, "output_parse", "failed", publicationFailureCategory(err))
		return run, errors.Join(err, m.recordRunPublicationDispositionWithDiagnostics(publicationCtx, run, "rejected_invalid_output"))
	}
	m.logPublicationStage(run, "output_parse", "completed", "none")
	run.ActionFingerprint = actionFingerprint(def, parsed)
	m.logPublicationStage(run, "transaction_reopen", "started", "none")
	publicationTx, publicationNode, publicationAliases, err := m.reopenAutomationOutputTransaction(publicationCtx, executionPrincipal, sess, def, inv, &run, rendered.Hash, imageInputHash)
	if err != nil {
		m.logPublicationStage(run, "transaction_reopen", "failed", publicationFailureCategory(err))
		return run, errors.Join(ErrPublicationReconciliationRequired, err, m.recordRunPublicationDispositionWithDiagnostics(publicationCtx, run, "publication_precondition_failed"))
	}
	m.logPublicationStage(run, "transaction_reopen", "completed", "none")
	m.logPublicationStage(run, "pending_persistence", "started", "none")
	if err := m.recordRunPublicationDispositionWithDiagnostics(publicationCtx, run, "publication_pending"); err != nil {
		m.logPublicationStage(run, "pending_persistence", "failed", publicationFailureCategory(err))
		m.rollbackAutomationOutputTransaction(publicationCtx, executionPrincipal, publicationTx.ID)
		return run, errors.Join(ErrPublicationReconciliationRequired, err)
	}
	m.logPublicationStage(run, "pending_persistence", "completed", "none")
	changed, publicationCommitID, err := m.commitAutomationOutputWithConflictRetry(publicationCtx, executionPrincipal, sess, publicationTx, def, &inv, &run, publicationNode, publicationAliases, parsed, rendered.Hash, imageInputHash)
	if err != nil {
		m.logPublicationStage(run, "publication", "failed", publicationFailureCategory(err))
		var dispositionErr error
		if errors.Is(err, ErrProcedureDisabled) || errors.Is(err, ErrProcedureAuthorizationRevoked) {
			dispositionErr = m.recordRunPublicationDispositionWithDiagnostics(publicationCtx, run, "suppressed_authorization_or_control")
		}
		return run, errors.Join(ErrPublicationReconciliationRequired, err, dispositionErr)
	}
	if !changed {
		run.Status = "skipped"
		run.CompletedAt = m.now()
		err := m.recordRunPublicationDispositionWithDiagnostics(publicationCtx, run, "not_changed_or_duplicate")
		m.logPublicationStage(run, "final_disposition", publicationLogResult(err), publicationFailureCategory(err))
		return run, err
	}
	run.Status = "succeeded"
	run.CompletedAt = m.now()
	outSum := sha256.Sum256([]byte(output))
	run.OutputHash = hex.EncodeToString(outSum[:])
	if err := m.recordRunPublicationDispositionWithDiagnostics(publicationCtx, run, "published", publicationCommitID); err != nil {
		m.logPublicationStage(run, "final_disposition", "failed", publicationFailureCategory(err))
		return run, errors.Join(ErrPublicationReconciliationRequired, err)
	}
	if err := m.acknowledgePublicationReceipt(publicationCtx, executionPrincipal, inv, run, publicationCommitID); err != nil {
		m.logPublicationStage(run, "receipt_acknowledgement", "failed", publicationFailureCategory(err))
		return run, errors.Join(ErrPublicationReconciliationRequired, err)
	}
	m.logPublicationStage(run, "final_disposition", "completed", "published")
	return run, nil
}

func (m *AutomationManager) automationOutputAlreadyApplied(ctx context.Context, principal string, inv automation.Invocation, key string) (graphchange.CommitResult, bool, error) {
	if m.graphs == nil || strings.TrimSpace(key) == "" {
		return graphchange.CommitResult{}, false, nil
	}
	return m.graphs.LookupIdempotentCommit(ctx, principal, inv.SpaceID, inv.DomainID.String(), key)
}

func (m *AutomationManager) acknowledgePublicationReceipt(ctx context.Context, principal string, inv automation.Invocation, run automation.Run, commitID string) error {
	if m.graphs == nil {
		return ErrPublicationReconciliationRequired
	}
	key := automationOutputIdempotencyKey(inv, run.ID)
	if strings.TrimSpace(key) == "" || strings.TrimSpace(commitID) == "" {
		return ErrPublicationReconciliationRequired
	}
	return m.graphs.AcknowledgeIdempotentCommit(ctx, principal, inv.SpaceID, inv.DomainID.String(), key, commitID)
}

func (m *AutomationManager) recordRunPublicationDisposition(ctx context.Context, run automation.Run, disposition string, receiptCommitID ...string) error {
	finalizeCtx, cancel := boundedDetachedContext(ctx, 5*time.Second)
	defer cancel()
	attemptID := ""
	if len(run.InferenceAttemptIDs) > 0 {
		attemptID = run.InferenceAttemptIDs[len(run.InferenceAttemptIDs)-1]
	}
	ledgers, err := m.store.ListProcedureInferenceLedgers(finalizeCtx)
	if err != nil {
		return mapStoreError(err)
	}
	var attempt automation.ProcedureInferenceAttempt
	found := false
	for _, ledger := range ledgers {
		for _, candidate := range ledger.Attempts {
			if (attemptID != "" && candidate.ID == attemptID) || (attemptID == "" && candidate.RunID == run.ID && (!found || candidate.ProviderAttemptNumber > attempt.ProviderAttemptNumber)) {
				attempt = candidate
				found = true
				if attemptID != "" {
					break
				}
			}
		}
		if found {
			break
		}
	}
	if !found {
		return ErrAutomationNotFound
	}
	if !automation.IsTerminalProcedureInferenceAttemptState(attempt.State) {
		return fmt.Errorf("procedure inference attempt %s is not terminal for publication disposition", attempt.ID)
	}
	disposition = strings.ToLower(strings.TrimSpace(disposition))
	nextReceiptCommitID := ""
	if len(receiptCommitID) > 0 {
		nextReceiptCommitID = strings.TrimSpace(receiptCommitID[0])
	}
	if attempt.PublicationDisposition == disposition {
		if nextReceiptCommitID == "" || attempt.PublicationReceiptCommitID == nextReceiptCommitID {
			return nil
		}
		if attempt.PublicationReceiptCommitID != "" {
			return fmt.Errorf("procedure inference attempt %s already records a different publication receipt", attempt.ID)
		}
	} else if attempt.PublicationDisposition != "" && attempt.PublicationDisposition != "publication_pending" {
		return fmt.Errorf("procedure inference attempt %s already records publication disposition %q", attempt.ID, attempt.PublicationDisposition)
	}
	attempt.RecordVersion++
	attempt.PublicationDisposition = disposition
	if nextReceiptCommitID != "" {
		attempt.PublicationReceiptCommitID = nextReceiptCommitID
	}
	return m.putProcedureInferenceAttemptRuntime(finalizeCtx, attempt)
}

func (m *AutomationManager) commitAutomationOutputWithConflictRetry(ctx context.Context, executionPrincipal string, sess sessionmodel.GraphSession, firstTx sessionmodel.GraphTransaction, def automation.Definition, inv *automation.Invocation, run *automation.Run, firstNode graph.Node, firstAliases map[string]any, parsed autooutput.Result, expectedRenderedInputHash string, expectedImageInputHash string) (bool, string, error) {
	const maxAttempts = 3
	engine := actions.Engine{Graphs: m.graphs}
	tx := firstTx
	node := firstNode
	aliases := firstAliases
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		m.logPublicationStage(*run, "procedure_control_recheck", "started", "none")
		if err := m.requireProcedureEnabledRuntime(ctx, *inv); err != nil {
			m.logPublicationStage(*run, "procedure_control_recheck", "failed", publicationFailureCategory(err))
			m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
			return false, "", err
		}
		m.logPublicationStage(*run, "procedure_control_recheck", "completed", "none")
		m.logPublicationStage(*run, "authorization_recheck", "started", "none")
		if err := m.revalidateProcedureAuthorization(ctx, def, *inv, run); err != nil {
			m.logPublicationStage(*run, "authorization_recheck", "failed", publicationFailureCategory(err))
			m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
			return false, "", err
		}
		m.logPublicationStage(*run, "authorization_recheck", "completed", "none")
		if attempt > 1 {
			m.logPublicationStage(*run, "conflict_transaction_reopen", "started", "none")
			var err error
			tx, node, aliases, err = m.reopenAutomationOutputTransaction(ctx, executionPrincipal, sess, def, *inv, run, expectedRenderedInputHash, expectedImageInputHash)
			if err != nil {
				m.logPublicationStage(*run, "conflict_transaction_reopen", "failed", publicationFailureCategory(err))
				return false, "", err
			}
			m.logPublicationStage(*run, "conflict_transaction_reopen", "completed", "none")
		}
		m.logPublicationStage(*run, "claim_check", "started", "none")
		if err := m.ensureClaimStillOwned(ctx, *inv); err != nil {
			m.logPublicationStage(*run, "claim_check", "failed", publicationFailureCategory(err))
			m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
			return false, "", err
		}
		m.logPublicationStage(*run, "claim_check", "completed", "none")
		m.logPublicationStage(*run, "claim_renewal", "started", "none")
		if err := m.renewClaim(ctx, inv); err != nil {
			m.logPublicationStage(*run, "claim_renewal", "failed", publicationFailureCategory(err))
			m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
			return false, "", err
		}
		run.ClaimOwnerNodeID = inv.ClaimOwnerNodeID
		run.ClaimVersion = inv.ClaimVersion
		run.ClaimToken = inv.ClaimToken
		m.logPublicationStage(*run, "claim_renewal", "completed", "none")
		m.logPublicationStage(*run, "receipt_probe", "started", "none")
		_, applied, err := m.automationOutputAlreadyApplied(ctx, executionPrincipal, *inv, run.OutputIdempotencyKey)
		if err != nil {
			m.logPublicationStage(*run, "receipt_probe", "failed", publicationFailureCategory(err))
			m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
			return false, "", err
		}
		m.logPublicationStage(*run, "receipt_probe", "completed", "none")
		if applied {
			m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
			run.Status = "skipped"
			run.Error = skipReasonDuplicateOutput
			return false, "", nil
		}
		m.logPublicationStage(*run, "action_apply", "started", "none")
		summary, err := engine.Apply(ctx, tx, actions.Context{Definition: def, RunID: run.ID, InvocationID: inv.ID, BindingID: inv.BindingID, TargetNodeID: run.TargetNodeID, ClaimOwnerNodeID: inv.ClaimOwnerNodeID, ClaimVersion: inv.ClaimVersion, ClaimToken: inv.ClaimToken, OutputIdempotencyKey: run.OutputIdempotencyKey, ProcedureID: inv.ProcedureID, AuthorizationRevision: run.AuthorizationRevision, GlobalAuthorizationRevision: run.GlobalAuthorizationRevision, SpaceAuthorizationRevision: run.SpaceAuthorizationRevision, Changed: node, Aliases: aliases, Result: parsed})
		if err != nil {
			m.logPublicationStage(*run, "action_apply", "failed", publicationFailureCategory(err))
			m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
			return false, "", err
		}
		m.logPublicationStage(*run, "action_apply", "completed", "none")
		if !summary.Changed {
			m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
			return false, "", nil
		}
		m.logPublicationStage(*run, "precommit_claim_renewal", "started", "none")
		if err := m.renewClaim(ctx, inv); err != nil {
			m.logPublicationStage(*run, "precommit_claim_renewal", "failed", publicationFailureCategory(err))
			m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
			return false, "", err
		}
		run.ClaimOwnerNodeID = inv.ClaimOwnerNodeID
		run.ClaimVersion = inv.ClaimVersion
		run.ClaimToken = inv.ClaimToken
		m.logPublicationStage(*run, "precommit_claim_renewal", "completed", "none")
		m.logPublicationStage(*run, "global_authorization_fence", "started", "none")
		if m.raftGroups != nil {
			revisions, ok := m.inference.(inferenceAuthorizationRevisionReader)
			if !ok || run.GlobalAuthorizationRevision == 0 {
				m.logPublicationStage(*run, "global_authorization_fence", "failed", "authorization_revoked")
				m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
				return false, "", ErrProcedureAuthorizationRevoked
			}
			if err := revisions.AssertGlobalAuthorizationRevision(ctx, run.GlobalAuthorizationRevision); err != nil {
				mappedErr := mapGlobalAuthorizationAssertionError(err)
				m.logPublicationStage(*run, "global_authorization_fence", "failed", publicationFailureCategory(mappedErr))
				m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
				return false, "", mappedErr
			}
		}
		m.logPublicationStage(*run, "global_authorization_fence", "completed", "none")
		m.logPublicationStage(*run, "procedure_control_fence", "started", "none")
		m.procedureControlMu.RLock()
		controlErr := m.procedureFenceAllows(ctx, inv.SpaceID, inv.DomainID, inv.ProcedureID)
		if controlErr != nil {
			m.procedureControlMu.RUnlock()
			m.logPublicationStage(*run, "procedure_control_fence", "failed", publicationFailureCategory(controlErr))
			m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
			return false, "", controlErr
		}
		m.logPublicationStage(*run, "procedure_control_fence", "completed", "none")
		m.logPublicationStage(*run, "graph_commit", "started", "none")
		graphCommit, err := m.graphs.CommitTransactionGraph(ctx, tx)
		m.procedureControlMu.RUnlock()
		if err != nil {
			m.logPublicationStage(*run, "graph_commit", "failed", publicationFailureCategory(err))
			m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
			if isGraphConflict(err) && attempt < maxAttempts {
				continue
			}
			return false, "", err
		}
		m.logPublicationStageAtRevision(*run, "graph_commit", "completed", "committed", graphCommit.CommittedRevision)
		finalizeCtx := ctx
		m.logPublicationStage(*run, "session_finalize", "started", "none")
		commit, err := m.sessions.CommitTransactionAtRevision(finalizeCtx, executionPrincipal, tx.ID, graphCommit.OperationCount, graphCommit.CommittedRevision)
		if err != nil {
			m.logPublicationStage(*run, "session_finalize", "failed", publicationFailureCategory(err))
			return false, "", err
		}
		m.logPublicationStage(*run, "session_finalize", "completed", "committed")
		m.logPublicationStage(*run, "graph_finalize_ack", "started", "none")
		if err := m.graphs.AcknowledgeTransactionFinalization(finalizeCtx, tx); err != nil {
			m.logPublicationStage(*run, "graph_finalize_ack", "failed", publicationFailureCategory(err))
			return false, "", err
		}
		m.logPublicationStage(*run, "graph_finalize_ack", "completed", "acknowledged")
		run.MutationID = commit.ID
		return true, graphCommit.CommitID, nil
	}
	return false, "", graphchange.ErrConflict
}

func (m *AutomationManager) reopenAutomationOutputTransaction(ctx context.Context, executionPrincipal string, sess sessionmodel.GraphSession, def automation.Definition, inv automation.Invocation, run *automation.Run, expectedRenderedInputHash string, expectedImageInputHash string) (sessionmodel.GraphTransaction, graph.Node, map[string]any, error) {
	baseRevision, err := m.graphs.CurrentDomainRevision(ctx, inv.SpaceID, inv.DomainID.String())
	if err != nil {
		return sessionmodel.GraphTransaction{}, graph.Node{}, nil, err
	}
	tx, err := m.sessions.BeginTransaction(ctx, sessionmodel.BeginTransactionInput{PrincipalID: executionPrincipal, SessionID: sess.ID, Mode: sessionmodel.TransactionModeReadWrite, BaseRevision: &baseRevision})
	if err != nil {
		return sessionmodel.GraphTransaction{}, graph.Node{}, nil, err
	}
	tx.IdempotencyKey = automationOutputIdempotencyKey(inv, run.ID)
	tx.RetainIdempotencyUntilAcknowledged = len(run.InferenceAttemptIDs) > 0
	node, err := m.graphs.GetNode(ctx, tx, inv.ChangedElementID)
	if err != nil {
		m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
		return sessionmodel.GraphTransaction{}, graph.Node{}, nil, err
	}
	condition, err := m.evaluateCondition(ctx, tx, def, node, inv.OldNode)
	if err != nil {
		m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
		return sessionmodel.GraphTransaction{}, graph.Node{}, nil, err
	}
	if !condition.Matched {
		m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
		return sessionmodel.GraphTransaction{}, graph.Node{}, nil, fmt.Errorf(conditionFalseReason)
	}
	targetAlias := firstNonEmptyString(strings.TrimSpace(def.Input.Target), "changed")
	targetNodeID, err := requireNodeAliasID(targetAlias, condition.Aliases, node)
	if err != nil {
		m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
		return sessionmodel.GraphTransaction{}, graph.Node{}, nil, err
	}
	run.TargetAlias = targetAlias
	run.TargetNodeID = targetNodeID
	if strings.TrimSpace(def.Safety.Idempotency.Scope) == "target" {
		idempotencyAlias := firstNonEmptyString(def.Safety.Idempotency.Target, run.TargetAlias)
		idempotencyTargetID, err := requireNodeAliasID(idempotencyAlias, condition.Aliases, node)
		if err != nil {
			m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
			return sessionmodel.GraphTransaction{}, graph.Node{}, nil, err
		}
		run.IdempotencyTargetNodeID = idempotencyTargetID
	}
	if err := preflightActionTargets(def, condition.Aliases, node); err != nil {
		m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
		return sessionmodel.GraphTransaction{}, graph.Node{}, nil, err
	}
	inputContext, err := m.evaluateInputContext(ctx, tx, def, condition.Aliases)
	if err != nil {
		m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
		return sessionmodel.GraphTransaction{}, graph.Node{}, nil, err
	}
	rendered, err := render.Render(def.Input, render.Context{Changed: node, Old: inv.OldNode, Aliases: condition.Aliases, Collections: inputContext.Collections})
	if err != nil {
		m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
		return sessionmodel.GraphTransaction{}, graph.Node{}, nil, err
	}
	images, err := m.imageInputsForProcedure(ctx, inv, def, *run, node, condition.Aliases, inputContext.Collections)
	if err != nil {
		m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
		return sessionmodel.GraphTransaction{}, graph.Node{}, nil, err
	}
	if rendered.Hash != expectedRenderedInputHash || automationImageInputsHash(images) != expectedImageInputHash {
		m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
		return sessionmodel.GraphTransaction{}, graph.Node{}, nil, fmt.Errorf("automation provider input changed before publication")
	}
	return tx, node, condition.Aliases, nil
}

func automationImageInputsHash(images []domaininference.ImageInput) string {
	if len(images) == 0 {
		return ""
	}
	h := sha256.New()
	var length [8]byte
	write := func(value []byte) {
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		_, _ = h.Write(length[:])
		_, _ = h.Write(value)
	}
	for _, image := range images {
		write([]byte(image.BlobID))
		write([]byte(image.MimeType))
		binary.BigEndian.PutUint64(length[:], uint64(image.SizeBytes))
		_, _ = h.Write(length[:])
		contentHash := sha256.Sum256(image.Data)
		_, _ = h.Write(contentHash[:])
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (m *AutomationManager) rollbackAutomationOutputTransaction(ctx context.Context, principalID string, transactionID string) {
	finalizeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if m.graphs != nil {
		_ = m.graphs.DiscardTransactionGraph(finalizeCtx, transactionID)
	}
	if m.sessions == nil {
		return
	}
	tx, err := m.sessions.RollbackTransaction(finalizeCtx, principalID, transactionID)
	if err != nil {
		return
	}
	if m.graphs != nil {
		_ = m.graphs.AcknowledgeTransactionFinalization(finalizeCtx, tx)
	}
}

func isGraphConflict(err error) bool {
	return errors.Is(err, graphchange.ErrConflict) || strings.Contains(strings.ToLower(err.Error()), "graph conflict")
}

func mapGlobalAuthorizationAssertionError(err error) error {
	if err == nil {
		return nil
	}
	if status.Code(err) == codes.FailedPrecondition {
		return ErrProcedureAuthorizationRevoked
	}
	return err
}

func targetNodeID(alias string, aliases map[string]any, changed graph.Node) string {
	id, _ := requireNodeAliasID(alias, aliases, changed)
	return id
}

func requireNodeAliasID(alias string, aliases map[string]any, changed graph.Node) (string, error) {
	alias = strings.TrimSpace(alias)
	if alias == "" || alias == "changed" {
		return changed.ID.String(), nil
	}
	if node, ok := aliasGraphNode(aliases[alias]); ok {
		return node.ID.String(), nil
	}
	return "", fmt.Errorf("automation target alias %q is not a node", alias)
}

func preflightActionTargets(def automation.Definition, aliases map[string]any, changed graph.Node) error {
	for i, action := range def.Output.Actions {
		if action.UpdateNode == nil {
			continue
		}
		if _, err := requireNodeAliasID(action.UpdateNode.Target, aliases, changed); err != nil {
			return fmt.Errorf("automation output.actions[%d].update_node.target: %w", i, err)
		}
	}
	return nil
}

func idempotencyElementID(def automation.Definition, run automation.Run, inv automation.Invocation, targetID string) string {
	if strings.TrimSpace(def.Safety.Idempotency.Scope) == "target" {
		return firstNonEmptyString(targetID, run.IdempotencyTargetNodeID, run.TargetNodeID, inv.ChangedElementID)
	}
	return inv.ChangedElementID
}

type inferenceGenerationError struct {
	cause                    error
	providerStarted          bool
	providerOutcomeAmbiguous bool
}

func (e *inferenceGenerationError) Error() string { return e.cause.Error() }
func (e *inferenceGenerationError) Unwrap() error { return e.cause }

func markInferenceGenerationError(err error, providerStarted, providerOutcomeAmbiguous bool) error {
	if err == nil {
		return nil
	}
	return &inferenceGenerationError{cause: err, providerStarted: providerStarted, providerOutcomeAmbiguous: providerOutcomeAmbiguous}
}

func inferenceFailurePublicationDisposition(err error) string {
	var generationErr *inferenceGenerationError
	if inferenceProviderOutcomeAmbiguous(err) {
		return "not_published_provider_ambiguous"
	}
	if errors.As(err, &generationErr) && generationErr.providerStarted {
		return "not_published_provider_failure"
	}
	return "not_published_pre_provider_failure"
}

func inferenceProviderOutcomeAmbiguous(err error) bool {
	if errors.Is(err, ErrInferenceAttemptAlreadyStarted) {
		return true
	}
	var generationErr *inferenceGenerationError
	return errors.As(err, &generationErr) && generationErr.providerOutcomeAmbiguous
}

func (m *AutomationManager) generateWithInference(ctx context.Context, def automation.Definition, inv automation.Invocation, rendered string, run *automation.Run, imageInputs ...domaininference.ImageInput) (string, error) {
	if err := m.requireProcedureEnabledRuntime(ctx, inv); err != nil {
		return "", err
	}
	if m.inference == nil {
		run.Usage = automation.TokenUsage{Status: string(domaininference.UsageStatusFailed)}
		return "", ErrInferenceUnavailable
	}
	ref := def.Inference
	profileID, err := parseOptionalUUID(ref.ProfileID)
	if err != nil {
		return "", fmt.Errorf("automation inference profile_id must be a UUID: %w", err)
	}
	params := inferenceParameters(ref.Parameters)
	if m.maxOutputTokens > 0 {
		if params.MaxOutputTokens > int(m.maxOutputTokens) {
			return "", fmt.Errorf("automation output token ceiling exceeded by definition: %d > %d", params.MaxOutputTokens, m.maxOutputTokens)
		}
		if params.MaxOutputTokens == 0 {
			params.MaxOutputTokens = int(m.maxOutputTokens)
		}
	}
	if m.maxInputTokens > 0 {
		if params.MaxInputTokens > int(m.maxInputTokens) {
			return "", fmt.Errorf("automation input token ceiling exceeded by definition: %d > %d", params.MaxInputTokens, m.maxInputTokens)
		}
		if params.MaxInputTokens == 0 {
			params.MaxInputTokens = int(m.maxInputTokens)
		}
	}
	operation := domaininference.Operation(strings.ToLower(strings.TrimSpace(ref.Operation)))
	if operation == "" {
		operation = domaininference.OperationChat
	}
	actor := firstNonEmptyString(inv.ActorPrincipalID, automationActor)
	onBehalf := firstNonEmptyString(inv.OnBehalfOfPrincipalID, inv.OwnerPrincipalID, inv.ActorPrincipalID, automationActor)
	run.ActorPrincipalID = actor
	run.OnBehalfOfPrincipalID = onBehalf
	run.OwnerPrincipalID = firstNonEmptyString(inv.OwnerPrincipalID, inv.AutomationOwnerPrincipalID, def.OwnerPrincipalID)
	run.AutomationOwnerPrincipalID = firstNonEmptyString(inv.AutomationOwnerPrincipalID, run.OwnerPrincipalID)
	run.EventOriginPrincipalID = inv.EventOriginPrincipalID
	metadata := map[string]any{"automation_id": def.ID, "automation_version": def.Version, "automation_run_id": run.ID, "invocation_id": inv.ID, "automation_owner_principal_id": run.AutomationOwnerPrincipalID, "owner_principal_id": run.OwnerPrincipalID, "event_origin_principal_id": run.EventOriginPrincipalID, "target_alias": run.TargetAlias, "target_node_id": run.TargetNodeID}
	if run.BindingID != "" {
		metadata["binding_id"] = run.BindingID
		metadata["binding_version"] = run.BindingVersion
	}
	if run.ProcedureID != "" {
		metadata["procedure_id"] = run.ProcedureID
		metadata["procedure_version"] = run.ProcedureVersion
	}
	if len(imageInputs) > 0 {
		metadata["image_input_count"] = len(imageInputs)
		var totalBytes int64
		for _, image := range imageInputs {
			totalBytes += image.SizeBytes
		}
		metadata["image_input_total_bytes"] = totalBytes
	}
	attempt, err := m.beginProcedureInferenceAttempt(ctx, inv, run, operation, uuidString(profileID))
	if err != nil {
		return "", err
	}
	afterAuthorization := func(hookCtx context.Context, resolved inferenceruntime.ResolveResult) error {
		authorized, err := m.authorizeProcedureInferenceAttempt(hookCtx, attempt, resolved)
		if err != nil {
			return err
		}
		attempt = authorized
		return nil
	}
	beforeProviderAttempt := func(hookCtx context.Context, resolved inferenceruntime.ResolveResult) error {
		if err := m.requireProcedureEnabledRuntime(hookCtx, inv); err != nil {
			return err
		}
		refreshed, err := m.refreshProcedureInferenceAuthorization(hookCtx, attempt, resolved)
		if err != nil {
			return err
		}
		attempt = refreshed
		started, err := m.startProcedureInferenceAttempt(hookCtx, attempt)
		attempt = started
		if err != nil {
			return err
		}
		return nil
	}
	resp, invokeErr := m.inference.Invoke(ctx, inferenceruntime.InvokeRequest{Resolve: inferenceruntime.ResolveRequest{SpaceID: inv.SpaceID, DomainID: inv.DomainID.String(), ProcedureID: inv.ProcedureID, ProcedureVersion: inv.ProcedureVersion, BindingID: inv.BindingID, BindingVersion: inv.BindingVersion, InvocationID: inv.ID, RunID: run.ID, NodeID: firstNonEmptyString(run.TargetNodeID, inv.ChangedElementID), Operation: operation, UsageMode: domaininference.UsageModeAutomation, ProfileRef: strings.TrimSpace(ref.Profile), ProfileID: profileID, EndpointRef: strings.TrimSpace(ref.EndpointRef), ModelRef: strings.TrimSpace(ref.ModelRef), CapabilityRef: strings.TrimSpace(ref.CapabilityRef), ActorPrincipalID: actor, OnBehalfOfPrincipalID: onBehalf, Parameters: params, Metadata: metadata}, Prompt: def.Prompt, Input: rendered, ImageInputs: imageInputs, RequestID: run.ID, AutomationID: def.ID, AutomationRunID: run.ID, Metadata: metadata, AfterAuthorization: afterAuthorization, BeforeProviderAttempt: beforeProviderAttempt})
	populateRunFromInference(run, ref, resp)
	attemptState, completionErr := m.completeProcedureInferenceAttempt(ctx, attempt, resp, invokeErr)
	providerStarted := !attempt.StartedAt.IsZero()
	if completionErr != nil {
		// Once provider access may have occurred, failure to commit the terminal
		// transition leaves the durable started attempt explicitly ambiguous.
		return "", markInferenceGenerationError(completionErr, providerStarted, providerStarted)
	}
	if invokeErr != nil {
		if attemptState == automation.InferenceAttemptStateAmbiguous {
			// Preserve the infrastructure/provider cause for classification while
			// separately fencing retries through explicit ambiguity metadata.
			return "", markInferenceGenerationError(invokeErr, true, true)
		}
		if errors.Is(invokeErr, inferenceruntime.ErrDenied) && strings.TrimSpace(resp.Decision.Reason) != "" {
			return "", markInferenceGenerationError(fmt.Errorf("%w: %s", invokeErr, resp.Decision.Reason), providerStarted, false)
		}
		return "", markInferenceGenerationError(invokeErr, providerStarted, false)
	}
	if m.maxInputTokens > 0 && run.Usage.InputTokens > m.maxInputTokens {
		return "", markInferenceGenerationError(fmt.Errorf("automation input token ceiling exceeded: %d > %d", run.Usage.InputTokens, m.maxInputTokens), true, false)
	}
	if m.maxOutputTokens > 0 && run.Usage.OutputTokens > m.maxOutputTokens {
		return "", markInferenceGenerationError(fmt.Errorf("automation output token ceiling exceeded: %d > %d", run.Usage.OutputTokens, m.maxOutputTokens), true, false)
	}
	return resp.Text, nil
}

func (m *AutomationManager) revalidateProcedureAuthorization(ctx context.Context, def automation.Definition, inv automation.Invocation, run *automation.Run) error {
	resolver, ok := m.inference.(inferenceAuthorizer)
	if !ok {
		return ErrProcedureAuthorizationRevoked
	}
	if run == nil {
		return ErrProcedureAuthorizationRevoked
	}
	profileID, err := parseOptionalUUID(firstNonEmptyString(run.InferenceProfileID, def.Inference.ProfileID))
	if err != nil {
		return ErrProcedureAuthorizationRevoked
	}
	endpointID, err := parseOptionalUUID(run.ModelEndpointID)
	if err != nil {
		return ErrProcedureAuthorizationRevoked
	}
	modelID, err := parseOptionalUUID(run.ModelID)
	if err != nil {
		return ErrProcedureAuthorizationRevoked
	}
	capabilityID, err := parseOptionalUUID(run.CapabilityID)
	if err != nil {
		return ErrProcedureAuthorizationRevoked
	}
	operation := domaininference.Operation(strings.ToLower(strings.TrimSpace(def.Inference.Operation)))
	if operation == "" {
		operation = domaininference.OperationChat
	}
	resolved, err := resolver.Resolve(ctx, inferenceruntime.ResolveRequest{SpaceID: inv.SpaceID, DomainID: inv.DomainID.String(), ProcedureID: inv.ProcedureID, ProcedureVersion: inv.ProcedureVersion, BindingID: inv.BindingID, BindingVersion: inv.BindingVersion, InvocationID: inv.ID, RunID: run.ID, NodeID: firstNonEmptyString(run.TargetNodeID, inv.ChangedElementID), Operation: operation, UsageMode: domaininference.UsageModeAutomation, ProfileRef: strings.TrimSpace(def.Inference.Profile), ProfileID: profileID, EndpointID: endpointID, ModelID: modelID, CapabilityID: capabilityID, ActorPrincipalID: run.ActorPrincipalID, OnBehalfOfPrincipalID: run.OnBehalfOfPrincipalID, Parameters: inferenceParameters(def.Inference.Parameters)})
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return err
	}
	if !resolved.Allowed || resolved.Endpoint.ID != endpointID || resolved.Model.ID != modelID || resolved.Capability.ID != capabilityID || uuidString(resolved.CredentialGrant.ID) != run.CredentialGrantID {
		return ErrProcedureAuthorizationRevoked
	}
	run.PolicyDecisionID = uuidString(resolved.Decision.ID)
	run.AuthorizationRevision = resolved.Decision.AuthorizationRevision
	run.GlobalAuthorizationRevision = resolved.Decision.GlobalAuthorizationRevision
	run.SpaceAuthorizationRevision = resolved.Decision.SpaceAuthorizationRevision
	if run.AuthorizationRevision == 0 {
		return ErrProcedureAuthorizationRevoked
	}
	return nil
}

func inferenceParameters(in automation.InferenceParameters) domaininference.Parameters {
	return domaininference.Parameters{Temperature: in.Temperature, MaxInputTokens: in.MaxInputTokens, MaxOutputTokens: in.MaxOutputTokens, ResponseFormat: strings.TrimSpace(in.ResponseFormat), Metadata: in.Metadata}
}

func parseOptionalUUID(value string) (uuid.UUID, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return uuid.Nil, nil
	}
	return uuid.Parse(value)
}

func populateRunFromInference(run *automation.Run, ref automation.InferenceRef, resp inferenceruntime.InvokeResponse) {
	run.InferenceProfile = strings.TrimSpace(ref.Profile)
	if resp.Decision.ProfileID != uuid.Nil {
		run.InferenceProfileID = resp.Decision.ProfileID.String()
	} else {
		run.InferenceProfileID = strings.TrimSpace(ref.ProfileID)
	}
	run.ModelEndpointID = uuidString(resp.Decision.EndpointID)
	run.ModelID = uuidString(resp.Decision.ModelID)
	run.CapabilityID = uuidString(resp.Decision.CapabilityID)
	run.CredentialID = uuidString(resp.Decision.CredentialID)
	run.CredentialGrantID = uuidString(resp.Decision.CredentialGrantID)
	run.PolicyDecisionID = uuidString(resp.Decision.ID)
	run.AuthorizationRevision = resp.Decision.AuthorizationRevision
	run.GlobalAuthorizationRevision = resp.Decision.GlobalAuthorizationRevision
	run.SpaceAuthorizationRevision = resp.Decision.SpaceAuthorizationRevision
	run.ProviderRequestID = resp.ProviderRequestID
	run.InferenceUsageEventIDs = appendUniqueString(run.InferenceUsageEventIDs, resp.UsageEventID)
	status := resp.Status
	if status == "" {
		status = domaininference.UsageStatusSucceeded
	}
	run.Usage = automation.TokenUsage{InputTokens: resp.Usage.InputTokens, OutputTokens: resp.Usage.OutputTokens, TotalTokens: resp.Usage.TotalTokens, Status: string(status), Metadata: map[string]any{"token_count_source": resp.Usage.TokenCountSource}}
}

func uuidString[T ~[16]byte](id T) string {
	u := uuid.UUID(id)
	if u == uuid.Nil {
		return ""
	}
	return u.String()
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
