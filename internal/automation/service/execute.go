package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/automation/actions"
	automation "github.com/myceldb/mycel/internal/automation/model"
	autooutput "github.com/myceldb/mycel/internal/automation/output"
	"github.com/myceldb/mycel/internal/automation/render"
	graph "github.com/myceldb/mycel/internal/graph/model"
	graphservice "github.com/myceldb/mycel/internal/graph/service"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	inferenceservice "github.com/myceldb/mycel/internal/inference/service"
	sessionservice "github.com/myceldb/mycel/internal/session/service"
)

const automationActor = "automation"

var ErrInferenceUnavailable = errors.New("automation inference subsystem is not configured")

func (m *AutomationManager) executeInvocation(ctx context.Context, def automation.Definition, inv automation.Invocation) (automation.Run, error) {
	now := m.now()
	runID := newRunID()
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
	sess, err := m.sessions.OpenSession(ctx, sessionservice.OpenSessionInput{PrincipalID: executionPrincipal, SpaceID: inv.SpaceID, DomainID: inv.DomainID.String()})
	if err != nil {
		return run, err
	}
	baseRevision, err := m.graphs.CurrentRevision(ctx, inv.SpaceID)
	if err != nil {
		return run, err
	}
	tx, err := m.sessions.BeginTransaction(ctx, sessionservice.BeginTransactionInput{PrincipalID: executionPrincipal, SessionID: sess.ID, Mode: sessionservice.TransactionModeReadWrite, BaseRevision: &baseRevision})
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
	engine := actions.Engine{Graphs: m.graphs}
	applied, err := engine.OutputAlreadyApplied(ctx, tx, run.OutputIdempotencyKey)
	if err != nil {
		m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
		return run, err
	}
	if applied {
		m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
		run.Status = "skipped"
		run.Error = skipReasonDuplicateOutput
		run.CompletedAt = m.now()
		return run, nil
	}
	if err := m.ensureClaimStillOwned(ctx, inv); err != nil {
		m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
		return run, err
	}
	output, err := m.generateWithInference(ctx, def, inv, rendered.Text, &run)
	if err != nil {
		m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
		return run, err
	}
	parsed, err := autooutput.Parse(def.Output.Mode, def.Output.Schema, output)
	if err != nil {
		m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
		return run, err
	}
	run.ActionFingerprint = actionFingerprint(def, parsed)
	changed, err := m.commitAutomationOutputWithConflictRetry(ctx, executionPrincipal, sess, tx, def, &inv, &run, node, condition.Aliases, parsed)
	if err != nil {
		return run, err
	}
	if !changed {
		run.Status = "skipped"
		run.CompletedAt = m.now()
		return run, nil
	}
	run.Status = "succeeded"
	run.CompletedAt = m.now()
	outSum := sha256.Sum256([]byte(output))
	run.OutputHash = hex.EncodeToString(outSum[:])
	return run, nil
}

func (m *AutomationManager) commitAutomationOutputWithConflictRetry(ctx context.Context, executionPrincipal string, sess sessionservice.GraphSession, firstTx sessionservice.GraphTransaction, def automation.Definition, inv *automation.Invocation, run *automation.Run, firstNode graph.Node, firstAliases map[string]any, parsed autooutput.Result) (bool, error) {
	const maxAttempts = 3
	engine := actions.Engine{Graphs: m.graphs}
	tx := firstTx
	node := firstNode
	aliases := firstAliases
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if attempt > 1 {
			var err error
			tx, node, aliases, err = m.reopenAutomationOutputTransaction(ctx, executionPrincipal, sess, def, *inv, run)
			if err != nil {
				return false, err
			}
		}
		if err := m.ensureClaimStillOwned(ctx, *inv); err != nil {
			m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
			return false, err
		}
		if err := m.renewClaim(ctx, inv); err != nil {
			m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
			return false, err
		}
		run.ClaimOwnerNodeID = inv.ClaimOwnerNodeID
		run.ClaimVersion = inv.ClaimVersion
		run.ClaimToken = inv.ClaimToken
		applied, err := engine.OutputAlreadyApplied(ctx, tx, run.OutputIdempotencyKey)
		if err != nil {
			m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
			return false, err
		}
		if applied {
			m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
			run.Status = "skipped"
			run.Error = skipReasonDuplicateOutput
			return false, nil
		}
		summary, err := engine.Apply(ctx, tx, actions.Context{Definition: def, RunID: run.ID, InvocationID: inv.ID, BindingID: inv.BindingID, TargetNodeID: run.TargetNodeID, ClaimOwnerNodeID: inv.ClaimOwnerNodeID, ClaimVersion: inv.ClaimVersion, ClaimToken: inv.ClaimToken, OutputIdempotencyKey: run.OutputIdempotencyKey, Changed: node, Aliases: aliases, Result: parsed})
		if err != nil {
			m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
			return false, err
		}
		if !summary.Changed {
			m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
			return false, nil
		}
		if err := m.renewClaim(ctx, inv); err != nil {
			m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
			return false, err
		}
		run.ClaimOwnerNodeID = inv.ClaimOwnerNodeID
		run.ClaimVersion = inv.ClaimVersion
		run.ClaimToken = inv.ClaimToken
		graphCommit, err := m.graphs.CommitTransactionGraph(ctx, tx)
		if err != nil {
			m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
			if isGraphConflict(err) && attempt < maxAttempts {
				continue
			}
			return false, err
		}
		commit, err := m.sessions.CommitTransactionAtRevision(ctx, executionPrincipal, tx.ID, graphCommit.OperationCount, graphCommit.CommittedRevision)
		if err != nil {
			return false, err
		}
		run.MutationID = commit.ID
		return true, nil
	}
	return false, graphservice.ErrConflict
}

func (m *AutomationManager) reopenAutomationOutputTransaction(ctx context.Context, executionPrincipal string, sess sessionservice.GraphSession, def automation.Definition, inv automation.Invocation, run *automation.Run) (sessionservice.GraphTransaction, graph.Node, map[string]any, error) {
	baseRevision, err := m.graphs.CurrentRevision(ctx, inv.SpaceID)
	if err != nil {
		return sessionservice.GraphTransaction{}, graph.Node{}, nil, err
	}
	tx, err := m.sessions.BeginTransaction(ctx, sessionservice.BeginTransactionInput{PrincipalID: executionPrincipal, SessionID: sess.ID, Mode: sessionservice.TransactionModeReadWrite, BaseRevision: &baseRevision})
	if err != nil {
		return sessionservice.GraphTransaction{}, graph.Node{}, nil, err
	}
	node, err := m.graphs.GetNode(ctx, tx, inv.ChangedElementID)
	if err != nil {
		m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
		return sessionservice.GraphTransaction{}, graph.Node{}, nil, err
	}
	condition, err := m.evaluateCondition(ctx, tx, def, node, inv.OldNode)
	if err != nil {
		m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
		return sessionservice.GraphTransaction{}, graph.Node{}, nil, err
	}
	if !condition.Matched {
		m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
		return sessionservice.GraphTransaction{}, graph.Node{}, nil, fmt.Errorf(conditionFalseReason)
	}
	targetAlias := firstNonEmptyString(strings.TrimSpace(def.Input.Target), "changed")
	targetNodeID, err := requireNodeAliasID(targetAlias, condition.Aliases, node)
	if err != nil {
		m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
		return sessionservice.GraphTransaction{}, graph.Node{}, nil, err
	}
	run.TargetAlias = targetAlias
	run.TargetNodeID = targetNodeID
	if strings.TrimSpace(def.Safety.Idempotency.Scope) == "target" {
		idempotencyAlias := firstNonEmptyString(def.Safety.Idempotency.Target, run.TargetAlias)
		idempotencyTargetID, err := requireNodeAliasID(idempotencyAlias, condition.Aliases, node)
		if err != nil {
			m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
			return sessionservice.GraphTransaction{}, graph.Node{}, nil, err
		}
		run.IdempotencyTargetNodeID = idempotencyTargetID
	}
	if err := preflightActionTargets(def, condition.Aliases, node); err != nil {
		m.rollbackAutomationOutputTransaction(ctx, executionPrincipal, tx.ID)
		return sessionservice.GraphTransaction{}, graph.Node{}, nil, err
	}
	return tx, node, condition.Aliases, nil
}

func (m *AutomationManager) rollbackAutomationOutputTransaction(ctx context.Context, principalID string, transactionID string) {
	if m.graphs != nil {
		m.graphs.DiscardTransactionGraph(ctx, transactionID)
	}
	if m.sessions != nil {
		_, _ = m.sessions.RollbackTransaction(ctx, principalID, transactionID)
	}
}

func isGraphConflict(err error) bool {
	return errors.Is(err, graphservice.ErrConflict) || strings.Contains(strings.ToLower(err.Error()), "graph conflict")
}

func newRunID() string { return uuid.NewString() }

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

func (m *AutomationManager) generateWithInference(ctx context.Context, def automation.Definition, inv automation.Invocation, rendered string, run *automation.Run) (string, error) {
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
	resp, err := m.inference.Invoke(ctx, inferenceservice.InvokeRequest{Resolve: inferenceservice.ResolveRequest{SpaceID: inv.SpaceID, DomainID: inv.DomainID.String(), NodeID: firstNonEmptyString(run.TargetNodeID, inv.ChangedElementID), Operation: operation, UsageMode: domaininference.UsageModeAutomation, ProfileRef: strings.TrimSpace(ref.Profile), ProfileID: profileID, EndpointRef: strings.TrimSpace(ref.EndpointRef), ModelRef: strings.TrimSpace(ref.ModelRef), CapabilityRef: strings.TrimSpace(ref.CapabilityRef), ActorPrincipalID: actor, OnBehalfOfPrincipalID: onBehalf, Parameters: params, Metadata: metadata}, Prompt: def.Prompt, Input: rendered, RequestID: run.ID, AutomationID: def.ID, AutomationRunID: run.ID, Metadata: metadata})
	populateRunFromInference(run, ref, resp)
	if err != nil {
		if errors.Is(err, inferenceservice.ErrDenied) && strings.TrimSpace(resp.Decision.Reason) != "" {
			return "", fmt.Errorf("%w: %s", err, resp.Decision.Reason)
		}
		return "", err
	}
	if m.maxInputTokens > 0 && run.Usage.InputTokens > m.maxInputTokens {
		return "", fmt.Errorf("automation input token ceiling exceeded: %d > %d", run.Usage.InputTokens, m.maxInputTokens)
	}
	if m.maxOutputTokens > 0 && run.Usage.OutputTokens > m.maxOutputTokens {
		return "", fmt.Errorf("automation output token ceiling exceeded: %d > %d", run.Usage.OutputTokens, m.maxOutputTokens)
	}
	return resp.Text, nil
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

func populateRunFromInference(run *automation.Run, ref automation.InferenceRef, resp inferenceservice.InvokeResponse) {
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
	run.ProviderRequestID = resp.ProviderRequestID
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
