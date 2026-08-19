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
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	inferenceservice "github.com/myceldb/mycel/internal/inference/service"
	sessionservice "github.com/myceldb/mycel/internal/session/service"
)

const automationActor = "automation"

var ErrInferenceUnavailable = errors.New("automation inference subsystem is not configured")

func (m *AutomationManager) executeInvocation(ctx context.Context, def automation.Definition, inv automation.Invocation) (automation.Run, error) {
	now := m.now()
	run := automation.Run{ID: newRunID(), DomainID: inv.DomainID, InvocationID: inv.ID, AttemptNumber: inv.AttemptCount + 1, Status: "running", ActorPrincipalID: firstNonEmptyString(inv.ActorPrincipalID, automationActor), OnBehalfOfPrincipalID: firstNonEmptyString(inv.OnBehalfOfPrincipalID, def.OwnerPrincipalID, automationActor), AutomationOwnerPrincipalID: firstNonEmptyString(inv.AutomationOwnerPrincipalID, def.OwnerPrincipalID), InferenceProfile: strings.TrimSpace(def.Inference.Profile), InferenceProfileID: strings.TrimSpace(def.Inference.ProfileID), StartedAt: now}
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
	sess, err := m.sessions.OpenSession(ctx, sessionservice.OpenSessionInput{PrincipalID: automationActor, SpaceID: inv.SpaceID, DomainID: inv.DomainID.String()})
	if err != nil {
		return run, err
	}
	tx, err := m.sessions.BeginTransaction(ctx, sessionservice.BeginTransactionInput{PrincipalID: automationActor, SessionID: sess.ID, Mode: sessionservice.TransactionModeReadWrite})
	if err != nil {
		return run, err
	}
	node, err := m.graphs.GetNode(ctx, tx, inv.ChangedElementID)
	if err != nil {
		_, _ = m.sessions.RollbackTransaction(ctx, automationActor, tx.ID)
		return run, err
	}
	condition, err := m.evaluateCondition(ctx, tx, def, node, inv.OldNode)
	if err != nil {
		_, _ = m.sessions.RollbackTransaction(ctx, automationActor, tx.ID)
		return run, err
	} else if !condition.Matched {
		_, _ = m.sessions.RollbackTransaction(ctx, automationActor, tx.ID)
		run.Status = "skipped"
		run.Error = conditionFalseReason
		run.CompletedAt = m.now()
		return run, nil
	}
	rendered, err := render.Render(def.Input, render.Context{Changed: node, Old: inv.OldNode, Aliases: condition.Aliases})
	if err != nil {
		_, _ = m.sessions.RollbackTransaction(ctx, automationActor, tx.ID)
		return run, err
	}
	run.RenderedInputHash = rendered.Hash
	if def.Safety.Idempotency.SkipIfOutputUnchanged {
		duplicate, err := m.hasSuccessfulInputHash(ctx, inv, rendered.Hash)
		if err != nil {
			_, _ = m.sessions.RollbackTransaction(ctx, automationActor, tx.ID)
			return run, err
		}
		if duplicate {
			_, _ = m.sessions.RollbackTransaction(ctx, automationActor, tx.ID)
			run.Status = "skipped"
			run.Error = skipReasonDuplicateInput
			run.CompletedAt = m.now()
			return run, nil
		}
	}
	output, err := m.generateWithInference(ctx, def, inv, rendered.Text, &run)
	if err != nil {
		_, _ = m.sessions.RollbackTransaction(ctx, automationActor, tx.ID)
		return run, err
	}
	parsed, err := autooutput.Parse(def.Output.Mode, def.Output.Schema, output)
	if err != nil {
		_, _ = m.sessions.RollbackTransaction(ctx, automationActor, tx.ID)
		return run, err
	}
	run.ActionFingerprint = actionFingerprint(def, parsed)
	summary, err := (actions.Engine{Graphs: m.graphs}).Apply(ctx, tx, actions.Context{Definition: def, RunID: run.ID, Changed: node, Result: parsed})
	if err != nil {
		_, _ = m.sessions.RollbackTransaction(ctx, automationActor, tx.ID)
		return run, err
	}
	if !summary.Changed {
		_, _ = m.sessions.RollbackTransaction(ctx, automationActor, tx.ID)
		run.Status = "skipped"
		run.CompletedAt = m.now()
		return run, nil
	}
	graphCommit, err := m.graphs.CommitTransactionGraph(ctx, tx)
	if err != nil {
		return run, err
	}
	commit, err := m.sessions.CommitTransactionAtRevision(ctx, automationActor, tx.ID, graphCommit.OperationCount, graphCommit.CommittedRevision)
	if err != nil {
		return run, err
	}
	run.MutationID = commit.ID
	run.Status = "succeeded"
	run.CompletedAt = m.now()
	outSum := sha256.Sum256([]byte(output))
	run.OutputHash = hex.EncodeToString(outSum[:])
	return run, nil
}

func newRunID() string { return uuid.NewString() }

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
	onBehalf := firstNonEmptyString(inv.OnBehalfOfPrincipalID, inv.ActorPrincipalID, automationActor)
	run.ActorPrincipalID = actor
	run.OnBehalfOfPrincipalID = onBehalf
	run.AutomationOwnerPrincipalID = firstNonEmptyString(inv.AutomationOwnerPrincipalID, def.OwnerPrincipalID)
	resp, err := m.inference.Invoke(ctx, inferenceservice.InvokeRequest{Resolve: inferenceservice.ResolveRequest{SpaceID: inv.SpaceID, DomainID: inv.DomainID.String(), NodeID: inv.ChangedElementID, Operation: operation, UsageMode: domaininference.UsageModeAutomation, ProfileRef: strings.TrimSpace(ref.Profile), ProfileID: profileID, EndpointRef: strings.TrimSpace(ref.EndpointRef), ModelRef: strings.TrimSpace(ref.ModelRef), CapabilityRef: strings.TrimSpace(ref.CapabilityRef), ActorPrincipalID: actor, OnBehalfOfPrincipalID: onBehalf, Parameters: params, Metadata: map[string]any{"automation_id": def.ID, "automation_version": def.Version, "automation_run_id": run.ID, "invocation_id": inv.ID, "automation_owner_principal_id": run.AutomationOwnerPrincipalID}}, Prompt: def.Prompt, Input: rendered, RequestID: run.ID, AutomationID: def.ID, AutomationRunID: run.ID, Metadata: map[string]any{"invocation_id": inv.ID, "automation_version": def.Version, "automation_owner_principal_id": run.AutomationOwnerPrincipalID}})
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
