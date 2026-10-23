package service

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/storage"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	inferenceruntime "github.com/myceldb/mycel/internal/inference/runtime"
)

const (
	maxProcedureInferenceAttemptsPerSpace = 10_000
	maxProcedureInferenceChangesPerSpace  = 50_000
)

var ErrInferenceAttemptAlreadyStarted = errors.New("procedure inference attempt may already have reached the provider")

func automationRunID(inv automation.Invocation, runAttemptNumber int) string {
	key := strings.Join([]string{
		"mycel",
		"automation",
		"run",
		"v1",
		strings.TrimSpace(inv.SpaceID),
		inv.DomainID.String(),
		strings.TrimSpace(inv.ID),
		fmt.Sprintf("%d", runAttemptNumber),
	}, "\x00")
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(key)).String()
}

func procedureInferenceAttemptID(runID, workflowStepID string, providerAttemptNumber int) string {
	key := strings.Join([]string{
		"mycel",
		"automation",
		"procedure-inference-attempt",
		"v1",
		strings.TrimSpace(runID),
		strings.TrimSpace(workflowStepID),
		fmt.Sprintf("%d", providerAttemptNumber),
	}, "\x00")
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(key)).String()
}

func procedureInferenceChangeEventID(attemptID string, recordVersion uint64) string {
	key := strings.Join([]string{
		"mycel",
		"automation",
		"procedure-inference-attempt-change",
		"v1",
		strings.TrimSpace(attemptID),
		fmt.Sprintf("%d", recordVersion),
	}, "\x00")
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(key)).String()
}

func (m *AutomationManager) putProcedureInferenceAttemptRuntime(ctx context.Context, attempt automation.ProcedureInferenceAttempt) error {
	attempt = automation.NormalizeProcedureInferenceAttempt(attempt)
	if m.raftGroups != nil {
		if err := m.requireLocalExecutionLeader(ctx, attempt.SpaceID); err != nil {
			return err
		}
	}
	return mapStoreError(m.commitAutomationMutation(ctx, automationMutationRecord{
		Kind:     "procedure_inference_attempt.transition",
		DomainID: attempt.DomainID,
		SpaceID:  attempt.SpaceID,
		ID:       attempt.ID,
		Payload:  rawAutomation(attempt),
	}))
}

func (m *AutomationManager) applyProcedureInferenceAttemptTransition(ctx context.Context, incoming automation.ProcedureInferenceAttempt) error {
	m.inferenceAttemptMu.Lock()
	defer m.inferenceAttemptMu.Unlock()
	incoming = automation.NormalizeProcedureInferenceAttempt(incoming)
	ledger, err := m.store.GetProcedureInferenceLedger(ctx, incoming.SpaceID)
	if errors.Is(err, storage.ErrNotFound) {
		ledger = automation.ProcedureInferenceLedger{SpaceID: incoming.SpaceID}
	} else if err != nil {
		return mapStoreError(err)
	}
	if strings.TrimSpace(ledger.SpaceID) == "" {
		ledger.SpaceID = incoming.SpaceID
	}
	if ledger.SpaceID != incoming.SpaceID {
		return fmt.Errorf("procedure inference ledger space mismatch")
	}

	index := -1
	var current *automation.ProcedureInferenceAttempt
	for i := range ledger.Attempts {
		if ledger.Attempts[i].ID == incoming.ID {
			index = i
			copy := ledger.Attempts[i]
			current = &copy
			break
		}
	}
	if current != nil && current.RecordVersion == incoming.RecordVersion {
		if automationJSONEqual(*current, incoming) {
			return nil
		}
		return fmt.Errorf("procedure inference attempt %q version %d has different content", incoming.ID, incoming.RecordVersion)
	}
	if current != nil && current.RecordVersion > incoming.RecordVersion {
		return fmt.Errorf("stale procedure inference attempt transition for %q", incoming.ID)
	}
	if err := automation.ValidateProcedureInferenceAttemptTransition(current, incoming); err != nil {
		return err
	}
	if current == nil && len(ledger.Attempts) >= maxProcedureInferenceAttemptsPerSpace {
		return fmt.Errorf("procedure inference attempt capacity reached for space %s", incoming.SpaceID)
	}
	otherActiveStates := make([]string, 0, len(ledger.Attempts))
	for i := range ledger.Attempts {
		if ledger.Attempts[i].ID != incoming.ID {
			otherActiveStates = append(otherActiveStates, ledger.Attempts[i].State)
		}
	}
	if !procedureInferenceTransitionFits(len(ledger.Changes), incoming.State, otherActiveStates) {
		return fmt.Errorf("procedure inference change capacity reached for space %s", incoming.SpaceID)
	}

	ledger.HighWaterSequence++
	if ledger.EarliestRetainedSequence == 0 {
		ledger.EarliestRetainedSequence = ledger.HighWaterSequence
	}
	if ledger.EarliestRetainedStateSequence == 0 {
		ledger.EarliestRetainedStateSequence = ledger.HighWaterSequence
	}
	occurredAt := procedureInferenceAttemptOccurredAt(incoming)
	change := automation.ProcedureInferenceAttemptChange{
		EventID:       procedureInferenceChangeEventID(incoming.ID, incoming.RecordVersion),
		SchemaVersion: automation.InferenceAttemptChangeSchemaV1,
		SpaceID:       incoming.SpaceID,
		Sequence:      ledger.HighWaterSequence,
		ChangeKind:    incoming.State,
		AttemptID:     incoming.ID,
		RecordVersion: incoming.RecordVersion,
		Attempt:       incoming,
		OccurredAt:    occurredAt,
		CommittedAt:   occurredAt,
	}
	if index >= 0 {
		ledger.Attempts[index] = incoming
	} else {
		ledger.Attempts = append(ledger.Attempts, incoming)
		sort.Slice(ledger.Attempts, func(i, j int) bool { return ledger.Attempts[i].ID < ledger.Attempts[j].ID })
	}
	ledger.Changes = append(ledger.Changes, change)
	return mapStoreError(m.store.PutProcedureInferenceLedger(ctx, ledger))
}

func procedureInferenceTransitionFits(changeCount int, incomingState string, otherAttemptStates []string) bool {
	reservedTransitions := procedureInferenceReservedTransitions(incomingState)
	for _, state := range otherAttemptStates {
		reservedTransitions += procedureInferenceReservedTransitions(state)
	}
	return changeCount+1+reservedTransitions <= maxProcedureInferenceChangesPerSpace
}

func procedureInferenceReservedTransitions(state string) int {
	switch state {
	case automation.InferenceAttemptStateRequested:
		return 3 // authorized, started, terminal
	case automation.InferenceAttemptStateAuthorized:
		return 2 // started, terminal
	case automation.InferenceAttemptStateStarted:
		return 1 // terminal
	default:
		return 0
	}
}

func procedureInferenceAttemptOccurredAt(attempt automation.ProcedureInferenceAttempt) time.Time {
	if !attempt.CompletedAt.IsZero() {
		return attempt.CompletedAt.UTC()
	}
	if !attempt.StartedAt.IsZero() {
		return attempt.StartedAt.UTC()
	}
	if !attempt.AuthorizedAt.IsZero() {
		return attempt.AuthorizedAt.UTC()
	}
	return attempt.RequestedAt.UTC()
}

func (m *AutomationManager) getProcedureInferenceAttempt(ctx context.Context, spaceID string, attemptID string) (automation.ProcedureInferenceAttempt, error) {
	ledger, err := m.store.GetProcedureInferenceLedger(ctx, strings.TrimSpace(spaceID))
	if err != nil {
		return automation.ProcedureInferenceAttempt{}, err
	}
	for _, attempt := range ledger.Attempts {
		if attempt.ID == strings.TrimSpace(attemptID) {
			return attempt, nil
		}
	}
	return automation.ProcedureInferenceAttempt{}, storage.ErrNotFound
}

func (m *AutomationManager) beginProcedureInferenceAttempt(ctx context.Context, inv automation.Invocation, run *automation.Run, operation domaininference.Operation, profileID string) (automation.ProcedureInferenceAttempt, error) {
	if run == nil {
		return automation.ProcedureInferenceAttempt{}, fmt.Errorf("automation run is required")
	}
	requested := newRequestedProcedureInferenceAttempt(inv, *run, string(operation), profileID, m.now())
	existing, err := m.getProcedureInferenceAttempt(ctx, inv.SpaceID, requested.ID)
	switch {
	case err == nil:
		if existing.State == automation.InferenceAttemptStateStarted || automation.IsTerminalProcedureInferenceAttemptState(existing.State) {
			run.InferenceAttemptIDs = appendUniqueString(run.InferenceAttemptIDs, existing.ID)
			return existing, ErrInferenceAttemptAlreadyStarted
		}
		requested = existing
	case errors.Is(err, storage.ErrNotFound):
		if err := m.putProcedureInferenceAttemptRuntime(ctx, requested); err != nil {
			return automation.ProcedureInferenceAttempt{}, err
		}
	default:
		return automation.ProcedureInferenceAttempt{}, mapStoreError(err)
	}
	run.InferenceAttemptIDs = appendUniqueString(run.InferenceAttemptIDs, requested.ID)
	return requested, nil
}

func (m *AutomationManager) authorizeProcedureInferenceAttempt(ctx context.Context, attempt automation.ProcedureInferenceAttempt, resolved inferenceruntime.ResolveResult) (automation.ProcedureInferenceAttempt, error) {
	if attempt.State == automation.InferenceAttemptStateAuthorized {
		return attempt, nil
	}
	if attempt.State != automation.InferenceAttemptStateRequested {
		return attempt, fmt.Errorf("cannot authorize procedure inference attempt in state %q", attempt.State)
	}
	authorized := attempt
	authorized.RecordVersion++
	authorized.State = automation.InferenceAttemptStateAuthorized
	authorized.AuthorizedAt = m.now().UTC()
	authorized.ProfileID = firstNonEmptyString(uuidString(resolved.Decision.ProfileID), authorized.ProfileID)
	authorized.EndpointID = uuidString(resolved.Decision.EndpointID)
	authorized.ModelID = uuidString(resolved.Decision.ModelID)
	authorized.CapabilityID = uuidString(resolved.Decision.CapabilityID)
	authorized.PolicyDecisionID = uuidString(resolved.Decision.ID)
	authorized.CredentialGrantID = uuidString(resolved.Decision.CredentialGrantID)
	if err := m.putProcedureInferenceAttemptRuntime(ctx, authorized); err != nil {
		return attempt, err
	}
	return authorized, nil
}

func (m *AutomationManager) startProcedureInferenceAttempt(ctx context.Context, attempt automation.ProcedureInferenceAttempt) (automation.ProcedureInferenceAttempt, error) {
	if attempt.State != automation.InferenceAttemptStateAuthorized {
		return attempt, fmt.Errorf("cannot start procedure inference attempt in state %q", attempt.State)
	}
	started := attempt
	started.RecordVersion++
	started.State = automation.InferenceAttemptStateStarted
	started.StartedAt = m.now().UTC()
	if err := m.putProcedureInferenceAttemptRuntime(ctx, started); err != nil {
		return attempt, err
	}
	return started, nil
}

func (m *AutomationManager) completeProcedureInferenceAttempt(ctx context.Context, attempt automation.ProcedureInferenceAttempt, resp inferenceruntime.InvokeResponse, invokeErr error) (string, error) {
	completed := attempt
	completed.RecordVersion++
	completed.CompletedAt = m.now().UTC()
	if !attempt.StartedAt.IsZero() && completed.CompletedAt.After(attempt.StartedAt) {
		completed.ProviderDurationMillis = completed.CompletedAt.Sub(attempt.StartedAt).Milliseconds()
	}
	completed.ProfileID = firstNonEmptyString(uuidString(resp.Decision.ProfileID), completed.ProfileID)
	completed.EndpointID = uuidString(resp.Decision.EndpointID)
	completed.ModelID = uuidString(resp.Decision.ModelID)
	completed.CapabilityID = uuidString(resp.Decision.CapabilityID)
	completed.PolicyDecisionID = uuidString(resp.Decision.ID)
	completed.CredentialGrantID = uuidString(resp.Decision.CredentialGrantID)
	completed.InputTokens = resp.Usage.InputTokens
	completed.OutputTokens = resp.Usage.OutputTokens
	completed.TotalTokens = resp.Usage.TotalTokens
	completed.TokenCountAuthority = strings.TrimSpace(resp.Usage.TokenCountSource)
	switch {
	case invokeErr == nil && attempt.State == automation.InferenceAttemptStateStarted:
		completed.State = automation.InferenceAttemptStateSucceeded
		completed.OutcomeCategory = "succeeded"
	case attempt.State == automation.InferenceAttemptStateStarted:
		// Once started is durable, an error is conservatively ambiguous because
		// the daemon cannot prove that the provider did not accept the request.
		completed.State = automation.InferenceAttemptStateAmbiguous
		completed.OutcomeCategory = "provider_outcome_unknown"
	case errors.Is(invokeErr, inferenceruntime.ErrDenied):
		completed.State = automation.InferenceAttemptStateDenied
		completed.OutcomeCategory = "authorization_denied"
	case errors.Is(invokeErr, context.Canceled), errors.Is(invokeErr, context.DeadlineExceeded):
		completed.State = automation.InferenceAttemptStateCanceled
		completed.OutcomeCategory = "canceled_before_provider"
	default:
		completed.State = automation.InferenceAttemptStateFailed
		completed.OutcomeCategory = "failed_before_provider"
	}
	finalizeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := m.putProcedureInferenceAttemptRuntime(finalizeCtx, completed); err != nil {
		return "", err
	}
	return completed.State, nil
}

func (m *AutomationManager) reconcilePreProviderTerminalAttempt(ctx context.Context, inv *automation.Invocation, def automation.Definition) (bool, error) {
	if inv == nil {
		return false, nil
	}
	ledger, err := m.store.GetProcedureInferenceLedger(ctx, inv.SpaceID)
	if errors.Is(err, storage.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, mapStoreError(err)
	}
	nextAttemptNumber := inv.AttemptCount + 1
	var recovered *automation.ProcedureInferenceAttempt
	for i := range ledger.Attempts {
		attempt := ledger.Attempts[i]
		if attempt.InvocationID == inv.ID && attempt.RunAttemptNumber == nextAttemptNumber && automation.IsTerminalProcedureInferenceAttemptState(attempt.State) && attempt.StartedAt.IsZero() {
			copy := attempt
			recovered = &copy
			break
		}
	}
	if recovered == nil {
		return false, nil
	}
	now := m.now()
	inv.AttemptCount = recovered.RunAttemptNumber
	inv.UpdatedAt = now
	inv.NextAttemptAt = time.Time{}
	stop := recovered.State == automation.InferenceAttemptStateDenied || inv.AttemptCount >= maxAttempts(def)
	if stop {
		inv.Status = "failed"
		inv.SkipReason = recovered.OutcomeCategory
	} else {
		// Keep the reclaimed claim while durably advancing the logical attempt.
		// The next execution therefore allocates a different deterministic run
		// and provider-attempt identity without waiting for another scan.
		inv.Status = "running"
	}
	if err := m.putInvocationRuntime(ctx, *inv); err != nil {
		return false, err
	}
	runStatus := "retryable"
	if stop {
		runStatus = "failed"
	}
	run := automation.Run{ID: recovered.RunID, DomainID: inv.DomainID, InvocationID: inv.ID, BindingID: inv.BindingID, BindingVersion: inv.BindingVersion, ProcedureID: inv.ProcedureID, ProcedureVersion: inv.ProcedureVersion, AttemptNumber: recovered.RunAttemptNumber, Status: runStatus, Error: recovered.OutcomeCategory, InferenceAttemptIDs: []string{recovered.ID}, ClaimOwnerNodeID: inv.ClaimOwnerNodeID, ClaimVersion: inv.ClaimVersion, ClaimToken: inv.ClaimToken, ActorPrincipalID: recovered.ActorPrincipalID, OwnerPrincipalID: recovered.OwnerPrincipalID, OnBehalfOfPrincipalID: recovered.OnBehalfOfPrincipalID, EventOriginPrincipalID: recovered.EventOriginPrincipalID, StartedAt: recovered.RequestedAt, CompletedAt: recovered.CompletedAt}
	if err := m.putRunRuntime(ctx, inv.SpaceID, run); err != nil {
		return false, err
	}
	return stop, nil
}

func appendUniqueString(values []string, value string) []string {
	value = strings.TrimSpace(value)
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	if value == "" {
		return values
	}
	return append(values, value)
}

func newRequestedProcedureInferenceAttempt(inv automation.Invocation, run automation.Run, operation, profileID string, now time.Time) automation.ProcedureInferenceAttempt {
	const providerAttemptNumber = 1
	return automation.ProcedureInferenceAttempt{
		ID:                     procedureInferenceAttemptID(run.ID, "", providerAttemptNumber),
		RecordVersion:          1,
		SpaceID:                inv.SpaceID,
		DomainID:               inv.DomainID,
		ProcedureID:            inv.ProcedureID,
		ProcedureVersion:       inv.ProcedureVersion,
		BindingID:              inv.BindingID,
		BindingVersion:         inv.BindingVersion,
		InvocationID:           inv.ID,
		RunID:                  run.ID,
		RunAttemptNumber:       run.AttemptNumber,
		ProviderAttemptNumber:  providerAttemptNumber,
		SourceEventID:          inv.EventID,
		EventOriginPrincipalID: inv.EventOriginPrincipalID,
		ActorPrincipalID:       run.ActorPrincipalID,
		OwnerPrincipalID:       run.OwnerPrincipalID,
		OnBehalfOfPrincipalID:  run.OnBehalfOfPrincipalID,
		Operation:              operation,
		ProfileID:              strings.TrimSpace(profileID),
		State:                  automation.InferenceAttemptStateRequested,
		RequestedAt:            now.UTC(),
	}
}
