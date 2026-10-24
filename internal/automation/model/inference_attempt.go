package model

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	graph "github.com/myceldb/mycel/internal/graph/model"
)

const (
	InferenceAttemptStateRequested  = "requested"
	InferenceAttemptStateAuthorized = "authorized"
	InferenceAttemptStateStarted    = "started"
	InferenceAttemptStateSucceeded  = "succeeded"
	InferenceAttemptStateFailed     = "failed"
	InferenceAttemptStateDenied     = "denied"
	InferenceAttemptStateCanceled   = "canceled"
	InferenceAttemptStateAmbiguous  = "ambiguous"

	InferenceAttemptChangeSchemaV1 = "procedure_inference_attempt_change.v1"

	// ProcedureInferenceEvidenceStringMaxBytes bounds every persisted free-form identity/correlation string.
	ProcedureInferenceEvidenceStringMaxBytes = 1024
	// ProcedureInferenceAttemptMaxBytes bounds one encoded authoritative attempt record.
	ProcedureInferenceAttemptMaxBytes = 64 << 10
)

// ProcedureInferenceAttempt is the authoritative, sanitized record for one
// possible provider side effect initiated by an automation run. It deliberately
// excludes prompts, outputs, graph values, credentials, provider request IDs,
// raw errors, and arbitrary metadata.
type ProcedureInferenceAttempt struct {
	ID                          string         `json:"id"`
	RecordVersion               uint64         `json:"record_version"`
	SpaceID                     string         `json:"space_id"`
	DomainID                    graph.DomainID `json:"domain_id"`
	ProcedureID                 string         `json:"procedure_id"`
	ProcedureVersion            int            `json:"procedure_version"`
	BindingID                   string         `json:"binding_id"`
	BindingVersion              int            `json:"binding_version"`
	InvocationID                string         `json:"invocation_id"`
	RunID                       string         `json:"run_id"`
	RunAttemptNumber            int            `json:"run_attempt_number"`
	WorkflowStepID              string         `json:"workflow_step_id,omitempty"`
	ProviderAttemptNumber       int            `json:"provider_attempt_number"`
	SourceEventID               string         `json:"source_event_id,omitempty"`
	SourceTransactionID         string         `json:"source_transaction_id,omitempty"`
	SourceGraphRevision         uint64         `json:"source_graph_revision,omitempty"`
	EventOriginPrincipalID      string         `json:"event_origin_principal_id,omitempty"`
	ActorPrincipalID            string         `json:"actor_principal_id,omitempty"`
	OwnerPrincipalID            string         `json:"owner_principal_id,omitempty"`
	OnBehalfOfPrincipalID       string         `json:"on_behalf_of_principal_id,omitempty"`
	Operation                   string         `json:"operation,omitempty"`
	ProfileID                   string         `json:"profile_id,omitempty"`
	EndpointID                  string         `json:"endpoint_id,omitempty"`
	ModelID                     string         `json:"model_id,omitempty"`
	CapabilityID                string         `json:"capability_id,omitempty"`
	PolicyDecisionID            string         `json:"policy_decision_id,omitempty"`
	CredentialGrantID           string         `json:"credential_grant_id,omitempty"`
	AuthorizationRevision       uint64         `json:"authorization_revision,omitempty"`
	GlobalAuthorizationRevision uint64         `json:"global_authorization_revision,omitempty"`
	SpaceAuthorizationRevision  uint64         `json:"space_authorization_revision,omitempty"`
	ConfigurationRevision       uint64         `json:"configuration_revision,omitempty"`
	State                       string         `json:"state"`
	OutcomeCategory             string         `json:"outcome_category,omitempty"`
	InputTokens                 int64          `json:"input_tokens,omitempty"`
	OutputTokens                int64          `json:"output_tokens,omitempty"`
	TotalTokens                 int64          `json:"total_tokens,omitempty"`
	TokenCountAuthority         string         `json:"token_count_authority,omitempty"`
	ProviderDurationMillis      int64          `json:"provider_duration_millis,omitempty"`
	PublicationDisposition      string         `json:"publication_disposition,omitempty"`
	RequestedAt                 time.Time      `json:"requested_at"`
	AuthorizedAt                time.Time      `json:"authorized_at,omitempty"`
	StartedAt                   time.Time      `json:"started_at,omitempty"`
	CompletedAt                 time.Time      `json:"completed_at,omitempty"`
}

// ProcedureInferenceAttemptChange is an immutable transition envelope. Sequence
// is monotonic within SpaceID and is assigned by the automation state machine.
type ProcedureInferenceAttemptChange struct {
	EventID       string                    `json:"event_id"`
	SchemaVersion string                    `json:"schema_version"`
	SpaceID       string                    `json:"space_id"`
	Sequence      uint64                    `json:"sequence"`
	ChangeKind    string                    `json:"change_kind"`
	AttemptID     string                    `json:"attempt_id"`
	RecordVersion uint64                    `json:"record_version"`
	Attempt       ProcedureInferenceAttempt `json:"attempt"`
	OccurredAt    time.Time                 `json:"occurred_at"`
	CommittedAt   time.Time                 `json:"committed_at"`
}

// ProcedureInferenceLedger atomically stores materialized attempts and their
// append-only change feed for one space. Retention watermarks are explicit so a
// future query cannot mistake pruned history for an empty range.
type ProcedureInferenceLedger struct {
	SpaceID                       string                            `json:"space_id"`
	HighWaterSequence             uint64                            `json:"high_water_sequence"`
	EarliestRetainedSequence      uint64                            `json:"earliest_retained_sequence,omitempty"`
	EarliestRetainedStateSequence uint64                            `json:"earliest_retained_state_sequence,omitempty"`
	Attempts                      []ProcedureInferenceAttempt       `json:"attempts,omitempty"`
	Changes                       []ProcedureInferenceAttemptChange `json:"changes,omitempty"`
}

func NormalizeProcedureInferenceAttempt(in ProcedureInferenceAttempt) ProcedureInferenceAttempt {
	in.ID = strings.TrimSpace(in.ID)
	in.SpaceID = strings.TrimSpace(in.SpaceID)
	in.ProcedureID = strings.TrimSpace(in.ProcedureID)
	in.BindingID = strings.TrimSpace(in.BindingID)
	in.InvocationID = strings.TrimSpace(in.InvocationID)
	in.RunID = strings.TrimSpace(in.RunID)
	in.WorkflowStepID = strings.TrimSpace(in.WorkflowStepID)
	in.SourceEventID = strings.TrimSpace(in.SourceEventID)
	in.SourceTransactionID = strings.TrimSpace(in.SourceTransactionID)
	in.EventOriginPrincipalID = strings.TrimSpace(in.EventOriginPrincipalID)
	in.ActorPrincipalID = strings.TrimSpace(in.ActorPrincipalID)
	in.OwnerPrincipalID = strings.TrimSpace(in.OwnerPrincipalID)
	in.OnBehalfOfPrincipalID = strings.TrimSpace(in.OnBehalfOfPrincipalID)
	in.Operation = strings.ToLower(strings.TrimSpace(in.Operation))
	in.ProfileID = strings.TrimSpace(in.ProfileID)
	in.EndpointID = strings.TrimSpace(in.EndpointID)
	in.ModelID = strings.TrimSpace(in.ModelID)
	in.CapabilityID = strings.TrimSpace(in.CapabilityID)
	in.PolicyDecisionID = strings.TrimSpace(in.PolicyDecisionID)
	in.CredentialGrantID = strings.TrimSpace(in.CredentialGrantID)
	in.State = strings.ToLower(strings.TrimSpace(in.State))
	in.OutcomeCategory = strings.ToLower(strings.TrimSpace(in.OutcomeCategory))
	in.TokenCountAuthority = strings.ToLower(strings.TrimSpace(in.TokenCountAuthority))
	in.PublicationDisposition = strings.ToLower(strings.TrimSpace(in.PublicationDisposition))
	return in
}

func ValidateProcedureInferenceAttempt(in ProcedureInferenceAttempt) error {
	in = NormalizeProcedureInferenceAttempt(in)
	if in.ID == "" || in.SpaceID == "" || in.DomainID == (graph.DomainID{}) {
		return fmt.Errorf("procedure inference attempt id, space_id, and domain_id are required")
	}
	if in.ProcedureID == "" || in.ProcedureVersion < 1 || in.BindingID == "" || in.BindingVersion < 1 {
		return fmt.Errorf("procedure inference attempt requires pinned procedure and binding versions")
	}
	if in.InvocationID == "" || in.RunID == "" || in.RunAttemptNumber < 1 || in.ProviderAttemptNumber < 1 {
		return fmt.Errorf("procedure inference attempt requires invocation, run, and positive attempt numbers")
	}
	if in.RecordVersion < 1 || in.RequestedAt.IsZero() {
		return fmt.Errorf("procedure inference attempt requires record_version and requested_at")
	}
	if !validInferenceAttemptState(in.State) {
		return fmt.Errorf("unsupported procedure inference attempt state %q", in.State)
	}
	if in.InputTokens < 0 || in.OutputTokens < 0 || in.TotalTokens < 0 || in.ProviderDurationMillis < 0 {
		return fmt.Errorf("procedure inference attempt counters cannot be negative")
	}
	boundedStrings := []struct {
		name  string
		value string
	}{
		{name: "id", value: in.ID}, {name: "space_id", value: in.SpaceID}, {name: "procedure_id", value: in.ProcedureID},
		{name: "binding_id", value: in.BindingID}, {name: "invocation_id", value: in.InvocationID}, {name: "run_id", value: in.RunID},
		{name: "workflow_step_id", value: in.WorkflowStepID}, {name: "source_event_id", value: in.SourceEventID},
		{name: "source_transaction_id", value: in.SourceTransactionID}, {name: "event_origin_principal_id", value: in.EventOriginPrincipalID},
		{name: "actor_principal_id", value: in.ActorPrincipalID}, {name: "owner_principal_id", value: in.OwnerPrincipalID},
		{name: "on_behalf_of_principal_id", value: in.OnBehalfOfPrincipalID}, {name: "operation", value: in.Operation},
		{name: "profile_id", value: in.ProfileID}, {name: "endpoint_id", value: in.EndpointID}, {name: "model_id", value: in.ModelID},
		{name: "capability_id", value: in.CapabilityID}, {name: "policy_decision_id", value: in.PolicyDecisionID},
		{name: "credential_grant_id", value: in.CredentialGrantID},
	}
	for _, field := range boundedStrings {
		if len(field.value) > ProcedureInferenceEvidenceStringMaxBytes {
			return fmt.Errorf("procedure inference attempt %s exceeds %d bytes", field.name, ProcedureInferenceEvidenceStringMaxBytes)
		}
	}
	if !validInferenceOutcomeCategory(in.OutcomeCategory) {
		return fmt.Errorf("unsupported procedure inference outcome category %q", in.OutcomeCategory)
	}
	if !validInferenceTokenCountAuthority(in.TokenCountAuthority) {
		return fmt.Errorf("unsupported procedure inference token count authority %q", in.TokenCountAuthority)
	}
	if !validInferencePublicationDisposition(in.PublicationDisposition) {
		return fmt.Errorf("unsupported procedure inference publication disposition %q", in.PublicationDisposition)
	}
	encoded, err := json.Marshal(in)
	if err != nil {
		return fmt.Errorf("encode procedure inference attempt: %w", err)
	}
	if len(encoded) > ProcedureInferenceAttemptMaxBytes {
		return fmt.Errorf("procedure inference attempt exceeds %d bytes", ProcedureInferenceAttemptMaxBytes)
	}
	return nil
}

func ValidateProcedureInferenceAttemptTransition(current *ProcedureInferenceAttempt, next ProcedureInferenceAttempt) error {
	if err := ValidateProcedureInferenceAttempt(next); err != nil {
		return err
	}
	if current == nil {
		if next.RecordVersion != 1 || next.State != InferenceAttemptStateRequested {
			return fmt.Errorf("new procedure inference attempt must start at requested version 1")
		}
		return nil
	}
	cur := NormalizeProcedureInferenceAttempt(*current)
	next = NormalizeProcedureInferenceAttempt(next)
	if cur.ID != next.ID || cur.SpaceID != next.SpaceID || cur.DomainID != next.DomainID || cur.InvocationID != next.InvocationID || cur.RunID != next.RunID || cur.ProviderAttemptNumber != next.ProviderAttemptNumber {
		return fmt.Errorf("procedure inference attempt identity is immutable")
	}
	if cur.ProcedureID != next.ProcedureID || cur.ProcedureVersion != next.ProcedureVersion || cur.BindingID != next.BindingID || cur.BindingVersion != next.BindingVersion {
		return fmt.Errorf("procedure inference attempt definition identity is immutable")
	}
	if next.RecordVersion != cur.RecordVersion+1 {
		return fmt.Errorf("procedure inference attempt record_version must advance by one")
	}
	if cur.State == InferenceAttemptStateAuthorized && next.State == InferenceAttemptStateAuthorized {
		before, after := cur, next
		before.RecordVersion, after.RecordVersion = 0, 0
		before.PolicyDecisionID, after.PolicyDecisionID = "", ""
		before.AuthorizationRevision, after.AuthorizationRevision = 0, 0
		before.GlobalAuthorizationRevision, after.GlobalAuthorizationRevision = 0, 0
		before.SpaceAuthorizationRevision, after.SpaceAuthorizationRevision = 0, 0
		if !reflect.DeepEqual(before, after) || next.PolicyDecisionID == "" || next.AuthorizationRevision == 0 {
			return fmt.Errorf("authorized procedure inference attempt refresh may only update committed authorization evidence")
		}
		return nil
	}
	if IsTerminalProcedureInferenceAttemptState(cur.State) && cur.State == next.State {
		before, after := cur, next
		before.RecordVersion, after.RecordVersion = 0, 0
		before.PublicationDisposition, after.PublicationDisposition = "", ""
		if !reflect.DeepEqual(before, after) {
			return fmt.Errorf("terminal procedure inference attempt may only add publication disposition")
		}
		if next.PublicationDisposition == "" {
			return fmt.Errorf("terminal procedure inference attempt disposition is required")
		}
		return nil
	}
	if !allowedInferenceAttemptTransition(cur.State, next.State) {
		return fmt.Errorf("unsupported procedure inference attempt transition %s -> %s", cur.State, next.State)
	}
	return nil
}

func IsTerminalProcedureInferenceAttemptState(state string) bool {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case InferenceAttemptStateSucceeded, InferenceAttemptStateFailed, InferenceAttemptStateDenied, InferenceAttemptStateCanceled, InferenceAttemptStateAmbiguous:
		return true
	default:
		return false
	}
}

func validInferenceOutcomeCategory(value string) bool {
	switch value {
	case "", "succeeded", "provider_outcome_unknown", "authorization_denied", "canceled_before_provider", "failed_before_provider":
		return true
	default:
		return false
	}
}

func validInferenceTokenCountAuthority(value string) bool {
	switch value {
	case "", "provider_reported", "estimated", "unavailable":
		return true
	default:
		return false
	}
}

func validInferencePublicationDisposition(value string) bool {
	switch value {
	case "", "published", "not_published_provider_failure", "suppressed_procedure_control", "rejected_invalid_output",
		"publication_precondition_failed", "publication_pending", "publication_failed", "suppressed_authorization_or_control",
		"not_changed_or_duplicate":
		return true
	default:
		return false
	}
}

func validInferenceAttemptState(state string) bool {
	switch state {
	case InferenceAttemptStateRequested, InferenceAttemptStateAuthorized, InferenceAttemptStateStarted,
		InferenceAttemptStateSucceeded, InferenceAttemptStateFailed, InferenceAttemptStateDenied,
		InferenceAttemptStateCanceled, InferenceAttemptStateAmbiguous:
		return true
	default:
		return false
	}
}

func allowedInferenceAttemptTransition(from, to string) bool {
	switch from {
	case InferenceAttemptStateRequested:
		return to == InferenceAttemptStateAuthorized || to == InferenceAttemptStateDenied || to == InferenceAttemptStateFailed || to == InferenceAttemptStateCanceled
	case InferenceAttemptStateAuthorized:
		return to == InferenceAttemptStateStarted || to == InferenceAttemptStateDenied || to == InferenceAttemptStateFailed || to == InferenceAttemptStateCanceled
	case InferenceAttemptStateStarted:
		return to == InferenceAttemptStateSucceeded || to == InferenceAttemptStateFailed || to == InferenceAttemptStateDenied || to == InferenceAttemptStateCanceled || to == InferenceAttemptStateAmbiguous
	default:
		return false
	}
}
