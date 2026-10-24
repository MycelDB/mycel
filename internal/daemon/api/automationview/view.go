package automationview

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	automation "github.com/myceldb/mycel/internal/automation/model"
	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	DefaultPageSize = 100
	MaxPageSize     = 500
	cursorVersion   = 1
)

var ErrInvalidCursor = errors.New("invalid procedure inference cursor")

type CursorCodec struct {
	key []byte
}

type cursorPayload struct {
	Version     int    `json:"version"`
	Kind        string `json:"kind"`
	SpaceID     string `json:"space_id"`
	BindingHash string `json:"binding_hash"`
	HighWater   uint64 `json:"high_water,omitempty"`
	Position    uint64 `json:"position,omitempty"`
	Offset      int    `json:"offset,omitempty"`
}

type CurrentPage struct {
	Attempts                      []automation.ProcedureInferenceAttempt
	NextPageToken                 string
	SnapshotCursor                string
	SnapshotHighWaterSequence     uint64
	EarliestRetainedStateSequence uint64
}

type ChangePage struct {
	Changes                       []automation.ProcedureInferenceAttemptChange
	NextCursor                    string
	HighWaterSequence             uint64
	EarliestRetainedSequence      uint64
	EarliestRetainedStateSequence uint64
	HistoryGap                    bool
}

func NewCursorCodec(key []byte) (*CursorCodec, error) {
	if len(key) < 16 {
		return nil, fmt.Errorf("procedure inference cursor key must contain at least 16 bytes")
	}
	return &CursorCodec{key: append([]byte(nil), key...)}, nil
}

func NormalizePageSize(pageSize int32) (int, error) {
	if pageSize < 0 || pageSize > MaxPageSize {
		return 0, fmt.Errorf("page_size must be between 0 and %d", MaxPageSize)
	}
	if pageSize == 0 {
		return DefaultPageSize, nil
	}
	return int(pageSize), nil
}

func (c *CursorCodec) ListCurrent(ledger automation.ProcedureInferenceLedger, filter *commonv1.ProcedureInferenceAttemptFilter, pageSize int, pageToken, principalBinding string) (CurrentPage, error) {
	bindingHash, err := requestBindingHash(ledger.SpaceID, filter, principalBinding)
	if err != nil {
		return CurrentPage{}, err
	}
	highWater := ledger.HighWaterSequence
	offset := 0
	if strings.TrimSpace(pageToken) != "" {
		cursor, err := c.decode(pageToken)
		if err != nil || cursor.Kind != "current" || cursor.SpaceID != ledger.SpaceID || cursor.BindingHash != bindingHash || cursor.Offset < 0 {
			return CurrentPage{}, ErrInvalidCursor
		}
		highWater = cursor.HighWater
		offset = cursor.Offset
	}
	if strings.TrimSpace(pageToken) != "" && ledger.EarliestRetainedStateSequence > 0 && highWater < ledger.EarliestRetainedStateSequence {
		return CurrentPage{}, ErrInvalidCursor
	}
	attempts, err := materializedAtSequence(ledger, highWater)
	if err != nil {
		return CurrentPage{}, err
	}
	attempts = filterAttempts(attempts, filter)
	sort.Slice(attempts, func(i, j int) bool {
		if !attempts[i].RequestedAt.Equal(attempts[j].RequestedAt) {
			return attempts[i].RequestedAt.After(attempts[j].RequestedAt)
		}
		return attempts[i].ID < attempts[j].ID
	})
	if offset > len(attempts) {
		return CurrentPage{}, ErrInvalidCursor
	}
	end := offset + pageSize
	if end > len(attempts) {
		end = len(attempts)
	}
	page := CurrentPage{Attempts: append([]automation.ProcedureInferenceAttempt(nil), attempts[offset:end]...), SnapshotHighWaterSequence: highWater, EarliestRetainedStateSequence: ledger.EarliestRetainedStateSequence}
	page.SnapshotCursor, err = c.encode(cursorPayload{Version: cursorVersion, Kind: "changes", SpaceID: ledger.SpaceID, BindingHash: bindingHash, HighWater: highWater, Position: highWater})
	if err != nil {
		return CurrentPage{}, err
	}
	if end < len(attempts) {
		page.NextPageToken, err = c.encode(cursorPayload{Version: cursorVersion, Kind: "current", SpaceID: ledger.SpaceID, BindingHash: bindingHash, HighWater: highWater, Offset: end})
	}
	return page, err
}

func (c *CursorCodec) ListChanges(ledger automation.ProcedureInferenceLedger, filter *commonv1.ProcedureInferenceAttemptFilter, pageSize int, afterCursor, principalBinding string) (ChangePage, error) {
	bindingHash, err := requestBindingHash(ledger.SpaceID, filter, principalBinding)
	if err != nil {
		return ChangePage{}, err
	}
	after := uint64(0)
	if strings.TrimSpace(afterCursor) != "" {
		cursor, err := c.decode(afterCursor)
		if err != nil || cursor.Kind != "changes" || cursor.SpaceID != ledger.SpaceID || cursor.BindingHash != bindingHash {
			return ChangePage{}, ErrInvalidCursor
		}
		after = cursor.Position
	}
	page := ChangePage{HighWaterSequence: ledger.HighWaterSequence, EarliestRetainedSequence: ledger.EarliestRetainedSequence, EarliestRetainedStateSequence: ledger.EarliestRetainedStateSequence}
	if ledger.EarliestRetainedSequence > 0 && after+1 < ledger.EarliestRetainedSequence {
		page.HistoryGap = true
		page.NextCursor = afterCursor
		return page, nil
	}
	lastExamined := after
	for _, change := range ledger.Changes {
		if change.Sequence <= after || change.Sequence > ledger.HighWaterSequence {
			continue
		}
		lastExamined = change.Sequence
		if matchesAttempt(change.Attempt, filter) {
			page.Changes = append(page.Changes, change)
			if len(page.Changes) == pageSize {
				break
			}
		}
	}
	if lastExamined < ledger.HighWaterSequence && len(page.Changes) < pageSize {
		lastExamined = ledger.HighWaterSequence
	}
	page.NextCursor, err = c.encode(cursorPayload{Version: cursorVersion, Kind: "changes", SpaceID: ledger.SpaceID, BindingHash: bindingHash, HighWater: ledger.HighWaterSequence, Position: lastExamined})
	return page, err
}

func GetAttempt(ledger automation.ProcedureInferenceLedger, attemptID string) (automation.ProcedureInferenceAttempt, bool) {
	for _, attempt := range ledger.Attempts {
		if attempt.ID == strings.TrimSpace(attemptID) {
			return attempt, true
		}
	}
	return automation.ProcedureInferenceAttempt{}, false
}

func AttemptToProto(in automation.ProcedureInferenceAttempt) *commonv1.ProcedureInferenceAttempt {
	return &commonv1.ProcedureInferenceAttempt{Id: in.ID, RecordVersion: in.RecordVersion, SpaceId: in.SpaceID, DomainId: in.DomainID.String(), ProcedureId: in.ProcedureID, ProcedureVersion: int32(in.ProcedureVersion), BindingId: in.BindingID, BindingVersion: int32(in.BindingVersion), InvocationId: in.InvocationID, RunId: in.RunID, RunAttemptNumber: int32(in.RunAttemptNumber), WorkflowStepId: in.WorkflowStepID, ProviderAttemptNumber: int32(in.ProviderAttemptNumber), SourceEventId: in.SourceEventID, SourceTransactionId: in.SourceTransactionID, SourceGraphRevision: in.SourceGraphRevision, EventOriginPrincipalId: in.EventOriginPrincipalID, ActorPrincipalId: in.ActorPrincipalID, OwnerPrincipalId: in.OwnerPrincipalID, OnBehalfOfPrincipalId: in.OnBehalfOfPrincipalID, Operation: operationToProto(in.Operation), ProfileId: in.ProfileID, EndpointId: in.EndpointID, ModelId: in.ModelID, CapabilityId: in.CapabilityID, PolicyDecisionId: in.PolicyDecisionID, CredentialGrantId: in.CredentialGrantID, AuthorizationRevision: in.AuthorizationRevision, ConfigurationRevision: in.ConfigurationRevision, State: stateToProto(in.State), OutcomeCategory: in.OutcomeCategory, InputTokens: in.InputTokens, OutputTokens: in.OutputTokens, TotalTokens: in.TotalTokens, TokenCountAuthority: in.TokenCountAuthority, ProviderDurationMillis: in.ProviderDurationMillis, PublicationDisposition: in.PublicationDisposition, RequestedAt: timestamp(in.RequestedAt), AuthorizedAt: timestamp(in.AuthorizedAt), StartedAt: timestamp(in.StartedAt), CompletedAt: timestamp(in.CompletedAt)}
}

func ChangeToProto(in automation.ProcedureInferenceAttemptChange) *commonv1.ProcedureInferenceAttemptChange {
	return &commonv1.ProcedureInferenceAttemptChange{EventId: in.EventID, SchemaVersion: in.SchemaVersion, SpaceId: in.SpaceID, Sequence: in.Sequence, ChangeKind: in.ChangeKind, AttemptId: in.AttemptID, RecordVersion: in.RecordVersion, Attempt: AttemptToProto(in.Attempt), OccurredAt: timestamp(in.OccurredAt), CommittedAt: timestamp(in.CommittedAt)}
}

func ControlBarrierToProto(in automation.ProcedureControlBarrier) *commonv1.ProcedureControlBarrier {
	target := commonv1.ProcedureControlTargetStatus_PROCEDURE_CONTROL_TARGET_STATUS_UNSPECIFIED
	if in.TargetStatus == automation.StatusEnabled {
		target = commonv1.ProcedureControlTargetStatus_PROCEDURE_CONTROL_TARGET_STATUS_ENABLED
	} else if in.TargetStatus == automation.StatusDisabled {
		target = commonv1.ProcedureControlTargetStatus_PROCEDURE_CONTROL_TARGET_STATUS_DISABLED
	}
	return &commonv1.ProcedureControlBarrier{SpaceId: in.SpaceID, DomainId: in.DomainID.String(), ProcedureId: in.ProcedureID, ProcedureVersion: int32(in.ProcedureVersion), TargetStatus: target, ControlEpoch: in.ControlEpoch, ControllingPrincipalId: in.ControllingPrincipalID, AffectedPartitions: append([]uint32(nil), in.AffectedPartitions...), AcknowledgedPartitions: append([]uint32(nil), in.AcknowledgedPartitions...), Completed: in.Completed, CompletionRevision: in.CompletionRevision, CommittedAt: timestamp(in.CommittedAt), CompletedAt: timestamp(in.CompletedAt)}
}

func SanitizedRunJSON(in automation.Run) ([]byte, error) {
	in.ClaimToken = ""
	in.CredentialID = ""
	in.ProviderRequestID = ""
	in.Context = nil
	in.Usage.Metadata = nil
	in.Error = sanitizedRunOutcome(in)
	return json.Marshal(in)
}

func RunToProto(in automation.Run) *commonv1.AutomationRun {
	return &commonv1.AutomationRun{Id: in.ID, DomainId: in.DomainID.String(), InvocationId: in.InvocationID, BindingId: in.BindingID, BindingVersion: int32(in.BindingVersion), ProcedureId: in.ProcedureID, ProcedureVersion: int32(in.ProcedureVersion), AttemptNumber: int32(in.AttemptNumber), State: runStateToProto(in.Status), OutcomeCategory: sanitizedRunOutcome(in), ActorPrincipalId: in.ActorPrincipalID, OwnerPrincipalId: in.OwnerPrincipalID, OnBehalfOfPrincipalId: in.OnBehalfOfPrincipalID, EventOriginPrincipalId: in.EventOriginPrincipalID, InferenceProfile: in.InferenceProfile, InferenceProfileId: in.InferenceProfileID, PolicyDecisionId: in.PolicyDecisionID, EndpointId: in.ModelEndpointID, ModelId: in.ModelID, CapabilityId: in.CapabilityID, InputTokens: in.Usage.InputTokens, OutputTokens: in.Usage.OutputTokens, TotalTokens: in.Usage.TotalTokens, TokenCountAuthority: tokenCountAuthority(in.Usage.Metadata), InferenceAttemptIds: append([]string(nil), in.InferenceAttemptIDs...), StartedAt: timestamp(in.StartedAt), CompletedAt: timestamp(in.CompletedAt)}
}

func tokenCountAuthority(metadata map[string]any) string {
	value, _ := metadata["token_count_source"].(string)
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "provider_reported", "estimated", "unavailable":
		return strings.ToLower(strings.TrimSpace(value))
	default:
		return ""
	}
}

func materializedAtSequence(ledger automation.ProcedureInferenceLedger, highWater uint64) ([]automation.ProcedureInferenceAttempt, error) {
	if ledger.EarliestRetainedStateSequence > 0 && highWater < ledger.EarliestRetainedStateSequence {
		return nil, ErrInvalidCursor
	}
	byID := make(map[string]automation.ProcedureInferenceAttempt, len(ledger.Attempts))
	for _, attempt := range ledger.Attempts {
		byID[attempt.ID] = attempt
	}
	if highWater < ledger.HighWaterSequence {
		changedAfter := map[string]struct{}{}
		for _, change := range ledger.Changes {
			if change.Sequence > highWater {
				changedAfter[change.AttemptID] = struct{}{}
			}
		}
		for attemptID := range changedAfter {
			delete(byID, attemptID)
			for i := len(ledger.Changes) - 1; i >= 0; i-- {
				change := ledger.Changes[i]
				if change.AttemptID == attemptID && change.Sequence <= highWater {
					byID[attemptID] = change.Attempt
					break
				}
			}
		}
	}
	out := make([]automation.ProcedureInferenceAttempt, 0, len(byID))
	for _, attempt := range byID {
		out = append(out, attempt)
	}
	return out, nil
}

func filterAttempts(in []automation.ProcedureInferenceAttempt, filter *commonv1.ProcedureInferenceAttemptFilter) []automation.ProcedureInferenceAttempt {
	out := make([]automation.ProcedureInferenceAttempt, 0, len(in))
	for _, attempt := range in {
		if matchesAttempt(attempt, filter) {
			out = append(out, attempt)
		}
	}
	return out
}

func matchesAttempt(a automation.ProcedureInferenceAttempt, f *commonv1.ProcedureInferenceAttemptFilter) bool {
	if f == nil {
		return true
	}
	if f.DomainId != "" && a.DomainID.String() != f.DomainId || f.ProcedureId != "" && a.ProcedureID != f.ProcedureId || f.ProcedureVersion != 0 && a.ProcedureVersion != int(f.ProcedureVersion) || f.BindingId != "" && a.BindingID != f.BindingId || f.BindingVersion != 0 && a.BindingVersion != int(f.BindingVersion) || f.InvocationId != "" && a.InvocationID != f.InvocationId || f.RunId != "" && a.RunID != f.RunId || f.WorkflowStepId != "" && a.WorkflowStepID != f.WorkflowStepId || f.EndpointId != "" && a.EndpointID != f.EndpointId || f.ModelId != "" && a.ModelID != f.ModelId || f.CapabilityId != "" && a.CapabilityID != f.CapabilityId || f.Operation != commonv1.InferenceOperation_INFERENCE_OPERATION_UNSPECIFIED && operationToProto(a.Operation) != f.Operation || f.EventOriginPrincipalId != "" && a.EventOriginPrincipalID != f.EventOriginPrincipalId || f.ActorPrincipalId != "" && a.ActorPrincipalID != f.ActorPrincipalId || f.OwnerPrincipalId != "" && a.OwnerPrincipalID != f.OwnerPrincipalId || f.OnBehalfOfPrincipalId != "" && a.OnBehalfOfPrincipalID != f.OnBehalfOfPrincipalId {
		return false
	}
	if len(f.States) > 0 {
		matched := false
		for _, state := range f.States {
			if stateToProto(a.State) == state {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return timeInRange(a.RequestedAt, f.RequestedAtOrAfter, f.RequestedBefore) && timeInRange(a.CompletedAt, f.CompletedAtOrAfter, f.CompletedBefore)
}

func timeInRange(value time.Time, atOrAfter, before *timestamppb.Timestamp) bool {
	if atOrAfter != nil && (value.IsZero() || value.Before(atOrAfter.AsTime())) {
		return false
	}
	if before != nil && (value.IsZero() || !value.Before(before.AsTime())) {
		return false
	}
	return true
}

func requestBindingHash(spaceID string, filter *commonv1.ProcedureInferenceAttemptFilter, principalBinding string) (string, error) {
	filterBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(filter)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(append([]byte(strings.TrimSpace(spaceID)+"\x00"+strings.TrimSpace(principalBinding)+"\x00"), filterBytes...))
	return base64.RawURLEncoding.EncodeToString(sum[:]), nil
}

func (c *CursorCodec) encode(payload cursorPayload) (string, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, c.key)
	_, _ = mac.Write(body)
	signed := append(body, mac.Sum(nil)...)
	return base64.RawURLEncoding.EncodeToString(signed), nil
}

func (c *CursorCodec) decode(token string) (cursorPayload, error) {
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(token))
	if err != nil || len(raw) <= sha256.Size {
		return cursorPayload{}, ErrInvalidCursor
	}
	body, signature := raw[:len(raw)-sha256.Size], raw[len(raw)-sha256.Size:]
	mac := hmac.New(sha256.New, c.key)
	_, _ = mac.Write(body)
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return cursorPayload{}, ErrInvalidCursor
	}
	var payload cursorPayload
	if err := json.Unmarshal(body, &payload); err != nil || payload.Version != cursorVersion || payload.SpaceID == "" || payload.BindingHash == "" {
		return cursorPayload{}, ErrInvalidCursor
	}
	return payload, nil
}

func timestamp(value time.Time) *timestamppb.Timestamp {
	if value.IsZero() {
		return nil
	}
	return timestamppb.New(value.UTC())
}

func operationToProto(value string) commonv1.InferenceOperation {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "chat":
		return commonv1.InferenceOperation_INFERENCE_OPERATION_CHAT
	case "embeddings":
		return commonv1.InferenceOperation_INFERENCE_OPERATION_EMBEDDINGS
	case "summarize":
		return commonv1.InferenceOperation_INFERENCE_OPERATION_SUMMARIZE
	case "classify":
		return commonv1.InferenceOperation_INFERENCE_OPERATION_CLASSIFY
	case "image_analysis":
		return commonv1.InferenceOperation_INFERENCE_OPERATION_IMAGE_ANALYSIS
	default:
		return commonv1.InferenceOperation_INFERENCE_OPERATION_UNSPECIFIED
	}
}

func stateToProto(value string) commonv1.ProcedureInferenceAttemptState {
	return map[string]commonv1.ProcedureInferenceAttemptState{"requested": commonv1.ProcedureInferenceAttemptState_PROCEDURE_INFERENCE_ATTEMPT_STATE_REQUESTED, "authorized": commonv1.ProcedureInferenceAttemptState_PROCEDURE_INFERENCE_ATTEMPT_STATE_AUTHORIZED, "started": commonv1.ProcedureInferenceAttemptState_PROCEDURE_INFERENCE_ATTEMPT_STATE_STARTED, "succeeded": commonv1.ProcedureInferenceAttemptState_PROCEDURE_INFERENCE_ATTEMPT_STATE_SUCCEEDED, "failed": commonv1.ProcedureInferenceAttemptState_PROCEDURE_INFERENCE_ATTEMPT_STATE_FAILED, "denied": commonv1.ProcedureInferenceAttemptState_PROCEDURE_INFERENCE_ATTEMPT_STATE_DENIED, "canceled": commonv1.ProcedureInferenceAttemptState_PROCEDURE_INFERENCE_ATTEMPT_STATE_CANCELED, "ambiguous": commonv1.ProcedureInferenceAttemptState_PROCEDURE_INFERENCE_ATTEMPT_STATE_AMBIGUOUS}[strings.ToLower(strings.TrimSpace(value))]
}

func runStateToProto(value string) commonv1.AutomationRunState {
	return map[string]commonv1.AutomationRunState{"running": commonv1.AutomationRunState_AUTOMATION_RUN_STATE_RUNNING, "succeeded": commonv1.AutomationRunState_AUTOMATION_RUN_STATE_SUCCEEDED, "skipped": commonv1.AutomationRunState_AUTOMATION_RUN_STATE_SKIPPED, "retryable": commonv1.AutomationRunState_AUTOMATION_RUN_STATE_RETRYABLE, "failed": commonv1.AutomationRunState_AUTOMATION_RUN_STATE_FAILED, "cancelled": commonv1.AutomationRunState_AUTOMATION_RUN_STATE_CANCELED, "canceled": commonv1.AutomationRunState_AUTOMATION_RUN_STATE_CANCELED}[strings.ToLower(strings.TrimSpace(value))]
}

func sanitizedRunOutcome(run automation.Run) string {
	if run.Status == "succeeded" || run.Status == "skipped" || run.Status == "failed" || run.Status == "retryable" || run.Status == "cancelled" || run.Status == "canceled" {
		return run.Status
	}
	return ""
}
