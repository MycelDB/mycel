package service

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	domaingraph "github.com/myceldb/mycel/internal/graph/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// AutomationOutputFenceValidation is the graph state machine's view of an
// automation-generated graph mutation. The graph subsystem deliberately keeps
// this as a small structural contract so it does not import the automation
// subsystem or its storage model.
type AutomationOutputFenceValidation struct {
	SpaceID              string
	DomainID             domaingraph.DomainID
	EntityKind           string
	EntityID             string
	AutomationID         string
	BindingID            string
	RunID                string
	InvocationID         string
	TargetNodeID         string
	ClaimOwnerNodeID     uint64
	ClaimVersion         uint64
	ClaimToken           string
	OutputIdempotencyKey string
}

type AutomationOutputFenceValidator interface {
	ValidateAutomationOutputFence(ctx context.Context, validation AutomationOutputFenceValidation) error
}

func (m *Module) SetAutomationOutputFenceValidator(validator AutomationOutputFenceValidator) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.automationOutputFenceValidator = validator
}

func (m *Module) validateAutomationOutputFences(ctx context.Context, record graphCommitRecord) error {
	validations, err := automationOutputFenceValidations(record)
	if err != nil {
		return err
	}
	if len(validations) == 0 {
		return nil
	}
	m.mu.Lock()
	validator := m.automationOutputFenceValidator
	m.mu.Unlock()
	if validator == nil {
		return status.Error(codes.FailedPrecondition, "automation output fencing validator is not configured")
	}
	for _, validation := range validations {
		if err := validator.ValidateAutomationOutputFence(ctx, validation); err != nil {
			return err
		}
	}
	return nil
}

func automationOutputFenceValidations(record graphCommitRecord) ([]AutomationOutputFenceValidation, error) {
	out := []AutomationOutputFenceValidation{}
	for _, node := range record.PutNodes {
		validation, ok, err := automationOutputFenceValidation(record.SpaceID, node.DomainID, "node", node.ID.String(), node.Meta)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, validation)
		}
	}
	for _, edge := range record.PutEdges {
		validation, ok, err := automationOutputFenceValidation(record.SpaceID, edge.DomainID, "edge", edge.ID.String(), edge.Meta)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, validation)
		}
	}
	return out, nil
}

func automationOutputFenceValidation(spaceID string, domainID domaingraph.DomainID, entityKind string, entityID string, meta map[string]any) (AutomationOutputFenceValidation, bool, error) {
	automationMeta, ok := automationMetadataMap(meta)
	if !ok {
		return AutomationOutputFenceValidation{}, false, nil
	}
	validation := AutomationOutputFenceValidation{
		SpaceID:              strings.TrimSpace(spaceID),
		DomainID:             domainID,
		EntityKind:           entityKind,
		EntityID:             strings.TrimSpace(entityID),
		AutomationID:         metadataString(automationMeta, "automation_id"),
		BindingID:            metadataString(automationMeta, "binding_id"),
		RunID:                metadataString(automationMeta, "run_id"),
		InvocationID:         metadataString(automationMeta, "invocation_id"),
		TargetNodeID:         metadataString(automationMeta, "target_node_id"),
		ClaimToken:           metadataString(automationMeta, "claim_token"),
		OutputIdempotencyKey: metadataString(automationMeta, "output_idempotency_key"),
	}
	validation.ClaimOwnerNodeID = metadataUint64(automationMeta, "claim_owner_node_id")
	validation.ClaimVersion = metadataUint64(automationMeta, "claim_version")
	if !validation.HasFenceMetadata() {
		return AutomationOutputFenceValidation{}, false, nil
	}
	if validation.InvocationID == "" {
		return AutomationOutputFenceValidation{}, true, status.Errorf(codes.FailedPrecondition, "automation output fence on %s %s is missing invocation_id", entityKind, entityID)
	}
	if validation.ClaimToken == "" {
		return AutomationOutputFenceValidation{}, true, status.Errorf(codes.FailedPrecondition, "automation output fence on %s %s is missing claim_token", entityKind, entityID)
	}
	if validation.ClaimVersion == 0 {
		return AutomationOutputFenceValidation{}, true, status.Errorf(codes.FailedPrecondition, "automation output fence on %s %s is missing claim_version", entityKind, entityID)
	}
	if validation.ClaimOwnerNodeID == 0 {
		return AutomationOutputFenceValidation{}, true, status.Errorf(codes.FailedPrecondition, "automation output fence on %s %s is missing claim_owner_node_id", entityKind, entityID)
	}
	if validation.OutputIdempotencyKey == "" {
		return AutomationOutputFenceValidation{}, true, status.Errorf(codes.FailedPrecondition, "automation output fence on %s %s is missing output_idempotency_key", entityKind, entityID)
	}
	return validation, true, nil
}

func (v AutomationOutputFenceValidation) HasFenceMetadata() bool {
	return v.InvocationID != "" || v.ClaimToken != "" || v.ClaimVersion != 0 || v.ClaimOwnerNodeID != 0 || v.OutputIdempotencyKey != ""
}

func automationMetadataMap(meta map[string]any) (map[string]any, bool) {
	if meta == nil {
		return nil, false
	}
	raw, ok := meta["automation"]
	if !ok || raw == nil {
		return nil, false
	}
	switch value := raw.(type) {
	case map[string]any:
		return value, true
	case map[string]string:
		out := make(map[string]any, len(value))
		for k, v := range value {
			out[k] = v
		}
		return out, true
	default:
		return nil, false
	}
}

func metadataString(meta map[string]any, key string) string {
	if meta == nil {
		return ""
	}
	value, ok := meta[key]
	if !ok || value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	case fmt.Stringer:
		return strings.TrimSpace(v.String())
	default:
		return strings.TrimSpace(fmt.Sprint(v))
	}
}

func metadataUint64(meta map[string]any, key string) uint64 {
	if meta == nil {
		return 0
	}
	value, ok := meta[key]
	if !ok || value == nil {
		return 0
	}
	switch v := value.(type) {
	case uint64:
		return v
	case uint:
		return uint64(v)
	case uint32:
		return uint64(v)
	case int:
		if v < 0 {
			return 0
		}
		return uint64(v)
	case int64:
		if v < 0 {
			return 0
		}
		return uint64(v)
	case int32:
		if v < 0 {
			return 0
		}
		return uint64(v)
	case float64:
		if v < 0 || math.Trunc(v) != v {
			return 0
		}
		return uint64(v)
	case float32:
		f := float64(v)
		if f < 0 || math.Trunc(f) != f {
			return 0
		}
		return uint64(v)
	case json.Number:
		parsed, err := strconv.ParseUint(v.String(), 10, 64)
		if err != nil {
			return 0
		}
		return parsed
	case string:
		parsed, err := strconv.ParseUint(strings.TrimSpace(v), 10, 64)
		if err != nil {
			return 0
		}
		return parsed
	default:
		return 0
	}
}
