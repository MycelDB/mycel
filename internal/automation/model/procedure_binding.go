package model

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	graph "github.com/myceldb/mycel/internal/graph/model"
)

const (
	TriggerTypeGraphEvent   = "graph_event"
	TriggerTypeSchedule     = "schedule"
	TriggerTypeOneTimeScan  = "one_time_scan"
	RuntimeEventOriginNone  = "disabled"
	RuntimeEventOriginAllow = "if_present"
)

// Procedure describes reusable graph work. It intentionally does not contain
// trigger, scope, or durable runtime principal fields; those belong to Binding.
type Procedure struct {
	ID                   string         `json:"id"`
	Name                 string         `json:"name,omitempty"`
	Version              int            `json:"version"`
	DomainID             graph.DomainID `json:"domain_id,omitempty"`
	Status               string         `json:"status,omitempty"`
	Description          string         `json:"description,omitempty"`
	Input                Input          `json:"input"`
	Inference            InferenceRef   `json:"inference,omitempty"`
	Prompt               string         `json:"prompt,omitempty"`
	Output               Output         `json:"output,omitempty"`
	Workflow             *Workflow      `json:"workflow,omitempty"`
	Safety               Safety         `json:"safety,omitempty"`
	CreatedByPrincipalID string         `json:"created_by_principal_id,omitempty"`
	UpdatedByPrincipalID string         `json:"updated_by_principal_id,omitempty"`
	CreatedAt            time.Time      `json:"created_at,omitempty"`
	UpdatedAt            time.Time      `json:"updated_at,omitempty"`
}

// Binding connects a graph procedure to trigger/schedule/manual runtime
// information, including the principal context used by invocations.
type Binding struct {
	ID                   string         `json:"id"`
	Name                 string         `json:"name,omitempty"`
	Version              int            `json:"version,omitempty"`
	DomainID             graph.DomainID `json:"domain_id,omitempty"`
	ProcedureID          string         `json:"procedure_id"`
	ProcedureVersion     int            `json:"procedure_version,omitempty"`
	Status               string         `json:"status"`
	Scope                BindingScope   `json:"scope,omitempty"`
	Trigger              BindingTrigger `json:"trigger"`
	Runtime              RuntimeContext `json:"runtime,omitempty"`
	Debounce             *Debounce      `json:"debounce,omitempty"`
	Idempotency          Idempotency    `json:"idempotency,omitempty"`
	CreatedByPrincipalID string         `json:"created_by_principal_id,omitempty"`
	UpdatedByPrincipalID string         `json:"updated_by_principal_id,omitempty"`
	CreatedAt            time.Time      `json:"created_at,omitempty"`
	UpdatedAt            time.Time      `json:"updated_at,omitempty"`
}

type BindingScope struct {
	SpaceID            string         `json:"space_id,omitempty"`
	DomainID           graph.DomainID `json:"domain_id,omitempty"`
	IncludeDescendants bool           `json:"include_descendants,omitempty"`
}

type BindingTrigger struct {
	Type      string           `json:"type,omitempty"`
	Events    []string         `json:"events,omitempty"`
	Labels    []string         `json:"labels,omitempty"`
	Condition Condition        `json:"condition,omitempty"`
	Schedule  *ScheduleTrigger `json:"schedule,omitempty"`
	Scan      *ScanTrigger     `json:"scan,omitempty"`
}

type RuntimeContext struct {
	ActorPrincipalID      string `json:"actor_principal_id,omitempty"`
	OwnerPrincipalID      string `json:"owner_principal_id,omitempty"`
	OnBehalfOfPrincipalID string `json:"on_behalf_of_principal_id,omitempty"`
	InferenceProfile      string `json:"inference_profile,omitempty"`
	InferenceProfileID    string `json:"inference_profile_id,omitempty"`
	EventOriginOverride   string `json:"event_origin_override,omitempty"`
}

func (p Procedure) Normalize() Procedure {
	p.ID = strings.TrimSpace(p.ID)
	p.Name = strings.TrimSpace(p.Name)
	p.Description = strings.TrimSpace(p.Description)
	p.CreatedByPrincipalID = strings.TrimSpace(p.CreatedByPrincipalID)
	p.UpdatedByPrincipalID = strings.TrimSpace(p.UpdatedByPrincipalID)
	if p.Version == 0 {
		p.Version = 1
	}
	p.Status = strings.ToLower(strings.TrimSpace(p.Status))
	if p.Status == "" {
		p.Status = StatusEnabled
	}
	d := Definition{ID: p.ID, Version: p.Version, Status: p.Status, Input: p.Input, Inference: p.Inference, Prompt: p.Prompt, Output: p.Output, Workflow: p.Workflow, Safety: p.Safety}.Normalize()
	p.Input = d.Input
	p.Inference = d.Inference
	p.Output = d.Output
	p.Workflow = d.Workflow
	p.Safety = d.Safety
	return p
}

func (b Binding) Normalize() Binding {
	b.ID = strings.TrimSpace(b.ID)
	b.Name = strings.TrimSpace(b.Name)
	b.ProcedureID = strings.TrimSpace(b.ProcedureID)
	if b.Version == 0 {
		b.Version = 1
	}
	b.Status = strings.ToLower(strings.TrimSpace(b.Status))
	if b.Status == "" {
		b.Status = StatusDisabled
	}
	if b.Scope.DomainID == (graph.DomainID{}) {
		b.Scope.DomainID = b.DomainID
	}
	b.Scope.SpaceID = strings.TrimSpace(b.Scope.SpaceID)
	b.Trigger.Type = strings.ToLower(strings.TrimSpace(b.Trigger.Type))
	if b.Trigger.Type == "" {
		switch {
		case b.Trigger.Schedule != nil:
			b.Trigger.Type = TriggerTypeSchedule
		case b.Trigger.Scan != nil && len(b.Trigger.Events) == 0:
			b.Trigger.Type = TriggerTypeOneTimeScan
		default:
			b.Trigger.Type = TriggerTypeGraphEvent
		}
	}
	for i := range b.Trigger.Events {
		b.Trigger.Events[i] = strings.ToLower(strings.TrimSpace(b.Trigger.Events[i]))
	}
	for i := range b.Trigger.Labels {
		b.Trigger.Labels[i] = strings.TrimSpace(b.Trigger.Labels[i])
	}
	b.Runtime.ActorPrincipalID = strings.TrimSpace(b.Runtime.ActorPrincipalID)
	b.Runtime.OwnerPrincipalID = strings.TrimSpace(b.Runtime.OwnerPrincipalID)
	b.Runtime.OnBehalfOfPrincipalID = strings.TrimSpace(b.Runtime.OnBehalfOfPrincipalID)
	b.Runtime.InferenceProfile = strings.TrimSpace(b.Runtime.InferenceProfile)
	b.Runtime.InferenceProfileID = strings.TrimSpace(b.Runtime.InferenceProfileID)
	b.Runtime.EventOriginOverride = strings.ToLower(strings.TrimSpace(b.Runtime.EventOriginOverride))
	if b.Runtime.EventOriginOverride == "" {
		b.Runtime.EventOriginOverride = RuntimeEventOriginNone
	}
	b.CreatedByPrincipalID = strings.TrimSpace(b.CreatedByPrincipalID)
	b.UpdatedByPrincipalID = strings.TrimSpace(b.UpdatedByPrincipalID)
	b.Idempotency.Scope = strings.ToLower(strings.TrimSpace(b.Idempotency.Scope))
	b.Idempotency.Target = strings.TrimSpace(b.Idempotency.Target)
	if b.Debounce != nil {
		b.Debounce.Duration = strings.TrimSpace(b.Debounce.Duration)
		b.Debounce.CoalesceBy = strings.TrimSpace(b.Debounce.CoalesceBy)
	}
	return b
}

func LegacyProcedureID(automationID string) string {
	return strings.TrimSpace(automationID) + ".procedure"
}

func ExpandDefinition(def Definition) (Procedure, Binding) {
	def = def.Normalize()
	procedure := Procedure{ID: LegacyProcedureID(def.ID), Name: def.Name, Version: def.Version, DomainID: def.DomainID, Status: StatusEnabled, Input: def.Input, Inference: def.Inference, Prompt: def.Prompt, Output: def.Output, Workflow: def.Workflow, Safety: def.Safety, CreatedByPrincipalID: def.CreatedByPrincipalID, UpdatedByPrincipalID: def.UpdatedByPrincipalID, CreatedAt: def.CreatedAt, UpdatedAt: def.UpdatedAt}.Normalize()
	binding := Binding{ID: def.ID, Name: def.Name, Version: def.Version, DomainID: def.DomainID, ProcedureID: procedure.ID, ProcedureVersion: procedure.Version, Status: def.Status, Scope: BindingScope{DomainID: def.DomainID}, Trigger: BindingTrigger{Type: TriggerTypeGraphEvent, Events: append([]string(nil), def.Trigger.Events...), Labels: append([]string(nil), def.Trigger.Labels...), Condition: def.Condition, Schedule: def.Trigger.Schedule, Scan: def.Trigger.Scan}, Runtime: RuntimeContext{ActorPrincipalID: "automation", OwnerPrincipalID: def.OwnerPrincipalID, OnBehalfOfPrincipalID: def.OwnerPrincipalID, InferenceProfile: def.Inference.Profile, InferenceProfileID: def.Inference.ProfileID, EventOriginOverride: RuntimeEventOriginAllow}, Debounce: def.Safety.Debounce, Idempotency: def.Safety.Idempotency, CreatedByPrincipalID: def.CreatedByPrincipalID, UpdatedByPrincipalID: def.UpdatedByPrincipalID, CreatedAt: def.CreatedAt, UpdatedAt: def.UpdatedAt}.Normalize()
	return procedure, binding
}

func ComposeDefinition(procedure Procedure, binding Binding) Definition {
	procedure = procedure.Normalize()
	binding = binding.Normalize()
	inference := procedure.Inference
	if binding.Runtime.InferenceProfile != "" {
		inference.Profile = binding.Runtime.InferenceProfile
	}
	if binding.Runtime.InferenceProfileID != "" {
		inference.ProfileID = binding.Runtime.InferenceProfileID
	}
	safety := procedure.Safety
	if binding.Debounce != nil {
		safety.Debounce = binding.Debounce
	}
	if binding.Idempotency.Scope != "" || binding.Idempotency.Target != "" || len(binding.Idempotency.InputHashFields) > 0 || binding.Idempotency.SkipIfOutputUnchanged {
		safety.Idempotency = binding.Idempotency
	}
	domainID := binding.Scope.DomainID
	if domainID == (graph.DomainID{}) {
		domainID = binding.DomainID
	}
	if domainID == (graph.DomainID{}) {
		domainID = procedure.DomainID
	}
	version := binding.Version
	if version == 0 {
		version = procedure.Version
	}
	status := binding.Status
	if procedure.Status == StatusDisabled {
		status = StatusDisabled
	}
	return Definition{ID: binding.ID, Name: firstNonEmpty(binding.Name, procedure.Name), Version: version, DomainID: domainID, Status: status, Trigger: Trigger{Events: append([]string(nil), binding.Trigger.Events...), Labels: append([]string(nil), binding.Trigger.Labels...), Schedule: binding.Trigger.Schedule, Scan: binding.Trigger.Scan}, Condition: binding.Trigger.Condition, Input: procedure.Input, Inference: inference, Prompt: procedure.Prompt, Output: procedure.Output, Workflow: procedure.Workflow, Safety: safety, OwnerPrincipalID: binding.Runtime.OwnerPrincipalID, CreatedByPrincipalID: binding.CreatedByPrincipalID, UpdatedByPrincipalID: binding.UpdatedByPrincipalID, CreatedAt: binding.CreatedAt, UpdatedAt: binding.UpdatedAt}.Normalize()
}

func ValidateProcedure(procedure Procedure) error {
	procedure = procedure.Normalize()
	if procedure.ID == "" {
		return fmt.Errorf("graph procedure id is required")
	}
	if procedure.Status != StatusEnabled && procedure.Status != StatusDisabled {
		return fmt.Errorf("graph procedure status must be enabled or disabled")
	}
	if err := validateInput(procedure.Input); err != nil {
		return err
	}
	def := Definition{ID: procedure.ID, Version: procedure.Version, Status: StatusDisabled, Input: procedure.Input, Inference: procedure.Inference, Prompt: procedure.Prompt, Output: procedure.Output, Workflow: procedure.Workflow, Safety: procedure.Safety}
	if err := rejectSecretBearingDefinition(def); err != nil {
		return err
	}
	if procedure.Workflow != nil {
		return validateWorkflow(*procedure.Workflow)
	}
	if err := validateSafety(procedure.Safety); err != nil {
		return err
	}
	if err := validateProcedureInferenceRef("graph procedure inference", procedure.Inference); err != nil {
		return err
	}
	if strings.TrimSpace(procedure.Prompt) == "" && procedure.Workflow == nil {
		return fmt.Errorf("graph procedure prompt is required")
	}
	if procedure.Workflow != nil {
		return nil
	}
	return validateOutput(procedure.Output, procedure.Safety)
}

func ValidateBinding(binding Binding, procedure *Procedure) error {
	binding = binding.Normalize()
	if binding.ID == "" {
		return fmt.Errorf("graph automation binding id is required")
	}
	if binding.ProcedureID == "" {
		return fmt.Errorf("graph automation binding procedure_id is required")
	}
	if binding.Status != StatusEnabled && binding.Status != StatusDisabled {
		return fmt.Errorf("graph automation binding status must be enabled or disabled")
	}
	if binding.Scope.DomainID == (graph.DomainID{}) {
		return fmt.Errorf("graph automation binding scope.domain_id is required")
	}
	if err := validateBindingTrigger(binding.Trigger); err != nil {
		return err
	}
	if err := validateRuntimeContext(binding.Runtime); err != nil {
		return err
	}
	if binding.Debounce != nil {
		if err := validateSafety(Safety{Debounce: binding.Debounce}); err != nil {
			return err
		}
	}
	if binding.Idempotency.Scope != "" || binding.Idempotency.Target != "" {
		if err := validateSafety(Safety{Idempotency: binding.Idempotency}); err != nil {
			return err
		}
	}
	if procedure != nil {
		p := procedure.Normalize()
		if binding.ProcedureID != p.ID {
			return fmt.Errorf("graph automation binding procedure_id does not match procedure")
		}
		if binding.Status == StatusEnabled && p.Status != StatusEnabled {
			return fmt.Errorf("graph automation binding cannot be enabled for a disabled procedure")
		}
		if binding.Status == StatusEnabled && hasInferenceRef(p.Inference) && strings.TrimSpace(p.Inference.Profile) == "" && strings.TrimSpace(p.Inference.ProfileID) == "" && strings.TrimSpace(binding.Runtime.InferenceProfile) == "" && strings.TrimSpace(binding.Runtime.InferenceProfileID) == "" {
			return fmt.Errorf("graph automation binding runtime inference_profile or inference_profile_id is required for inference procedure")
		}
	}
	return nil
}

func validateBindingTrigger(trigger BindingTrigger) error {
	trigger = Binding{Trigger: trigger}.Normalize().Trigger
	switch trigger.Type {
	case TriggerTypeGraphEvent:
		if len(trigger.Events) == 0 {
			return fmt.Errorf("graph automation binding trigger.events is required for graph_event")
		}
	case TriggerTypeSchedule:
		if trigger.Schedule == nil {
			return fmt.Errorf("graph automation binding trigger.schedule is required for schedule")
		}
	case TriggerTypeOneTimeScan:
		if trigger.Scan == nil {
			return fmt.Errorf("graph automation binding trigger.scan is required for one_time_scan")
		}
	default:
		return fmt.Errorf("unsupported graph automation binding trigger.type %q", trigger.Type)
	}
	for _, event := range trigger.Events {
		switch event {
		case EventNodeCreated, EventNodeUpdated:
		default:
			return fmt.Errorf("unsupported graph automation binding event %q", event)
		}
	}
	if trigger.Schedule != nil {
		if _, err := time.ParseDuration(strings.TrimSpace(trigger.Schedule.Interval)); err != nil {
			return fmt.Errorf("graph automation binding trigger.schedule.interval must be a valid duration")
		}
	}
	if trigger.Scan != nil {
		gql := strings.TrimSpace(strings.ToLower(trigger.Scan.GQL))
		if gql == "" || !strings.Contains(gql, " limit ") {
			return fmt.Errorf("graph automation binding trigger.scan.gql must be bounded with LIMIT")
		}
	}
	if strings.TrimSpace(trigger.Condition.GQL) != "" && !strings.Contains(trigger.Condition.GQL, "changed") {
		return fmt.Errorf("graph automation binding trigger.condition must reference changed")
	}
	return nil
}

func validateRuntimeContext(runtime RuntimeContext) error {
	for path, value := range map[string]string{"runtime.actor_principal_id": runtime.ActorPrincipalID, "runtime.owner_principal_id": runtime.OwnerPrincipalID, "runtime.on_behalf_of_principal_id": runtime.OnBehalfOfPrincipalID, "runtime.inference_profile": runtime.InferenceProfile, "runtime.inference_profile_id": runtime.InferenceProfileID} {
		if err := validateReferenceToken("graph automation binding "+path, value); err != nil {
			return err
		}
	}
	if strings.TrimSpace(runtime.InferenceProfileID) != "" {
		if _, err := uuid.Parse(strings.TrimSpace(runtime.InferenceProfileID)); err != nil {
			return fmt.Errorf("graph automation binding runtime.inference_profile_id must be a UUID")
		}
	}
	override := strings.ToLower(strings.TrimSpace(runtime.EventOriginOverride))
	if override != "" && override != RuntimeEventOriginNone && override != RuntimeEventOriginAllow {
		return fmt.Errorf("graph automation binding runtime.event_origin_override must be disabled or if_present")
	}
	return nil
}

func validateProcedureInferenceRef(path string, ref InferenceRef) error {
	if !hasInferenceRef(ref) {
		return nil
	}
	operation := strings.ToLower(strings.TrimSpace(ref.Operation))
	if operation == "" {
		return fmt.Errorf("%s.operation is required", path)
	}
	if operation != "chat" && operation != "summarize" && operation != "classify" {
		return fmt.Errorf("%s.operation must be chat, summarize, or classify", path)
	}
	if err := validateReferenceToken(path+".profile", ref.Profile); err != nil {
		return err
	}
	if err := validateReferenceToken(path+".profile_id", ref.ProfileID); err != nil {
		return err
	}
	if strings.TrimSpace(ref.ProfileID) != "" {
		if _, err := uuid.Parse(strings.TrimSpace(ref.ProfileID)); err != nil {
			return fmt.Errorf("%s.profile_id must be a UUID", path)
		}
	}
	for name, value := range map[string]string{"capability_ref": ref.CapabilityRef, "endpoint_ref": ref.EndpointRef, "model_ref": ref.ModelRef} {
		if err := validateReferenceToken(path+"."+name, value); err != nil {
			return err
		}
	}
	params := ref.Parameters
	if params.Temperature != nil && (*params.Temperature < 0 || *params.Temperature > 2) {
		return fmt.Errorf("%s.parameters.temperature must be between 0 and 2", path)
	}
	if params.MaxInputTokens < 0 {
		return fmt.Errorf("%s.parameters.maxInputTokens must be non-negative", path)
	}
	if params.MaxOutputTokens < 0 {
		return fmt.Errorf("%s.parameters.maxOutputTokens must be non-negative", path)
	}
	switch strings.ToLower(strings.TrimSpace(params.ResponseFormat)) {
	case "", "text", "json", "json_object":
	default:
		return fmt.Errorf("%s.parameters.responseFormat must be text, json, or json_object", path)
	}
	return nil
}
