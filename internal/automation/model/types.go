package model

import (
	"encoding/json"
	"strings"
	"time"

	graph "github.com/myceldb/mycel/internal/graph/model"
)

const (
	StatusEnabled  = "enabled"
	StatusDisabled = "disabled"

	EventNodeCreated = "node.created"
	EventNodeUpdated = "node.updated"

	InputModeFields   = "fields"
	InputModeMarkdown = "markdown"
	InputModeTemplate = "template"

	OutputModeText = "text"
	OutputModeJSON = "json"
)

type Definition struct {
	ID                   string          `json:"id"`
	Name                 string          `json:"name,omitempty"`
	Version              int             `json:"version"`
	DomainID             graph.DomainID  `json:"domain_id"`
	Status               string          `json:"status"`
	Trigger              Trigger         `json:"on"`
	Condition            Condition       `json:"condition"`
	Input                Input           `json:"input"`
	Inference            InferenceRef    `json:"inference,omitempty"`
	LegacyModelRef       *LegacyModelRef `json:"model,omitempty"`
	Prompt               string          `json:"prompt"`
	Output               Output          `json:"output,omitempty"`
	Workflow             *Workflow       `json:"workflow,omitempty"`
	Safety               Safety          `json:"safety,omitempty"`
	OwnerPrincipalID     string          `json:"owner_principal_id,omitempty"`
	CreatedByPrincipalID string          `json:"created_by_principal_id,omitempty"`
	UpdatedByPrincipalID string          `json:"updated_by_principal_id,omitempty"`
	CreatedAt            time.Time       `json:"created_at,omitempty"`
	UpdatedAt            time.Time       `json:"updated_at,omitempty"`
}

type Workflow struct {
	Steps []WorkflowStep `json:"steps"`
}

type WorkflowStep struct {
	ID             string            `json:"id"`
	Kind           string            `json:"kind"`
	DependsOn      []string          `json:"dependsOn,omitempty"`
	Condition      Condition         `json:"condition,omitempty"`
	Input          Input             `json:"input,omitempty"`
	Inference      InferenceRef      `json:"inference,omitempty"`
	LegacyModelRef *LegacyModelRef   `json:"model,omitempty"`
	Prompt         string            `json:"prompt,omitempty"`
	Output         Output            `json:"output,omitempty"`
	Approval       string            `json:"approval,omitempty"`
	Tool           string            `json:"tool,omitempty"`
	ToolInput      map[string]string `json:"toolInput,omitempty"`
	MaxAttempts    int               `json:"maxAttempts,omitempty"`
}

const (
	WorkflowStepCondition = "condition"
	WorkflowStepRender    = "render"
	WorkflowStepLLM       = "llm"
	WorkflowStepAction    = "action"
	WorkflowStepProposal  = "proposal"
	WorkflowStepTool      = "tool"
)

type Trigger struct {
	Events   []string         `json:"events,omitempty"`
	Labels   []string         `json:"labels,omitempty"`
	Schedule *ScheduleTrigger `json:"schedule,omitempty"`
	Scan     *ScanTrigger     `json:"scan,omitempty"`
}

type ScheduleTrigger struct {
	Interval string `json:"interval"`
}

type ScanTrigger struct {
	GQL string `json:"gql"`
}

type Condition struct {
	GQL string `json:"gql"`
}

type Input struct {
	Target   string   `json:"target,omitempty"`
	Fields   []string `json:"fields,omitempty"`
	Mode     string   `json:"mode,omitempty"`
	Template string   `json:"template,omitempty"`
}

type InferenceRef struct {
	Operation     string              `json:"operation,omitempty"`
	Profile       string              `json:"profile,omitempty"`
	ProfileID     string              `json:"profile_id,omitempty"`
	CapabilityRef string              `json:"capability_ref,omitempty"`
	EndpointRef   string              `json:"endpoint_ref,omitempty"`
	ModelRef      string              `json:"model_ref,omitempty"`
	Parameters    InferenceParameters `json:"parameters,omitempty"`
}

type InferenceParameters struct {
	Temperature     *float64       `json:"temperature,omitempty"`
	MaxInputTokens  int            `json:"maxInputTokens,omitempty"`
	MaxOutputTokens int            `json:"maxOutputTokens,omitempty"`
	ResponseFormat  string         `json:"responseFormat,omitempty"`
	Metadata        map[string]any `json:"metadata,omitempty"`
}

type LegacyModelRef struct {
	Provider        string   `json:"provider,omitempty"`
	Model           string   `json:"model,omitempty"`
	Temperature     *float64 `json:"temperature,omitempty"`
	MaxOutputTokens int      `json:"maxOutputTokens,omitempty"`
}

type Output struct {
	Mode    string          `json:"mode"`
	Schema  json.RawMessage `json:"schema,omitempty"`
	Actions []Action        `json:"actions"`
}

type Action struct {
	UpdateNode *UpdateNodeAction `json:"update_node,omitempty"`
	CreateNode *CreateNodeAction `json:"create_node,omitempty"`
	CreateEdge *EdgeAction       `json:"create_edge,omitempty"`
	UpsertEdge *EdgeAction       `json:"upsert_edge,omitempty"`
}

type UpdateNodeAction struct {
	Target string            `json:"target"`
	Set    map[string]string `json:"set"`
}

type CreateNodeAction struct {
	As         string            `json:"as,omitempty"`
	Labels     []string          `json:"labels,omitempty"`
	Properties map[string]string `json:"properties,omitempty"`
	Payload    map[string]string `json:"payload,omitempty"`
	ForEach    string            `json:"for_each,omitempty"`
	UpsertKey  []string          `json:"upsert_key,omitempty"`
}

type EdgeAction struct {
	From       string            `json:"from"`
	To         string            `json:"to"`
	Label      string            `json:"label"`
	Properties map[string]string `json:"properties,omitempty"`
	ForEach    string            `json:"for_each,omitempty"`
}

type Safety struct {
	IgnoreSelfWrites bool        `json:"ignoreSelfWrites"`
	Idempotency      Idempotency `json:"idempotency,omitempty"`
	MaxActionItems   int         `json:"maxActionItems,omitempty"`
	MaxAttempts      int         `json:"maxAttempts,omitempty"`
}

type Idempotency struct {
	InputHashFields       []string `json:"inputHashFields,omitempty"`
	SkipIfOutputUnchanged bool     `json:"skipIfOutputUnchanged,omitempty"`
}

type Invocation struct {
	ID                         string         `json:"id"`
	DomainID                   graph.DomainID `json:"domain_id"`
	SpaceID                    string         `json:"space_id,omitempty"`
	AutomationID               string         `json:"automation_id"`
	AutomationVersion          int            `json:"automation_version"`
	EventID                    string         `json:"event_id"`
	ChangedElementID           string         `json:"changed_element_id"`
	ChangedElementKind         string         `json:"changed_element_kind"`
	OldNode                    *graph.Node    `json:"old_node,omitempty"`
	EventType                  string         `json:"event_type"`
	ActorPrincipalID           string         `json:"actor_principal_id,omitempty"`
	OnBehalfOfPrincipalID      string         `json:"on_behalf_of_principal_id,omitempty"`
	AutomationOwnerPrincipalID string         `json:"automation_owner_principal_id,omitempty"`
	InputHash                  string         `json:"input_hash,omitempty"`
	Status                     string         `json:"status"`
	SkipReason                 string         `json:"skip_reason,omitempty"`
	AttemptCount               int            `json:"attempt_count,omitempty"`
	NextAttemptAt              time.Time      `json:"next_attempt_at,omitempty"`
	CreatedAt                  time.Time      `json:"created_at,omitempty"`
	UpdatedAt                  time.Time      `json:"updated_at,omitempty"`
}

type WorkflowInstance struct {
	ID                string         `json:"id"`
	DomainID          graph.DomainID `json:"domain_id"`
	AutomationID      string         `json:"automation_id"`
	AutomationVersion int            `json:"automation_version"`
	InvocationID      string         `json:"invocation_id,omitempty"`
	ChangedElementID  string         `json:"changed_element_id,omitempty"`
	Status            string         `json:"status"`
	CreatedAt         time.Time      `json:"created_at,omitempty"`
	UpdatedAt         time.Time      `json:"updated_at,omitempty"`
	CompletedAt       time.Time      `json:"completed_at,omitempty"`
}

type Proposal struct {
	ID         string         `json:"id"`
	DomainID   graph.DomainID `json:"domain_id"`
	InstanceID string         `json:"instance_id,omitempty"`
	StepID     string         `json:"step_id,omitempty"`
	Status     string         `json:"status"`
	Actions    []Action       `json:"actions,omitempty"`
	Summary    string         `json:"summary,omitempty"`
	Reviewer   string         `json:"reviewer,omitempty"`
	CreatedAt  time.Time      `json:"created_at,omitempty"`
	UpdatedAt  time.Time      `json:"updated_at,omitempty"`
}

type Policy struct {
	DomainID         graph.DomainID `json:"domain_id"`
	MaxWorkflowSteps int            `json:"max_workflow_steps,omitempty"`
	MaxToolCalls     int            `json:"max_tool_calls,omitempty"`
	MaxProviderCalls int            `json:"max_provider_calls,omitempty"`
	RequireApproval  bool           `json:"require_approval,omitempty"`
	MaxInputTokens   int64          `json:"max_input_tokens,omitempty"`
	MaxOutputTokens  int64          `json:"max_output_tokens,omitempty"`
	MaxElapsedMillis int64          `json:"max_elapsed_millis,omitempty"`
	AllowCrossDomain bool           `json:"allow_cross_domain,omitempty"`
	AllowedTools     []string       `json:"allowed_tools,omitempty"`
}

type WorkflowStepRun struct {
	ID                string         `json:"id"`
	DomainID          graph.DomainID `json:"domain_id"`
	InstanceID        string         `json:"instance_id"`
	StepID            string         `json:"step_id"`
	AttemptNumber     int            `json:"attempt_number"`
	Status            string         `json:"status"`
	RenderedInputHash string         `json:"rendered_input_hash,omitempty"`
	OutputHash        string         `json:"output_hash,omitempty"`
	Error             string         `json:"error,omitempty"`
	StartedAt         time.Time      `json:"started_at,omitempty"`
	CompletedAt       time.Time      `json:"completed_at,omitempty"`
}

type Run struct {
	ID                         string         `json:"id"`
	DomainID                   graph.DomainID `json:"domain_id"`
	InvocationID               string         `json:"invocation_id"`
	AttemptNumber              int            `json:"attempt_number"`
	Status                     string         `json:"status"`
	RenderedInputHash          string         `json:"rendered_input_hash,omitempty"`
	ActorPrincipalID           string         `json:"actor_principal_id,omitempty"`
	OnBehalfOfPrincipalID      string         `json:"on_behalf_of_principal_id,omitempty"`
	AutomationOwnerPrincipalID string         `json:"automation_owner_principal_id,omitempty"`
	InferenceProfile           string         `json:"inference_profile,omitempty"`
	InferenceProfileID         string         `json:"inference_profile_id,omitempty"`
	ModelEndpointID            string         `json:"model_endpoint_id,omitempty"`
	ModelID                    string         `json:"model_id,omitempty"`
	CapabilityID               string         `json:"model_endpoint_capability_id,omitempty"`
	CredentialID               string         `json:"credential_id,omitempty"`
	CredentialGrantID          string         `json:"credential_grant_id,omitempty"`
	PolicyDecisionID           string         `json:"policy_decision_id,omitempty"`
	OutputHash                 string         `json:"output_hash,omitempty"`
	ProviderRequestID          string         `json:"provider_request_id,omitempty"`
	Usage                      TokenUsage     `json:"usage,omitempty"`
	ActionFingerprint          string         `json:"action_fingerprint,omitempty"`
	MutationID                 string         `json:"mutation_id,omitempty"`
	Error                      string         `json:"error,omitempty"`
	StartedAt                  time.Time      `json:"started_at,omitempty"`
	CompletedAt                time.Time      `json:"completed_at,omitempty"`
}

type TokenUsage struct {
	InputTokens       int64          `json:"input_tokens,omitempty"`
	OutputTokens      int64          `json:"output_tokens,omitempty"`
	TotalTokens       int64          `json:"total_tokens,omitempty"`
	CachedInputTokens int64          `json:"cached_input_tokens,omitempty"`
	ReasoningTokens   int64          `json:"reasoning_tokens,omitempty"`
	Status            string         `json:"status,omitempty"`
	Metadata          map[string]any `json:"metadata,omitempty"`
}

func (d Definition) Normalize() Definition {
	d.ID = strings.TrimSpace(d.ID)
	d.Name = strings.TrimSpace(d.Name)
	d.OwnerPrincipalID = strings.TrimSpace(d.OwnerPrincipalID)
	d.CreatedByPrincipalID = strings.TrimSpace(d.CreatedByPrincipalID)
	d.UpdatedByPrincipalID = strings.TrimSpace(d.UpdatedByPrincipalID)
	if d.Version == 0 {
		d.Version = 1
	}
	d.Status = strings.ToLower(strings.TrimSpace(d.Status))
	if d.Status == "" {
		d.Status = StatusDisabled
	}
	d.Input.Target = firstNonEmpty(strings.TrimSpace(d.Input.Target), "changed")
	d.Input.Mode = strings.ToLower(strings.TrimSpace(d.Input.Mode))
	if d.Input.Mode == "" {
		if strings.TrimSpace(d.Input.Template) != "" {
			d.Input.Mode = InputModeTemplate
		} else {
			d.Input.Mode = InputModeFields
		}
	}
	d.Output.Mode = strings.ToLower(strings.TrimSpace(d.Output.Mode))
	if d.Output.Mode == "" {
		d.Output.Mode = OutputModeText
	}
	for i := range d.Trigger.Events {
		d.Trigger.Events[i] = strings.ToLower(strings.TrimSpace(d.Trigger.Events[i]))
	}
	for i := range d.Trigger.Labels {
		d.Trigger.Labels[i] = strings.TrimSpace(d.Trigger.Labels[i])
	}
	if !d.Safety.IgnoreSelfWrites {
		d.Safety.IgnoreSelfWrites = true
	}
	return d
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
