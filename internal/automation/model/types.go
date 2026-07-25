package model

import (
	"strings"
	"time"

	graph "github.com/myceldb/mycel/internal/graph/model"
)

const (
	StatusEnabled  = "enabled"
	StatusDisabled = "disabled"

	EventNodeCreated = "node.created"
	EventNodeUpdated = "node.updated"
)

type Definition struct {
	ID        string         `json:"id"`
	Name      string         `json:"name,omitempty"`
	Version   int            `json:"version"`
	DomainID  graph.DomainID `json:"domain_id"`
	Status    string         `json:"status"`
	Trigger   Trigger        `json:"on"`
	Condition Condition      `json:"condition"`
	Input     Input          `json:"input"`
	Model     Model          `json:"model,omitempty"`
	Prompt    string         `json:"prompt"`
	Output    Output         `json:"output"`
	Safety    Safety         `json:"safety,omitempty"`
	CreatedAt time.Time      `json:"created_at,omitempty"`
	UpdatedAt time.Time      `json:"updated_at,omitempty"`
}

type Trigger struct {
	Events []string `json:"events"`
	Labels []string `json:"labels,omitempty"`
}

type Condition struct {
	GQL string `json:"gql"`
}

type Input struct {
	Target string   `json:"target"`
	Fields []string `json:"fields"`
}

type Model struct {
	Provider string `json:"provider,omitempty"`
	Model    string `json:"model,omitempty"`
}

type Output struct {
	Mode    string   `json:"mode"`
	Actions []Action `json:"actions"`
}

type Action struct {
	UpdateNode *UpdateNodeAction `json:"update_node,omitempty"`
}

type UpdateNodeAction struct {
	Target string            `json:"target"`
	Set    map[string]string `json:"set"`
}

type Safety struct {
	IgnoreSelfWrites bool        `json:"ignoreSelfWrites"`
	Idempotency      Idempotency `json:"idempotency,omitempty"`
}

type Idempotency struct {
	InputHashFields       []string `json:"inputHashFields,omitempty"`
	SkipIfOutputUnchanged bool     `json:"skipIfOutputUnchanged,omitempty"`
}

type Invocation struct {
	ID                 string         `json:"id"`
	DomainID           graph.DomainID `json:"domain_id"`
	SpaceID            string         `json:"space_id,omitempty"`
	AutomationID       string         `json:"automation_id"`
	AutomationVersion  int            `json:"automation_version"`
	EventID            string         `json:"event_id"`
	ChangedElementID   string         `json:"changed_element_id"`
	ChangedElementKind string         `json:"changed_element_kind"`
	EventType          string         `json:"event_type"`
	InputHash          string         `json:"input_hash,omitempty"`
	Status             string         `json:"status"`
	SkipReason         string         `json:"skip_reason,omitempty"`
	CreatedAt          time.Time      `json:"created_at,omitempty"`
	UpdatedAt          time.Time      `json:"updated_at,omitempty"`
}

type Run struct {
	ID                string         `json:"id"`
	DomainID          graph.DomainID `json:"domain_id"`
	InvocationID      string         `json:"invocation_id"`
	AttemptNumber     int            `json:"attempt_number"`
	Status            string         `json:"status"`
	RenderedInputHash string         `json:"rendered_input_hash,omitempty"`
	Provider          string         `json:"provider,omitempty"`
	Model             string         `json:"model,omitempty"`
	OutputHash        string         `json:"output_hash,omitempty"`
	MutationID        string         `json:"mutation_id,omitempty"`
	Error             string         `json:"error,omitempty"`
	StartedAt         time.Time      `json:"started_at,omitempty"`
	CompletedAt       time.Time      `json:"completed_at,omitempty"`
}

func (d Definition) Normalize() Definition {
	d.ID = strings.TrimSpace(d.ID)
	d.Name = strings.TrimSpace(d.Name)
	if d.Version == 0 {
		d.Version = 1
	}
	d.Status = strings.ToLower(strings.TrimSpace(d.Status))
	if d.Status == "" {
		d.Status = StatusDisabled
	}
	d.Input.Target = firstNonEmpty(strings.TrimSpace(d.Input.Target), "changed")
	d.Output.Mode = strings.ToLower(strings.TrimSpace(d.Output.Mode))
	if d.Output.Mode == "" {
		d.Output.Mode = "text"
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
