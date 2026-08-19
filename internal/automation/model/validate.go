package model

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/query/gql"
	"github.com/myceldb/mycel/internal/query/gql/analysis"
	planmodel "github.com/myceldb/mycel/internal/query/gql/planning/model"
)

const defaultMaxActions = 10

func ValidateDefinition(def Definition) error {
	def = def.Normalize()
	if def.ID == "" {
		return fmt.Errorf("automation id is required")
	}
	if def.Status != StatusEnabled && def.Status != StatusDisabled {
		return fmt.Errorf("automation status must be enabled or disabled")
	}
	if len(def.Trigger.Events) == 0 && def.Trigger.Schedule == nil {
		return fmt.Errorf("automation trigger must include at least one event or schedule")
	}
	for _, event := range def.Trigger.Events {
		switch event {
		case EventNodeCreated, EventNodeUpdated:
		default:
			return fmt.Errorf("unsupported automation event %q", event)
		}
	}
	if def.Trigger.Schedule != nil {
		if _, err := time.ParseDuration(strings.TrimSpace(def.Trigger.Schedule.Interval)); err != nil {
			return fmt.Errorf("automation schedule.interval must be a valid duration")
		}
	}
	if def.Trigger.Scan != nil {
		gql := strings.TrimSpace(strings.ToLower(def.Trigger.Scan.GQL))
		if gql == "" || !strings.Contains(gql, " limit ") {
			return fmt.Errorf("automation scan.gql must be bounded with LIMIT")
		}
	}
	if strings.TrimSpace(def.Condition.GQL) != "" && !strings.Contains(def.Condition.GQL, "changed") {
		return fmt.Errorf("automation condition must reference changed")
	}
	if err := validateInput(def.Input); err != nil {
		return err
	}
	if err := rejectSecretBearingDefinition(def); err != nil {
		return err
	}
	if def.Workflow != nil {
		return validateWorkflow(*def.Workflow)
	}
	if err := validateSafety(def.Safety); err != nil {
		return err
	}
	if err := validateInferenceRef("automation inference", def.Inference, true); err != nil {
		return err
	}
	if strings.TrimSpace(def.Prompt) == "" {
		return fmt.Errorf("automation prompt is required")
	}
	return validateOutput(def.Output, def.Safety)
}

func validateWorkflow(workflow Workflow) error {
	if len(workflow.Steps) == 0 {
		return fmt.Errorf("automation workflow.steps must include at least one step")
	}
	if len(workflow.Steps) > 50 {
		return fmt.Errorf("automation workflow.steps exceeds maximum of 50")
	}
	seen := map[string]WorkflowStep{}
	for _, step := range workflow.Steps {
		step.ID = strings.TrimSpace(step.ID)
		if step.ID == "" {
			return fmt.Errorf("automation workflow step id is required")
		}
		if _, exists := seen[step.ID]; exists {
			return fmt.Errorf("duplicate workflow step id %q", step.ID)
		}
		switch strings.TrimSpace(step.Kind) {
		case WorkflowStepCondition, WorkflowStepRender, WorkflowStepLLM, WorkflowStepAction, WorkflowStepProposal, WorkflowStepTool:
		default:
			return fmt.Errorf("unsupported workflow step kind %q", step.Kind)
		}
		if strings.TrimSpace(step.Kind) == WorkflowStepTool && strings.TrimSpace(step.Tool) == "" {
			return fmt.Errorf("workflow tool step %q requires tool", step.ID)
		}
		if strings.TrimSpace(step.Kind) == WorkflowStepLLM {
			if err := validateInferenceRef(fmt.Sprintf("workflow llm step %q inference", step.ID), step.Inference, true); err != nil {
				return err
			}
		} else if hasInferenceRef(step.Inference) {
			if err := validateInferenceRef(fmt.Sprintf("workflow step %q inference", step.ID), step.Inference, false); err != nil {
				return err
			}
		}
		seen[step.ID] = step
	}
	visiting, visited := map[string]bool{}, map[string]bool{}
	var visit func(string) error
	visit = func(id string) error {
		if visited[id] {
			return nil
		}
		if visiting[id] {
			return fmt.Errorf("workflow dependency cycle at %q", id)
		}
		visiting[id] = true
		for _, dep := range seen[id].DependsOn {
			dep = strings.TrimSpace(dep)
			if _, ok := seen[dep]; !ok {
				return fmt.Errorf("workflow step %q depends on unknown step %q", id, dep)
			}
			if err := visit(dep); err != nil {
				return err
			}
		}
		visiting[id] = false
		visited[id] = true
		return nil
	}
	for id := range seen {
		if err := visit(id); err != nil {
			return err
		}
	}
	return nil
}

func validateInput(input Input) error {
	if err := validateIdentifierRef("automation input target", input.Target); err != nil {
		return err
	}
	switch input.Mode {
	case InputModeFields, InputModeMarkdown:
		if len(input.Fields) == 0 {
			return fmt.Errorf("automation input.fields must include at least one field")
		}
	case InputModeTemplate, InputModeGQLTemplate:
		if strings.TrimSpace(input.Template) == "" {
			return fmt.Errorf("automation input.template is required for %s mode", input.Mode)
		}
	default:
		return fmt.Errorf("unsupported automation input.mode %q", input.Mode)
	}
	for _, field := range input.Fields {
		if strings.TrimSpace(field) == "" {
			return fmt.Errorf("automation input field path is required")
		}
	}
	if err := validateContextQueries(input); err != nil {
		return err
	}
	return nil
}

func validateContextQueries(input Input) error {
	for name, query := range input.Context {
		if err := validateIdentifierRef("automation input.context name", name); err != nil {
			return err
		}
		queryText := strings.TrimSpace(query.GQL)
		if queryText == "" {
			return fmt.Errorf("automation input.context.%s.gql is required", name)
		}
		plan, err := gql.Compile(queryText)
		if err != nil {
			return fmt.Errorf("automation input.context.%s.gql is invalid: %w", name, err)
		}
		if plan.AccessMode == analysis.ReadWrite {
			return fmt.Errorf("automation input.context.%s.gql must be read-only", name)
		}
		planLimit := queryPlanLimit(plan)
		if planLimit <= 0 {
			return fmt.Errorf("automation input.context.%s.gql must be bounded with FETCH FIRST", name)
		}
		if planLimit > 500 {
			return fmt.Errorf("automation input.context.%s.gql limit must be <= 500", name)
		}
		if !queryPlanPatternsAreLabelBounded(plan) {
			return fmt.Errorf("automation input.context.%s.gql relationship patterns must include a label", name)
		}
		if query.Limit < 0 {
			return fmt.Errorf("automation input.context.%s.limit must be non-negative", name)
		}
		if query.Limit > 500 {
			return fmt.Errorf("automation input.context.%s.limit must be <= 500", name)
		}
		if !queryPlanReferencesAlias(plan, []string{"changed", input.Target}) {
			return fmt.Errorf("automation input.context.%s.gql must reference changed or the input target", name)
		}
	}
	return nil
}

func queryPlanLimit(plan planmodel.Plan) int64 {
	limit := int64(0)
	for _, op := range plan.Operations {
		opLimit := int64(0)
		switch typed := op.(type) {
		case planmodel.QueryNodesOperation:
			opLimit = typed.Limit
		case planmodel.QueryPatternOperation:
			opLimit = typed.Limit
		case planmodel.QueryPathOperation:
			opLimit = typed.Limit
		}
		if opLimit <= 0 {
			return 0
		}
		if limit == 0 || opLimit < limit {
			limit = opLimit
		}
	}
	return limit
}

func queryPlanReferencesAlias(plan planmodel.Plan, aliases []string) bool {
	allowed := map[string]struct{}{}
	for _, alias := range aliases {
		alias = strings.TrimSpace(alias)
		if alias != "" {
			allowed[alias] = struct{}{}
		}
	}
	for _, op := range plan.Operations {
		switch typed := op.(type) {
		case planmodel.QueryNodesOperation:
			if _, ok := allowed[typed.Variable]; ok {
				return true
			}
		case planmodel.QueryPatternOperation:
			if _, ok := allowed[typed.Start.Variable]; ok {
				return true
			}
			if _, ok := allowed[typed.End.Variable]; ok {
				return true
			}
		case planmodel.QueryPathOperation:
			if _, ok := allowed[typed.Start.Variable]; ok {
				return true
			}
			for _, segment := range typed.Segments {
				if _, ok := allowed[segment.Node.Variable]; ok {
					return true
				}
			}
		}
	}
	return false
}

func queryPlanPatternsAreLabelBounded(plan planmodel.Plan) bool {
	for _, op := range plan.Operations {
		switch typed := op.(type) {
		case planmodel.QueryPatternOperation:
			if len(typed.Relationship.Labels) == 0 {
				return false
			}
		case planmodel.QueryPathOperation:
			for _, segment := range typed.Segments {
				if len(segment.Relationship.Labels) == 0 {
					return false
				}
			}
		}
	}
	return true
}

func validateSafety(safety Safety) error {
	scope := strings.TrimSpace(safety.Idempotency.Scope)
	if scope != "" && scope != "changed" && scope != "target" {
		return fmt.Errorf("automation safety.idempotency.scope must be changed or target")
	}
	if scope == "target" && strings.TrimSpace(safety.Idempotency.Target) == "" {
		return fmt.Errorf("automation safety.idempotency.target is required for target scope")
	}
	if strings.TrimSpace(safety.Idempotency.Target) != "" {
		if err := validateIdentifierRef("automation safety.idempotency.target", safety.Idempotency.Target); err != nil {
			return err
		}
	}
	if safety.Debounce != nil {
		duration, err := time.ParseDuration(strings.TrimSpace(safety.Debounce.Duration))
		if err != nil || duration <= 0 {
			return fmt.Errorf("automation safety.debounce.duration must be a positive duration")
		}
		if duration > 24*time.Hour {
			return fmt.Errorf("automation safety.debounce.duration must be <= 24h")
		}
		if err := validateIdentifierRef("automation safety.debounce.coalesceBy", safety.Debounce.CoalesceBy); err != nil {
			return err
		}
	}
	return nil
}

func validateIdentifierRef(path string, value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return fmt.Errorf("%s is required", path)
	}
	if strings.ContainsAny(value, " \t\n\r") {
		return fmt.Errorf("%s must not contain whitespace", path)
	}
	return nil
}

func validateInferenceRef(path string, ref InferenceRef, required bool) error {
	if required && !hasInferenceRef(ref) {
		return fmt.Errorf("%s is required", path)
	}
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
	if strings.TrimSpace(ref.Profile) == "" && strings.TrimSpace(ref.ProfileID) == "" {
		return fmt.Errorf("%s.profile or profile_id is required", path)
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

func hasInferenceRef(ref InferenceRef) bool {
	return strings.TrimSpace(ref.Operation) != "" || strings.TrimSpace(ref.Profile) != "" || strings.TrimSpace(ref.ProfileID) != "" || strings.TrimSpace(ref.CapabilityRef) != "" || strings.TrimSpace(ref.EndpointRef) != "" || strings.TrimSpace(ref.ModelRef) != "" || ref.Parameters.Temperature != nil || ref.Parameters.MaxInputTokens != 0 || ref.Parameters.MaxOutputTokens != 0 || strings.TrimSpace(ref.Parameters.ResponseFormat) != "" || len(ref.Parameters.Metadata) != 0
}

func validateReferenceToken(path string, value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	if strings.ContainsAny(value, " \t\n\r") {
		return fmt.Errorf("%s must not contain whitespace", path)
	}
	lower := strings.ToLower(value)
	if strings.HasPrefix(lower, "secret://") || strings.HasPrefix(lower, "file://") {
		return fmt.Errorf("%s must not reference secrets", path)
	}
	if u, err := url.Parse(value); err == nil && u.Scheme != "" && u.Host != "" {
		return fmt.Errorf("%s must not be a raw endpoint URL", path)
	}
	return nil
}

func validateOutput(output Output, safety Safety) error {
	switch output.Mode {
	case OutputModeText, OutputModeJSON:
	default:
		return fmt.Errorf("unsupported automation output.mode %q", output.Mode)
	}
	if output.Mode == OutputModeJSON {
		if len(output.Schema) == 0 {
			return fmt.Errorf("automation output.schema is required for json mode")
		}
		var schema any
		if err := json.Unmarshal(output.Schema, &schema); err != nil {
			return fmt.Errorf("automation output.schema must be valid JSON: %w", err)
		}
	}
	if len(output.Actions) == 0 {
		return fmt.Errorf("automation output.actions must include at least one action")
	}
	maxActions := defaultMaxActions
	if safety.MaxActionItems > 0 {
		maxActions = safety.MaxActionItems
	}
	if len(output.Actions) > maxActions {
		return fmt.Errorf("automation output.actions exceeds maximum of %d", maxActions)
	}
	for i, action := range output.Actions {
		if err := validateAction(action); err != nil {
			return fmt.Errorf("automation output.actions[%d]: %w", i, err)
		}
	}
	return nil
}

func validateAction(action Action) error {
	count := 0
	if action.UpdateNode != nil {
		count++
	}
	if action.CreateNode != nil {
		count++
	}
	if action.CreateEdge != nil {
		count++
	}
	if action.UpsertEdge != nil {
		count++
	}
	if count != 1 {
		return fmt.Errorf("exactly one action kind is required")
	}
	if action.UpdateNode != nil {
		return validateUpdateNode(*action.UpdateNode)
	}
	if action.CreateNode != nil {
		return validateCreateNode(*action.CreateNode)
	}
	if action.CreateEdge != nil {
		return validateEdgeAction("create_edge", *action.CreateEdge)
	}
	return validateEdgeAction("upsert_edge", *action.UpsertEdge)
}

func validateUpdateNode(action UpdateNodeAction) error {
	if strings.TrimSpace(action.Target) == "" {
		return fmt.Errorf("update_node target is required")
	}
	if len(action.Set) == 0 {
		return fmt.Errorf("update_node.set must contain at least one field")
	}
	for path, value := range action.Set {
		if strings.TrimSpace(path) == "" {
			return fmt.Errorf("update_node.set field path is required")
		}
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("update_node.set value is required")
		}
	}
	return nil
}

func validateCreateNode(action CreateNodeAction) error {
	if strings.TrimSpace(action.As) == "" {
		return fmt.Errorf("create_node.as is required")
	}
	if len(action.Labels) == 0 {
		return fmt.Errorf("create_node.labels must include at least one label")
	}
	for _, label := range action.Labels {
		if strings.TrimSpace(label) == "" {
			return fmt.Errorf("create_node label is required")
		}
	}
	if len(action.Properties) == 0 && len(action.Payload) == 0 {
		return fmt.Errorf("create_node must set at least one property or payload field")
	}
	if err := validateStringMap("create_node.properties", action.Properties); err != nil {
		return err
	}
	if err := validateStringMap("create_node.payload", action.Payload); err != nil {
		return err
	}
	if strings.TrimSpace(action.ForEach) != "" && !strings.HasPrefix(strings.TrimSpace(action.ForEach), "$result.") {
		return fmt.Errorf("create_node.for_each must reference $result")
	}
	return nil
}

func validateEdgeAction(name string, action EdgeAction) error {
	if strings.TrimSpace(action.From) == "" {
		return fmt.Errorf("%s.from is required", name)
	}
	if strings.TrimSpace(action.To) == "" {
		return fmt.Errorf("%s.to is required", name)
	}
	if strings.TrimSpace(action.Label) == "" {
		return fmt.Errorf("%s.label is required", name)
	}
	if err := validateStringMap(name+".properties", action.Properties); err != nil {
		return err
	}
	if strings.TrimSpace(action.ForEach) != "" && !strings.HasPrefix(strings.TrimSpace(action.ForEach), "$result.") {
		return fmt.Errorf("%s.for_each must reference $result", name)
	}
	return nil
}

func validateStringMap(name string, values map[string]string) error {
	for key, value := range values {
		if strings.TrimSpace(key) == "" {
			return fmt.Errorf("%s field path is required", name)
		}
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s value is required", name)
		}
	}
	return nil
}

func rejectSecretBearingDefinition(def Definition) error {
	raw, err := json.Marshal(def)
	if err != nil {
		return err
	}
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return err
	}
	return rejectSecretBearingValue("automation definition", decoded, "")
}

func rejectSecretBearingValue(path string, value any, keyHint string) error {
	sensitiveKey := isSensitiveDefinitionKey(keyHint)
	switch v := value.(type) {
	case map[string]any:
		for key, child := range v {
			if err := rejectSecretBearingValue(path+"."+key, child, key); err != nil {
				return err
			}
		}
	case []any:
		for i, child := range v {
			if err := rejectSecretBearingValue(fmt.Sprintf("%s[%d]", path, i), child, keyHint); err != nil {
				return err
			}
		}
	case string:
		text := strings.TrimSpace(v)
		lower := strings.ToLower(text)
		if strings.HasPrefix(lower, "secret://") || strings.HasPrefix(lower, "file://") {
			return fmt.Errorf("%s must not contain secret references", path)
		}
		if strings.HasPrefix(lower, "bearer ") {
			return fmt.Errorf("%s must not contain bearer tokens", path)
		}
		if sensitiveKey && text != "" {
			return fmt.Errorf("%s must not contain embedded credentials or endpoint URLs", path)
		}
		if isURLKey(keyHint) {
			if u, err := url.Parse(text); err == nil && u.Scheme != "" && u.Host != "" {
				return fmt.Errorf("%s must not contain raw endpoint URLs", path)
			}
		}
	}
	return nil
}

func isSensitiveDefinitionKey(key string) bool {
	key = normalizedDefinitionKey(key)
	sensitive := map[string]bool{"apikey": true, "api_key": true, "token": true, "bearertoken": true, "bearer_token": true, "secret": true, "secretref": true, "secret_ref": true, "credential": true, "credentialid": true, "credential_id": true, "password": true, "authorization": true}
	return sensitive[key]
}

func isURLKey(key string) bool {
	key = normalizedDefinitionKey(key)
	return key == "url" || key == "endpoint" || key == "endpointurl" || key == "endpoint_url" || key == "baseurl" || key == "base_url"
}

func normalizedDefinitionKey(key string) string {
	key = strings.ToLower(strings.TrimSpace(key))
	key = strings.ReplaceAll(key, "-", "_")
	return key
}
