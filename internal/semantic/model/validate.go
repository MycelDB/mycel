package semantic

import (
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/query/gql"
	"github.com/myceldb/mycel/internal/query/gql/analysis"
	planmodel "github.com/myceldb/mycel/internal/query/gql/planning/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

const semanticSelectorMaxRows int64 = 500

var semanticBudgetIdentityPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]*$`)

type ValidationSeverity string

const (
	ValidationSeverityError   ValidationSeverity = "error"
	ValidationSeverityWarning ValidationSeverity = "warning"
	ValidationSeverityInfo    ValidationSeverity = "info"
)

type ValidationDiagnostic struct {
	Severity ValidationSeverity `json:"severity"`
	Path     string             `json:"path"`
	Message  string             `json:"message"`
}

type ValidationResult struct {
	Valid       bool                   `json:"valid"`
	Diagnostics []ValidationDiagnostic `json:"diagnostics,omitempty"`
	Rule        SemanticGenerationRule `json:"normalized_rule"`
}

func ValidateSemanticGenerationRule(rule SemanticGenerationRule) ValidationResult {
	rule = NormalizeSemanticGenerationRule(rule)
	result := ValidationResult{Valid: true, Rule: rule}
	addError := func(path, format string, args ...any) {
		result.Valid = false
		result.Diagnostics = append(result.Diagnostics, ValidationDiagnostic{Severity: ValidationSeverityError, Path: path, Message: fmt.Sprintf(format, args...)})
	}
	if strings.TrimSpace(rule.Key) == "" {
		addError("key", "semantic rule key is required")
	}
	if rule.DomainID == uuid.Nil {
		addError("domain_id", "domain_id is required")
	}
	validateTrigger(resultAppend{&result}, rule.Trigger)
	validateSelector(resultAppend{&result}, rule.Selector)
	validateSource(resultAppend{&result}, rule.Source)
	validateBindings(resultAppend{&result}, rule.Embeddings)
	return result
}

func ValidateSemanticGenerationRuleForStorage(spaceID domainspace.SpaceID, rule SemanticGenerationRule) ValidationResult {
	result := ValidateSemanticGenerationRule(rule)
	if result.Rule.SpaceID == uuid.Nil {
		result.Valid = false
		result.Diagnostics = append(result.Diagnostics, ValidationDiagnostic{Severity: ValidationSeverityError, Path: "space_id", Message: "space_id is required"})
	} else if result.Rule.SpaceID != spaceID {
		result.Valid = false
		result.Diagnostics = append(result.Diagnostics, ValidationDiagnostic{Severity: ValidationSeverityError, Path: "space_id", Message: "space_id does not match store"})
	}
	return result
}

func NormalizeSemanticGenerationRule(rule SemanticGenerationRule) SemanticGenerationRule {
	rule.Key = NormalizeSemanticRuleKey(rule.Key)
	rule.DisplayName = strings.TrimSpace(rule.DisplayName)
	rule.Description = strings.TrimSpace(rule.Description)
	rule.Trigger = NormalizeSemanticTriggerPolicy(rule.Trigger)
	if rule.Selector.Mode == "" && len(rule.Selector.Labels) > 0 {
		rule.Selector.Mode = SemanticTargetSelectorNodeType
	}
	for i, label := range rule.Selector.Labels {
		rule.Selector.Labels[i] = strings.TrimSpace(label)
	}
	rule.Selector.GQL = strings.TrimSpace(rule.Selector.GQL)
	rule.Selector.TargetAlias = strings.TrimSpace(rule.Selector.TargetAlias)
	if rule.Source.Mode == "" {
		rule.Source.Mode = SemanticSourceSelf
	}
	for i, property := range rule.Source.IncludeProperties {
		rule.Source.IncludeProperties[i] = strings.TrimSpace(property)
	}
	for i, property := range rule.Source.ExcludeProperties {
		rule.Source.ExcludeProperties[i] = strings.TrimSpace(property)
	}
	rule.Source.ContextGQL = strings.TrimSpace(rule.Source.ContextGQL)
	for i, binding := range rule.Embeddings {
		rule.Embeddings[i] = NormalizeSemanticEmbeddingBinding(binding)
	}
	if rule.Storage.PhysicalIndex == "" {
		rule.Storage = DefaultSemanticStoragePolicy()
	}
	return rule
}

type resultAppend struct{ result *ValidationResult }

func (a resultAppend) err(path, format string, args ...any) {
	a.result.Valid = false
	a.result.Diagnostics = append(a.result.Diagnostics, ValidationDiagnostic{Severity: ValidationSeverityError, Path: path, Message: fmt.Sprintf(format, args...)})
}

func validateTrigger(out resultAppend, trigger SemanticTriggerPolicy) {
	if trigger.Debounce < 0 {
		out.err("trigger.debounce", "trigger debounce must be non-negative")
	}
	for i, event := range trigger.Events {
		switch event {
		case DefaultSemanticTriggerEventChanged,
			SemanticTriggerEventNodeCreated,
			SemanticTriggerEventNodeUpdated,
			SemanticTriggerEventNodeDeleted,
			SemanticTriggerEventEdgeChanged,
			SemanticTriggerEventEdgeCreated,
			SemanticTriggerEventEdgeUpdated,
			SemanticTriggerEventEdgeDeleted:
		default:
			out.err(fmt.Sprintf("trigger.events[%d]", i), "unsupported semantic trigger event %q", event)
		}
	}
	for i, label := range trigger.Labels {
		if strings.TrimSpace(label) == "" {
			out.err(fmt.Sprintf("trigger.labels[%d]", i), "trigger label must not be empty")
		}
	}
}

func validateSelector(out resultAppend, selector SemanticTargetSelector) {
	if selector.MaxResults < 0 {
		out.err("selector.max_results", "selector max_results must be non-negative")
	}
	switch selector.Mode {
	case SemanticTargetSelectorNodeType:
		if len(selector.Labels) == 0 {
			out.err("selector.labels", "node_type selector requires at least one label")
		}
		for i, label := range selector.Labels {
			if strings.TrimSpace(label) == "" {
				out.err(fmt.Sprintf("selector.labels[%d]", i), "selector label must not be empty")
			}
		}
		if selector.GQL != "" || selector.TargetAlias != "" || len(selector.NodeIDs) > 0 {
			out.err("selector", "node_type selector must not set gql, target_alias, or node_ids")
		}
	case SemanticTargetSelectorExplicit:
		if len(selector.NodeIDs) == 0 {
			out.err("selector.node_ids", "explicit_nodes selector requires node_ids")
		}
		for i, id := range selector.NodeIDs {
			if id == uuid.Nil {
				out.err(fmt.Sprintf("selector.node_ids[%d]", i), "selector node_id must not be nil")
			}
		}
		if len(selector.Labels) > 0 || selector.GQL != "" || selector.TargetAlias != "" {
			out.err("selector", "explicit_nodes selector must not set labels, gql, or target_alias")
		}
	case SemanticTargetSelectorGQL:
		out.err("selector.mode", "gql selector execution is not supported")
	case "":
		out.err("selector.mode", "selector mode is required")
	default:
		out.err("selector.mode", "unsupported selector mode %q", selector.Mode)
	}
}

func validateSource(out resultAppend, source SemanticSourceAssemblyPolicy) {
	if source.MaxDepth != nil && *source.MaxDepth < 0 {
		out.err("source.max_depth", "source max_depth must be non-negative")
	}
	if source.MinimumTextLength < 0 {
		out.err("source.minimum_text_length", "source minimum_text_length must be non-negative")
	}
	for i, property := range source.IncludeProperties {
		if strings.TrimSpace(property) == "" {
			out.err(fmt.Sprintf("source.include_properties[%d]", i), "include property must not be empty")
		}
	}
	for i, property := range source.ExcludeProperties {
		if strings.TrimSpace(property) == "" {
			out.err(fmt.Sprintf("source.exclude_properties[%d]", i), "exclude property must not be empty")
		}
	}
	switch source.Mode {
	case SemanticSourceSelf, SemanticSourceSubtree:
		if source.ContextGQL != "" {
			out.err("source.context_gql", "context_gql requires context_query source mode")
		}
	case SemanticSourceContextQuery:
		out.err("source.mode", "context_query source execution is not supported")
	case "":
		out.err("source.mode", "source mode is required")
	default:
		out.err("source.mode", "unsupported source mode %q", source.Mode)
	}
}

func validateBindings(out resultAppend, bindings []SemanticEmbeddingBinding) {
	if len(bindings) == 0 {
		out.err("embeddings", "at least one embedding binding is required")
		return
	}
	seen := map[string]bool{}
	for i, binding := range bindings {
		prefix := fmt.Sprintf("embeddings[%d]", i)
		if binding.Key == "" {
			out.err(prefix+".key", "embedding binding key is required")
		} else if seen[binding.Key] {
			out.err(prefix+".key", "duplicate embedding binding key %q", binding.Key)
		}
		seen[binding.Key] = true
		if binding.Enabled {
			if strings.TrimSpace(binding.Purpose) == "" {
				out.err(prefix+".purpose", "enabled embedding binding purpose is required")
			}
			if strings.TrimSpace(binding.IntelligenceProfile) == "" && binding.IntelligenceProfileID == uuid.Nil {
				out.err(prefix+".intelligence_profile", "enabled embedding binding requires intelligence_profile or intelligence_profile_id")
			}
			if strings.TrimSpace(binding.VectorStore) == "" && binding.VectorStoreID == uuid.Nil {
				out.err(prefix+".vector_store", "enabled embedding binding requires vector_store or vector_store_id")
			}
		}
		if binding.ProviderBudget != nil {
			validateProviderBudget(out, prefix+".provider_budget", *binding.ProviderBudget)
		}
	}
}

func validateProviderBudget(out resultAppend, prefix string, budget SemanticProviderBudgetPolicy) {
	if len(budget.CorrelationID) == 0 || len(budget.CorrelationID) > 128 || !semanticBudgetIdentityPattern.MatchString(budget.CorrelationID) {
		out.err(prefix+".correlation_id", "provider budget correlation_id is invalid")
	}
	if budget.MaxRequests <= 0 {
		out.err(prefix+".max_requests", "provider budget max_requests must be positive")
	}
	if budget.MaxRetryAttempts < 0 {
		out.err(prefix+".max_retry_attempts", "provider budget max_retry_attempts must be non-negative")
	}
	if budget.MaxRetriesPerRequest < 0 || budget.MaxRetriesPerRequest > budget.MaxRetryAttempts {
		out.err(prefix+".max_retries_per_request", "provider budget per-request retries must be non-negative and no greater than aggregate retries")
	}
	maxAttempts := int64(0)
	if budget.MaxRequests > 0 && budget.MaxRetryAttempts >= 0 {
		if budget.MaxRetryAttempts > math.MaxInt64-budget.MaxRequests {
			out.err(prefix, "provider budget attempt limit overflows")
		} else {
			maxAttempts = budget.MaxRequests + budget.MaxRetryAttempts
			if maxAttempts > 10_000 {
				out.err(prefix, "provider budget attempt limit exceeds daemon capacity")
			}
		}
	}
	if budget.MaxConcurrentProviderCalls <= 0 || (maxAttempts > 0 && budget.MaxConcurrentProviderCalls > maxAttempts) {
		out.err(prefix+".max_concurrent_provider_calls", "provider budget concurrency must be positive and no greater than the attempt limit")
	}
	switch budget.BatchingMode {
	case SemanticProviderBatchingSingle:
		if budget.MaxBatchItems != 1 {
			out.err(prefix+".max_batch_items", "single provider batching requires max_batch_items equal to one")
		}
	case SemanticProviderBatchingBatch:
		if budget.MaxBatchItems <= 0 || budget.MaxBatchItems > 1_000 {
			out.err(prefix+".max_batch_items", "batch provider budget max_batch_items must be between one and 1000")
		}
	default:
		out.err(prefix+".batching_mode", "provider budget batching_mode must be single or batch")
	}
	if budget.MaxInputTokensPerRequest <= 0 || budget.MaxInputTokens <= 0 || budget.MaxInputTokensPerRequest > budget.MaxInputTokens {
		out.err(prefix+".max_input_tokens", "provider budget input token limits are inconsistent")
	}
	if budget.RequestTimeout <= 0 || budget.TotalTimeout <= 0 || budget.RequestTimeout%time.Millisecond != 0 || budget.TotalTimeout%time.Millisecond != 0 || budget.RequestTimeout > budget.TotalTimeout {
		out.err(prefix+".request_timeout", "provider budget timeouts must use exact millisecond precision, be positive, and request_timeout must not exceed total_timeout")
	}
	if budget.Currency != "USD" {
		out.err(prefix+".cost.currency", "provider budget currency must be USD")
	}
	if len(budget.PriceDefinitionVersion) == 0 || len(budget.PriceDefinitionVersion) > 128 || !semanticBudgetIdentityPattern.MatchString(budget.PriceDefinitionVersion) {
		out.err(prefix+".cost.price_definition_version", "provider budget price definition version is invalid")
	}
	if budget.InputMicrosPerMillionTokens <= 0 || budget.MaxEstimatedCostMicros <= 0 {
		out.err(prefix+".cost", "provider budget input price and maximum estimated cost must be positive")
	}
}

func validateBoundedGQL(out resultAppend, path string, query string, targetAlias string, maxResults int) {
	query = strings.TrimSpace(query)
	if query == "" {
		out.err(path, "gql query is required")
		return
	}
	plan, err := gql.Compile(query)
	if err != nil {
		out.err(path, "gql query is invalid: %v", err)
		return
	}
	if plan.AccessMode == analysis.ReadWrite {
		out.err(path, "gql query must be read-only")
	}
	limit := semanticPlanLimit(plan)
	if limit <= 0 {
		out.err(path, "gql query must be bounded with FETCH FIRST")
	} else if limit > semanticSelectorMaxRows {
		out.err(path, "gql query limit must be <= %d", semanticSelectorMaxRows)
	}
	if maxResults > 0 && limit > 0 && int64(maxResults) > limit {
		out.err(path, "max_results must be <= gql FETCH FIRST limit")
	}
	if !semanticPlanPatternsAreLabelBounded(plan) {
		out.err(path, "relationship patterns must include a label")
	}
	if strings.TrimSpace(targetAlias) != "" && !semanticPlanReferencesAlias(plan, targetAlias) {
		out.err(path, "gql query must reference target alias %q", targetAlias)
	}
}

func semanticPlanLimit(plan planmodel.Plan) int64 {
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

func semanticPlanReferencesAlias(plan planmodel.Plan, alias string) bool {
	alias = strings.TrimSpace(alias)
	if alias == "" {
		return false
	}
	for _, op := range plan.Operations {
		switch typed := op.(type) {
		case planmodel.QueryNodesOperation:
			if typed.Variable == alias || returnItemsReferenceAlias(typed.Returns, alias) {
				return true
			}
		case planmodel.QueryPatternOperation:
			if typed.Start.Variable == alias || typed.End.Variable == alias || returnItemsReferenceAlias(typed.Returns, alias) {
				return true
			}
		case planmodel.QueryPathOperation:
			if typed.Start.Variable == alias || returnItemsReferenceAlias(typed.Returns, alias) {
				return true
			}
			for _, segment := range typed.Segments {
				if segment.Node.Variable == alias {
					return true
				}
			}
		}
	}
	return false
}

func returnItemsReferenceAlias(items []planmodel.ReturnItem, alias string) bool {
	for _, item := range items {
		if item.Variable == alias {
			return true
		}
	}
	return false
}

func semanticPlanPatternsAreLabelBounded(plan planmodel.Plan) bool {
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
