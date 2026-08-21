package semantic

import (
	"strings"
	"testing"

	"github.com/google/uuid"
	graph "github.com/myceldb/mycel/internal/graph/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestValidateSemanticGenerationRuleAcceptsNodeTypeSelector(t *testing.T) {
	rule := validSemanticRuleForValidation()
	res := ValidateSemanticGenerationRule(rule)
	if !res.Valid {
		t.Fatalf("expected rule to be valid, diagnostics=%+v", res.Diagnostics)
	}
	if res.Rule.Key != "journal-search" || res.Rule.Trigger.Events[0] != DefaultSemanticTriggerEventChanged {
		t.Fatalf("unexpected normalization: %+v", res.Rule)
	}
}

func TestValidateSemanticGenerationRuleRejectsMissingNodeTypeLabels(t *testing.T) {
	rule := validSemanticRuleForValidation()
	rule.Selector.Labels = nil
	res := ValidateSemanticGenerationRule(rule)
	if res.Valid || !diagnosticsContain(res.Diagnostics, "selector.labels") {
		t.Fatalf("expected selector.labels diagnostic, got valid=%v diagnostics=%+v", res.Valid, res.Diagnostics)
	}
}

func TestValidateSemanticGenerationRuleAcceptsBoundedGQLSelector(t *testing.T) {
	rule := validSemanticRuleForValidation()
	rule.Selector = SemanticTargetSelector{Mode: SemanticTargetSelectorGQL, GQL: "MATCH (target:Note) RETURN target FETCH FIRST 10 ROWS ONLY", TargetAlias: "target", MaxResults: 5}
	res := ValidateSemanticGenerationRule(rule)
	if !res.Valid {
		t.Fatalf("expected bounded GQL selector to be valid, diagnostics=%+v", res.Diagnostics)
	}
}

func TestValidateSemanticGenerationRuleRejectsMissingGQLTargetAlias(t *testing.T) {
	rule := validSemanticRuleForValidation()
	rule.Selector = SemanticTargetSelector{Mode: SemanticTargetSelectorGQL, GQL: "MATCH (target:Note) RETURN target FETCH FIRST 10 ROWS ONLY"}
	res := ValidateSemanticGenerationRule(rule)
	if res.Valid || !diagnosticsContain(res.Diagnostics, "selector.target_alias") {
		t.Fatalf("expected selector.target_alias diagnostic, got valid=%v diagnostics=%+v", res.Valid, res.Diagnostics)
	}
}

func TestValidateSemanticGenerationRuleRejectsUnsafeGQLSelectors(t *testing.T) {
	tests := []struct {
		name string
		gql  string
		want string
	}{
		{name: "write", gql: "MATCH (target:Note) SET target.flag = true RETURN target FETCH FIRST 10 ROWS ONLY", want: "read-only"},
		{name: "unbounded", gql: "MATCH (target:Note) RETURN target", want: "FETCH FIRST"},
		{name: "unlabeled relationship", gql: "MATCH (target:Note)-->(other:Note) RETURN target FETCH FIRST 10 ROWS ONLY", want: "relationship patterns"},
		{name: "missing alias", gql: "MATCH (other:Note) RETURN other FETCH FIRST 10 ROWS ONLY", want: "target alias"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rule := validSemanticRuleForValidation()
			rule.Selector = SemanticTargetSelector{Mode: SemanticTargetSelectorGQL, GQL: tt.gql, TargetAlias: "target"}
			res := ValidateSemanticGenerationRule(rule)
			if res.Valid || !diagnosticsMessageContains(res.Diagnostics, tt.want) {
				t.Fatalf("expected diagnostic containing %q, got valid=%v diagnostics=%+v", tt.want, res.Valid, res.Diagnostics)
			}
		})
	}
}

func TestValidateSemanticGenerationRuleRejectsBindingWithoutAccessProfile(t *testing.T) {
	rule := validSemanticRuleForValidation()
	rule.Embeddings[0].IntelligenceProfile = ""
	rule.Embeddings[0].IntelligenceProfileID = uuid.Nil
	res := ValidateSemanticGenerationRule(rule)
	if res.Valid || !diagnosticsContain(res.Diagnostics, "embeddings[0].intelligence_profile") {
		t.Fatalf("expected intelligence profile diagnostic, got valid=%v diagnostics=%+v", res.Valid, res.Diagnostics)
	}
}

func TestValidateSemanticGenerationRuleRejectsNegativeValuesAndContextShape(t *testing.T) {
	negative := -1
	rule := validSemanticRuleForValidation()
	rule.Trigger.Debounce = -1
	rule.Source.MaxDepth = &negative
	rule.Source.MinimumTextLength = -1
	rule.Source.ContextGQL = "MATCH (n:Note) RETURN n FETCH FIRST 1 ROW ONLY"
	res := ValidateSemanticGenerationRule(rule)
	for _, path := range []string{"trigger.debounce", "source.max_depth", "source.minimum_text_length", "source.context_gql"} {
		if !diagnosticsContain(res.Diagnostics, path) {
			t.Fatalf("expected diagnostic for %s, got %+v", path, res.Diagnostics)
		}
	}
}

func TestValidateSemanticGenerationRuleForStorageChecksSpace(t *testing.T) {
	rule := validSemanticRuleForValidation()
	res := ValidateSemanticGenerationRuleForStorage(domainspace.SpaceID(uuid.New()), rule)
	if res.Valid || !diagnosticsContain(res.Diagnostics, "space_id") {
		t.Fatalf("expected space_id diagnostic, got valid=%v diagnostics=%+v", res.Valid, res.Diagnostics)
	}
}

func validSemanticRuleForValidation() SemanticGenerationRule {
	return SemanticGenerationRule{
		SpaceID:  domainspace.SpaceID(uuid.New()),
		DomainID: graph.DomainID(uuid.New()),
		Key:      " Journal-Search ",
		Enabled:  true,
		Selector: SemanticTargetSelector{Mode: SemanticTargetSelectorNodeType, Labels: []string{"Note"}},
		Source:   SemanticSourceAssemblyPolicy{Mode: SemanticSourceSelf, IncludeProperties: []string{"payload.text"}},
		Embeddings: []SemanticEmbeddingBinding{{
			Key:                 "search",
			Purpose:             "semantic_search",
			IntelligenceProfile: "default-embeddings",
			VectorStore:         "local",
			Enabled:             true,
		}},
	}
}

func diagnosticsContain(diags []ValidationDiagnostic, path string) bool {
	for _, diag := range diags {
		if diag.Path == path {
			return true
		}
	}
	return false
}

func diagnosticsMessageContains(diags []ValidationDiagnostic, fragment string) bool {
	for _, diag := range diags {
		if strings.Contains(diag.Message, fragment) {
			return true
		}
	}
	return false
}
