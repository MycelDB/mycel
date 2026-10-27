package semantic

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/google/uuid"
	graph "github.com/myceldb/mycel/internal/graph/model"
)

func TestSemanticRecordConfigurationHashBindsConcreteRouteAndCompatibleFields(t *testing.T) {
	rule := SemanticGenerationRule{ID: uuid.New(), SpaceID: uuid.New(), DomainID: uuid.New(), Key: "rule", DisplayName: "display-a", Selector: SemanticTargetSelector{Mode: SemanticTargetSelectorNodeType, Labels: []string{"Document"}}, Source: SemanticSourceAssemblyPolicy{Mode: SemanticSourceSelf, IncludeProperties: []string{"text"}}, Embeddings: []SemanticEmbeddingBinding{{Key: "search", IntelligenceProfileID: uuid.New(), VectorStoreID: uuid.New(), Enabled: true}}, Storage: DefaultSemanticStoragePolicy()}
	binding := rule.Embeddings[0]
	endpointID, modelID, capabilityID := ModelEndpointID(uuid.New()), InferenceModelID(uuid.New()), ModelEndpointCapabilityID(uuid.New())
	baseline := SemanticRecordConfigurationHash(rule, binding, endpointID, modelID, capabilityID, "fixture/8", 8)
	compatible := rule
	compatible.DisplayName = "display-b"
	if got := SemanticRecordConfigurationHash(compatible, binding, endpointID, modelID, capabilityID, "fixture/8", 8); got != baseline {
		t.Fatalf("display-only update rotated configuration hash: %q != %q", got, baseline)
	}
	budgetOnly := rule
	budgetOnly.Embeddings = append([]SemanticEmbeddingBinding(nil), rule.Embeddings...)
	budgetOnly.Embeddings[0].ProviderBudget = &SemanticProviderBudgetPolicy{CorrelationID: "changed-budget", MaxRequests: 2}
	if got := SemanticRuleConfigurationHash(budgetOnly); got != SemanticRuleConfigurationHash(rule) {
		t.Fatalf("provider-budget-only update rotated configuration hash: %q != %q", got, SemanticRuleConfigurationHash(rule))
	}
	if got := SemanticRecordConfigurationHash(rule, binding, endpointID, InferenceModelID(uuid.New()), capabilityID, "fixture/8", 8); got == baseline {
		t.Fatal("model route change did not rotate configuration hash")
	}
	if got := SemanticRecordConfigurationHash(rule, binding, endpointID, modelID, capabilityID, "fixture/16", 16); got == baseline {
		t.Fatal("vector-space/dimension change did not rotate configuration hash")
	}
}

func TestNormalizeSemanticRuleDisplayNameUsesCaseInsensitiveTrimmedComparison(t *testing.T) {
	if got := NormalizeSemanticRuleDisplayName("  Journal SEARCH  "); got != "journal search" {
		t.Fatalf("NormalizeSemanticRuleDisplayName() = %q, want journal search", got)
	}
}

func TestNormalizeSemanticTriggerPolicyDefaultsToChanged(t *testing.T) {
	got := NormalizeSemanticTriggerPolicy(SemanticTriggerPolicy{})
	if len(got.Events) != 1 || got.Events[0] != DefaultSemanticTriggerEventChanged {
		t.Fatalf("NormalizeSemanticTriggerPolicy() events = %#v, want [changed]", got.Events)
	}
}

func TestNormalizeSemanticTriggerPolicyAcceptsLegacyUnderscoreGraphEventAliases(t *testing.T) {
	got := NormalizeSemanticTriggerPolicy(SemanticTriggerPolicy{Events: []string{"node_created", "node_updated", "node_deleted", "edge_changed", "edge_created", "edge_updated", "edge_deleted"}})
	want := []string{"node.created", "node.updated", "node.deleted", "edge.changed", "edge.created", "edge.updated", "edge.deleted"}
	if !reflect.DeepEqual(got.Events, want) {
		t.Fatalf("NormalizeSemanticTriggerPolicy() events = %#v, want %#v", got.Events, want)
	}
}

func TestNormalizeSemanticTriggerPolicyCanonicalizesSetOrderAndDuplicates(t *testing.T) {
	got := NormalizeSemanticTriggerPolicy(SemanticTriggerPolicy{Events: []string{"edge.deleted", "node_updated", "changed", "node.updated", "edge.created"}})
	want := []string{"changed", "node.updated", "edge.created", "edge.deleted"}
	if !reflect.DeepEqual(got.Events, want) {
		t.Fatalf("NormalizeSemanticTriggerPolicy() events = %#v, want %#v", got.Events, want)
	}
}

func TestSemanticGenerationRuleHasBindingScopedPurposeAndNoDirectModelFields(t *testing.T) {
	ruleType := reflect.TypeOf(SemanticGenerationRule{})
	for _, field := range []string{"Purpose", "ModelEndpointID", "ModelID", "ModelEndpointCapabilityID"} {
		if _, ok := ruleType.FieldByName(field); ok {
			t.Fatalf("SemanticGenerationRule has forbidden user-authored field %s", field)
		}
	}

	rule := SemanticGenerationRule{
		Key: "journal-search",
		Embeddings: []SemanticEmbeddingBinding{
			{Key: "search", Purpose: "semantic_search"},
			{Key: "context", Purpose: "chat_context"},
		},
	}
	if rule.Embeddings[0].Purpose == "" || rule.Embeddings[1].Purpose == "" || rule.Embeddings[0].Purpose == rule.Embeddings[1].Purpose {
		t.Fatalf("expected binding-scoped purposes, got %#v", rule.Embeddings)
	}
}

func TestNormalizeSemanticEmbeddingBindingUsesIntelligenceAccessProfile(t *testing.T) {
	got := NormalizeSemanticEmbeddingBinding(SemanticEmbeddingBinding{
		Key:                 " Search ",
		Purpose:             " semantic_search ",
		IntelligenceProfile: " embedding-default ",
		VectorStore:         " local-vectors ",
	})
	if got.Key != "search" {
		t.Fatalf("Key = %q, want search", got.Key)
	}
	if got.IntelligenceProfile != "embedding-default" {
		t.Fatalf("IntelligenceProfile = %q, want embedding-default", got.IntelligenceProfile)
	}
}

func TestSemanticDirtyWorkKeyIncludesRuleBindingAndTarget(t *testing.T) {
	ruleID := SemanticRuleID(uuid.New())
	targetID := graph.NodeID(uuid.New())
	item := SemanticDirtyWorkItem{SemanticRuleID: ruleID, EmbeddingBindingKey: "search", TargetNodeID: targetID}

	got := item.RuleBindingTargetKey()
	if got.SemanticRuleID != ruleID || got.EmbeddingBindingKey != "search" || got.TargetNodeID != targetID {
		t.Fatalf("RuleBindingTargetKey() = %#v", got)
	}
}

func TestSemanticVectorRecordJSONUsesRuleBindingAndAccessProvenance(t *testing.T) {
	rec := SemanticVectorRecord{
		ID:                    SemanticRecordID(uuid.New()),
		SemanticRuleID:        SemanticRuleID(uuid.New()),
		EmbeddingBindingKey:   "search",
		TargetNodeID:          graph.NodeID(uuid.New()),
		IntelligenceProfileID: IntelligenceProfileID(uuid.New()),
		ModelEndpointID:       ModelEndpointID(uuid.New()),
		ModelID:               InferenceModelID(uuid.New()),
		VectorStoreID:         VectorStoreID(uuid.New()),
	}

	data, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	for _, key := range []string{"semantic_rule_id", "embedding_binding_key", "target_node_id", "intelligence_profile_id", "model_endpoint_id", "model_id", "vector_store_id"} {
		if _, ok := got[key]; !ok {
			t.Fatalf("marshaled SemanticVectorRecord missing %q in %s", key, string(data))
		}
	}
	if _, ok := got["semantic_index_id"]; ok {
		t.Fatalf("rule-native SemanticVectorRecord should not emit semantic_index_id: %s", string(data))
	}
}

func TestDefaultSemanticStoragePolicyIsSearchableExact(t *testing.T) {
	got := DefaultSemanticStoragePolicy()
	if !got.Searchable || got.PhysicalIndex != SemanticPhysicalIndexExact {
		t.Fatalf("DefaultSemanticStoragePolicy() = %#v", got)
	}
}
