package semantic

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/google/uuid"
	graph "github.com/myceldb/mycel/internal/graph/model"
)

func TestNormalizeSemanticTriggerPolicyDefaultsToChanged(t *testing.T) {
	got := NormalizeSemanticTriggerPolicy(SemanticTriggerPolicy{})
	if len(got.Events) != 1 || got.Events[0] != DefaultSemanticTriggerEventChanged {
		t.Fatalf("NormalizeSemanticTriggerPolicy() events = %#v, want [changed]", got.Events)
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
