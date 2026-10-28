package client

import (
	"testing"

	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestNormalizeSemanticFilterAndEligibility(t *testing.T) {
	payload, err := structpb.NewStruct(map[string]any{"kind": "report", "nullable": nil})
	if err != nil {
		t.Fatal(err)
	}
	filter, err := normalizeSemanticFilter(&clientv1.SemanticEligibilityFilter{
		NodeLabels: []string{"Published", "Document", "Document"},
		NodeIds:    []string{"00000000-0000-4000-8000-000000000002", "00000000-0000-4000-8000-000000000001"},
		Properties: []*clientv1.SemanticPropertyPredicate{
			{Key: "kind", Operator: clientv1.SemanticPropertyOperator_SEMANTIC_PROPERTY_OPERATOR_EQUAL, Value: structpb.NewStringValue("report")},
			{Key: "nullable", Operator: clientv1.SemanticPropertyOperator_SEMANTIC_PROPERTY_OPERATOR_EXISTS},
			{Key: "absent", Operator: clientv1.SemanticPropertyOperator_SEMANTIC_PROPERTY_OPERATOR_MISSING},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(filter.GetNodeLabels()) != 2 || filter.GetNodeLabels()[0] != "Document" || filter.GetNodeIds()[0] != "00000000-0000-4000-8000-000000000001" {
		t.Fatalf("filter was not normalized: %+v", filter)
	}
	node := &clientv1.Node{NodeId: "00000000-0000-4000-8000-000000000001", Labels: []string{"Document", "Published"}, Payload: payload}
	if !semanticNodeEligible(node, filter) {
		t.Fatal("expected node to be eligible")
	}
	payload.Fields["kind"] = structpb.NewStringValue("memo")
	if semanticNodeEligible(node, filter) {
		t.Fatal("property mismatch remained eligible")
	}
}

func TestNormalizedSemanticFilterHasStableCursorIdentity(t *testing.T) {
	first, err := normalizeSemanticFilter(&clientv1.SemanticEligibilityFilter{NodeLabels: []string{"B", "A"}, NodeIds: []string{"00000000-0000-4000-8000-000000000002", "00000000-0000-4000-8000-000000000001"}})
	if err != nil {
		t.Fatal(err)
	}
	second, err := normalizeSemanticFilter(&clientv1.SemanticEligibilityFilter{NodeLabels: []string{"A", "B"}, NodeIds: []string{"00000000-0000-4000-8000-000000000001", "00000000-0000-4000-8000-000000000002"}})
	if err != nil {
		t.Fatal(err)
	}
	left := &clientv1.SemanticSearchRequest{SpaceId: "space", DomainId: "domain", Query: "query", Filter: first}
	right := &clientv1.SemanticSearchRequest{SpaceId: "space", DomainId: "domain", Query: "query", Filter: second}
	leftHash, err := semanticRequestFingerprint(left)
	if err != nil {
		t.Fatal(err)
	}
	rightHash, err := semanticRequestFingerprint(right)
	if err != nil {
		t.Fatal(err)
	}
	if leftHash != rightHash {
		t.Fatal("equivalent normalized filters produced different cursor identities")
	}
}

func TestNormalizeSemanticFilterRejectsUnsupportedPredicates(t *testing.T) {
	_, err := normalizeSemanticFilter(&clientv1.SemanticEligibilityFilter{Properties: []*clientv1.SemanticPropertyPredicate{{Key: "kind"}}})
	if err == nil {
		t.Fatal("expected unsupported filter operator error")
	}
	_, err = normalizeSemanticFilter(&clientv1.SemanticEligibilityFilter{Properties: []*clientv1.SemanticPropertyPredicate{{Key: "kind", Operator: clientv1.SemanticPropertyOperator_SEMANTIC_PROPERTY_OPERATOR_EXISTS, Value: structpb.NewStringValue("unexpected")}}})
	if err == nil {
		t.Fatal("expected exists-with-value error")
	}
}
