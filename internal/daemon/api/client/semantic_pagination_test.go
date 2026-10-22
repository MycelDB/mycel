package client

import (
	"strings"
	"testing"

	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestSemanticPageCursorBindsAuthorizedRequestAndView(t *testing.T) {
	request := &clientv1.SemanticSearchRequest{SpaceId: "space-a", DomainId: "domain-a", SemanticRuleId: proto.String("rule-a"), EmbeddingBindingKey: proto.String("search"), Query: "axis-0", Limit: 2, MinScore: proto.Float64(0.25)}
	token, err := encodeSemanticNextPageToken(request, "principal-a", 42, "view-a", "candidates-a", 2)
	if err != nil {
		t.Fatal(err)
	}
	request.PageToken = proto.String(token)
	offset, cursor, err := semanticPageOffset(request, "principal-a", 42, "view-a")
	if err != nil || offset != 2 || cursor.CandidateFingerprint != "candidates-a" {
		t.Fatalf("semanticPageOffset() offset=%d cursor=%+v err=%v", offset, cursor, err)
	}

	tests := []struct {
		name      string
		principal string
		revision  uint64
		view      string
		mutate    func(*clientv1.SemanticSearchRequest)
	}{
		{name: "principal", principal: "principal-b"},
		{name: "graph revision", principal: "principal-a", revision: 43, view: "view-a"},
		{name: "semantic view", principal: "principal-a", revision: 42, view: "view-b"},
		{name: "space", principal: "principal-a", revision: 42, view: "view-a", mutate: func(r *clientv1.SemanticSearchRequest) { r.SpaceId = "space-b" }},
		{name: "domain", principal: "principal-a", revision: 42, view: "view-a", mutate: func(r *clientv1.SemanticSearchRequest) { r.DomainId = "domain-b" }},
		{name: "query", principal: "principal-a", revision: 42, view: "view-a", mutate: func(r *clientv1.SemanticSearchRequest) { r.Query = "axis-1" }},
		{name: "rule", principal: "principal-a", revision: 42, view: "view-a", mutate: func(r *clientv1.SemanticSearchRequest) { r.SemanticRuleId = proto.String("rule-b") }},
		{name: "binding", principal: "principal-a", revision: 42, view: "view-a", mutate: func(r *clientv1.SemanticSearchRequest) { r.EmbeddingBindingKey = proto.String("summary") }},
		{name: "minimum score", principal: "principal-a", revision: 42, view: "view-a", mutate: func(r *clientv1.SemanticSearchRequest) { r.MinScore = proto.Float64(0.5) }},
		{name: "page size", principal: "principal-a", revision: 42, view: "view-a", mutate: func(r *clientv1.SemanticSearchRequest) { r.Limit = 3 }},
		{name: "filter", principal: "principal-a", revision: 42, view: "view-a", mutate: func(r *clientv1.SemanticSearchRequest) {
			r.Filter = &clientv1.SemanticEligibilityFilter{Properties: []*clientv1.SemanticPropertyPredicate{{Key: "kind", Operator: clientv1.SemanticPropertyOperator_SEMANTIC_PROPERTY_OPERATOR_EQUAL, Value: structpb.NewStringValue("report")}}}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			copy := proto.Clone(request).(*clientv1.SemanticSearchRequest)
			if test.mutate != nil {
				test.mutate(copy)
			}
			principal := test.principal
			if principal == "" {
				principal = "principal-a"
			}
			revision := test.revision
			if revision == 0 {
				revision = 42
			}
			view := test.view
			if view == "" {
				view = "view-a"
			}
			if _, _, err := semanticPageOffset(copy, principal, revision, view); err == nil {
				t.Fatal("semanticPageOffset() accepted stale cursor")
			}
		})
	}
}

func TestSemanticPageTokenRequiresServerIntegrityKey(t *testing.T) {
	request := &clientv1.SemanticSearchRequest{SpaceId: "space-a", DomainId: "domain-a", Query: "axis-0", Limit: 2}
	keyA := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	keyB := []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	token, err := encodeSemanticNextPageTokenWithKey(request, "principal-a", 42, "view-a", "candidates-a", 2, keyA)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeSemanticPageCursorWithKey(token, keyA); err != nil {
		t.Fatalf("token failed with issuing key: %v", err)
	}
	if _, err := decodeSemanticPageCursorWithKey(token, keyB); err == nil {
		t.Fatal("token succeeded with a different integrity key")
	}
}

func TestSemanticPageCursorRejectsTampering(t *testing.T) {
	request := &clientv1.SemanticSearchRequest{SpaceId: "space", DomainId: "domain", Query: "query", Limit: 1}
	token, err := encodeSemanticNextPageToken(request, "principal", 1, "view", "candidates", 1)
	if err != nil {
		t.Fatal(err)
	}
	last := token[len(token)-1]
	replacement := byte('A')
	if last == replacement {
		replacement = 'B'
	}
	tampered := token[:len(token)-1] + string(replacement)
	request.PageToken = proto.String(tampered)
	if _, _, err := semanticPageOffset(request, "principal", 1, "view"); err == nil || !strings.Contains(err.Error(), "invalid semantic page token") {
		t.Fatalf("semanticPageOffset() err=%v, want invalid token", err)
	}
}
