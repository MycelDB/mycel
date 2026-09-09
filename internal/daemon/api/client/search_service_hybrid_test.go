package client

import (
	"testing"

	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestHybridOptionsFromProtoDefaultsAndNormalizes(t *testing.T) {
	opts, err := hybridOptionsFromProto(nil)
	if err != nil {
		t.Fatalf("hybridOptionsFromProto(nil) returned error: %v", err)
	}
	if opts.Weights.Lexical != 0.5 || opts.Weights.Semantic != 0.5 {
		t.Fatalf("default weights = %#v, want 0.5/0.5", opts.Weights)
	}
	opts, err = hybridOptionsFromProto(&clientv1.HybridSearchOptions{LexicalWeight: 2, SemanticWeight: 1, RequireBoth: true})
	if err != nil {
		t.Fatalf("hybridOptionsFromProto weighted returned error: %v", err)
	}
	if opts.Weights.Lexical < 0.66 || opts.Weights.Lexical > 0.67 || opts.Weights.Semantic < 0.33 || opts.Weights.Semantic > 0.34 || !opts.RequireBoth {
		t.Fatalf("weighted opts = %#v", opts)
	}
}

func TestHybridOptionsFromProtoRejectsInvalidWeights(t *testing.T) {
	if _, err := hybridOptionsFromProto(&clientv1.HybridSearchOptions{LexicalWeight: -1, SemanticWeight: 1}); err == nil {
		t.Fatalf("hybridOptionsFromProto negative weights returned nil error")
	}
}

func TestSearchFiltersFromProto(t *testing.T) {
	filters, err := searchFiltersFromProto(&clientv1.SearchFilters{NodeLabels: []string{"Note"}, NodeIds: []string{"node-a"}, Properties: []*clientv1.PropertyFilter{{Path: "tags", Operator: clientv1.FilterOperator_FILTER_OPERATOR_CONTAINS, Values: []string{"k3s"}}}})
	if err != nil {
		t.Fatalf("searchFiltersFromProto returned error: %v", err)
	}
	if len(filters.NodeLabels) != 1 || len(filters.NodeIDs) != 1 || len(filters.Properties) != 1 {
		t.Fatalf("filters = %#v", filters)
	}
}

func TestSearchFiltersFromProtoRejectsUnsupportedOperator(t *testing.T) {
	_, err := searchFiltersFromProto(&clientv1.SearchFilters{Properties: []*clientv1.PropertyFilter{{Path: "tags", Operator: clientv1.FilterOperator_FILTER_OPERATOR_UNSPECIFIED}}})
	if err == nil {
		t.Fatalf("searchFiltersFromProto unsupported operator returned nil error")
	}
}

func TestIsNoSemanticSearchAvailable(t *testing.T) {
	if !isNoSemanticSearchAvailable(status.Error(codes.FailedPrecondition, "no enabled semantic search rule is available for the domain")) {
		t.Fatalf("expected no semantic search availability detection")
	}
	if isNoSemanticSearchAvailable(status.Error(codes.Internal, "no enabled semantic search rule is available for the domain")) {
		t.Fatalf("internal error should not be treated as safe semantic unavailability")
	}
}
