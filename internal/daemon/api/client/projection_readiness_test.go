package client

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	graph "github.com/myceldb/mycel/internal/graph/model"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	semanticsearch "github.com/myceldb/mycel/internal/semantic/search"
	daemonsemantic "github.com/myceldb/mycel/internal/semantic/service"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type readinessSemanticManager struct {
	*fakeHybridSemanticManager
	searchErr   error
	searchCalls int
}

func (m *readinessSemanticManager) Search(context.Context, daemonsemantic.SearchInput) (semanticsearch.Result, error) {
	m.searchCalls++
	return semanticsearch.Result{}, m.searchErr
}

func (m *readinessSemanticManager) GetReliabilityStatus(context.Context, daemonsemantic.ReliabilityStatusInput) (daemonsemantic.ReliabilityStatus, error) {
	return daemonsemantic.ReliabilityStatus{SemanticGeneration: 3, FreshThroughGraphRevision: 5}, nil
}

func requireProjectionReadinessStatus(t *testing.T, err error, code codes.Code, metadata map[string]string) {
	t.Helper()
	st := status.Convert(err)
	if st.Code() != code || len(st.Details()) != 1 {
		t.Fatalf("error=%v details=%v, want %s with ErrorInfo", err, st.Details(), code)
	}
	info, ok := st.Details()[0].(*errdetails.ErrorInfo)
	if !ok || info.Reason != "SEMANTIC_PROJECTION_NOT_READY" || info.Domain != "myceldb.io" || !reflect.DeepEqual(info.Metadata, metadata) {
		t.Fatalf("readiness details=%+v", st.Details())
	}
}

func TestDirectAndHybridSearchPreserveProjectionReadinessWithoutLexicalFallback(t *testing.T) {
	for _, test := range []struct {
		state string
		code  codes.Code
	}{
		{state: "completed", code: codes.Unavailable},
		{state: "running", code: codes.Unavailable},
		{state: "required", code: codes.FailedPrecondition},
		{state: "failed", code: codes.FailedPrecondition},
	} {
		t.Run(test.state, func(t *testing.T) {
			fixture := newSearchPaginationFixture(1)
			ruleID := domainsemantic.SemanticRuleID(uuid.New())
			readiness := &daemonsemantic.ProjectionReadinessError{
				SemanticRuleID: ruleID, EmbeddingBindingKey: "search", LocalNode: 2,
				Expected: daemonsemantic.ProjectionReadinessTuple{LeaderNode: 2, LeaderTerm: 9, SemanticGeneration: 3, RebuildEpoch: "epoch-3", FreshThrough: 5},
				Observed: &daemonsemantic.ProjectionReadinessTuple{LeaderNode: 2, LeaderTerm: 8, SemanticGeneration: 3, RebuildEpoch: "epoch-3", FreshThrough: 5},
				Reasons:  []string{daemonsemantic.ProjectionReadinessTermMismatch}, RebuildState: test.state,
			}
			semantic := &readinessSemanticManager{
				fakeHybridSemanticManager: &fakeHybridSemanticManager{rules: []domainsemantic.SemanticGenerationRule{{ID: ruleID, SpaceID: fixture.spaceID, DomainID: fixture.domainID, Key: "retained-rule", Enabled: true, Embeddings: []domainsemantic.SemanticEmbeddingBinding{{Key: "search", Purpose: "semantic_search", Enabled: true}}}}},
				searchErr:                 fmt.Errorf("readiness check: %w", readiness),
			}
			spaces := fakeHybridSpaceManager{domain: graph.Domain{ID: fixture.domainID, SpaceID: fixture.spaceID, SearchMode: graph.DomainSearchModeNormal, SemanticMode: graph.DomainSemanticModeNormal}}
			direct := NewSemanticService(semantic, spaces, fixture.graphs)
			query := "CONFIDENTIAL-query-content-must-not-appear-in-diagnostics"
			response, err := direct.SemanticSearch(fixture.ctx, &clientv1.SemanticSearchRequest{SpaceId: fixture.spaceID.String(), DomainId: fixture.domainID.String(), SemanticRuleId: proto.String(ruleID.String()), EmbeddingBindingKey: proto.String("search"), Query: query, Limit: 3})
			if response != nil {
				t.Fatalf("direct search returned uncertified results: %+v", response)
			}
			requireProjectionReadinessStatus(t, err, test.code, readiness.DiagnosticMetadata())
			if strings.Contains(err.Error(), query) {
				t.Fatal("readiness error leaked graph/query content")
			}
			fixture.service.semantic, fixture.service.spaces = semantic, spaces
			for _, requireBoth := range []bool{false, true} {
				response, err := fixture.service.Search(fixture.ctx, &clientv1.SearchRequest{
					SpaceId: fixture.spaceID.String(), DomainId: fixture.domainID.String(), Mode: clientv1.SearchMode_SEARCH_MODE_HYBRID,
					Lexical:  &clientv1.LexicalSearchOptions{Query: proto.String("raft")},
					Semantic: &clientv1.SemanticSearchOptions{SemanticRuleId: proto.String(ruleID.String()), EmbeddingBindingKey: proto.String("search"), Query: proto.String(query)},
					Hybrid:   &clientv1.HybridSearchOptions{RequireBoth: requireBoth},
				})
				if response != nil {
					t.Fatalf("hybrid search requireBoth=%t returned lexical fallback after readiness failure: %+v", requireBoth, response)
				}
				requireProjectionReadinessStatus(t, err, test.code, readiness.DiagnosticMetadata())
			}
			if semantic.searchCalls != 3 {
				t.Fatalf("semantic search calls=%d, want direct plus both hybrid modes", semantic.searchCalls)
			}
		})
	}
}

func TestMapSemanticErrorDoesNotReclassifyUnrelatedStorageErrors(t *testing.T) {
	err := mapSemanticError(errors.New("read local semantic search index state for projection recovery: storage corruption"), "hybrid semantic search")
	if status.Code(err) != codes.Internal || len(status.Convert(err).Details()) != 0 {
		t.Fatalf("unrelated storage failure was hidden as readiness: %v", err)
	}
}
