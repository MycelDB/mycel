package client

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	clusterbackend "github.com/myceldb/mycel/internal/clustering/backend"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	clustermodel "github.com/myceldb/mycel/internal/clustering/model"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	graph "github.com/myceldb/mycel/internal/graph/model"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	semanticsearch "github.com/myceldb/mycel/internal/semantic/search"
	daemonspace "github.com/myceldb/mycel/internal/space/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

type searchForwardRoute struct{}

func (searchForwardRoute) GraphWriteRoute(context.Context, string) (consensus.NodeID, consensus.NodeID, error) {
	return 2, 1, nil
}

type searchForwardSpaces struct {
	daemonspace.Manager
	domain  graph.Domain
	allowed string
	mu      sync.Mutex
	seen    []string
}

func (s *searchForwardSpaces) GetVisibleDomain(_ context.Context, principalID, spaceID, domainID, _ string) (graph.Domain, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seen = append(s.seen, principalID)
	if principalID != s.allowed || spaceID != s.domain.SpaceID.String() || domainID != s.domain.ID.String() {
		return graph.Domain{}, daemonspace.ErrUnauthorized
	}
	return s.domain, nil
}

func TestStatelessSearchAndStatusForwardOverBackendWireAndReauthorizePrincipal(t *testing.T) {
	fixture := newSearchPaginationFixture(1)
	principalID := uuid.NewString()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx = daemonauth.ContextWithPrincipal(ctx, daemonauth.Principal{Kind: daemonauth.PrincipalKindHuman, PrincipalID: principalID, Username: "alice"})
	spaces := &searchForwardSpaces{domain: graph.Domain{ID: fixture.domainID, SpaceID: fixture.spaceID, SearchMode: graph.DomainSearchModeNormal, SemanticMode: graph.DomainSemanticModeNormal}, allowed: principalID}
	fixture.service.spaces = spaces
	nodeID := graph.NodeID(uuid.MustParse(fixture.lexical.results[0].NodeID))
	fixture.service.semantic = &fakeHybridSemanticManager{
		rules:   []domainsemantic.SemanticGenerationRule{{ID: domainsemantic.SemanticRuleID(uuid.New()), SpaceID: fixture.spaceID, DomainID: fixture.domainID, Enabled: true, Embeddings: []domainsemantic.SemanticEmbeddingBinding{{Key: "search", Purpose: "semantic_search", Enabled: true}}}},
		results: []semanticsearch.SearchResult{{NodeID: nodeID, Score: 1}},
	}
	backend := clusterbackend.NewService(clustermodel.NodeIdentity{NodeID: "node_2", ClusterID: "cluster_a", ClusterAdmitted: true}, clustermodel.NodeStateClustered, nil).
		WithClientRequestForwarder(ForwardedClientHandler{LocalNode: 2, Search: fixture.service})
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	clusterpb.RegisterClusterBackendServiceServer(server, backend)
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-done })
	router := NewBackendClientRequestRouter(true, "cluster_a", 1, []string{"local", "bufnet"}, "test-cluster-token")
	router.Client.DialOptions = []grpc.DialOption{grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }), grpc.WithTransportCredentials(insecure.NewCredentials())}
	ingress := NewSearchService(fixture.lexical, spaces, fixture.graphs).WithGraphWriteRouteProvider(searchForwardRoute{}).WithClientRequestRouter(router)

	index, err := ingress.GetLexicalIndexStatus(ctx, &clientv1.GetLexicalIndexStatusRequest{SpaceId: fixture.spaceID.String(), DomainId: fixture.domainID.String()})
	if err != nil || index.GetStatus().GetIndexedGraphRevision() != 10 {
		t.Fatalf("stateless forwarded index status=%+v err=%v", index, err)
	}
	for _, mode := range []clientv1.SearchMode{clientv1.SearchMode_SEARCH_MODE_LEXICAL, clientv1.SearchMode_SEARCH_MODE_HYBRID} {
		req := &clientv1.SearchRequest{SpaceId: fixture.spaceID.String(), DomainId: fixture.domainID.String(), Mode: mode, Lexical: &clientv1.LexicalSearchOptions{Query: proto.String("raft")}, PageSize: 10}
		if mode == clientv1.SearchMode_SEARCH_MODE_HYBRID {
			req.Semantic = &clientv1.SemanticSearchOptions{Query: proto.String("semantic recovery")}
			req.Hybrid = &clientv1.HybridSearchOptions{RequireBoth: true}
		}
		res, err := ingress.Search(ctx, req)
		if err != nil || len(res.GetResults()) != 1 || res.GetResults()[0].GetNodeId() != nodeID.String() {
			t.Fatalf("forwarded %s response=%+v err=%v", mode, res, err)
		}
		if mode == clientv1.SearchMode_SEARCH_MODE_HYBRID && len(res.GetResults()[0].GetSources()) != 2 {
			t.Fatalf("forwarded hybrid search lost a retrieval channel: %+v", res)
		}
	}
	if diag := router.Diagnostics(); diag.ForwardAttempts != 3 || diag.ForwardSuccesses != 3 {
		t.Fatalf("requests did not traverse the backend wire: %+v", diag)
	}
	spaces.mu.Lock()
	if len(spaces.seen) < 6 {
		t.Errorf("expected authorization at ingress and target, got %d reads", len(spaces.seen))
	}
	for _, seen := range spaces.seen {
		if seen != principalID {
			t.Errorf("forwarded principal changed: %q", seen)
		}
	}
	spaces.mu.Unlock()

	payload, err := proto.Marshal(&clientv1.SearchRequest{SpaceId: fixture.spaceID.String(), DomainId: fixture.domainID.String(), Lexical: &clientv1.LexicalSearchOptions{Query: proto.String("raft")}})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name      string
		principal clusterbackend.ForwardedPrincipal
		target    consensus.NodeID
		payload   []byte
		code      codes.Code
	}{
		{name: "missing principal", target: 2, payload: payload, code: codes.Unauthenticated},
		{name: "unauthorized principal", principal: clusterbackend.ForwardedPrincipal{Kind: "human", PrincipalID: uuid.NewString()}, target: 2, payload: payload, code: codes.PermissionDenied},
		{name: "wrong target", principal: clusterbackend.ForwardedPrincipal{Kind: "human", PrincipalID: principalID}, target: 3, payload: payload, code: codes.FailedPrecondition},
		{name: "invalid payload", principal: clusterbackend.ForwardedPrincipal{Kind: "human", PrincipalID: principalID}, target: 2, payload: []byte{0xff}, code: codes.InvalidArgument},
	} {
		t.Run(test.name, func(t *testing.T) {
			before := fixture.lexical.searchCalls
			_, err := router.Client.ForwardClientRequest(ctx, "bufnet", clusterbackend.ForwardClientRequestInput{ClusterID: "cluster_a", Operation: clientv1.SearchService_Search_FullMethodName, RequesterNode: 1, TargetNode: test.target, Principal: test.principal, PayloadType: clusterbackend.PayloadTypeProto, Payload: test.payload})
			if status.Code(err) != test.code || fixture.lexical.searchCalls != before {
				t.Fatalf("forwarded request guard: error=%v retrieval calls before/after=%d/%d", err, before, fixture.lexical.searchCalls)
			}
		})
	}
}
