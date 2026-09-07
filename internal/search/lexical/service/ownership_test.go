package service

import (
	"context"
	"errors"
	"testing"

	"github.com/myceldb/mycel/internal/search/lexical/index"
)

func TestCoordinatorLocalOwnerIndexesAndSearches(t *testing.T) {
	resolver := &fakeOwnerResolver{owner: Owner{Known: true, NodeID: "node-local", IsLocal: true}}
	coord := NewCoordinator(New(t.TempDir(), "space-1", "domain-1"), resolver, nil)
	ctx := context.Background()
	if err := coord.ApplyDocuments(ctx, "space-1", "domain-1", []index.IndexedDocument{{Document: testDoc("node-a", "raft database"), GraphRevision: 1}}, 1); err != nil {
		t.Fatalf("ApplyDocuments returned error: %v", err)
	}
	res, err := coord.Search(ctx, SearchRequest{SpaceID: "space-1", DomainID: "domain-1", Query: "raft", Options: index.SearchOptions{PageSize: 10}})
	if err != nil {
		t.Fatalf("Search returned error: %v", err)
	}
	if got := resultIDs(res.Results); len(got) != 1 || got[0] != "node-a" {
		t.Fatalf("results = %#v", got)
	}
}

func TestCoordinatorFollowerForwardsSearchAndStatus(t *testing.T) {
	resolver := &fakeOwnerResolver{owner: Owner{Known: true, NodeID: "node-leader", IsLocal: false}}
	forwarder := &fakeForwarder{
		searchResponse: index.SearchResponse{Results: []index.Result{{NodeID: "remote-node", Score: 1}}},
		statusResponse: Status{SpaceID: "space-1", DomainID: "domain-1", State: StateFresh},
	}
	coord := NewCoordinator(New(t.TempDir(), "space-1", "domain-1"), resolver, forwarder)
	ctx := context.Background()
	res, err := coord.Search(ctx, SearchRequest{SpaceID: "space-1", DomainID: "domain-1", Query: "raft", Options: index.SearchOptions{PageSize: 10}})
	if err != nil {
		t.Fatalf("Search returned error: %v", err)
	}
	if forwarder.searchOwner != "node-leader" || forwarder.searchRequest.Query != "raft" {
		t.Fatalf("forwarded search = owner %q req %#v", forwarder.searchOwner, forwarder.searchRequest)
	}
	if got := resultIDs(res.Results); len(got) != 1 || got[0] != "remote-node" {
		t.Fatalf("results = %#v", got)
	}
	status, err := coord.Status(ctx, StatusRequest{SpaceID: "space-1", DomainID: "domain-1", LatestKnownGraphRevision: 5})
	if err != nil {
		t.Fatalf("Status returned error: %v", err)
	}
	if status.State != StateFresh || forwarder.statusOwner != "node-leader" {
		t.Fatalf("status = %#v forwarded owner = %q", status, forwarder.statusOwner)
	}
	if err := coord.ApplyDocuments(ctx, "space-1", "domain-1", []index.IndexedDocument{{Document: testDoc("node-a", "raft"), GraphRevision: 1}}, 1); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("ApplyDocuments err = %v, want ErrNotOwner", err)
	}
}

func TestCoordinatorUnknownOwnerFailsClosed(t *testing.T) {
	resolver := &fakeOwnerResolver{owner: Owner{Known: false}}
	coord := NewCoordinator(New(t.TempDir(), "space-1", "domain-1"), resolver, nil)
	_, err := coord.Search(context.Background(), SearchRequest{SpaceID: "space-1", DomainID: "domain-1", Query: "raft"})
	if !errors.Is(err, ErrOwnerUnavailable) {
		t.Fatalf("Search err = %v, want ErrOwnerUnavailable", err)
	}
}

func TestCoordinatorLeadershipChangeStopsOldOwnerAndAllowsNewOwner(t *testing.T) {
	resolver := &fakeOwnerResolver{owner: Owner{Known: true, NodeID: "node-local", IsLocal: true}}
	coord := NewCoordinator(New(t.TempDir(), "space-1", "domain-1"), resolver, nil)
	ctx := context.Background()
	if err := coord.ApplyDocuments(ctx, "space-1", "domain-1", []index.IndexedDocument{{Document: testDoc("node-a", "raft"), GraphRevision: 1}}, 1); err != nil {
		t.Fatalf("local apply: %v", err)
	}
	resolver.owner = Owner{Known: true, NodeID: "node-next", IsLocal: false}
	if err := coord.ApplyDocuments(ctx, "space-1", "domain-1", []index.IndexedDocument{{Document: testDoc("node-b", "raft"), GraphRevision: 2}}, 2); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("old owner apply err = %v, want ErrNotOwner", err)
	}
	resolver.owner = Owner{Known: true, NodeID: "node-local", IsLocal: true}
	if err := coord.ApplyDocuments(ctx, "space-1", "domain-1", []index.IndexedDocument{{Document: testDoc("node-b", "raft"), GraphRevision: 2}}, 2); err != nil {
		t.Fatalf("new owner apply: %v", err)
	}
	res, err := coord.Search(ctx, SearchRequest{SpaceID: "space-1", DomainID: "domain-1", Query: "raft", Options: index.SearchOptions{PageSize: 10}})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if got := resultIDs(res.Results); len(got) != 2 {
		t.Fatalf("results = %#v, want 2 docs", got)
	}
}

type fakeOwnerResolver struct {
	owner Owner
	err   error
}

func (f *fakeOwnerResolver) ResolveLexicalOwner(context.Context, string, string) (Owner, error) {
	return f.owner, f.err
}

type fakeForwarder struct {
	searchOwner    string
	searchRequest  SearchRequest
	searchResponse index.SearchResponse
	searchErr      error
	statusOwner    string
	statusRequest  StatusRequest
	statusResponse Status
	statusErr      error
}

func (f *fakeForwarder) ForwardSearch(_ context.Context, ownerNodeID string, req SearchRequest) (index.SearchResponse, error) {
	f.searchOwner = ownerNodeID
	f.searchRequest = req
	return f.searchResponse, f.searchErr
}

func (f *fakeForwarder) ForwardStatus(_ context.Context, ownerNodeID string, req StatusRequest) (Status, error) {
	f.statusOwner = ownerNodeID
	f.statusRequest = req
	return f.statusResponse, f.statusErr
}
