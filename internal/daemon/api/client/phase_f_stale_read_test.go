package client

import (
	"testing"

	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPhaseFStaleReadOptInRejectedByDefault(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	options := &clientv1.ReadOptions{AllowStale: true}

	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	if _, err := graphSvc.ListNodes(fixture.ctx, &clientv1.ListNodesRequest{TransactionId: readTx, ReadOptions: options}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ListNodes(stale opt-in) code=%v err=%v; want FailedPrecondition", status.Code(err), err)
	}
	if _, err := graphSvc.GetNode(fixture.ctx, &clientv1.GetNodeRequest{TransactionId: readTx, NodeId: "00000000-0000-0000-0000-000000000001", ReadOptions: options}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("GetNode(stale opt-in) code=%v err=%v; want FailedPrecondition", status.Code(err), err)
	}
	if _, err := graphSvc.ListEdges(fixture.ctx, &clientv1.ListEdgesRequest{TransactionId: readTx, ReadOptions: options}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ListEdges(stale opt-in) code=%v err=%v; want FailedPrecondition", status.Code(err), err)
	}
	if _, err := graphSvc.GetParent(fixture.ctx, &clientv1.GetParentRequest{TransactionId: readTx, ChildNodeId: "00000000-0000-0000-0000-000000000001", ReadOptions: options}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("GetParent(stale opt-in) code=%v err=%v; want FailedPrecondition", status.Code(err), err)
	}

	querySvc := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces)
	query := &clientv1.GraphQuery{Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "n"}}}
	if _, err := querySvc.ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: query, ReadOptions: options}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ExecuteQuery(stale opt-in) code=%v err=%v; want FailedPrecondition", status.Code(err), err)
	}
	if _, err := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: readTx, Query: "MATCH (n) RETURN n", ReadOptions: options}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ExecuteGQL(stale opt-in) code=%v err=%v; want FailedPrecondition", status.Code(err), err)
	}
	if _, err := querySvc.ExecuteGQLScript(fixture.ctx, &clientv1.ExecuteGQLScriptRequest{TransactionId: readTx, Script: "MATCH (n) RETURN n", ReadOptions: options}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ExecuteGQLScript(stale opt-in) code=%v err=%v; want FailedPrecondition", status.Code(err), err)
	}

	metadataSvc := NewMetadataCatalogService(fixture.sessions, fixture.graphs)
	if _, err := metadataSvc.ListTags(fixture.ctx, &clientv1.ListTagsRequest{TransactionId: readTx, ReadOptions: options}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ListTags(stale opt-in) code=%v err=%v; want FailedPrecondition", status.Code(err), err)
	}
	if _, err := metadataSvc.ListPropertyNames(fixture.ctx, &clientv1.ListPropertyNamesRequest{TransactionId: readTx, ReadOptions: options}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ListPropertyNames(stale opt-in) code=%v err=%v; want FailedPrecondition", status.Code(err), err)
	}
}

func TestPhaseFDefaultReadOptionsNeverReturnStaleMetadata(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	created, err := NewGraphService(fixture.sessions, fixture.graphs).CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx, Node: &clientv1.NodeCreate{Labels: []string{"Note"}}})
	if err != nil {
		t.Fatalf("CreateNode() error = %v", err)
	}
	if _, err := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces).CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	got, err := NewGraphService(fixture.sessions, fixture.graphs).GetNode(fixture.ctx, &clientv1.GetNodeRequest{TransactionId: readTx, NodeId: created.GetNode().GetNodeId()})
	if err != nil {
		t.Fatalf("GetNode(default read) error = %v", err)
	}
	if got.GetReadMetadata() != nil && got.GetReadMetadata().GetStale() {
		t.Fatalf("default read returned stale metadata: %#v", got.GetReadMetadata())
	}
}
