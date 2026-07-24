package client

import (
	"reflect"
	"testing"

	"github.com/google/uuid"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestGraphServiceEdgeUsesLabelsPropertiesPayloadAndMeta(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	tx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)

	fromID := uuid.NewString()
	toID := uuid.NewString()
	if _, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: tx, Node: &clientv1.NodeCreate{NodeId: &fromID}}); err != nil {
		t.Fatalf("CreateNode(from) error = %v", err)
	}
	if _, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: tx, Node: &clientv1.NodeCreate{NodeId: &toID}}); err != nil {
		t.Fatalf("CreateNode(to) error = %v", err)
	}

	properties := mustStruct(t, map[string]any{"confidence": 0.92, "source": "manual"})
	payload := mustStruct(t, map[string]any{"text": "relationship annotation"})
	meta := mustStruct(t, map[string]any{"created_by": "test"})
	created, err := graphSvc.CreateEdge(fixture.ctx, &clientv1.CreateEdgeRequest{TransactionId: tx, Edge: &clientv1.EdgeCreate{FromNodeId: fromID, ToNodeId: toID, Labels: []string{"REFERENCES", "CITES"}, Properties: properties, Payload: payload, Meta: meta}})
	if err != nil {
		t.Fatalf("CreateEdge() error = %v", err)
	}
	edge := created.GetEdge()
	if edge.GetDomainId() != fixture.domainID || edge.GetFromNodeId() != fromID || edge.GetToNodeId() != toID {
		t.Fatalf("unexpected edge identity/connectivity: %+v", edge)
	}
	if !reflect.DeepEqual(edge.GetLabels(), []string{"REFERENCES", "CITES"}) || !reflect.DeepEqual(edge.GetProperties().AsMap(), properties.AsMap()) || !reflect.DeepEqual(edge.GetPayload().AsMap(), payload.AsMap()) || !reflect.DeepEqual(edge.GetMeta().AsMap(), meta.AsMap()) {
		t.Fatalf("edge fields mismatch: %+v", edge)
	}
	if edge.GetCreateTime() == nil || edge.GetUpdateTime() == nil {
		t.Fatalf("expected edge timestamps: %+v", edge)
	}

	updatedProperties := mustStruct(t, map[string]any{"confidence": 0.5})
	updated, err := graphSvc.UpdateEdge(fixture.ctx, &clientv1.UpdateEdgeRequest{TransactionId: tx, Edge: &clientv1.Edge{EdgeId: edge.GetEdgeId(), Labels: []string{"IGNORED"}, Properties: updatedProperties}, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"properties"}}})
	if err != nil {
		t.Fatalf("UpdateEdge() error = %v", err)
	}
	updatedEdge := updated.GetEdge()
	if !reflect.DeepEqual(updatedEdge.GetLabels(), []string{"REFERENCES", "CITES"}) {
		t.Fatalf("labels changed despite properties-only mask: %+v", updatedEdge.GetLabels())
	}
	if !reflect.DeepEqual(updatedEdge.GetProperties().AsMap(), updatedProperties.AsMap()) {
		t.Fatalf("properties not updated: %+v", updatedEdge.GetProperties().AsMap())
	}
	if !reflect.DeepEqual(updatedEdge.GetPayload().AsMap(), payload.AsMap()) || !reflect.DeepEqual(updatedEdge.GetMeta().AsMap(), meta.AsMap()) {
		t.Fatalf("payload/meta changed unexpectedly: %+v", updatedEdge)
	}
}

func TestQueryServiceExecuteGQLCreatesRelationshipFromMatchedNodes(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	querySvc := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	if _, err := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: writeTx, Query: "INSERT (:Person {firstName: 'Martin', lastName: 'Beauvais'})"}); err != nil {
		t.Fatalf("insert Martin: %v", err)
	}
	if _, err := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: writeTx, Query: "INSERT (:Person {firstName: 'Ivy', lastName: 'Beauvais'})"}); err != nil {
		t.Fatalf("insert Ivy: %v", err)
	}
	created, err := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: writeTx, Query: "MATCH (martin:Person {firstName: 'Martin', lastName: 'Beauvais'}), (ivy:Person {firstName: 'Ivy', lastName: 'Beauvais'}) CREATE (martin)-[:Spouse]->(ivy)"})
	if err != nil {
		t.Fatalf("create relationship: %v", err)
	}
	if created.GetResult().GetCounters().GetEdgesInserted() != 1 {
		t.Fatalf("edges_inserted = %d, want 1", created.GetResult().GetCounters().GetEdgesInserted())
	}
	if _, err := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces).CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	res, err := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: readTx, Query: "MATCH (martin:Person)-[r:Spouse]->(ivy:Person) RETURN martin.firstName, r, ivy.firstName"})
	if err != nil {
		t.Fatalf("query relationship: %v", err)
	}
	if len(res.GetResult().GetRows()) != 1 || res.GetResult().GetRows()[0].GetFields()["r"].GetEdge() == nil {
		t.Fatalf("unexpected relationship rows: %+v", res.GetResult().GetRows())
	}
}

func TestQueryServiceExecuteGQLReturnsRelationshipPatternRows(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)

	fromID := uuid.NewString()
	toID := uuid.NewString()
	if _, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx, Node: &clientv1.NodeCreate{NodeId: &fromID, Labels: []string{"Note"}, Properties: mustStruct(t, map[string]any{"title": "Source"})}}); err != nil {
		t.Fatalf("CreateNode(from) error = %v", err)
	}
	if _, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx, Node: &clientv1.NodeCreate{NodeId: &toID, Labels: []string{"Note"}, Properties: mustStruct(t, map[string]any{"title": "Target"})}}); err != nil {
		t.Fatalf("CreateNode(to) error = %v", err)
	}
	createdEdge, err := graphSvc.CreateEdge(fixture.ctx, &clientv1.CreateEdgeRequest{TransactionId: writeTx, Edge: &clientv1.EdgeCreate{FromNodeId: fromID, ToNodeId: toID, Labels: []string{"REFERENCES"}, Properties: mustStruct(t, map[string]any{"confidence": 0.9})}})
	if err != nil {
		t.Fatalf("CreateEdge() error = %v", err)
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}

	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	res, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: readTx, Query: "MATCH (a:Note)-[r:REFERENCES {confidence: 0.9}]->(b:Note) RETURN a, r, r.confidence, b.title"})
	if err != nil {
		t.Fatalf("ExecuteGQL() error = %v", err)
	}
	rows := res.GetResult().GetRows()
	if len(rows) != 1 {
		t.Fatalf("row count = %d, rows=%+v", len(rows), rows)
	}
	fields := rows[0].GetFields()
	if fields["a"].GetNode().GetNodeId() != fromID || fields["b.title"].GetScalar().GetStringValue() != "Target" {
		t.Fatalf("unexpected node fields: %+v", fields)
	}
	if fields["r"].GetEdge().GetEdgeId() != createdEdge.GetEdge().GetEdgeId() || !reflect.DeepEqual(fields["r"].GetEdge().GetLabels(), []string{"REFERENCES"}) || fields["r.confidence"].GetScalar().GetNumberValue() != 0.9 {
		t.Fatalf("unexpected edge fields: %+v", fields)
	}
	graph := res.GetResult().GetGraph()
	if len(graph.GetNodes()) != 1 || len(graph.GetEdges()) != 1 {
		t.Fatalf("result graph = %+v, want returned node and edge", graph)
	}
}

func mustStruct(t *testing.T, values map[string]any) *structpb.Struct {
	t.Helper()
	out, err := structpb.NewStruct(values)
	if err != nil {
		t.Fatalf("NewStruct(%v): %v", values, err)
	}
	return out
}
