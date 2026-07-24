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

func mustStruct(t *testing.T, values map[string]any) *structpb.Struct {
	t.Helper()
	out, err := structpb.NewStruct(values)
	if err != nil {
		t.Fatalf("NewStruct(%v): %v", values, err)
	}
	return out
}
