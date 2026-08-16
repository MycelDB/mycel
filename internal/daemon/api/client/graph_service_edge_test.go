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

func TestQueryServiceExecuteGQLComparisonPredicates(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	querySvc := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	for _, query := range []string{
		"INSERT (:Person {name: 'Elizabeth II', role: 'Monarch', birthYear: 1926})",
		"INSERT (:Person {name: 'Charles III', role: 'Monarch', birthYear: 1948})",
		"INSERT (:Person {name: 'William', birthYear: 1982})",
	} {
		if _, err := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: writeTx, Query: query}); err != nil {
			t.Fatalf("insert query %q: %v", query, err)
		}
	}
	if _, err := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces).CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	res, err := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: readTx, Query: "MATCH (p:Person) WHERE p.role = 'Monarch' AND p.birthYear > 1940 RETURN p.name, p.birthYear"})
	if err != nil {
		t.Fatalf("comparison query: %v", err)
	}
	rows := res.GetResult().GetRows()
	if len(rows) != 1 || rows[0].GetFields()["p.name"].GetScalar().GetStringValue() != "Charles III" {
		t.Fatalf("unexpected rows: %+v", rows)
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

func TestQueryServiceExecuteGQLSetUpdatesNodeProperties(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	querySvc := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	if _, err := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: writeTx, Query: "INSERT (:Person {name: 'Martin'})"}); err != nil {
		t.Fatalf("insert person: %v", err)
	}
	res, err := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: writeTx, Query: "MATCH (p:Person {name: 'Martin'}) SET p.age = 57, p.sex = 'Male' RETURN p, p.age, p.sex"})
	if err != nil {
		t.Fatalf("set properties: %v", err)
	}
	if got := res.GetResult().GetCounters().GetNodesUpdated(); got != 1 {
		t.Fatalf("nodes_updated = %d, want 1", got)
	}
	rows := res.GetResult().GetRows()
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	fields := rows[0].GetFields()
	if fields["p"].GetNode().GetProperties().AsMap()["age"] != float64(57) || fields["p.sex"].GetScalar().GetStringValue() != "Male" {
		t.Fatalf("unexpected update row: %+v", rows[0])
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	read, err := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: readTx, Query: "MATCH (p:Person {name: 'Martin'}) RETURN p.age, p.sex"})
	if err != nil {
		t.Fatalf("read properties: %v", err)
	}
	readFields := read.GetResult().GetRows()[0].GetFields()
	if readFields["p.age"].GetScalar().GetNumberValue() != 57 || readFields["p.sex"].GetScalar().GetStringValue() != "Male" {
		t.Fatalf("unexpected persisted properties: %+v", read.GetResult().GetRows())
	}
}

func TestQueryServiceExecuteGQLSetUpdatesEdgeProperties(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	querySvc := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	script := `
INSERT (:Person {name: 'Martin'});
INSERT (:Person {name: 'Ivy'});
MATCH (martin:Person {name: 'Martin'}), (ivy:Person {name: 'Ivy'}) CREATE (martin)-[:PARTNER_OF]->(ivy);
`
	if _, err := querySvc.ExecuteGQLScript(fixture.ctx, &clientv1.ExecuteGQLScriptRequest{TransactionId: writeTx, Script: script, StopOnError: true}); err != nil {
		t.Fatalf("seed graph: %v", err)
	}
	res, err := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: writeTx, Query: "MATCH (a:Person)-[r:PARTNER_OF]->(b:Person) SET r.since = 2007 RETURN r, r.since"})
	if err != nil {
		t.Fatalf("set edge properties: %v", err)
	}
	rows := res.GetResult().GetRows()
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	fields := rows[0].GetFields()
	if fields["r"].GetEdge().GetProperties().AsMap()["since"] != float64(2007) || fields["r.since"].GetScalar().GetNumberValue() != 2007 {
		t.Fatalf("unexpected edge update row: %+v", rows[0])
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	read, err := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: readTx, Query: "MATCH (a:Person)-[r:PARTNER_OF]->(b:Person) RETURN r.since"})
	if err != nil {
		t.Fatalf("read edge properties: %v", err)
	}
	if read.GetResult().GetRows()[0].GetFields()["r.since"].GetScalar().GetNumberValue() != 2007 {
		t.Fatalf("unexpected persisted edge properties: %+v", read.GetResult().GetRows())
	}
}

func TestQueryServiceExecuteGQLAliasedProjection(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	querySvc := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	if _, err := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: writeTx, Query: "INSERT (:Person {name: 'Levi', age: 12})"}); err != nil {
		t.Fatalf("insert person: %v", err)
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	res, err := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: readTx, Query: "MATCH (p:Person {name: 'Levi'}) RETURN p.name AS name, p.age AS age"})
	if err != nil {
		t.Fatalf("aliased query: %v", err)
	}
	fields := res.GetResult().GetRows()[0].GetFields()
	if fields["name"].GetScalar().GetStringValue() != "Levi" || fields["age"].GetScalar().GetNumberValue() != 12 {
		t.Fatalf("unexpected aliased fields: %+v", fields)
	}
	if _, ok := fields["p.name"]; ok {
		t.Fatalf("default field name present despite alias: %+v", fields)
	}
}

func TestQueryServiceExecuteGQLParameters(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	querySvc := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	params := map[string]*structpb.Value{
		"name": mustValue(t, "Levi"),
		"age":  mustValue(t, 12),
		"sex":  mustValue(t, "Male"),
	}
	if _, err := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: writeTx, Query: "MERGE (p:Person {name: $name}) RETURN p", Params: params}); err != nil {
		t.Fatalf("parameterized merge: %v", err)
	}
	res, err := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: writeTx, Query: "MATCH (p:Person {name: $name}) SET p.age = $age, p.sex = $sex RETURN p.age AS age, p.sex AS sex", Params: params})
	if err != nil {
		t.Fatalf("parameterized set: %v", err)
	}
	fields := res.GetResult().GetRows()[0].GetFields()
	if fields["age"].GetScalar().GetNumberValue() != 12 || fields["sex"].GetScalar().GetStringValue() != "Male" {
		t.Fatalf("unexpected parameterized fields: %+v", fields)
	}
	_, err = querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: writeTx, Query: "MATCH (p:Person {name: $missing}) RETURN p"})
	if err == nil {
		t.Fatal("ExecuteGQL() with missing param error = nil, want error")
	}
}

func TestQueryServiceExecuteGQLDeleteEdge(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	querySvc := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	script := `
INSERT (:Person {name: 'Vincent'});
INSERT (:Person {name: 'Levi'});
MATCH (a:Person {name: 'Vincent'}), (b:Person {name: 'Levi'}) CREATE (a)-[:FRIEND_OF]->(b);
`
	if _, err := querySvc.ExecuteGQLScript(fixture.ctx, &clientv1.ExecuteGQLScriptRequest{TransactionId: writeTx, Script: script, StopOnError: true}); err != nil {
		t.Fatalf("seed graph: %v", err)
	}
	deleted, err := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: writeTx, Query: "MATCH (a:Person)-[r:FRIEND_OF]->(b:Person {name: 'Levi'}) DELETE r RETURN a, r, b"})
	if err != nil {
		t.Fatalf("delete edge: %v", err)
	}
	if got := deleted.GetResult().GetCounters().GetEdgesDeleted(); got != 1 {
		t.Fatalf("edges_deleted = %d, want 1", got)
	}
	res, err := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: writeTx, Query: "MATCH (a:Person)-[r:FRIEND_OF]->(b:Person) RETURN r"})
	if err != nil {
		t.Fatalf("query deleted edge: %v", err)
	}
	if got := len(res.GetResult().GetRows()); got != 0 {
		t.Fatalf("rows after delete = %d, want 0", got)
	}
}

func TestQueryServiceExecuteGQLMergeNodeAndRelationship(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	querySvc := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	first, err := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: writeTx, Query: "MERGE (p:Person {name: 'Martin'}) RETURN p"})
	if err != nil {
		t.Fatalf("merge node first: %v", err)
	}
	second, err := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: writeTx, Query: "MERGE (p:Person {name: 'Martin'}) RETURN p"})
	if err != nil {
		t.Fatalf("merge node second: %v", err)
	}
	if first.GetResult().GetCounters().GetNodesInserted() != 1 || second.GetResult().GetCounters().GetNodesInserted() != 0 {
		t.Fatalf("node merge counters first=%+v second=%+v", first.GetResult().GetCounters(), second.GetResult().GetCounters())
	}
	if _, err := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: writeTx, Query: "MERGE (p:Person {name: 'Ivy'}) RETURN p"}); err != nil {
		t.Fatalf("merge endpoint: %v", err)
	}
	relFirst, err := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: writeTx, Query: "MATCH (a:Person {name: 'Martin'}), (b:Person {name: 'Ivy'}) MERGE (a)-[r:PARTNER_OF]->(b) RETURN a, r, b"})
	if err != nil {
		t.Fatalf("merge relationship first: %v", err)
	}
	relSecond, err := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: writeTx, Query: "MATCH (a:Person {name: 'Martin'}), (b:Person {name: 'Ivy'}) MERGE (a)-[r:PARTNER_OF]->(b) RETURN a, r, b"})
	if err != nil {
		t.Fatalf("merge relationship second: %v", err)
	}
	if relFirst.GetResult().GetCounters().GetEdgesInserted() != 1 || relSecond.GetResult().GetCounters().GetEdgesInserted() != 0 {
		t.Fatalf("relationship merge counters first=%+v second=%+v", relFirst.GetResult().GetCounters(), relSecond.GetResult().GetCounters())
	}
	listed, err := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: writeTx, Query: "MATCH (a:Person)-[r:PARTNER_OF]->(b:Person) RETURN r"})
	if err != nil {
		t.Fatalf("list merged relationship: %v", err)
	}
	if got := len(listed.GetResult().GetRows()); got != 1 {
		t.Fatalf("merged relationship rows = %d, want 1", got)
	}
}

func TestQueryServiceExecuteGQLMergeRelationshipRejectsBroadEndpointMatches(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	querySvc := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	script := `
INSERT (:Person {name: 'Duplicate'});
INSERT (:Person {name: 'Duplicate'});
INSERT (:Person {name: 'Target'});
`
	if _, err := querySvc.ExecuteGQLScript(fixture.ctx, &clientv1.ExecuteGQLScriptRequest{TransactionId: writeTx, Script: script, StopOnError: true}); err != nil {
		t.Fatalf("seed graph: %v", err)
	}
	_, err := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: writeTx, Query: "MATCH (a:Person {name: 'Duplicate'}), (b:Person {name: 'Target'}) MERGE (a)-[r:KNOWS]->(b) RETURN a, r, b"})
	if err == nil {
		t.Fatal("broad relationship merge error = nil, want guardrail error")
	}
}

func TestQueryServiceExecuteGQLScriptAggregatesGraphEdges(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	querySvc := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	script := `
INSERT (:Person {name: 'Martin'});
INSERT (:Person {name: 'Vincent'});
MATCH (martin:Person {name: 'Martin'}), (vincent:Person {name: 'Vincent'})
CREATE (martin)-[:FATHER_OF]->(vincent);
MATCH (parent:Person)-[father:FATHER_OF]->(child:Person)
RETURN parent, father, child
FETCH FIRST 20 ROWS ONLY;
`
	res, err := querySvc.ExecuteGQLScript(fixture.ctx, &clientv1.ExecuteGQLScriptRequest{TransactionId: writeTx, Script: script, StopOnError: true})
	if err != nil {
		t.Fatalf("ExecuteGQLScript() error = %v", err)
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}
	if got := len(res.GetResult().GetGraph().GetEdges()); got != 1 {
		t.Fatalf("aggregate graph edges = %d, want 1; graph=%+v", got, res.GetResult().GetGraph())
	}
	if got := len(res.GetResult().GetGraph().GetNodes()); got != 2 {
		t.Fatalf("aggregate graph nodes = %d, want 2; graph=%+v", got, res.GetResult().GetGraph())
	}
}

func TestQueryServiceReadWriteNoopCommitDoesNotConflictNextWrite(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	querySvc := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)

	seedTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	if _, err := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: seedTx, Query: "INSERT (:Person {name: 'Vincent'})"}); err != nil {
		t.Fatalf("seed ExecuteGQL() error = %v", err)
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: seedTx}); err != nil {
		t.Fatalf("seed CommitTransaction() error = %v", err)
	}

	readInWriteTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	if _, err := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: readInWriteTx, Query: "MATCH (p:Person) RETURN p"}); err != nil {
		t.Fatalf("read ExecuteGQL() error = %v", err)
	}
	readCommit, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: readInWriteTx})
	if err != nil {
		t.Fatalf("read-in-write CommitTransaction() error = %v", err)
	}
	if readCommit.GetCommit().GetOperationCount() != 0 || readCommit.GetCommit().GetCommittedRevision() != 1 {
		t.Fatalf("read-in-write commit = %#v, want operation_count=0 committed_revision=1", readCommit.GetCommit())
	}

	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	if _, err := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: writeTx, Query: "INSERT (:Person {name: 'Levi'})"}); err != nil {
		t.Fatalf("write ExecuteGQL() error = %v", err)
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("write CommitTransaction() error = %v", err)
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

func TestQueryServiceExecuteGQLReturnsMultiHopPathRows(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)

	aID := uuid.NewString()
	bID := uuid.NewString()
	cID := uuid.NewString()
	if _, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx, Node: &clientv1.NodeCreate{NodeId: &aID, Labels: []string{"Note"}, Properties: mustStruct(t, map[string]any{"title": "A"})}}); err != nil {
		t.Fatalf("CreateNode(a) error = %v", err)
	}
	if _, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx, Node: &clientv1.NodeCreate{NodeId: &bID, Labels: []string{"Note"}, Properties: mustStruct(t, map[string]any{"title": "B"})}}); err != nil {
		t.Fatalf("CreateNode(b) error = %v", err)
	}
	if _, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx, Node: &clientv1.NodeCreate{NodeId: &cID, Labels: []string{"Concept"}, Properties: mustStruct(t, map[string]any{"name": "C"})}}); err != nil {
		t.Fatalf("CreateNode(c) error = %v", err)
	}
	if _, err := graphSvc.CreateEdge(fixture.ctx, &clientv1.CreateEdgeRequest{TransactionId: writeTx, Edge: &clientv1.EdgeCreate{FromNodeId: aID, ToNodeId: bID, Labels: []string{"REFERENCES"}}}); err != nil {
		t.Fatalf("CreateEdge(a-b) error = %v", err)
	}
	if _, err := graphSvc.CreateEdge(fixture.ctx, &clientv1.CreateEdgeRequest{TransactionId: writeTx, Edge: &clientv1.EdgeCreate{FromNodeId: bID, ToNodeId: cID, Labels: []string{"MENTIONS"}}}); err != nil {
		t.Fatalf("CreateEdge(b-c) error = %v", err)
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}

	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	res, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: readTx, Query: "MATCH (a:Note)-[:REFERENCES]->(b:Note)-[:MENTIONS]->(c:Concept) RETURN a.title, b.title, c.name"})
	if err != nil {
		t.Fatalf("ExecuteGQL() error = %v", err)
	}
	rows := res.GetResult().GetRows()
	if len(rows) != 1 {
		t.Fatalf("row count = %d, rows=%+v", len(rows), rows)
	}
	fields := rows[0].GetFields()
	if fields["a.title"].GetScalar().GetStringValue() != "A" || fields["b.title"].GetScalar().GetStringValue() != "B" || fields["c.name"].GetScalar().GetStringValue() != "C" {
		t.Fatalf("unexpected fields: %+v", fields)
	}
}

func TestQueryServiceExecuteGQLVariableLengthTraversal(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	aID, bID, cID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	for _, node := range []struct{ id, title string }{{aID, "A"}, {bID, "B"}, {cID, "C"}} {
		if _, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx, Node: &clientv1.NodeCreate{NodeId: &node.id, Labels: []string{"Note"}, Properties: mustStruct(t, map[string]any{"title": node.title})}}); err != nil {
			t.Fatalf("CreateNode(%s) error = %v", node.title, err)
		}
	}
	if _, err := graphSvc.CreateEdge(fixture.ctx, &clientv1.CreateEdgeRequest{TransactionId: writeTx, Edge: &clientv1.EdgeCreate{FromNodeId: aID, ToNodeId: bID, Labels: []string{"REFERENCES"}}}); err != nil {
		t.Fatalf("CreateEdge(a-b) error = %v", err)
	}
	if _, err := graphSvc.CreateEdge(fixture.ctx, &clientv1.CreateEdgeRequest{TransactionId: writeTx, Edge: &clientv1.EdgeCreate{FromNodeId: bID, ToNodeId: cID, Labels: []string{"REFERENCES"}}}); err != nil {
		t.Fatalf("CreateEdge(b-c) error = %v", err)
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	res, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: readTx, Query: "MATCH (a:Note {title: 'A'})-[:REFERENCES*1..2]->(b:Note) RETURN b.title"})
	if err != nil {
		t.Fatalf("ExecuteGQL() error = %v", err)
	}
	if len(res.GetResult().GetRows()) != 2 {
		t.Fatalf("row count = %d, rows=%+v", len(res.GetResult().GetRows()), res.GetResult().GetRows())
	}
}

func TestQueryServiceExecuteGQLPathBindingReturnsPathAndGraph(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	aID, bID, cID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	for _, node := range []struct{ id, title string }{{aID, "A"}, {bID, "B"}, {cID, "C"}} {
		if _, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx, Node: &clientv1.NodeCreate{NodeId: &node.id, Labels: []string{"Note"}, Properties: mustStruct(t, map[string]any{"title": node.title})}}); err != nil {
			t.Fatalf("CreateNode(%s) error = %v", node.title, err)
		}
	}
	if _, err := graphSvc.CreateEdge(fixture.ctx, &clientv1.CreateEdgeRequest{TransactionId: writeTx, Edge: &clientv1.EdgeCreate{FromNodeId: aID, ToNodeId: bID, Labels: []string{"REFERENCES"}}}); err != nil {
		t.Fatalf("CreateEdge(a-b) error = %v", err)
	}
	if _, err := graphSvc.CreateEdge(fixture.ctx, &clientv1.CreateEdgeRequest{TransactionId: writeTx, Edge: &clientv1.EdgeCreate{FromNodeId: bID, ToNodeId: cID, Labels: []string{"REFERENCES"}}}); err != nil {
		t.Fatalf("CreateEdge(b-c) error = %v", err)
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}

	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	res, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: readTx, Query: "MATCH path = (a:Note {title: 'A'})-[:REFERENCES*1..2]->(b:Note {title: 'C'}) RETURN GRAPH path"})
	if err != nil {
		t.Fatalf("ExecuteGQL() error = %v", err)
	}
	rows := res.GetResult().GetRows()
	if len(rows) != 1 {
		t.Fatalf("row count = %d, rows=%+v", len(rows), rows)
	}
	pathValue := rows[0].GetFields()["path"].GetScalar().AsInterface().(map[string]any)
	if len(pathValue["nodes"].([]any)) != 3 || len(pathValue["edges"].([]any)) != 2 {
		t.Fatalf("path scalar = %#v", pathValue)
	}
	if len(res.GetResult().GetGraph().GetNodes()) != 3 || len(res.GetResult().GetGraph().GetEdges()) != 2 {
		t.Fatalf("graph = %+v", res.GetResult().GetGraph())
	}
}

func TestQueryServiceExecuteGQLPathGraphFollowsPageRows(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	aID, bID, cID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	for _, node := range []struct{ id, title string }{{aID, "A"}, {bID, "B"}, {cID, "C"}} {
		if _, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx, Node: &clientv1.NodeCreate{NodeId: &node.id, Labels: []string{"Note"}, Properties: mustStruct(t, map[string]any{"title": node.title})}}); err != nil {
			t.Fatalf("CreateNode(%s) error = %v", node.title, err)
		}
	}
	if _, err := graphSvc.CreateEdge(fixture.ctx, &clientv1.CreateEdgeRequest{TransactionId: writeTx, Edge: &clientv1.EdgeCreate{FromNodeId: aID, ToNodeId: bID, Labels: []string{"REFERENCES"}}}); err != nil {
		t.Fatalf("CreateEdge(a-b) error = %v", err)
	}
	if _, err := graphSvc.CreateEdge(fixture.ctx, &clientv1.CreateEdgeRequest{TransactionId: writeTx, Edge: &clientv1.EdgeCreate{FromNodeId: aID, ToNodeId: cID, Labels: []string{"REFERENCES"}}}); err != nil {
		t.Fatalf("CreateEdge(a-c) error = %v", err)
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}

	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	res, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: readTx, Query: "MATCH path = (a:Note {title: 'A'})-[:REFERENCES]->(b:Note) RETURN GRAPH path", PageSize: 1})
	if err != nil {
		t.Fatalf("ExecuteGQL() error = %v", err)
	}
	if len(res.GetResult().GetRows()) != 1 || res.GetResult().GetNextPageToken() == "" {
		t.Fatalf("unexpected page rows=%d next=%q", len(res.GetResult().GetRows()), res.GetResult().GetNextPageToken())
	}
	if len(res.GetResult().GetGraph().GetNodes()) != 2 || len(res.GetResult().GetGraph().GetEdges()) != 1 {
		t.Fatalf("paged graph = %+v", res.GetResult().GetGraph())
	}
}

func TestQueryServiceExecuteGQLTextAndSemanticPredicates(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	nodeID := uuid.NewString()
	if _, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx, Node: &clientv1.NodeCreate{NodeId: &nodeID, Labels: []string{"Note"}, Payload: mustStruct(t, map[string]any{"text": "graph memory for family notes"})}}); err != nil {
		t.Fatalf("CreateNode() error = %v", err)
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}
	querySvc := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces)
	for _, query := range []string{
		"MATCH (n:Note) WHERE TEXT_CONTAINS(n.payload.text, 'family') RETURN n.payload.text",
		"MATCH (n:Note) WHERE SEMANTIC_SIMILAR(n, 'family', TOP 10) RETURN n.payload.text",
	} {
		readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
		res, err := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: readTx, Query: query})
		if err != nil {
			t.Fatalf("ExecuteGQL(%q) error = %v", query, err)
		}
		if len(res.GetResult().GetRows()) != 1 {
			t.Fatalf("ExecuteGQL(%q) rows=%+v", query, res.GetResult().GetRows())
		}
	}
}

func mustValue(t *testing.T, value any) *structpb.Value {
	t.Helper()
	out, err := structpb.NewValue(value)
	if err != nil {
		t.Fatalf("NewValue(%v): %v", value, err)
	}
	return out
}

func mustStruct(t *testing.T, values map[string]any) *structpb.Struct {
	t.Helper()
	out, err := structpb.NewStruct(values)
	if err != nil {
		t.Fatalf("NewStruct(%v): %v", values, err)
	}
	return out
}
