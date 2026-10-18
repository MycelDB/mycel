package engine_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"
	graph "github.com/myceldb/mycel/internal/graph/model"
	graphquery "github.com/myceldb/mycel/internal/graph/query"
	queryengine "github.com/myceldb/mycel/internal/query/engine"
	queryexecution "github.com/myceldb/mycel/internal/query/execution"
	querylogical "github.com/myceldb/mycel/internal/query/logical"
	queryphysical "github.com/myceldb/mycel/internal/query/physical"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
)

type recordingEntityReader struct {
	calls int
	tx    sessionmodel.GraphTransaction
	nodes []graph.Node
	byID  map[string]graph.Node
}

func (r *recordingEntityReader) ReadViewIdentity(_ context.Context, tx sessionmodel.GraphTransaction) (string, error) {
	r.calls++
	r.tx = tx
	return "0", nil
}

func (r *recordingEntityReader) GetNode(_ context.Context, tx sessionmodel.GraphTransaction, nodeID string) (graph.Node, error) {
	r.calls++
	r.tx = tx
	if node, ok := r.byID[nodeID]; ok {
		return node, nil
	}
	return graph.Node{}, nil
}

func (r *recordingEntityReader) GetEdge(_ context.Context, tx sessionmodel.GraphTransaction, _ string) (graph.Edge, error) {
	r.calls++
	r.tx = tx
	return graph.Edge{}, nil
}

func (r *recordingEntityReader) ListNodes(_ context.Context, tx sessionmodel.GraphTransaction, _ int, _ string) ([]graph.Node, string, error) {
	r.calls++
	r.tx = tx
	return append([]graph.Node(nil), r.nodes...), "", nil
}

func (r *recordingEntityReader) ScanNodes(_ context.Context, tx sessionmodel.GraphTransaction, _ graphquery.NodeScan) ([]graph.Node, string, graphquery.IndexedReadStats, error) {
	r.calls++
	r.tx = tx
	return append([]graph.Node(nil), r.nodes...), "", graphquery.IndexedReadStats{NodesLoaded: len(r.nodes), IndexEntriesScanned: len(r.nodes)}, nil
}

func (r *recordingEntityReader) ListEdges(_ context.Context, tx sessionmodel.GraphTransaction, _ int, _ string) ([]graph.Edge, string, error) {
	r.calls++
	r.tx = tx
	return nil, "", nil
}

type repeatedBindingTraversal struct {
	edges []graph.Edge
}

func (r repeatedBindingTraversal) ScanAdjacency(_ context.Context, _ sessionmodel.GraphTransaction, scan graphquery.AdjacencyScan) ([]graph.Edge, string, graphquery.IndexedReadStats, error) {
	matched := make([]graph.Edge, 0, len(r.edges))
	for _, edge := range r.edges {
		endpointMatches := scan.Direction == graphquery.AdjacencyDirectionOut && edge.FromID.String() == scan.NodeID
		endpointMatches = endpointMatches || scan.Direction == graphquery.AdjacencyDirectionIn && edge.ToID.String() == scan.NodeID
		if !endpointMatches || scan.Label != "" && !graph.EdgeHasLabels(edge, []string{scan.Label}) {
			continue
		}
		matched = append(matched, edge)
	}
	if scan.Limit > 0 && len(matched) > scan.Limit {
		matched = matched[:scan.Limit]
	}
	return matched, "", graphquery.IndexedReadStats{IndexEntriesScanned: len(matched), EdgesLoaded: len(matched), AdjacencyScanCalls: 1}, nil
}

type panicIndexReader struct{}

type panicSemanticCandidateReader struct{}

type panicMutationWriter struct{}

type recordingMutationWriter struct {
	panicMutationWriter
	creates        int
	updates        int
	updateAttempts int
	nodeUpdates    map[string]int
	edgeUpdates    map[string]int
	cancel         context.CancelFunc
}

func (panicMutationWriter) BeginMutationStatement(ctx context.Context, _ sessionmodel.GraphTransaction) (context.Context, any, error) {
	return ctx, struct{}{}, nil
}
func (panicMutationWriter) FinishMutationStatement(context.Context, sessionmodel.GraphTransaction, any, bool) error {
	return nil
}

type mutationWriterState struct {
	creates int
	updates int
}

func (w *recordingMutationWriter) BeginMutationStatement(ctx context.Context, _ sessionmodel.GraphTransaction) (context.Context, any, error) {
	return ctx, mutationWriterState{creates: w.creates, updates: w.updates}, nil
}
func (w *recordingMutationWriter) FinishMutationStatement(_ context.Context, _ sessionmodel.GraphTransaction, state any, commit bool) error {
	if !commit {
		captured := state.(mutationWriterState)
		w.creates = captured.creates
		w.updates = captured.updates
	}
	return nil
}
func (w *recordingMutationWriter) CreateNode(context.Context, sessionmodel.GraphTransaction, graph.NodeInput) (graph.Node, error) {
	w.creates++
	if w.cancel != nil {
		w.cancel()
	}
	return graph.Node{ID: uuid.New(), Labels: []string{"Note"}}, nil
}

func (w *recordingMutationWriter) UpdateNode(_ context.Context, _ sessionmodel.GraphTransaction, input graph.UpdateNodeInput) (graph.Node, error) {
	w.updateAttempts++
	w.updates++
	if w.nodeUpdates == nil {
		w.nodeUpdates = map[string]int{}
	}
	w.nodeUpdates[input.NodeID]++
	return graph.Node{ID: uuid.MustParse(input.NodeID), Labels: input.Labels, Properties: input.Properties, Payload: input.Payload}, nil
}

func (w *recordingMutationWriter) UpdateEdge(_ context.Context, _ sessionmodel.GraphTransaction, input graph.UpdateEdgeInput) (graph.Edge, error) {
	if w.edgeUpdates == nil {
		w.edgeUpdates = map[string]int{}
	}
	w.edgeUpdates[input.EdgeID]++
	return graph.Edge{ID: uuid.MustParse(input.EdgeID), Labels: input.Labels, Properties: input.Properties, Payload: input.Payload}, nil
}

func (panicMutationWriter) CreateNode(context.Context, sessionmodel.GraphTransaction, graph.NodeInput) (graph.Node, error) {
	panic("Explain must not create nodes")
}
func (panicMutationWriter) UpdateNode(context.Context, sessionmodel.GraphTransaction, graph.UpdateNodeInput) (graph.Node, error) {
	panic("Explain must not update nodes")
}
func (panicMutationWriter) UpsertNode(context.Context, sessionmodel.GraphTransaction, graph.NodeInput) (graph.Node, error) {
	panic("Explain must not upsert nodes")
}
func (panicMutationWriter) DeleteNode(context.Context, sessionmodel.GraphTransaction, string, bool) ([]string, []string, error) {
	panic("Explain must not delete nodes")
}
func (panicMutationWriter) CreateEdge(context.Context, sessionmodel.GraphTransaction, graph.EdgeInput) (graph.Edge, error) {
	panic("Explain must not create edges")
}
func (panicMutationWriter) UpdateEdge(context.Context, sessionmodel.GraphTransaction, graph.UpdateEdgeInput) (graph.Edge, error) {
	panic("Explain must not update edges")
}
func (panicMutationWriter) DeleteEdge(context.Context, sessionmodel.GraphTransaction, string) (string, error) {
	panic("Explain must not delete edges")
}

func (panicSemanticCandidateReader) SearchCandidates(context.Context, queryengine.SemanticCandidateRequest) (queryengine.SemanticCandidateBatch, error) {
	panic("Explain must not invoke semantic inference")
}

func (panicIndexReader) ConfigureIndexes(context.Context, sessionmodel.GraphTransaction, string, []schemamodel.IndexDefinition) error {
	panic("planning must not configure indexes")
}

func (panicIndexReader) ScanLabel(context.Context, sessionmodel.GraphTransaction, graphquery.LabelScan) ([]graph.Node, string, graphquery.IndexedReadStats, error) {
	panic("planning must not scan labels")
}

func (panicIndexReader) ScanTag(context.Context, sessionmodel.GraphTransaction, graphquery.TagScan) ([]graph.Node, string, graphquery.IndexedReadStats, error) {
	panic("planning must not scan tags")
}

func (panicIndexReader) ScanNodePropertyOrdered(context.Context, sessionmodel.GraphTransaction, graphquery.OrderedNodePropertyScan) ([]graph.Node, string, graphquery.IndexedReadStats, error) {
	panic("planning must not scan ordered indexes")
}

type panicCatalog struct{}

func (panicCatalog) GetDomainSchema(context.Context, graph.DomainID) (schemamodel.DomainSchema, error) {
	panic("binding must not load schema through the catalog")
}

func TestEnginePrepareUsesNativeBroadScanWithoutAccess(t *testing.T) {
	entities := &recordingEntityReader{}
	engine := queryengine.New(queryengine.Ports{Entities: entities, Catalog: panicCatalog{}})
	tx := sessionmodel.GraphTransaction{
		ID:           "tx-1",
		SessionID:    "session-1",
		PrincipalID:  "principal-1",
		SpaceID:      "space-1",
		DomainID:     "domain-1",
		Mode:         sessionmodel.TransactionModeReadWrite,
		State:        sessionmodel.TransactionStateActive,
		BaseRevision: 42,
	}
	statement := validReadStatement()

	prepared, err := engine.Prepare(context.Background(), broadPlanningContext(tx), statement)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if prepared.RequiresLegacy() {
		t.Fatalf("prepared kind = %q, want native broad scan", prepared.Kind)
	}
	if !reflect.DeepEqual(prepared.Planning.Transaction, tx) {
		t.Fatalf("planning transaction = %#v, want %#v", prepared.Planning.Transaction, tx)
	}
	if !reflect.DeepEqual(prepared.Statement, statement) {
		t.Fatalf("prepared statement = %#v, want %#v", prepared.Statement, statement)
	}
	if prepared.Physical.Family != queryphysical.FamilyBoundedFullScan || prepared.Physical.Access.Kind != queryphysical.AccessNodeScan || !operatorTreeContains(prepared.Physical.Root, queryphysical.OperatorNodeScan) {
		t.Fatalf("physical plan = %#v", prepared.Physical)
	}
	if len(prepared.Binding.Variables) != 1 || prepared.Binding.Variables[0].Name != "n" {
		t.Fatalf("bound variables = %#v", prepared.Binding.Variables)
	}
	if entities.calls != 0 {
		t.Fatalf("entity reads during prepare = %d, want 0", entities.calls)
	}
	capabilities := engine.Capabilities()
	if !capabilities.EntityReads || !capabilities.Catalog || capabilities.Indexes || capabilities.Mutations {
		t.Fatalf("capabilities = %#v", capabilities)
	}
}

func TestEnginePrepareSelectsIndexedPhysicalPlanWithoutIndexAccess(t *testing.T) {
	entities := &recordingEntityReader{}
	engine := queryengine.New(queryengine.Ports{Entities: entities, Indexes: panicIndexReader{}})
	schema := schemamodel.DomainSchema{
		Version: "v4", Mode: schemamodel.SchemaModeStrict,
		NodeTypes: []schemamodel.NodeType{{Name: "Document", Labels: []string{"Document"}, Properties: []schemamodel.FieldSpec{{Name: "name", Type: schemamodel.FieldTypeString}}}},
		Indexes:   []schemamodel.IndexDefinition{{Name: "document_name", TargetKind: schemamodel.IndexTargetNode, TargetType: "Document", Field: schemamodel.FieldPath{Namespace: "properties", Name: "name"}, Kind: schemamodel.IndexKindOrdered}},
	}
	statement := validReadStatement()
	left := querylogical.FieldValue("n", "name")
	right := querylogical.Value{Kind: querylogical.ValueString, String: "Ada"}
	statement.Read.Predicate = querylogical.LeafPredicate(querylogical.Leaf{Kind: querylogical.LeafComparison, Left: &left, Right: &right, Operator: "="})

	prepared, err := engine.Prepare(context.Background(), queryengine.PlanningContext{Schema: &schema}, statement)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if prepared.Physical.Family != queryphysical.FamilyIndexedEquality || len(prepared.Physical.Indexes) != 1 || prepared.Physical.Indexes[0].Name != "document_name" {
		t.Fatalf("physical plan = %#v", prepared.Physical)
	}
	if entities.calls != 0 {
		t.Fatalf("entity reads during indexed planning = %d, want 0", entities.calls)
	}
	diagnostics, err := engine.Explain(context.Background(), queryengine.PlanningContext{Schema: &schema}, statement)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	if diagnostics.Plan != prepared.Physical.Name || diagnostics.PlanKind != string(prepared.Physical.Family) || !reflect.DeepEqual(diagnostics.Indexes, []string{"document_name"}) {
		t.Fatalf("Explain diagnostics %#v do not match prepared plan %#v", diagnostics, prepared.Physical)
	}
	if entities.calls != 0 {
		t.Fatalf("entity reads during indexed Explain = %d, want 0", entities.calls)
	}
}

func TestEngineExplainMutationPlanDoesNotInvokeWriter(t *testing.T) {
	engine := queryengine.New(queryengine.Ports{Mutations: panicMutationWriter{}})
	statement := querylogical.MutationStatement(querylogical.SourceGQL, querylogical.Mutation{
		Kind: querylogical.MutationInsertNode,
		Node: &querylogical.NodePattern{Labels: []string{"Note"}},
	})
	planning := queryengine.PlanningContext{Transaction: sessionmodel.GraphTransaction{Mode: sessionmodel.TransactionModeReadWrite, State: sessionmodel.TransactionStateActive}}
	diagnostics, err := engine.Explain(context.Background(), planning, statement)
	if err != nil {
		t.Fatalf("explain mutation plan: %v", err)
	}
	if diagnostics.PlanKind != string(queryphysical.FamilyMutation) || diagnostics.LegacyBridge || diagnostics.AccessPath != string(queryphysical.AccessMutation) {
		t.Fatalf("mutation diagnostics = %#v", diagnostics)
	}
}

func TestEngineExecuteMutationValidatesCursorBeforeWriting(t *testing.T) {
	writer := &recordingMutationWriter{}
	engine := queryengine.New(queryengine.Ports{Mutations: writer})
	tx := sessionmodel.GraphTransaction{ID: "tx-1", Mode: sessionmodel.TransactionModeReadWrite, State: sessionmodel.TransactionStateActive}
	statement := querylogical.MutationStatement(querylogical.SourceGQL, querylogical.Mutation{
		Kind: querylogical.MutationInsertNode, Node: &querylogical.NodePattern{Labels: []string{"Note"}},
		Page: querylogical.PageSpec{Cursor: "not-an-offset"},
	})
	prepared, err := engine.Prepare(context.Background(), queryengine.PlanningContext{Transaction: tx}, statement)
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	_, err = engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx}, prepared)
	assertQueryErrorCode(t, err, querylogical.ErrorInvalidQuery)
	if writer.creates != 0 {
		t.Fatalf("CreateNode calls = %d, want 0", writer.creates)
	}
}

func TestEngineExecuteMutationRestoresSavepointAfterShapingBudgetFailure(t *testing.T) {
	entities := &recordingEntityReader{}
	writer := &recordingMutationWriter{}
	engine := queryengine.New(queryengine.Ports{Entities: entities, Mutations: writer})
	tx := sessionmodel.GraphTransaction{ID: "tx-1", Mode: sessionmodel.TransactionModeReadWrite, State: sessionmodel.TransactionStateActive}
	statement := querylogical.MutationStatement(querylogical.SourceGQL, querylogical.Mutation{
		Kind:    querylogical.MutationMergeNode,
		Node:    &querylogical.NodePattern{Alias: "n", Labels: []string{"Note"}},
		Returns: []querylogical.Projection{{Kind: querylogical.ProjectionNode, Alias: "n", OutputName: "n"}},
	})
	prepared, err := engine.Prepare(context.Background(), broadPlanningContext(tx), statement)
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	_, err = engine.Execute(context.Background(), queryengine.ExecutionContext{
		Transaction: tx, Budget: queryexecution.Budget{MaxMemoryBytes: 1},
	}, prepared)
	assertQueryErrorCode(t, err, querylogical.ErrorResourceExhausted)
	if writer.creates != 0 {
		t.Fatalf("staged creates after failed statement = %d, want 0", writer.creates)
	}
}

func TestEngineExecuteMatchedSetCoalescesRepeatedEntityBindings(t *testing.T) {
	a := graph.Node{ID: uuid.New(), Labels: []string{"Person"}, Properties: map[string]any{"name": "A"}}
	b := graph.Node{ID: uuid.New(), Labels: []string{"Person"}, Properties: map[string]any{"name": "B"}}
	c := graph.Node{ID: uuid.New(), Labels: []string{"Person"}, Properties: map[string]any{"name": "C"}}
	first := graph.Edge{ID: uuid.New(), FromID: a.ID, ToID: b.ID, Labels: []string{"LINK"}}
	back := graph.Edge{ID: uuid.New(), FromID: b.ID, ToID: a.ID, Labels: []string{"NEXT"}}
	branch := graph.Edge{ID: uuid.New(), FromID: b.ID, ToID: c.ID, Labels: []string{"NEXT"}}
	entities := &recordingEntityReader{
		nodes: []graph.Node{a},
		byID:  map[string]graph.Node{a.ID.String(): a, b.ID.String(): b, c.ID.String(): c},
	}
	writer := &recordingMutationWriter{}
	engine := queryengine.New(queryengine.Ports{
		Entities: entities, Traversals: repeatedBindingTraversal{edges: []graph.Edge{first, back, branch}}, Mutations: writer,
	})
	tx := sessionmodel.GraphTransaction{ID: "tx-repeated-set", Mode: sessionmodel.TransactionModeReadWrite, State: sessionmodel.TransactionStateActive}
	match := &querylogical.Query{
		Source: querylogical.SourceGQL, AccessMode: querylogical.AccessModeReadOnly,
		Start: querylogical.NodePattern{Alias: "n", NodeIDs: []string{a.ID.String()}},
		Steps: []querylogical.Step{
			{Edge: querylogical.EdgePattern{Alias: "r", Labels: []string{"LINK"}, Direction: querylogical.DirectionOutgoing}, Depth: querylogical.DepthBounds{Min: 1, Max: 1}, Target: querylogical.NodePattern{Alias: "m"}},
			{Edge: querylogical.EdgePattern{Alias: "s", Labels: []string{"NEXT"}, Direction: querylogical.DirectionOutgoing}, Depth: querylogical.DepthBounds{Min: 1, Max: 1}, Target: querylogical.NodePattern{Alias: "x"}},
		},
	}
	statement := querylogical.MutationStatement(querylogical.SourceGQL, querylogical.Mutation{
		Kind: querylogical.MutationMatchSet, Match: match,
		Assignments: []querylogical.Assignment{
			{Field: querylogical.FieldRef{Alias: "n", Namespace: querylogical.NamespaceProperties, Name: "alpha"}, Value: querylogical.Value{Kind: querylogical.ValueString, String: "one"}},
			{Field: querylogical.FieldRef{Alias: "x", Namespace: querylogical.NamespaceProperties, Name: "beta"}, Value: querylogical.Value{Kind: querylogical.ValueString, String: "two"}},
			{Field: querylogical.FieldRef{Alias: "r", Namespace: querylogical.NamespaceProperties, Name: "gamma"}, Value: querylogical.Value{Kind: querylogical.ValueString, String: "edge"}},
		},
		Returns: []querylogical.Projection{
			{Kind: querylogical.ProjectionNode, Alias: "n", OutputName: "n"},
			{Kind: querylogical.ProjectionNode, Alias: "x", OutputName: "x"},
			{Kind: querylogical.ProjectionEdge, Alias: "r", OutputName: "r"},
		},
		Page: querylogical.PageSpec{Size: 10},
	})
	prepared, err := engine.Prepare(context.Background(), broadPlanningContext(tx), statement)
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	result, err := engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx}, prepared)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if writer.nodeUpdates[a.ID.String()] != 1 || writer.nodeUpdates[c.ID.String()] != 1 || len(writer.nodeUpdates) != 2 {
		t.Fatalf("node update calls = %+v, want one each for repeated A and C", writer.nodeUpdates)
	}
	if writer.edgeUpdates[first.ID.String()] != 1 || len(writer.edgeUpdates) != 1 {
		t.Fatalf("edge update calls = %+v, want one for repeated first edge", writer.edgeUpdates)
	}
	if len(result.Rows) == 0 {
		t.Fatal("result rows = 0, want propagated bindings")
	}
	for _, row := range result.Rows {
		n := row.Fields["n"].Node
		x := row.Fields["x"].Node
		r := row.Fields["r"].Edge
		if n == nil || n.Properties["alpha"].String != "one" || r == nil || r.Properties["gamma"].String != "edge" {
			t.Fatalf("propagated repeated bindings = %+v", row.Fields)
		}
		if x != nil && x.Properties["beta"].String != "two" {
			t.Fatalf("propagated target alias = %+v", x)
		}
	}
}

func TestEngineExecuteMatchedMutationAccountsClonedRowsBeforeWriting(t *testing.T) {
	node := graph.Node{ID: uuid.New(), Labels: []string{"Note"}, Properties: map[string]any{"body": strings.Repeat("x", 64<<10)}}
	entities := &recordingEntityReader{nodes: []graph.Node{node}}
	writer := &recordingMutationWriter{}
	engine := queryengine.New(queryengine.Ports{Entities: entities, Mutations: writer})
	tx := sessionmodel.GraphTransaction{ID: "tx-1", Mode: sessionmodel.TransactionModeReadWrite, State: sessionmodel.TransactionStateActive}
	match := validReadStatement().Read
	match.Source = querylogical.SourceGQL
	match.Start = querylogical.NodePattern{Alias: "n"}
	statement := querylogical.MutationStatement(querylogical.SourceGQL, querylogical.Mutation{
		Kind:  querylogical.MutationMatchSet,
		Match: match,
		Assignments: []querylogical.Assignment{{
			Field: querylogical.FieldRef{Alias: "n", Namespace: querylogical.NamespaceProperties, Name: "status"},
			Value: querylogical.Value{Kind: querylogical.ValueString, String: "updated"},
		}},
		Returns: []querylogical.Projection{{Kind: querylogical.ProjectionNode, Alias: "n", OutputName: "n"}},
	})
	prepared, err := engine.Prepare(context.Background(), broadPlanningContext(tx), statement)
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	rowBytes := queryexecution.EstimateBindingRow(queryexecution.BindingRow{Nodes: map[string]graph.Node{"n": node}})
	_, err = engine.Execute(context.Background(), queryengine.ExecutionContext{
		Transaction: tx,
		Budget:      queryexecution.Budget{MaxMemoryBytes: rowBytes*2 + 1024, BatchSize: 1},
	}, prepared)
	assertQueryErrorCode(t, err, querylogical.ErrorResourceExhausted)
	if writer.updates != 0 {
		t.Fatalf("UpdateNode calls = %d, want 0 before clone memory is reserved", writer.updates)
	}
}

func TestEngineExecuteMatchedMutationKeepsCloneChargedThroughShaping(t *testing.T) {
	node := graph.Node{ID: uuid.New(), Labels: []string{"Note"}, Properties: map[string]any{"body": strings.Repeat("x", 16<<10)}}
	entities := &recordingEntityReader{nodes: []graph.Node{node}}
	writer := &recordingMutationWriter{}
	engine := queryengine.New(queryengine.Ports{Entities: entities, Mutations: writer})
	tx := sessionmodel.GraphTransaction{ID: "tx-shaped-memory", Mode: sessionmodel.TransactionModeReadWrite, State: sessionmodel.TransactionStateActive}
	match := validReadStatement().Read
	match.Source = querylogical.SourceGQL
	match.Start = querylogical.NodePattern{Alias: "n"}
	returns := make([]querylogical.Projection, 16)
	for index := range returns {
		returns[index] = querylogical.Projection{Kind: querylogical.ProjectionNode, Alias: "n", OutputName: fmt.Sprintf("n%d", index)}
	}
	statement := querylogical.MutationStatement(querylogical.SourceGQL, querylogical.Mutation{
		Kind: querylogical.MutationMatchSet, Match: match,
		Assignments: []querylogical.Assignment{{
			Field: querylogical.FieldRef{Alias: "n", Namespace: querylogical.NamespaceProperties, Name: "status"},
			Value: querylogical.Value{Kind: querylogical.ValueString, String: "updated"},
		}},
		Returns: returns,
		Page:    querylogical.PageSpec{Size: 10},
	})
	prepared, err := engine.Prepare(context.Background(), broadPlanningContext(tx), statement)
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	rowBytes := queryexecution.EstimateBindingRow(queryexecution.BindingRow{Nodes: map[string]graph.Node{"n": node}})
	_, err = engine.Execute(context.Background(), queryengine.ExecutionContext{
		Transaction: tx,
		Budget:      queryexecution.Budget{MaxMemoryBytes: rowBytes * 4, BatchSize: 1},
	}, prepared)
	assertQueryErrorCode(t, err, querylogical.ErrorResourceExhausted)
	if writer.updateAttempts != 1 {
		t.Fatalf("UpdateNode attempts = %d, want 1 before shaping failure", writer.updateAttempts)
	}
	if writer.updates != 0 {
		t.Fatalf("staged updates after shaping failure = %d, want rollback", writer.updates)
	}
}

func TestEngineExecuteMutationRestoresSavepointWhenDependencyCancelsAfterWriting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	entities := &recordingEntityReader{}
	writer := &recordingMutationWriter{cancel: cancel}
	engine := queryengine.New(queryengine.Ports{Entities: entities, Mutations: writer})
	tx := sessionmodel.GraphTransaction{ID: "tx-1", Mode: sessionmodel.TransactionModeReadWrite, State: sessionmodel.TransactionStateActive}
	statement := querylogical.MutationStatement(querylogical.SourceGQL, querylogical.Mutation{
		Kind: querylogical.MutationMergeNode, Node: &querylogical.NodePattern{Alias: "n", Labels: []string{"Note"}},
		Returns: []querylogical.Projection{{Kind: querylogical.ProjectionNode, Alias: "n", OutputName: "n"}},
	})
	prepared, err := engine.Prepare(ctx, broadPlanningContext(tx), statement)
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	_, err = engine.Execute(ctx, queryengine.ExecutionContext{Transaction: tx}, prepared)
	assertQueryErrorCode(t, err, querylogical.ErrorCanceled)
	if writer.creates != 0 {
		t.Fatalf("staged creates after canceled statement = %d, want 0", writer.creates)
	}
}

func TestEngineExplainSemanticPlanDoesNotInvokeCandidates(t *testing.T) {
	entities := &recordingEntityReader{}
	engine := queryengine.New(queryengine.Ports{Entities: entities, SemanticCandidates: panicSemanticCandidateReader{}})
	statement := validReadStatement()
	field := querylogical.FieldRef{Alias: "n", Namespace: querylogical.NamespaceProperties}
	statement.Read.Predicate = querylogical.LeafPredicate(querylogical.Leaf{Kind: querylogical.LeafSemantic, Field: &field, Query: "graph databases", Limit: 5})
	planning := broadPlanningContext(sessionmodel.GraphTransaction{})
	planning.Domain.SemanticSearchable = true
	planning.Domain.SemanticCandidatesAvailable = true

	diagnostics, err := engine.Explain(context.Background(), planning, statement)
	if err != nil {
		t.Fatalf("explain semantic plan: %v", err)
	}
	if diagnostics.PlanKind != string(queryphysical.FamilySemantic) || diagnostics.ResourceLimits.MaxSemanticCandidates != 5 {
		t.Fatalf("semantic diagnostics = %#v", diagnostics)
	}
	if entities.calls != 0 {
		t.Fatalf("entity reads during semantic Explain = %d", entities.calls)
	}
}

func TestEngineExplainDescribesNativeBroadScanWithoutExecuting(t *testing.T) {
	entities := &recordingEntityReader{}
	engine := queryengine.New(queryengine.Ports{Entities: entities})

	diagnostics, err := engine.Explain(context.Background(), broadPlanningContext(sessionmodel.GraphTransaction{}), validReadStatement())
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	if diagnostics.Plan != "StreamingNodeScan" || diagnostics.PlanKind != string(queryphysical.FamilyBoundedFullScan) {
		t.Fatalf("diagnostics = %#v", diagnostics)
	}
	if diagnostics.PlannerVersion != "qrx10" || diagnostics.FallbackMode != "broad_graph_fallback" || !diagnostics.ExplainOnly || diagnostics.LegacyBridge || diagnostics.AccessPath != string(queryphysical.AccessNodeScan) {
		t.Fatalf("diagnostics = %#v", diagnostics)
	}
	if len(diagnostics.Operators) == 0 || diagnostics.ResourceLimits.MaxNodesLoaded == 0 || diagnostics.RowsScanned != 0 || diagnostics.NodesLoaded != 0 {
		t.Fatalf("static diagnostics = %#v", diagnostics)
	}
	if entities.calls != 0 {
		t.Fatalf("entity reads during explain = %d, want 0", entities.calls)
	}
}

func TestEnginePrepareReturnsTypedBindingErrorsBeforeAccess(t *testing.T) {
	entities := &recordingEntityReader{}
	engine := queryengine.New(queryengine.Ports{Entities: entities})
	statement := validReadStatement()
	statement.Read.Start.Alias = ""

	_, err := engine.Prepare(context.Background(), queryengine.PlanningContext{}, statement)
	assertQueryErrorCode(t, err, querylogical.ErrorInvalidQuery)
	if entities.calls != 0 {
		t.Fatalf("entity reads during rejected prepare = %d, want 0", entities.calls)
	}
}

func TestEngineExecuteShapesExplicitLegacyBindingsWithoutAccess(t *testing.T) {
	entities := &recordingEntityReader{}
	engine := queryengine.New(queryengine.Ports{Entities: entities})
	tx := sessionmodel.GraphTransaction{ID: "tx-6", SessionID: "session-6", PrincipalID: "principal-6", SpaceID: "space-6", DomainID: "domain-6", Mode: sessionmodel.TransactionModeReadOnly, State: sessionmodel.TransactionStateActive, BaseRevision: 61}
	statement := validReadStatement()
	statement.Read.Returns = []querylogical.Projection{{Kind: querylogical.ProjectionNode, Alias: "n", OutputName: "node"}}
	prepared, err := engine.Prepare(context.Background(), broadPlanningContext(tx), statement)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	node := graph.Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000006"), DomainID: uuid.MustParse("10000000-0000-0000-0000-000000000006"), Labels: []string{"Document"}}
	result, err := engine.Execute(context.Background(), queryengine.ExecutionContext{
		Transaction: tx,
		LegacyRows:  []queryexecution.BindingRow{{Nodes: map[string]graph.Node{"n": node}}},
		Budget:      queryengine.WorkBudget{BatchSize: 1},
	}, prepared)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0].Fields["node"].Node == nil || result.Rows[0].Fields["node"].Node.ID != node.ID.String() {
		t.Fatalf("result = %#v", result)
	}
	if result.Diagnostics.PlannerVersion != "qrx10" || result.Diagnostics.ExplainOnly || result.Diagnostics.LegacyBridge || result.Diagnostics.RowsReturned != 1 {
		t.Fatalf("diagnostics = %#v", result.Diagnostics)
	}
	if entities.calls != 0 {
		t.Fatalf("entity reads during neutral shaping = %d, want 0", entities.calls)
	}

	_, err = engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: sessionmodel.GraphTransaction{ID: "different"}, LegacyRows: []queryexecution.BindingRow{}}, prepared)
	assertQueryErrorCode(t, err, querylogical.ErrorInvalidQuery)
}

func TestEngineExecuteLegacyBindingsEnforcesWorkBudget(t *testing.T) {
	engine := queryengine.New(queryengine.Ports{Entities: &recordingEntityReader{}})
	tx := sessionmodel.GraphTransaction{ID: "tx-budget", Mode: sessionmodel.TransactionModeReadOnly, State: sessionmodel.TransactionStateActive}
	statement := validReadStatement()
	statement.Read.Returns = []querylogical.Projection{{Kind: querylogical.ProjectionNode, Alias: "n"}}
	prepared, err := engine.Prepare(context.Background(), broadPlanningContext(tx), statement)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	rows := []queryexecution.BindingRow{{Nodes: map[string]graph.Node{"n": {}}}, {Nodes: map[string]graph.Node{"n": {}}}}
	_, err = engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx, LegacyRows: rows, Budget: queryengine.WorkBudget{MaxCandidates: 1}}, prepared)
	assertQueryErrorCode(t, err, querylogical.ErrorResourceExhausted)
}

func TestEngineExecuteRunsNativeBroadScanThroughEntityPort(t *testing.T) {
	entities := &recordingEntityReader{}
	engine := queryengine.New(queryengine.Ports{Entities: entities})
	tx := sessionmodel.GraphTransaction{ID: "tx-broad", BaseRevision: 17}
	prepared, err := engine.Prepare(context.Background(), broadPlanningContext(tx), validReadStatement())
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	result, err := engine.Execute(context.Background(), queryengine.ExecutionContext{Transaction: tx}, prepared)
	if err != nil {
		t.Fatalf("execute broad scan: %v", err)
	}
	if len(result.Rows) != 0 || entities.calls != 1 || entities.tx != tx {
		t.Fatalf("result=%#v calls=%d tx=%#v", result, entities.calls, entities.tx)
	}
}

func TestEngineFacadeMapsCanceledContextsToTypedErrors(t *testing.T) {
	engine := queryengine.New(queryengine.Ports{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := engine.Prepare(ctx, queryengine.PlanningContext{}, validReadStatement())
	assertQueryErrorCode(t, err, querylogical.ErrorCanceled)
	_, err = engine.Explain(ctx, queryengine.PlanningContext{}, validReadStatement())
	assertQueryErrorCode(t, err, querylogical.ErrorCanceled)
	_, err = engine.Execute(ctx, queryengine.ExecutionContext{}, queryengine.PreparedQuery{})
	assertQueryErrorCode(t, err, querylogical.ErrorCanceled)
}

func broadPlanningContext(tx sessionmodel.GraphTransaction) queryengine.PlanningContext {
	return queryengine.PlanningContext{Transaction: tx, Domain: queryengine.DomainPolicy{BroadSearchable: true}}
}

func validReadStatement() querylogical.Statement {
	return querylogical.ReadStatement(querylogical.Query{
		Source:     querylogical.SourceStructured,
		AccessMode: querylogical.AccessModeReadOnly,
		Start:      querylogical.NodePattern{Alias: "n", Labels: []string{"Document"}},
	})
}

func operatorTreeContains(operator queryphysical.Operator, kind queryphysical.OperatorKind) bool {
	if operator.Kind == kind {
		return true
	}
	for _, input := range operator.Inputs {
		if operatorTreeContains(input, kind) {
			return true
		}
	}
	return false
}

func assertQueryErrorCode(t *testing.T, err error, code querylogical.ErrorCode) {
	t.Helper()
	var queryErr *querylogical.QueryError
	if !errors.As(err, &queryErr) || queryErr.Code != code {
		t.Fatalf("error = %#v, want query code %q", err, code)
	}
}
