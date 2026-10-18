package binding_test

import (
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/myceldb/mycel/internal/query/binding"
	"github.com/myceldb/mycel/internal/query/gql"
	querylogical "github.com/myceldb/mycel/internal/query/logical"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
)

func TestBindReadValidatesAliasesSchemaExpressionsAndOutputs(t *testing.T) {
	query := validReadQuery(querylogical.SourceStructured)
	bound, err := binding.Bind(querylogical.ReadStatement(query), readEnvironment())
	if err != nil {
		t.Fatalf("bind read: %v", err)
	}
	wantVariables := []struct {
		name string
		kind binding.VariableKind
	}{{"p", binding.VariableNode}, {"path", binding.VariablePath}, {"e", binding.VariableEdge}, {"c", binding.VariableNode}}
	if len(bound.Variables) != len(wantVariables) {
		t.Fatalf("variables = %#v", bound.Variables)
	}
	for i, want := range wantVariables {
		if bound.Variables[i].Name != want.name || bound.Variables[i].Kind != want.kind {
			t.Fatalf("variable %d = %#v, want %q/%q", i, bound.Variables[i], want.name, want.kind)
		}
	}
	if bound.SchemaVersion != "v1" {
		t.Fatalf("schema version = %q, want v1", bound.SchemaVersion)
	}
}

func TestBindAcceptsLegacyBareScalarNodeProjection(t *testing.T) {
	query := validReadQuery(querylogical.SourceStructured)
	field := querylogical.FieldRef{Alias: "p", Namespace: querylogical.NamespaceProperties}
	query.Returns = append(query.Returns, querylogical.Projection{Kind: querylogical.ProjectionScalar, Alias: "p", Field: &field, OutputName: "bare"})
	if _, err := binding.Bind(querylogical.ReadStatement(query), readEnvironment()); err != nil {
		t.Fatalf("Bind(bare scalar): %v", err)
	}
}

func TestBindEquivalentInvalidReadsUseSameTypedCategory(t *testing.T) {
	for _, source := range []querylogical.Source{querylogical.SourceStructured, querylogical.SourceGQL} {
		t.Run(string(source), func(t *testing.T) {
			query := validReadQuery(source)
			query.Predicate = querylogical.LeafPredicate(querylogical.Leaf{
				Kind:  querylogical.LeafPropertyExists,
				Field: &querylogical.FieldRef{Alias: "missing", Namespace: querylogical.NamespaceProperties, Name: "name"},
			})
			_, err := binding.Bind(querylogical.ReadStatement(query), readEnvironment())
			assertQueryError(t, err, querylogical.ErrorInvalidQuery, `field alias "missing" is not defined`)
		})
	}
}

func TestBindRejectsInvalidReadSemantics(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*querylogical.Query)
		code   querylogical.ErrorCode
		want   string
	}{
		{name: "duplicate alias", mutate: func(q *querylogical.Query) { q.Steps[0].Target.Alias = "p" }, code: querylogical.ErrorInvalidQuery, want: "duplicate variable alias"},
		{name: "unknown node label", mutate: func(q *querylogical.Query) { q.Start.Labels = []string{"Missing"} }, code: querylogical.ErrorInvalidQuery, want: "unknown node label"},
		{name: "unknown payload field", mutate: func(q *querylogical.Query) { q.Returns[2].Field.Name = "missing" }, code: querylogical.ErrorInvalidQuery, want: "unknown payload field"},
		{name: "endpoint constraint", mutate: func(q *querylogical.Query) {
			q.Start.Labels, q.Steps[0].Target.Labels = []string{"Company"}, []string{"Person"}
		}, code: querylogical.ErrorInvalidQuery, want: "endpoint constraints"},
		{name: "projection kind", mutate: func(q *querylogical.Query) { q.Returns[0].Kind = querylogical.ProjectionEdge }, code: querylogical.ErrorInvalidQuery, want: "is not an edge"},
		{name: "aggregate star", mutate: func(q *querylogical.Query) { q.Aggregates[0].Function = querylogical.AggregateSum }, code: querylogical.ErrorInvalidQuery, want: "only COUNT supports star"},
		{name: "aggregate binding", mutate: func(q *querylogical.Query) {
			argument := querylogical.BindingValue("p")
			q.Aggregates[0] = querylogical.Aggregate{Function: querylogical.AggregateMin, Argument: &argument, OutputName: "minimum"}
		}, code: querylogical.ErrorInvalidQuery, want: "requires a value argument"},
		{name: "order alias", mutate: func(q *querylogical.Query) { q.OrderBy[0].Value = querylogical.FieldValue("missing", "name") }, code: querylogical.ErrorInvalidQuery, want: "not defined"},
		{name: "traversal direction", mutate: func(q *querylogical.Query) { q.Steps[0].Edge.Direction = querylogical.DirectionUnspecified }, code: querylogical.ErrorInvalidQuery, want: "traversal direction is required"},
		{name: "traversal cap", mutate: func(q *querylogical.Query) { q.Steps[0].Depth.Max = 65 }, code: querylogical.ErrorInvalidQuery, want: "must not exceed 64"},
		{name: "non finite", mutate: func(q *querylogical.Query) {
			q.Start.Properties = map[string]querylogical.Value{"age": {Kind: querylogical.ValueNumber, Number: math.Inf(1)}}
		}, code: querylogical.ErrorInvalidQuery, want: "must be finite"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			query := validReadQuery(querylogical.SourceStructured)
			test.mutate(&query)
			_, err := binding.Bind(querylogical.ReadStatement(query), readEnvironment())
			assertQueryError(t, err, test.code, test.want)
		})
	}
}

func TestBindValidatesParametersAndPredicateCapabilities(t *testing.T) {
	query := querylogical.Query{
		Source:     querylogical.SourceStructured,
		AccessMode: querylogical.AccessModeReadOnly,
		Start:      querylogical.NodePattern{Alias: "p", Labels: []string{"Person"}},
	}
	parameter := querylogical.Value{Kind: querylogical.ValueParameter, Parameter: "name"}
	field := querylogical.FieldValue("p", "name")
	query.Predicate = querylogical.LeafPredicate(querylogical.Leaf{Kind: querylogical.LeafComparison, Left: &field, Right: &parameter, Operator: "="})
	environment := readEnvironment()
	environment.Parameters = map[string]querylogical.Value{"name": {Kind: querylogical.ValueString, String: "Ada"}}
	if _, err := binding.Bind(querylogical.ReadStatement(query), environment); err != nil {
		t.Fatalf("bind parameter: %v", err)
	}
	delete(environment.Parameters, "name")
	_, err := binding.Bind(querylogical.ReadStatement(query), environment)
	assertQueryError(t, err, querylogical.ErrorInvalidQuery, "missing parameter")
	environment.Parameters = map[string]querylogical.Value{
		"name":  {Kind: querylogical.ValueParameter, Parameter: "other"},
		"other": {Kind: querylogical.ValueParameter, Parameter: "name"},
	}
	_, err = binding.Bind(querylogical.ReadStatement(query), environment)
	assertQueryError(t, err, querylogical.ErrorInvalidQuery, "resolves recursively")

	semanticField := querylogical.FieldRef{Alias: "p", Namespace: querylogical.NamespaceProperties}
	query.Predicate = querylogical.LeafPredicate(querylogical.Leaf{Kind: querylogical.LeafSemantic, Field: &semanticField, Query: "graph databases", Limit: 5})
	environment = readEnvironment()
	environment.Domain.SemanticSearchable = false
	_, err = binding.Bind(querylogical.ReadStatement(query), environment)
	assertQueryError(t, err, querylogical.ErrorUnavailableAccessPath, "semantic candidate access is unavailable")
}

func TestBindRejectsReadWriteAccessAndReadOnlyDomains(t *testing.T) {
	mutation := querylogical.MutationStatement(querylogical.SourceGQL, querylogical.Mutation{
		Kind: querylogical.MutationInsertNode,
		Node: &querylogical.NodePattern{Labels: []string{"Person"}, Properties: map[string]querylogical.Value{"name": {Kind: querylogical.ValueString, String: "Ada"}}},
	})
	environment := readEnvironment()
	environment.Transaction.Mode = sessionmodel.TransactionModeReadOnly
	_, err := binding.Bind(mutation, environment)
	assertQueryError(t, err, querylogical.ErrorInvalidQuery, "requires a read-write transaction")

	environment.Transaction.Mode = sessionmodel.TransactionModeReadWrite
	environment.Domain.ReadOnly = true
	_, err = binding.Bind(mutation, environment)
	assertQueryError(t, err, querylogical.ErrorUnavailableAccessPath, "domain is read-only")
}

func TestBindRejectsNegativeMutationPagination(t *testing.T) {
	statement := querylogical.MutationStatement(querylogical.SourceGQL, querylogical.Mutation{
		Kind: querylogical.MutationInsertNode, Node: &querylogical.NodePattern{Labels: []string{"Person"}},
		Page: querylogical.PageSpec{Size: -1},
	})
	_, err := binding.Bind(statement, writeEnvironment())
	assertQueryError(t, err, querylogical.ErrorInvalidQuery, "mutation pagination values must be non-negative")
}

func TestBindRejectsMergeRelationshipWithoutEndpointIdentifiers(t *testing.T) {
	statement := querylogical.MutationStatement(querylogical.SourceGQL, querylogical.Mutation{
		Kind: querylogical.MutationMergeRelationship,
		Nodes: []querylogical.NodePattern{
			{Alias: "a", Labels: []string{"Person"}, Properties: map[string]querylogical.Value{"name": {Kind: querylogical.ValueString, String: "Ada"}}},
			{Alias: "b", Labels: []string{"Person"}},
		},
		Relationship: &querylogical.RelationshipMutation{FromAlias: "a", ToAlias: "b", Labels: []string{"KNOWS"}},
	})
	_, err := binding.Bind(statement, writeEnvironment())
	assertQueryError(t, err, querylogical.ErrorInvalidQuery, `merge relationship match for "b" requires at least one identifying property`)
}

func TestBindDefersBroadSearchPolicyToPhysicalPlanning(t *testing.T) {
	query := validReadQuery(querylogical.SourceStructured)
	environment := readEnvironment()
	environment.Domain.BroadSearchable = false
	if _, err := binding.Bind(querylogical.ReadStatement(query), environment); err != nil {
		t.Fatalf("bind before access-path classification: %v", err)
	}
}

func TestBindGQLMutationStatementFamilies(t *testing.T) {
	tests := []struct {
		name   string
		gql    string
		kind   querylogical.MutationKind
		params map[string]any
	}{
		{name: "insert", gql: "INSERT (:Person {name: 'Ada', age: 37})", kind: querylogical.MutationInsertNode},
		{name: "merge node", gql: "MERGE (p:Person {name: 'Ada'}) RETURN p", kind: querylogical.MutationMergeNode},
		{name: "match set", gql: "MATCH (p:Person {name: 'Ada'}) SET p.age = 38 RETURN p, count(*) AS total", kind: querylogical.MutationMatchSet},
		{name: "match delete", gql: "MATCH (a:Person)-[r:KNOWS]->(b:Person) DELETE r RETURN a, b", kind: querylogical.MutationMatchDelete},
		{name: "create relationship", gql: "MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)", kind: querylogical.MutationCreateRelationship},
		{name: "merge relationship", gql: "MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}) MERGE (a)-[r:KNOWS]->(b) RETURN a, r, b", kind: querylogical.MutationMergeRelationship},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan, err := gql.CompileWithParams(test.gql, test.params)
			if err != nil {
				t.Fatalf("compile GQL: %v", err)
			}
			statement, ok := querylogical.StatementFromGQLPlan(plan)
			if !ok || statement.Mutation == nil {
				t.Fatalf("mutation statement = %#v, ok %v", statement, ok)
			}
			if statement.Mutation.Kind != test.kind {
				t.Fatalf("mutation kind = %q, want %q", statement.Mutation.Kind, test.kind)
			}
			if _, err := binding.Bind(statement, writeEnvironment()); err != nil {
				t.Fatalf("bind mutation: %v\nstatement: %#v", err, statement)
			}
		})
	}
}

func validReadQuery(source querylogical.Source) querylogical.Query {
	name := querylogical.FieldValue("p", "name")
	literal := querylogical.Value{Kind: querylogical.ValueString, String: "Ada"}
	companyName := querylogical.FieldRef{Alias: "c", Namespace: querylogical.NamespacePayload, Name: "summary"}
	return querylogical.Query{
		Source:     source,
		AccessMode: querylogical.AccessModeReadOnly,
		Start:      querylogical.NodePattern{Alias: "p", Labels: []string{"Person"}},
		PathAlias:  "path",
		Steps: []querylogical.Step{{
			Edge:   querylogical.EdgePattern{Alias: "e", Labels: []string{"WORKS_AT"}, Direction: querylogical.DirectionOutgoing},
			Depth:  querylogical.DepthBounds{Min: 1, Max: 1},
			Target: querylogical.NodePattern{Alias: "c", Labels: []string{"Company"}},
		}},
		Predicate: querylogical.LeafPredicate(querylogical.Leaf{Kind: querylogical.LeafComparison, Left: &name, Right: &literal, Operator: "="}),
		Returns: []querylogical.Projection{
			{Kind: querylogical.ProjectionNode, Alias: "p", OutputName: "person"},
			{Kind: querylogical.ProjectionEdge, Alias: "e", OutputName: "employment"},
			{Kind: querylogical.ProjectionScalar, Alias: "c", Field: &companyName, OutputName: "summary"},
			{Kind: querylogical.ProjectionPath, Alias: "path", OutputName: "path"},
		},
		Aggregates: []querylogical.Aggregate{{Function: querylogical.AggregateCount, Star: true, OutputName: "total"}},
		OrderBy:    []querylogical.Order{{Value: querylogical.FieldValue("c", "name"), Direction: querylogical.SortAscending}},
		Page:       querylogical.PageSpec{Size: 25, Limit: 100},
		Safety:     querylogical.SafetyLimits{MaxNodes: 1000, MaxEdges: 2000},
	}
}

func readEnvironment() binding.Environment {
	schema := strictBindingSchema()
	return binding.Environment{
		Transaction: sessionmodel.GraphTransaction{Mode: sessionmodel.TransactionModeReadOnly, State: sessionmodel.TransactionStateActive},
		Schema:      &schema,
		Domain: binding.DomainPolicy{
			BroadSearchable: true, ExplicitSearchable: true,
			SemanticSearchable: true, SemanticCandidatesAvailable: true,
		},
	}
}

func writeEnvironment() binding.Environment {
	environment := readEnvironment()
	environment.Transaction.Mode = sessionmodel.TransactionModeReadWrite
	return environment
}

func strictBindingSchema() schemamodel.DomainSchema {
	return schemamodel.DomainSchema{
		Version: "v1", Mode: schemamodel.SchemaModeStrict,
		NodeTypes: []schemamodel.NodeType{
			{Name: "person", Labels: []string{"Person"}, Properties: []schemamodel.FieldSpec{{Name: "name", Type: schemamodel.FieldTypeString}, {Name: "age", Type: schemamodel.FieldTypeInt}}, Payload: []schemamodel.FieldSpec{{Name: "bio", Type: schemamodel.FieldTypeString}}, Meta: []schemamodel.FieldSpec{{Name: "source", Type: schemamodel.FieldTypeString}}},
			{Name: "company", Labels: []string{"Company"}, Properties: []schemamodel.FieldSpec{{Name: "name", Type: schemamodel.FieldTypeString}}, Payload: []schemamodel.FieldSpec{{Name: "summary", Type: schemamodel.FieldTypeString}}},
		},
		EdgeTypes: []schemamodel.EdgeType{
			{Name: "works_at", Labels: []string{"WORKS_AT"}, From: schemamodel.EndpointSpec{NodeTypes: []string{"person"}}, To: schemamodel.EndpointSpec{NodeTypes: []string{"company"}}, Properties: []schemamodel.FieldSpec{{Name: "since", Type: schemamodel.FieldTypeInt}}},
			{Name: "knows", Labels: []string{"KNOWS"}, From: schemamodel.EndpointSpec{NodeTypes: []string{"person"}}, To: schemamodel.EndpointSpec{NodeTypes: []string{"person"}}},
		},
	}
}

func assertQueryError(t *testing.T, err error, code querylogical.ErrorCode, message string) {
	t.Helper()
	if err == nil {
		t.Fatalf("error = nil, want %q", message)
	}
	var queryErr *querylogical.QueryError
	if !errors.As(err, &queryErr) || queryErr.Code != code {
		t.Fatalf("error = %#v, want query code %q", err, code)
	}
	if !strings.Contains(err.Error(), message) {
		t.Fatalf("error = %q, want substring %q", err, message)
	}
}
