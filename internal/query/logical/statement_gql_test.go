package logical_test

import (
	"reflect"
	"testing"

	"github.com/myceldb/mycel/internal/query/gql"
	querylogical "github.com/myceldb/mycel/internal/query/logical"
)

func TestStatementFromGQLPlanWrapsReadNormalizationWithoutDrift(t *testing.T) {
	plan, err := gql.Compile("MATCH (p:Person) WHERE p.name = 'Ada' RETURN p.name FETCH FIRST 5 ROWS ONLY")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	query, queryOK := querylogical.FromGQLPlan(plan)
	statement, statementOK := querylogical.StatementFromGQLPlan(plan)
	if !queryOK || !statementOK || statement.Read == nil {
		t.Fatalf("query ok %v, statement ok %v, statement %#v", queryOK, statementOK, statement)
	}
	if !reflect.DeepEqual(*statement.Read, query) {
		t.Fatalf("statement read = %#v, want %#v", *statement.Read, query)
	}
	if statement.Kind != querylogical.StatementRead || statement.AccessMode != querylogical.AccessModeReadOnly {
		t.Fatalf("statement = %#v", statement)
	}
}

func TestStatementFromGQLPlanPreservesResolvedMutationValues(t *testing.T) {
	plan, err := gql.CompileWithParams(
		"MATCH (p:Person {name: $name}) SET p.payload.bio = $bio RETURN p, p.payload.bio",
		map[string]any{"name": "Ada", "bio": "database pioneer"},
	)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	statement, ok := querylogical.StatementFromGQLPlan(plan, querylogical.NormalizeOptions{PageSize: 17, Cursor: "4"})
	if !ok || statement.Mutation == nil {
		t.Fatalf("statement = %#v, ok %v", statement, ok)
	}
	mutation := statement.Mutation
	if mutation.Kind != querylogical.MutationMatchSet || mutation.Match == nil {
		t.Fatalf("mutation = %#v", mutation)
	}
	if len(mutation.Assignments) != 1 {
		t.Fatalf("assignments = %#v", mutation.Assignments)
	}
	assignment := mutation.Assignments[0]
	if assignment.Field.Alias != "p" || assignment.Field.Namespace != querylogical.NamespacePayload || assignment.Field.Name != "bio" {
		t.Fatalf("assignment field = %#v", assignment.Field)
	}
	if assignment.Value.Kind != querylogical.ValueString || assignment.Value.String != "database pioneer" {
		t.Fatalf("assignment value = %#v", assignment.Value)
	}
	if mutation.Match.Predicate == nil || len(mutation.Returns) != 2 {
		t.Fatalf("mutation = %#v", mutation)
	}
	if mutation.Page.Size != 17 || mutation.Page.Cursor != "4" {
		t.Fatalf("mutation page = %#v", mutation.Page)
	}
}

func TestStatementFromGQLPlanPreservesMutationAggregates(t *testing.T) {
	plan, err := gql.Compile("MATCH (p:Person {name: 'Ada'}) SET p.age = 38 RETURN count(*) AS total")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	statement, ok := querylogical.StatementFromGQLPlan(plan)
	if !ok || statement.Mutation == nil {
		t.Fatalf("statement = %#v, ok %v", statement, ok)
	}
	mutation := statement.Mutation
	if len(mutation.Returns) != 0 || len(mutation.Aggregates) != 1 {
		t.Fatalf("mutation outputs = returns %#v aggregates %#v", mutation.Returns, mutation.Aggregates)
	}
	aggregate := mutation.Aggregates[0]
	if aggregate.Function != querylogical.AggregateCount || !aggregate.Star || aggregate.OutputName != "total" {
		t.Fatalf("aggregate = %#v", aggregate)
	}
}

func TestStatementFromGQLPlanPreservesRelationshipMutationBindings(t *testing.T) {
	plan, err := gql.Compile("MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}) MERGE (a)-[r:KNOWS {since: 2020}]->(b) RETURN a, r, b")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	statement, ok := querylogical.StatementFromGQLPlan(plan)
	if !ok || statement.Mutation == nil || statement.Mutation.Relationship == nil {
		t.Fatalf("statement = %#v, ok %v", statement, ok)
	}
	mutation := statement.Mutation
	if mutation.Kind != querylogical.MutationMergeRelationship || len(mutation.Nodes) != 2 {
		t.Fatalf("mutation = %#v", mutation)
	}
	relationship := mutation.Relationship
	if relationship.Alias != "r" || relationship.FromAlias != "a" || relationship.ToAlias != "b" || len(relationship.Labels) != 1 || relationship.Labels[0] != "KNOWS" {
		t.Fatalf("relationship = %#v", relationship)
	}
	if relationship.Properties["since"].Kind != querylogical.ValueNumber || relationship.Properties["since"].Number != 2020 {
		t.Fatalf("relationship properties = %#v", relationship.Properties)
	}
}
