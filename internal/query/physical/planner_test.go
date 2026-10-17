package physical_test

import (
	"errors"
	"testing"

	querybinding "github.com/myceldb/mycel/internal/query/binding"
	querylogical "github.com/myceldb/mycel/internal/query/logical"
	queryphysical "github.com/myceldb/mycel/internal/query/physical"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
)

func TestBuildSelectsLegacyAccessFamiliesInPrecedenceOrder(t *testing.T) {
	path := baseQuery()
	path.Start.NodeIDs = []string{"node-1"}
	path.PathAlias = "path"
	path.Steps = []querylogical.Step{traversalStep("edge")}

	adjacency := baseQuery()
	adjacency.Start.NodeIDs = []string{"node-1"}
	adjacency.Steps = []querylogical.Step{traversalStep("edge")}

	equality := baseQuery()
	equality.Predicate = comparison("n", "name", "=", stringValue("Ada"))

	semantic := baseQuery()
	semanticField := querylogical.FieldRef{Alias: "n", Namespace: querylogical.NamespaceProperties}
	semantic.Predicate = querylogical.LeafPredicate(querylogical.Leaf{Kind: querylogical.LeafSemantic, Field: &semanticField, Query: "graph", Limit: 10})

	compound := baseQuery()
	compound.Predicate = querylogical.CombinePredicate(querylogical.PredicateAndOp, []querylogical.Predicate{
		*comparison("n", "age", ">=", integerValue(21)),
		*comparison("n", "name", "=", stringValue("Ada")),
	})

	ordered := baseQuery()
	ordered.OrderBy = []querylogical.Order{{Value: querylogical.FieldValue("n", "name"), Direction: querylogical.SortAscending}}

	subtree := ordered
	subtree.Steps = []querylogical.Step{traversalStep("")}

	full := baseQuery()
	field := querylogical.FieldRef{Alias: "n", Namespace: querylogical.NamespaceProperties, Name: "name"}
	full.Predicate = querylogical.LeafPredicate(querylogical.Leaf{Kind: querylogical.LeafNull, Field: &field, IsNull: true})

	tests := []struct {
		name       string
		query      querylogical.Query
		family     queryphysical.Family
		operator   queryphysical.OperatorKind
		cursorKind string
	}{
		{name: "path before adjacency", query: path, family: queryphysical.FamilyIndexedPath, operator: queryphysical.OperatorIndexedPath, cursorKind: "offset"},
		{name: "adjacency", query: adjacency, family: queryphysical.FamilyIndexedAdjacency, operator: queryphysical.OperatorAdjacency, cursorKind: "adjacency_key"},
		{name: "equality before compound", query: equality, family: queryphysical.FamilyIndexedEquality, operator: queryphysical.OperatorOrderedIndex, cursorKind: "index_key"},
		{name: "semantic", query: semantic, family: queryphysical.FamilySemantic, operator: queryphysical.OperatorSemanticCandidates, cursorKind: "offset"},
		{name: "compound", query: compound, family: queryphysical.FamilyIndexedPredicate, operator: queryphysical.OperatorCompoundIndex, cursorKind: "offset"},
		{name: "ordered", query: ordered, family: queryphysical.FamilyIndexedOrder, operator: queryphysical.OperatorOrderedIndex, cursorKind: "index_key"},
		{name: "subtree", query: subtree, family: queryphysical.FamilyIndexedSubtree, operator: queryphysical.OperatorSubtree, cursorKind: "root_index_key"},
		{name: "full", query: full, family: queryphysical.FamilyBoundedFullScan, operator: queryphysical.OperatorFullGraph, cursorKind: "offset"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan := buildRead(t, test.query, plannerSchema(), plannerEnvironment())
			if plan.Family != test.family || plan.Cursor.Kind != test.cursorKind {
				t.Fatalf("plan = %#v, want family %q cursor %q", plan, test.family, test.cursorKind)
			}
			if !plan.RequiresLegacy() || plan.Root.Kind != queryphysical.OperatorLegacyBridge {
				t.Fatalf("root = %#v, want legacy bridge", plan.Root)
			}
			if !operatorTreeContains(plan.Root, test.operator) {
				t.Fatalf("operator tree %#v does not contain %q", plan.Root, test.operator)
			}
		})
	}
}

func TestBuildAcceptsOptionalOneHopAdjacencyAndBoundsWork(t *testing.T) {
	query := baseQuery()
	query.Start.NodeIDs = []string{"node-1", "node-2"}
	step := traversalStep("edge")
	step.Depth = querylogical.DepthBounds{Min: 0, Max: 1}
	query.Steps = []querylogical.Step{step}

	plan := buildRead(t, query, plannerSchema(), plannerEnvironment())
	if plan.Family != queryphysical.FamilyIndexedAdjacency || plan.Access.Steps[0].MinDepth != 0 {
		t.Fatalf("plan = %#v", plan)
	}
	if plan.Limits.MaxStartNodes == 0 || plan.Limits.MaxNodesLoaded == 0 || plan.Limits.MaxEdgesLoaded == 0 {
		t.Fatalf("adjacency limits = %#v", plan.Limits)
	}
	query.Page.Size = 1
	withSmallPage := buildRead(t, query, plannerSchema(), plannerEnvironment())
	if plan.Limits != withSmallPage.Limits {
		t.Fatalf("limits changed with page size: %#v != %#v", plan.Limits, withSmallPage.Limits)
	}
}

func TestBuildSelectsIndexedPathStartAccess(t *testing.T) {
	label := baseQuery()
	label.PathAlias = "path"
	label.Steps = []querylogical.Step{traversalStep("edge")}

	tag := label
	tagField := querylogical.FieldRef{Alias: "n", Namespace: querylogical.NamespaceProperties, Name: "tags"}
	tag.Predicate = querylogical.LeafPredicate(querylogical.Leaf{Kind: querylogical.LeafHasTag, Field: &tagField, Tag: "featured"})

	predicate := label
	predicate.Predicate = comparison("n", "name", "=", stringValue("Ada"))

	for _, test := range []struct {
		name   string
		query  querylogical.Query
		access queryphysical.AccessKind
		index  string
	}{
		{name: "label", query: label, access: queryphysical.AccessLabel, index: "label:Document"},
		{name: "tag", query: tag, access: queryphysical.AccessTag, index: "tag:featured"},
		{name: "ordered predicate", query: predicate, access: queryphysical.AccessCompoundProperty, index: "document_name"},
	} {
		t.Run(test.name, func(t *testing.T) {
			plan := buildRead(t, test.query, plannerSchema(), plannerEnvironment())
			if plan.Family != queryphysical.FamilyIndexedPath || plan.Access.Kind != test.access {
				t.Fatalf("plan = %#v", plan)
			}
			found := false
			for _, index := range plan.Indexes {
				found = found || index.Name == test.index
			}
			if !found {
				t.Fatalf("indexes = %#v, want %q", plan.Indexes, test.index)
			}
		})
	}
}

func TestBuildRecordsIndexesPredicatesOrderingAndBounds(t *testing.T) {
	query := baseQuery()
	query.Predicate = querylogical.CombinePredicate(querylogical.PredicateAndOp, []querylogical.Predicate{
		*comparison("n", "age", ">=", integerValue(21)),
		*querylogical.LeafPredicate(querylogical.Leaf{Kind: querylogical.LeafNull, Field: fieldRef("n", "name"), IsNull: false}),
	})
	plan := buildRead(t, query, plannerSchema(), plannerEnvironment())
	if plan.Family != queryphysical.FamilyIndexedPredicate || len(plan.Access.Branches) != 1 || len(plan.Access.Branches[0].Scans) != 1 {
		t.Fatalf("compound plan = %#v", plan)
	}
	if plan.PushedPredicate == nil || plan.ResidualPredicate == nil {
		t.Fatalf("predicate placement = pushed %#v residual %#v", plan.PushedPredicate, plan.ResidualPredicate)
	}
	if len(plan.Indexes) != 1 || plan.Indexes[0].Name != "document_age" {
		t.Fatalf("indexes = %#v", plan.Indexes)
	}

	ordered := baseQuery()
	ordered.OrderBy = []querylogical.Order{{Value: querylogical.FieldValue("n", "age"), Direction: querylogical.SortDescending}}
	ordered.Predicate = querylogical.CombinePredicate(querylogical.PredicateAndOp, []querylogical.Predicate{
		*comparison("n", "age", ">=", integerValue(18)),
		*comparison("n", "age", "<=", integerValue(65)),
	})
	orderedPlan := buildRead(t, ordered, plannerSchema(), plannerEnvironment())
	if orderedPlan.Access.Low == nil || orderedPlan.Access.High == nil || !orderedPlan.Access.Low.Inclusive || !orderedPlan.Access.High.Inclusive {
		t.Fatalf("ordered bounds = low %#v high %#v", orderedPlan.Access.Low, orderedPlan.Access.High)
	}
	if !orderedPlan.Ordering.Stable || orderedPlan.Ordering.Source != "document_age" || len(orderedPlan.Ordering.Terms) != 1 {
		t.Fatalf("ordering = %#v", orderedPlan.Ordering)
	}

	semantic := baseQuery()
	semanticField := querylogical.FieldRef{Alias: "n", Namespace: querylogical.NamespaceProperties}
	nameField := querylogical.FieldRef{Alias: "n", Namespace: querylogical.NamespaceProperties, Name: "name"}
	semantic.Predicate = querylogical.CombinePredicate(querylogical.PredicateAndOp, []querylogical.Predicate{
		*querylogical.LeafPredicate(querylogical.Leaf{Kind: querylogical.LeafSemantic, Field: &semanticField, Query: "graph", Limit: 7}),
		*querylogical.LeafPredicate(querylogical.Leaf{Kind: querylogical.LeafNull, Field: &nameField, IsNull: false}),
	})
	semanticPlan := buildRead(t, semantic, plannerSchema(), plannerEnvironment())
	if semanticPlan.Family != queryphysical.FamilySemantic || semanticPlan.PushedPredicate == nil || semanticPlan.ResidualPredicate == nil || semanticPlan.Limits.MaxSemanticCandidates != 7 {
		t.Fatalf("semantic plan = %#v", semanticPlan)
	}
}

func TestBuildRecordsBoundedPathAndFullScanLimits(t *testing.T) {
	path := baseQuery()
	path.Start.NodeIDs = []string{"node-1"}
	path.PathAlias = "path"
	step := traversalStep("edge")
	step.Depth = querylogical.DepthBounds{Min: 1, Max: 3}
	path.Steps = []querylogical.Step{step}
	plan := buildRead(t, path, plannerSchema(), plannerEnvironment())
	if plan.Limits.MaxStartNodes != 1000 || plan.Limits.MaxIntermediateRows != 10000 || plan.Limits.MaxNodesLoaded != 50000 || plan.Limits.MaxEdgesLoaded != 50000 || plan.Limits.MaxTraversalDepth != 3 {
		t.Fatalf("path limits = %#v", plan.Limits)
	}

	full := baseQuery()
	full.Safety = querylogical.SafetyLimits{MaxNodes: 123, MaxEdges: 456}
	plan = buildRead(t, full, plannerSchema(), plannerEnvironment())
	if !plan.FullScan || plan.Limits.MaxNodesLoaded != 123 || plan.Limits.MaxEdgesLoaded != 456 {
		t.Fatalf("full scan = %#v", plan)
	}
}

func TestBuildGQLRangeUsesBroadFallbackAndPolicy(t *testing.T) {
	query := baseQuery()
	query.Source = querylogical.SourceGQL
	query.Predicate = querylogical.CombinePredicate(querylogical.PredicateAndOp, []querylogical.Predicate{
		*comparison("n", "name", "=", stringValue("Ada")),
		*comparison("n", "age", ">", integerValue(18)),
	})
	ordered := query
	ordered.OrderBy = []querylogical.Order{{Value: querylogical.FieldValue("n", "age"), Direction: querylogical.SortAscending}}

	for _, test := range []struct {
		name  string
		query querylogical.Query
	}{{name: "unordered", query: query}, {name: "ordered node", query: ordered}} {
		t.Run(test.name, func(t *testing.T) {
			plan := buildRead(t, test.query, plannerSchema(), plannerEnvironment())
			if plan.Family != queryphysical.FamilyBoundedFullScan {
				t.Fatalf("plan = %#v", plan)
			}
			environment := plannerEnvironment()
			environment.Domain.BroadSearchable = false
			assertPlanError(t, test.query, plannerSchema(), environment, querylogical.ErrorUnavailableAccessPath)
		})
	}
}

func TestBuildGQLOrderedStringFilterRetainsLegacyIndexFamilyAsResidual(t *testing.T) {
	query := baseQuery()
	query.Source = querylogical.SourceGQL
	query.OrderBy = []querylogical.Order{{Value: querylogical.FieldValue("n", "age"), Direction: querylogical.SortAscending}}
	name := querylogical.FieldValue("n", "name")
	query.Predicate = querylogical.LeafPredicate(querylogical.Leaf{Kind: querylogical.LeafString, Operator: "contains", Operand: &name, Query: "Ada"})

	plan := buildRead(t, query, plannerSchema(), plannerEnvironment())
	if plan.Family != queryphysical.FamilyIndexedOrder || plan.PushedPredicate != nil || plan.ResidualPredicate == nil {
		t.Fatalf("plan = %#v", plan)
	}
}

func TestBuildMalformedPathPredicatesUseBroadFallback(t *testing.T) {
	base := baseQuery()
	base.PathAlias = "path"
	base.Steps = []querylogical.Step{traversalStep("edge")}
	left := stringValue("Ada")
	right := querylogical.FieldValue("n", "name")
	reversedLess := querylogical.LeafPredicate(querylogical.Leaf{Kind: querylogical.LeafComparison, Operator: "<", Left: &left, Right: &right})
	targetField := querylogical.FieldValue("m", "name")
	targetString := querylogical.LeafPredicate(querylogical.Leaf{Kind: querylogical.LeafString, Operator: "contains", Operand: &targetField, Query: "Ada"})

	for _, test := range []struct {
		name      string
		predicate *querylogical.Predicate
	}{{name: "reversed less-than", predicate: reversedLess}, {name: "target alias string", predicate: targetString}} {
		t.Run(test.name, func(t *testing.T) {
			query := base
			query.Predicate = test.predicate
			plan := buildRead(t, query, plannerSchema(), plannerEnvironment())
			if plan.Family != queryphysical.FamilyBoundedFullScan {
				t.Fatalf("plan = %#v", plan)
			}
		})
	}
}

func TestBuildRejectsUnavailableOrInvalidAccessPaths(t *testing.T) {
	tests := []struct {
		name        string
		query       querylogical.Query
		schema      schemamodel.DomainSchema
		environment queryphysical.Environment
		code        querylogical.ErrorCode
	}{
		{name: "broad disabled", query: baseQuery(), schema: plannerSchema(), environment: func() queryphysical.Environment {
			env := plannerEnvironment()
			env.Domain.BroadSearchable = false
			return env
		}(), code: querylogical.ErrorUnavailableAccessPath},
		{name: "missing ordered index", query: func() querylogical.Query {
			q := baseQuery()
			q.OrderBy = []querylogical.Order{{Value: querylogical.FieldValue("n", "missing"), Direction: querylogical.SortAscending}}
			return q
		}(), schema: plannerSchemaWithField("missing"), environment: plannerEnvironment(), code: querylogical.ErrorUnavailableAccessPath},
		{name: "adjacency depth", query: func() querylogical.Query {
			q := baseQuery()
			q.Start.NodeIDs = []string{"node-1"}
			step := traversalStep("edge")
			step.Depth.Max = 2
			q.Steps = []querylogical.Step{step}
			return q
		}(), schema: plannerSchema(), environment: plannerEnvironment(), code: querylogical.ErrorInvalidQuery},
		{name: "malformed structured less-than", query: func() querylogical.Query {
			q := baseQuery()
			left := stringValue("Ada")
			right := querylogical.FieldValue("n", "name")
			q.Predicate = querylogical.LeafPredicate(querylogical.Leaf{Kind: querylogical.LeafComparison, Operator: "<", Left: &left, Right: &right})
			return q
		}(), schema: plannerSchema(), environment: plannerEnvironment(), code: querylogical.ErrorInvalidQuery},
		{name: "missing index capability", query: func() querylogical.Query {
			q := baseQuery()
			q.Predicate = comparison("n", "name", "=", stringValue("Ada"))
			return q
		}(), schema: plannerSchema(), environment: func() queryphysical.Environment {
			env := plannerEnvironment()
			env.Capabilities.Indexes = false
			return env
		}(), code: querylogical.ErrorUnavailableAccessPath},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			statement := querylogical.ReadStatement(test.query)
			bound, err := querybinding.Bind(statement, bindingEnvironment(&test.schema, false))
			if err != nil {
				t.Fatalf("bind: %v", err)
			}
			_, err = queryphysical.Build(bound, test.environment)
			assertQueryErrorCode(t, err, test.code)
		})
	}
}

func TestBuildPlansMutationOnlyForAvailableLegacyMutationAccess(t *testing.T) {
	schema := plannerSchema()
	statement := querylogical.MutationStatement(querylogical.SourceGQL, querylogical.Mutation{
		Kind: querylogical.MutationInsertNode,
		Node: &querylogical.NodePattern{Labels: []string{"Document"}, Properties: map[string]querylogical.Value{"name": stringValue("Ada")}},
	})
	bound, err := querybinding.Bind(statement, bindingEnvironment(&schema, true))
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	plan, err := queryphysical.Build(bound, plannerEnvironment())
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if plan.Family != queryphysical.FamilyLegacyMutation || !plan.RequiresLegacy() {
		t.Fatalf("mutation plan = %#v", plan)
	}
	environment := plannerEnvironment()
	environment.Capabilities.Mutations = false
	_, err = queryphysical.Build(bound, environment)
	assertQueryErrorCode(t, err, querylogical.ErrorUnavailableAccessPath)
}

func buildRead(t *testing.T, query querylogical.Query, schema schemamodel.DomainSchema, environment queryphysical.Environment) queryphysical.Plan {
	t.Helper()
	statement := querylogical.ReadStatement(query)
	bound, err := querybinding.Bind(statement, bindingEnvironment(&schema, false))
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	environment.Schema = &schema
	plan, err := queryphysical.Build(bound, environment)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	return plan
}

func baseQuery() querylogical.Query {
	return querylogical.Query{
		Source: querylogical.SourceStructured, AccessMode: querylogical.AccessModeReadOnly,
		Start:   querylogical.NodePattern{Alias: "n", Labels: []string{"Document"}},
		Returns: []querylogical.Projection{{Kind: querylogical.ProjectionNode, Alias: "n", OutputName: "document"}},
		Page:    querylogical.PageSpec{Size: 25, Limit: 100},
	}
}

func traversalStep(edgeAlias string) querylogical.Step {
	return querylogical.Step{
		Edge:   querylogical.EdgePattern{Alias: edgeAlias, Labels: []string{"LINKS"}, Direction: querylogical.DirectionOutgoing},
		Depth:  querylogical.DepthBounds{Min: 1, Max: 1},
		Target: querylogical.NodePattern{Alias: "m", Labels: []string{"Document"}},
	}
}

func comparison(alias string, name string, operator string, right querylogical.Value) *querylogical.Predicate {
	left := querylogical.FieldValue(alias, name)
	return querylogical.LeafPredicate(querylogical.Leaf{Kind: querylogical.LeafComparison, Left: &left, Right: &right, Operator: operator})
}

func fieldRef(alias string, name string) *querylogical.FieldRef {
	field := querylogical.FieldRef{Alias: alias, Namespace: querylogical.NamespaceProperties, Name: name}
	return &field
}

func stringValue(value string) querylogical.Value {
	return querylogical.Value{Kind: querylogical.ValueString, String: value}
}

func integerValue(value int64) querylogical.Value {
	return querylogical.Value{Kind: querylogical.ValueInteger, Integer: value}
}

func bindingEnvironment(schema *schemamodel.DomainSchema, write bool) querybinding.Environment {
	mode := sessionmodel.TransactionModeReadOnly
	if write {
		mode = sessionmodel.TransactionModeReadWrite
	}
	return querybinding.Environment{
		Transaction: sessionmodel.GraphTransaction{Mode: mode, State: sessionmodel.TransactionStateActive},
		Schema:      schema,
		Domain: querybinding.DomainPolicy{
			BroadSearchable: true, ExplicitSearchable: true,
			SemanticSearchable: true, SemanticCandidatesAvailable: true,
		},
	}
}

func plannerEnvironment() queryphysical.Environment {
	return queryphysical.Environment{
		Domain: querybinding.DomainPolicy{
			BroadSearchable: true, ExplicitSearchable: true,
			SemanticSearchable: true, SemanticCandidatesAvailable: true,
		},
		Capabilities: queryphysical.Capabilities{
			EntityReads: true, Traversals: true, Indexes: true,
			SemanticCandidates: true, Mutations: true,
		},
	}
}

func plannerSchema() schemamodel.DomainSchema {
	return schemamodel.DomainSchema{
		Version: "v4", Mode: schemamodel.SchemaModeStrict,
		NodeTypes: []schemamodel.NodeType{{
			Name: "Document", Labels: []string{"Document"},
			Properties: []schemamodel.FieldSpec{{Name: "name", Type: schemamodel.FieldTypeString}, {Name: "age", Type: schemamodel.FieldTypeInt}},
		}},
		EdgeTypes: []schemamodel.EdgeType{{
			Name: "Links", Labels: []string{"LINKS"},
			From: schemamodel.EndpointSpec{NodeTypes: []string{"Document"}}, To: schemamodel.EndpointSpec{NodeTypes: []string{"Document"}},
		}},
		Indexes: []schemamodel.IndexDefinition{
			{Name: "document_name", TargetKind: schemamodel.IndexTargetNode, TargetType: "Document", Field: schemamodel.FieldPath{Namespace: "properties", Name: "name"}, Kind: schemamodel.IndexKindOrdered},
			{Name: "document_age", TargetKind: schemamodel.IndexTargetNode, TargetType: "Document", Field: schemamodel.FieldPath{Namespace: "properties", Name: "age"}, Kind: schemamodel.IndexKindOrdered},
		},
	}
}

func plannerSchemaWithField(name string) schemamodel.DomainSchema {
	schema := plannerSchema()
	schema.NodeTypes[0].Properties = append(schema.NodeTypes[0].Properties, schemamodel.FieldSpec{Name: name, Type: schemamodel.FieldTypeString})
	return schema
}

func operatorTreeContains(root queryphysical.Operator, kind queryphysical.OperatorKind) bool {
	if root.Kind == kind {
		return true
	}
	for _, input := range root.Inputs {
		if operatorTreeContains(input, kind) {
			return true
		}
	}
	return false
}

func assertPlanError(t *testing.T, query querylogical.Query, schema schemamodel.DomainSchema, environment queryphysical.Environment, code querylogical.ErrorCode) {
	t.Helper()
	bound, err := querybinding.Bind(querylogical.ReadStatement(query), bindingEnvironment(&schema, false))
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	_, err = queryphysical.Build(bound, environment)
	assertQueryErrorCode(t, err, code)
}

func assertQueryErrorCode(t *testing.T, err error, code querylogical.ErrorCode) {
	t.Helper()
	var queryErr *querylogical.QueryError
	if !errors.As(err, &queryErr) || queryErr.Code != code {
		t.Fatalf("error = %#v, want %q", err, code)
	}
}
