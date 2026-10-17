package physical

import (
	"encoding/json"

	querylogical "github.com/myceldb/mycel/internal/query/logical"
)

// Explain serializes one physical plan into static, transport-neutral
// diagnostics. It never reports runtime counters and performs no access calls.
func Explain(plan Plan) querylogical.Diagnostics {
	indexes := make([]string, 0, len(plan.Indexes))
	for _, index := range plan.Indexes {
		indexes = append(indexes, index.Name)
	}
	return querylogical.Diagnostics{
		Plan:                plan.Name,
		PlanKind:            string(plan.Family),
		Indexes:             indexes,
		Operators:           explainOperators(plan.Root),
		Ordering:            explainOrdering(plan.Ordering),
		AccessPath:          string(plan.Access.Kind),
		SchemaVersion:       plan.SchemaVersion,
		FullScan:            plan.FullScan,
		PushedPredicates:    explainPredicate(plan.PushedPredicate),
		ResidualPredicates:  explainPredicate(plan.ResidualPredicate),
		NextCursorKind:      plan.Cursor.Kind,
		ExplainOnly:         true,
		FallbackMode:        plan.FallbackMode,
		LegacyBridge:        plan.RequiresLegacy(),
		LegacyBridgeReason:  plan.LegacyBridgeReason,
		CursorPlanBound:     plan.Cursor.PlanBound,
		CursorSnapshotBound: plan.Cursor.Snapshot,
		CursorSchemaBound:   plan.Cursor.Schema,
		CursorLegacyForm:    plan.Cursor.LegacyForm,
		ResourceLimits: querylogical.DiagnosticResourceLimits{
			MaxIndexEntries:       plan.Limits.MaxIndexEntries,
			MaxStartNodes:         plan.Limits.MaxStartNodes,
			MaxNodesLoaded:        plan.Limits.MaxNodesLoaded,
			MaxEdgesLoaded:        plan.Limits.MaxEdgesLoaded,
			MaxIntermediateRows:   plan.Limits.MaxIntermediateRows,
			MaxSemanticCandidates: plan.Limits.MaxSemanticCandidates,
			MaxTraversalDepth:     plan.Limits.MaxTraversalDepth,
		},
	}
}

func explainOperators(root Operator) []string {
	operators := []string{}
	var visit func(Operator)
	visit = func(operator Operator) {
		name := string(operator.Kind)
		if operator.Name != "" {
			name += ":" + operator.Name
		}
		operators = append(operators, name)
		for _, input := range operator.Inputs {
			visit(input)
		}
	}
	visit(root)
	return operators
}

func explainOrdering(ordering OrderingGuarantee) []string {
	if len(ordering.Terms) == 0 && ordering.Source == "" && !ordering.Stable {
		return nil
	}
	out := make([]string, 0, len(ordering.Terms)+1)
	if ordering.Source != "" {
		out = append(out, "source:"+ordering.Source)
	}
	for _, term := range ordering.Terms {
		if encoded, err := json.Marshal(term); err == nil {
			out = append(out, string(encoded))
		}
	}
	if ordering.Stable {
		out = append(out, "stable")
	}
	return out
}

func explainPredicate(predicate *querylogical.Predicate) []string {
	if predicate == nil {
		return nil
	}
	encoded, err := json.Marshal(predicate)
	if err != nil {
		return []string{"<unserializable predicate>"}
	}
	return []string{string(encoded)}
}
