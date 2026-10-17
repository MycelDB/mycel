package physical_test

import (
	"reflect"
	"testing"

	querylogical "github.com/myceldb/mycel/internal/query/logical"
	queryphysical "github.com/myceldb/mycel/internal/query/physical"
)

func TestExplainSerializesStaticPhysicalPlanWithoutRuntimeStatistics(t *testing.T) {
	field := querylogical.FieldRef{Alias: "n", Namespace: querylogical.NamespaceProperties, Name: "date"}
	predicate := querylogical.LeafPredicate(querylogical.Leaf{
		Kind: querylogical.LeafComparison, Operator: "=",
		Left:  valuePointer(querylogical.FieldValue("n", "date")),
		Right: valuePointer(querylogical.Value{Kind: querylogical.ValueString, String: "2026-10-17"}),
	})
	plan := queryphysical.Plan{
		Name: "OrderedNodePropertyIndexScan", Family: queryphysical.FamilyIndexedOrder,
		Root: queryphysical.Operator{Kind: queryphysical.OperatorLegacyBridge, Name: "LegacyBridge", Inputs: []queryphysical.Operator{{
			Kind: queryphysical.OperatorProject, Inputs: []queryphysical.Operator{{Kind: queryphysical.OperatorOrderedIndex, Name: "OrderedNodePropertyIndexScan"}},
		}}},
		Access:          queryphysical.AccessPath{Kind: queryphysical.AccessOrderedProperty, Field: &field, Index: "journal_by_date"},
		Indexes:         []queryphysical.IndexUse{{Name: "journal_by_date"}},
		PushedPredicate: predicate, ResidualPredicate: predicate,
		Ordering:      queryphysical.OrderingGuarantee{Terms: []querylogical.Order{{Value: querylogical.FieldValue("n", "date"), Direction: querylogical.SortAscending}}, Stable: true, Source: "journal_by_date"},
		Cursor:        queryphysical.CursorStrategy{Kind: "index_key", PlanBound: true, Snapshot: true, Schema: true, LegacyForm: true},
		Limits:        queryphysical.ResourceLimits{MaxIndexEntries: 100, MaxStartNodes: 10, MaxNodesLoaded: 50, MaxEdgesLoaded: 60, MaxIntermediateRows: 70, MaxSemanticCandidates: 20, MaxTraversalDepth: 5},
		SchemaVersion: "v5", LegacyBridgeReason: "legacy execution",
	}

	diagnostics := queryphysical.Explain(plan)
	if repeated := queryphysical.Explain(plan); !reflect.DeepEqual(diagnostics, repeated) {
		t.Fatalf("Explain is not deterministic: %#v != %#v", diagnostics, repeated)
	}
	if diagnostics.Plan != plan.Name || diagnostics.PlanKind != string(plan.Family) || diagnostics.AccessPath != string(plan.Access.Kind) || diagnostics.SchemaVersion != "v5" {
		t.Fatalf("plan diagnostics = %#v", diagnostics)
	}
	if !reflect.DeepEqual(diagnostics.Indexes, []string{"journal_by_date"}) || len(diagnostics.Operators) != 3 || len(diagnostics.Ordering) != 3 {
		t.Fatalf("plan structure diagnostics = %#v", diagnostics)
	}
	if len(diagnostics.PushedPredicates) != 1 || len(diagnostics.ResidualPredicates) != 1 {
		t.Fatalf("predicate diagnostics = %#v", diagnostics)
	}
	if !diagnostics.LegacyBridge || diagnostics.LegacyBridgeReason != "legacy execution" || diagnostics.NextCursorKind != "index_key" || !diagnostics.CursorPlanBound || !diagnostics.CursorSnapshotBound || !diagnostics.CursorSchemaBound || !diagnostics.CursorLegacyForm {
		t.Fatalf("bridge/cursor diagnostics = %#v", diagnostics)
	}
	if diagnostics.ResourceLimits.MaxIndexEntries != 100 || diagnostics.ResourceLimits.MaxTraversalDepth != 5 {
		t.Fatalf("resource diagnostics = %#v", diagnostics.ResourceLimits)
	}
	if diagnostics.IndexEntriesScanned != 0 || diagnostics.NodesLoaded != 0 || diagnostics.EdgesLoaded != 0 || diagnostics.RowsScanned != 0 || diagnostics.RowsProduced != 0 || diagnostics.RowsReturned != 0 || diagnostics.CandidateCount != 0 || diagnostics.ExecutionMillis != 0 {
		t.Fatalf("Explain reported runtime statistics: %#v", diagnostics)
	}
}

func valuePointer(value querylogical.Value) *querylogical.Value { return &value }
