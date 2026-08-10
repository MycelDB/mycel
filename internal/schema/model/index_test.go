package model

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

func validIndexSchema() DomainSchema {
	return DomainSchema{
		DomainID:  uuid.New(),
		Mode:      SchemaModeStrict,
		NodeTypes: []NodeType{{Name: "JournalEntry", Labels: []string{"JournalEntry"}, Properties: []FieldSpec{{Name: "date", Type: FieldTypeDate, Required: true}, {Name: "title", Type: FieldTypeString}}}},
		EdgeTypes: []EdgeType{{Name: "REFERENCES", Labels: []string{"REFERENCES"}, From: EndpointSpec{NodeTypes: []string{"JournalEntry"}}, To: EndpointSpec{NodeTypes: []string{"JournalEntry"}}, Properties: []FieldSpec{{Name: "confidence", Type: FieldTypeFloat}}}},
		Indexes:   []IndexDefinition{{Name: "journal_entries_by_date", TargetKind: IndexTargetNode, TargetType: "JournalEntry", Field: FieldPath{Namespace: "properties", Name: "date"}, Kind: IndexKindOrdered}},
	}
}

func TestValidateAcceptsNodeAndEdgeIndexes(t *testing.T) {
	s := validIndexSchema()
	s.Indexes = append(s.Indexes, IndexDefinition{Name: "references_by_confidence", TargetKind: IndexTargetEdge, TargetType: "REFERENCES", Field: FieldPath{Namespace: "properties", Name: "confidence"}, Kind: IndexKindOrdered, Direction: IndexSortDirectionDesc})
	got := s.Normalize()
	if got.Indexes[0].Direction != IndexSortDirectionAsc || len(got.Indexes[0].Labels) != 1 || got.Indexes[0].Labels[0] != "JournalEntry" {
		t.Fatalf("index not normalized: %+v", got.Indexes[0])
	}
	if err := Validate(got); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestValidateRejectsInvalidIndexes(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*DomainSchema)
		wantErr string
	}{
		{name: "duplicate", mutate: func(s *DomainSchema) { s.Indexes = append(s.Indexes, s.Indexes[0]) }, wantErr: "duplicate index"},
		{name: "unknown target", mutate: func(s *DomainSchema) { s.Indexes[0].TargetType = "Missing" }, wantErr: "unknown node type"},
		{name: "unsupported namespace", mutate: func(s *DomainSchema) { s.Indexes[0].Field.Namespace = "payload" }, wantErr: "not supported"},
		{name: "unknown field", mutate: func(s *DomainSchema) { s.Indexes[0].Field.Name = "missing" }, wantErr: "unknown field"},
		{name: "repeated", mutate: func(s *DomainSchema) { s.NodeTypes[0].Properties[0].Repeated = true }, wantErr: "cannot be repeated"},
		{name: "not orderable", mutate: func(s *DomainSchema) { s.NodeTypes[0].Properties[0].Type = FieldTypeJSON }, wantErr: "not orderable"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := validIndexSchema()
			tc.mutate(&s)
			err := Validate(s)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Validate() error = %v, want contains %q", err, tc.wantErr)
			}
		})
	}
}

func TestDiffIndexes(t *testing.T) {
	oldSchema := validIndexSchema().Normalize()
	newSchema := oldSchema
	newSchema.Indexes = []IndexDefinition{
		{Name: "journal_entries_by_date", TargetKind: IndexTargetNode, TargetType: "JournalEntry", Field: FieldPath{Namespace: "properties", Name: "date"}, Kind: IndexKindOrdered, Direction: IndexSortDirectionDesc},
		{Name: "journal_entries_by_title", TargetKind: IndexTargetNode, TargetType: "JournalEntry", Field: FieldPath{Namespace: "properties", Name: "title"}, Kind: IndexKindOrdered},
	}
	diff := DiffIndexes(oldSchema, newSchema)
	if len(diff.Added) != 1 || diff.Added[0].Name != "journal_entries_by_title" {
		t.Fatalf("unexpected added diff: %+v", diff.Added)
	}
	if len(diff.Changed) != 1 || diff.Changed[0].Old.Direction != IndexSortDirectionAsc || diff.Changed[0].New.Direction != IndexSortDirectionDesc {
		t.Fatalf("unexpected changed diff: %+v", diff.Changed)
	}
	removed := DiffIndexes(newSchema, DomainSchema{DomainID: oldSchema.DomainID, NodeTypes: oldSchema.NodeTypes}).Removed
	if len(removed) != 2 {
		t.Fatalf("unexpected removed diff: %+v", removed)
	}
}
