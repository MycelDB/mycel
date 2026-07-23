package analysis

import (
	"strings"
	"testing"

	"github.com/myceldb/mycel/internal/query/gql/ast/model"
)

func TestAnalyzeInsertNodeAST(t *testing.T) {
	query := model.Query{Statement: model.InsertStatement{Pattern: model.NodePattern{
		Labels: []string{"Person"},
		Properties: []model.Property{
			{Key: "name", Value: model.Value{Kind: model.StringValue, Value: "Alice"}},
			{Key: "age", Value: model.Value{Kind: model.IntValue, Value: int64(42)}},
		},
	}}}

	analysis, err := Analyze(query)
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	if analysis.AccessMode != ReadWrite {
		t.Fatalf("Analyze().AccessMode = %q, want %q", analysis.AccessMode, ReadWrite)
	}
}

func TestAnalyzeRejectsInvalidInsertNodeAST(t *testing.T) {
	tests := []struct {
		name    string
		query   model.Query
		wantErr string
	}{
		{
			name:    "missing statement",
			query:   model.Query{},
			wantErr: "query statement is required",
		},
		{
			name: "missing label",
			query: model.Query{Statement: model.InsertStatement{Pattern: model.NodePattern{
				Properties: []model.Property{{Key: "name", Value: model.Value{Kind: model.StringValue, Value: "Alice"}}},
			}}},
			wantErr: "requires at least one label",
		},
		{
			name: "duplicate label",
			query: model.Query{Statement: model.InsertStatement{Pattern: model.NodePattern{
				Labels: []string{"Person", "Person"},
			}}},
			wantErr: "duplicate label",
		},
		{
			name: "duplicate property key",
			query: model.Query{Statement: model.InsertStatement{Pattern: model.NodePattern{
				Labels: []string{"Person"},
				Properties: []model.Property{
					{Key: "name", Value: model.Value{Kind: model.StringValue, Value: "Alice"}},
					{Key: "name", Value: model.Value{Kind: model.StringValue, Value: "Bob"}},
				},
			}}},
			wantErr: "duplicate property key",
		},
		{
			name: "invalid value payload",
			query: model.Query{Statement: model.InsertStatement{Pattern: model.NodePattern{
				Labels:     []string{"Person"},
				Properties: []model.Property{{Key: "age", Value: model.Value{Kind: model.IntValue, Value: 42}}},
			}}},
			wantErr: "int value kind requires int64 payload",
		},
		{
			name: "unsupported value kind",
			query: model.Query{Statement: model.InsertStatement{Pattern: model.NodePattern{
				Labels:     []string{"Person"},
				Properties: []model.Property{{Key: "tags", Value: model.Value{Kind: model.ValueKind("list"), Value: []string{"a"}}}},
			}}},
			wantErr: "unsupported value kind",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Analyze(tt.query)
			if err == nil {
				t.Fatal("Analyze() error = nil, want error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Analyze() error = %q, want containing %q", err, tt.wantErr)
			}
		})
	}
}
