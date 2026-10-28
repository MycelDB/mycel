package cmd

import (
	"testing"

	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
)

func TestValidateHybridSearchQueries(t *testing.T) {
	tests := []struct {
		name           string
		lexicalQuery   string
		semanticQuery  string
		lexicalWeight  float64
		semanticWeight float64
		requireBoth    bool
		wantErr        bool
	}{
		{name: "default channels", lexicalQuery: "raft", semanticQuery: "how raft works"},
		{name: "missing default semantic", lexicalQuery: "raft", wantErr: true},
		{name: "lexical only", lexicalQuery: "raft", lexicalWeight: 1},
		{name: "semantic only", semanticQuery: "how raft works", semanticWeight: 1},
		{name: "require both", lexicalQuery: "raft", semanticQuery: "how raft works", lexicalWeight: 1, requireBoth: true},
		{name: "require both missing semantic", lexicalQuery: "raft", lexicalWeight: 1, requireBoth: true, wantErr: true},
		{name: "negative weight", lexicalQuery: "raft", semanticQuery: "how raft works", lexicalWeight: -1, semanticWeight: 1, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateHybridSearchQueries(tt.lexicalQuery, tt.semanticQuery, tt.lexicalWeight, tt.semanticWeight, tt.requireBoth)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateHybridSearchQueries() error = %v, wantErr %t", err, tt.wantErr)
			}
		})
	}
}

func TestParsePropertyFilterSpec(t *testing.T) {
	filter, err := parsePropertyFilterSpec("tags:contains:k3s")
	if err != nil {
		t.Fatalf("parsePropertyFilterSpec returned error: %v", err)
	}
	if filter.GetPath() != "tags" || filter.GetOperator() != clientv1.FilterOperator_FILTER_OPERATOR_CONTAINS || len(filter.GetValues()) != 1 || filter.GetValues()[0] != "k3s" {
		t.Fatalf("filter = %#v", filter)
	}

	filter, err = parsePropertyFilterSpec("status:in:draft,published")
	if err != nil {
		t.Fatalf("parsePropertyFilterSpec returned error: %v", err)
	}
	if filter.GetOperator() != clientv1.FilterOperator_FILTER_OPERATOR_IN || len(filter.GetValues()) != 2 || filter.GetValues()[1] != "published" {
		t.Fatalf("in filter = %#v", filter)
	}

	filter, err = parsePropertyFilterSpec("archived:exists")
	if err != nil {
		t.Fatalf("parsePropertyFilterSpec exists returned error: %v", err)
	}
	if filter.GetOperator() != clientv1.FilterOperator_FILTER_OPERATOR_EXISTS || len(filter.GetValues()) != 0 {
		t.Fatalf("exists filter = %#v", filter)
	}
}

func TestParsePropertyFilterSpecRejectsInvalidInput(t *testing.T) {
	for _, spec := range []string{"", "status", ":equals:x", "status:bad:x"} {
		if filter, err := parsePropertyFilterSpec(spec); err == nil || filter != nil {
			t.Fatalf("parsePropertyFilterSpec(%q) = %#v, %v; want nil, error", spec, filter, err)
		}
	}
}
