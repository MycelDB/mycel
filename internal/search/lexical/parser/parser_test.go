package parser

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/myceldb/mycel/internal/search/lexical/query"
)

func TestParseTermsPhrasesBooleanAndGrouping(t *testing.T) {
	res, err := Parse(`(graph OR semantic) AND "vector database" -draft`)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	if res.Expr.Kind != query.KindAnd {
		t.Fatalf("root kind = %v, want AND", res.Expr.Kind)
	}
	got := res.Expr.String()
	for _, want := range []string{"graph", "semantic", `"vector database"`, "NOT(draft)"} {
		if !strings.Contains(got, want) {
			t.Fatalf("expr %q missing %q", got, want)
		}
	}
	if !reflect.DeepEqual(res.NormalizedTerms, []string{"graph", "semantic", "vector", "database", "draft"}) {
		t.Fatalf("normalized = %#v", res.NormalizedTerms)
	}
}

func TestParseImplicitAndAndNot(t *testing.T) {
	res, err := Parse(`graph search NOT draft`)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	got := res.Expr.String()
	if got != "(graph AND search AND NOT(draft))" {
		t.Fatalf("expr = %q", got)
	}
}

func TestParseOperatorPrecedence(t *testing.T) {
	res, err := Parse(`graph OR semantic AND search`)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	if got := res.Expr.String(); got != "(graph OR (semantic AND search))" {
		t.Fatalf("expr = %q", got)
	}
}

func TestParseUnsupportedSyntax(t *testing.T) {
	for _, input := range []string{"graph*", "title:graph", "graph~1", "created:[2026 TO 2027]"} {
		t.Run(input, func(t *testing.T) {
			_, err := Parse(input)
			if err == nil {
				t.Fatalf("Parse succeeded, want unsupported syntax error")
			}
			var parseErr *ParseError
			if !errors.As(err, &parseErr) || len(parseErr.UnsupportedSyntax) == 0 {
				t.Fatalf("err = %#v, want ParseError with unsupported syntax", err)
			}
		})
	}
}

func TestParseInvalidSyntax(t *testing.T) {
	for _, input := range []string{"", "(", `"unterminated`, "graph OR"} {
		t.Run(input, func(t *testing.T) {
			if _, err := Parse(input); err == nil {
				t.Fatalf("Parse succeeded, want error")
			}
		})
	}
}
