package gql

import (
	"fmt"
	"strings"

	"github.com/myceldb/mycel/internal/query/gql/analysis"
	planmodel "github.com/myceldb/mycel/internal/query/gql/planning/model"
)

type ScriptPlan struct {
	Statements []StatementPlan
	AccessMode analysis.AccessMode
}

type StatementPlan struct {
	Index     int
	Statement string
	Plan      planmodel.Plan
}

func SplitScript(script string) ([]string, error) {
	statements := []string{}
	var current strings.Builder
	inString := rune(0)
	escaped := false
	for _, r := range script {
		if inString != 0 {
			current.WriteRune(r)
			if escaped {
				escaped = false
				continue
			}
			if r == '\\' {
				escaped = true
				continue
			}
			if r == inString {
				inString = 0
			}
			continue
		}
		switch r {
		case '\'', '"':
			inString = r
			current.WriteRune(r)
		case ';':
			statement := strings.TrimSpace(current.String())
			if statement == "" {
				return nil, fmt.Errorf("empty GQL statement")
			}
			statements = append(statements, statement)
			current.Reset()
		default:
			current.WriteRune(r)
		}
	}
	if inString != 0 {
		return nil, fmt.Errorf("unterminated string literal")
	}
	statement := strings.TrimSpace(current.String())
	if statement != "" {
		statements = append(statements, statement)
	}
	if len(statements) == 0 {
		return nil, fmt.Errorf("script is empty")
	}
	return statements, nil
}

func CompileScript(script string) (ScriptPlan, error) {
	statements, err := SplitScript(script)
	if err != nil {
		return ScriptPlan{}, err
	}
	out := ScriptPlan{AccessMode: analysis.ReadOnly}
	for index, statement := range statements {
		plan, err := Compile(statement)
		if err != nil {
			return ScriptPlan{}, fmt.Errorf("statement %d: %w", index+1, err)
		}
		if plan.AccessMode == analysis.ReadWrite {
			out.AccessMode = analysis.ReadWrite
		}
		out.Statements = append(out.Statements, StatementPlan{Index: index + 1, Statement: statement, Plan: plan})
	}
	return out, nil
}
