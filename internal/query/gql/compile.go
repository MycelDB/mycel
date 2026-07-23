package gql

import (
	gqlanalysis "github.com/myceldb/mycel/internal/query/gql/analysis"
	gqlantlr "github.com/myceldb/mycel/internal/query/gql/antlr"
	gqlast "github.com/myceldb/mycel/internal/query/gql/ast"
	ast "github.com/myceldb/mycel/internal/query/gql/ast/model"
	gqlplanning "github.com/myceldb/mycel/internal/query/gql/planning"
	planmodel "github.com/myceldb/mycel/internal/query/gql/planning/model"
)

// Parse parses a small Mycel GQL query into Mycel's AST. The initial slice
// supports one node INSERT statement.
func Parse(query string) (ast.Query, error) {
	tree, err := gqlantlr.Parse(query)
	if err != nil {
		return ast.Query{}, err
	}
	return gqlast.Build(tree)
}

// Compile parses, analyzes, and plans a small Mycel GQL query.
func Compile(query string) (planmodel.Plan, error) {
	parsed, err := Parse(query)
	if err != nil {
		return planmodel.Plan{}, err
	}
	return CompileAST(parsed)
}

// CompileAST analyzes and plans Mycel's GQL AST.
func CompileAST(query ast.Query) (planmodel.Plan, error) {
	analyzed, err := gqlanalysis.Analyze(query)
	if err != nil {
		return planmodel.Plan{}, err
	}
	return gqlplanning.Plan(analyzed)
}
