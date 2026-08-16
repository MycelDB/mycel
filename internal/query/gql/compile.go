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
	return CompileWithParams(query, nil)
}

// CompileWithParams parses, analyzes, and plans Mycel GQL with supplied scalar parameters.
func CompileWithParams(query string, params map[string]any) (planmodel.Plan, error) {
	parsed, err := Parse(query)
	if err != nil {
		return planmodel.Plan{}, err
	}
	return CompileASTWithParams(parsed, params)
}

// CompileAST analyzes and plans Mycel's GQL AST.
func CompileAST(query ast.Query) (planmodel.Plan, error) {
	return CompileASTWithParams(query, nil)
}

// CompileASTWithParams analyzes and plans Mycel's GQL AST using supplied scalar parameters.
func CompileASTWithParams(query ast.Query, params map[string]any) (planmodel.Plan, error) {
	analyzed, err := gqlanalysis.AnalyzeWithParams(query, params)
	if err != nil {
		return planmodel.Plan{}, err
	}
	return gqlplanning.Plan(analyzed)
}

// CompileASTWithSchema analyzes and plans Mycel's GQL AST using optional
// schema-aware semantic validation.
func CompileASTWithSchema(query ast.Query, schema gqlanalysis.SchemaContext) (planmodel.Plan, error) {
	return CompileASTWithSchemaAndParams(query, schema, nil)
}

// CompileASTWithSchemaAndParams analyzes and plans Mycel's GQL AST using optional schema and parameters.
func CompileASTWithSchemaAndParams(query ast.Query, schema gqlanalysis.SchemaContext, params map[string]any) (planmodel.Plan, error) {
	analyzed, err := gqlanalysis.AnalyzeWithSchemaAndParams(query, schema, params)
	if err != nil {
		return planmodel.Plan{}, err
	}
	return gqlplanning.Plan(analyzed)
}

// CompileWithSchema parses, analyzes, and plans Mycel GQL with optional
// schema-aware semantic validation.
func CompileWithSchema(query string, schema gqlanalysis.SchemaContext) (planmodel.Plan, error) {
	return CompileWithSchemaAndParams(query, schema, nil)
}

// CompileWithSchemaAndParams parses, analyzes, and plans Mycel GQL with optional schema and parameters.
func CompileWithSchemaAndParams(query string, schema gqlanalysis.SchemaContext, params map[string]any) (planmodel.Plan, error) {
	parsed, err := Parse(query)
	if err != nil {
		return planmodel.Plan{}, err
	}
	return CompileASTWithSchemaAndParams(parsed, schema, params)
}
