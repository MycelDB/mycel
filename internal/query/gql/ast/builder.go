package ast

import (
	"fmt"
	"strconv"
	"strings"

	antlr4 "github.com/antlr4-go/antlr/v4"
	"github.com/myceldb/mycel/internal/query/gql/antlr/generated"
	"github.com/myceldb/mycel/internal/query/gql/ast/model"
)

// Builder converts an ANTLR parse tree to Mycel's GQL AST.
type Builder interface {
	Build(tree antlr4.Tree) (model.Query, error)
}

type builder struct{}

func NewBuilder() Builder { return builder{} }

func Build(tree antlr4.Tree) (model.Query, error) { return NewBuilder().Build(tree) }

func (builder) Build(tree antlr4.Tree) (model.Query, error) {
	queryCtx, ok := tree.(*generated.QueryContext)
	if !ok || queryCtx.Statement() == nil {
		return model.Query{}, fmt.Errorf("expected query parse tree")
	}
	stmtCtx, ok := queryCtx.Statement().(*generated.StatementContext)
	if !ok {
		return model.Query{}, fmt.Errorf("expected statement")
	}
	if insert := stmtCtx.InsertStatement(); insert != nil {
		return buildInsertStatement(insert)
	}
	if match := stmtCtx.MatchStatement(); match != nil {
		return buildMatchStatement(match)
	}
	return model.Query{}, fmt.Errorf("expected supported statement")
}

func buildInsertStatement(ctx generated.IInsertStatementContext) (model.Query, error) {
	insertCtx, ok := ctx.(*generated.InsertStatementContext)
	if !ok || insertCtx.NodePattern() == nil {
		return model.Query{}, fmt.Errorf("expected insert statement")
	}
	node, err := buildNodePattern(insertCtx.NodePattern())
	if err != nil {
		return model.Query{}, err
	}
	return model.Query{Statement: model.InsertStatement{Pattern: node}}, nil
}

func buildMatchStatement(ctx generated.IMatchStatementContext) (model.Query, error) {
	matchCtx, ok := ctx.(*generated.MatchStatementContext)
	if !ok || matchCtx.NodePattern() == nil {
		return model.Query{}, fmt.Errorf("expected match statement")
	}
	node, err := buildNodePattern(matchCtx.NodePattern())
	if err != nil {
		return model.Query{}, err
	}
	var where *model.WhereClause
	if whereCtx := matchCtx.WhereClause(); whereCtx != nil {
		built, err := buildWhereClause(whereCtx)
		if err != nil {
			return model.Query{}, err
		}
		where = &built
	}
	returns := make([]model.ReturnItem, 0, len(matchCtx.AllReturnItem()))
	for _, item := range matchCtx.AllReturnItem() {
		returnCtx, ok := item.(*generated.ReturnItemContext)
		if !ok {
			return model.Query{}, fmt.Errorf("invalid return item")
		}
		built, err := buildReturnItem(returnCtx)
		if err != nil {
			return model.Query{}, err
		}
		returns = append(returns, built)
	}
	return model.Query{Statement: model.MatchStatement{Pattern: node, Where: where, Returns: returns}}, nil
}

func buildReturnItem(ctx *generated.ReturnItemContext) (model.ReturnItem, error) {
	if prop := ctx.PropertyReference(); prop != nil {
		propCtx, ok := prop.(*generated.PropertyReferenceContext)
		if !ok || len(propCtx.AllIDENTIFIER()) != 2 {
			return model.ReturnItem{}, fmt.Errorf("invalid return property")
		}
		return model.ReturnItem{Kind: model.ReturnProperty, Variable: propCtx.IDENTIFIER(0).GetText(), Property: propCtx.IDENTIFIER(1).GetText()}, nil
	}
	if variable := ctx.Variable(); variable != nil {
		variableCtx, ok := variable.(*generated.VariableContext)
		if !ok || variableCtx.IDENTIFIER() == nil {
			return model.ReturnItem{}, fmt.Errorf("invalid return variable")
		}
		return model.ReturnItem{Kind: model.ReturnVariable, Variable: variableCtx.IDENTIFIER().GetText()}, nil
	}
	return model.ReturnItem{}, fmt.Errorf("invalid return item")
}

func buildWhereClause(ctx generated.IWhereClauseContext) (model.WhereClause, error) {
	whereCtx, ok := ctx.(*generated.WhereClauseContext)
	if !ok || whereCtx.Predicate() == nil {
		return model.WhereClause{}, fmt.Errorf("invalid where clause")
	}
	predicateCtx, ok := whereCtx.Predicate().(*generated.PredicateContext)
	if !ok {
		return model.WhereClause{}, fmt.Errorf("invalid predicate")
	}
	where := model.WhereClause{Predicates: make([]model.PropertyComparison, 0, len(predicateCtx.AllPropertyComparison()))}
	for _, comparison := range predicateCtx.AllPropertyComparison() {
		built, err := buildPropertyComparison(comparison)
		if err != nil {
			return model.WhereClause{}, err
		}
		where.Predicates = append(where.Predicates, built)
	}
	return where, nil
}

func buildPropertyComparison(ctx generated.IPropertyComparisonContext) (model.PropertyComparison, error) {
	comparisonCtx, ok := ctx.(*generated.PropertyComparisonContext)
	if !ok || len(comparisonCtx.AllIDENTIFIER()) != 2 || comparisonCtx.Value() == nil {
		return model.PropertyComparison{}, fmt.Errorf("invalid property comparison")
	}
	value, err := buildValue(comparisonCtx.Value())
	if err != nil {
		return model.PropertyComparison{}, err
	}
	return model.PropertyComparison{Variable: comparisonCtx.IDENTIFIER(0).GetText(), Property: comparisonCtx.IDENTIFIER(1).GetText(), Value: value}, nil
}

func buildNodePattern(ctx generated.INodePatternContext) (model.NodePattern, error) {
	nodeCtx, ok := ctx.(*generated.NodePatternContext)
	if !ok {
		return model.NodePattern{}, fmt.Errorf("expected node pattern")
	}
	var node model.NodePattern
	if variable := nodeCtx.Variable(); variable != nil {
		v, ok := variable.(*generated.VariableContext)
		if !ok || v.IDENTIFIER() == nil {
			return model.NodePattern{}, fmt.Errorf("invalid node variable")
		}
		node.Variable = v.IDENTIFIER().GetText()
	}
	if labels := nodeCtx.LabelExpression(); labels != nil {
		labelCtx, ok := labels.(*generated.LabelExpressionContext)
		if !ok {
			return model.NodePattern{}, fmt.Errorf("invalid label expression")
		}
		for _, label := range labelCtx.AllLabelName() {
			nameCtx, ok := label.(*generated.LabelNameContext)
			if !ok || nameCtx.IDENTIFIER() == nil {
				return model.NodePattern{}, fmt.Errorf("invalid label name")
			}
			node.Labels = append(node.Labels, nameCtx.IDENTIFIER().GetText())
		}
	}
	if props := nodeCtx.PropertyMap(); props != nil {
		propCtx, ok := props.(*generated.PropertyMapContext)
		if !ok {
			return model.NodePattern{}, fmt.Errorf("invalid property map")
		}
		for _, pair := range propCtx.AllPropertyPair() {
			prop, err := buildPropertyPair(pair)
			if err != nil {
				return model.NodePattern{}, err
			}
			node.Properties = append(node.Properties, prop)
		}
	}
	return node, nil
}

func buildPropertyPair(ctx generated.IPropertyPairContext) (model.Property, error) {
	pairCtx, ok := ctx.(*generated.PropertyPairContext)
	if !ok || pairCtx.PropertyKey() == nil || pairCtx.Value() == nil {
		return model.Property{}, fmt.Errorf("invalid property pair")
	}
	keyCtx, ok := pairCtx.PropertyKey().(*generated.PropertyKeyContext)
	if !ok || keyCtx.IDENTIFIER() == nil {
		return model.Property{}, fmt.Errorf("invalid property key")
	}
	value, err := buildValue(pairCtx.Value())
	if err != nil {
		return model.Property{}, fmt.Errorf("property %q: %w", keyCtx.IDENTIFIER().GetText(), err)
	}
	return model.Property{Key: keyCtx.IDENTIFIER().GetText(), Value: value}, nil
}

func unquoteString(text string) (string, error) {
	if strings.HasPrefix(text, "'") && strings.HasSuffix(text, "'") {
		return strconv.Unquote(`"` + strings.ReplaceAll(text[1:len(text)-1], `"`, `\"`) + `"`)
	}
	return strconv.Unquote(text)
}

func buildValue(ctx generated.IValueContext) (model.Value, error) {
	valueCtx, ok := ctx.(*generated.ValueContext)
	if !ok {
		return model.Value{}, fmt.Errorf("invalid value")
	}
	switch {
	case valueCtx.STRING() != nil:
		v, err := unquoteString(valueCtx.STRING().GetText())
		return model.Value{Kind: model.StringValue, Value: v}, err
	case valueCtx.INTEGER() != nil:
		v, err := strconv.ParseInt(valueCtx.INTEGER().GetText(), 10, 64)
		return model.Value{Kind: model.IntValue, Value: v}, err
	case valueCtx.FLOAT() != nil:
		v, err := strconv.ParseFloat(valueCtx.FLOAT().GetText(), 64)
		return model.Value{Kind: model.FloatValue, Value: v}, err
	case valueCtx.TRUE() != nil:
		return model.Value{Kind: model.BoolValue, Value: true}, nil
	case valueCtx.FALSE() != nil:
		return model.Value{Kind: model.BoolValue, Value: false}, nil
	case valueCtx.NULL() != nil:
		return model.Value{Kind: model.NullValue, Value: nil}, nil
	default:
		return model.Value{}, fmt.Errorf("unsupported value %q", valueCtx.GetText())
	}
}
