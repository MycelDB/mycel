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
	if !ok || queryCtx.InsertStatement() == nil {
		return model.Query{}, fmt.Errorf("expected query parse tree")
	}
	insertCtx, ok := queryCtx.InsertStatement().(*generated.InsertStatementContext)
	if !ok || insertCtx.NodePattern() == nil {
		return model.Query{}, fmt.Errorf("expected insert statement")
	}
	node, err := buildNodePattern(insertCtx.NodePattern())
	if err != nil {
		return model.Query{}, err
	}
	return model.Query{Statement: model.InsertStatement{Pattern: node}}, nil
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
