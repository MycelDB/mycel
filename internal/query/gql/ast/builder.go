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
	if matchCreate := stmtCtx.MatchCreateStatement(); matchCreate != nil {
		return buildMatchCreateStatement(matchCreate)
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

func buildMatchCreateStatement(ctx generated.IMatchCreateStatementContext) (model.Query, error) {
	matchCtx, ok := ctx.(*generated.MatchCreateStatementContext)
	if !ok || len(matchCtx.AllNodePattern()) < 2 || matchCtx.CreateRelationshipPattern() == nil {
		return model.Query{}, fmt.Errorf("expected match create statement")
	}
	matches := make([]model.NodePattern, 0, len(matchCtx.AllNodePattern()))
	for _, nodeCtx := range matchCtx.AllNodePattern() {
		node, err := buildNodePattern(nodeCtx)
		if err != nil {
			return model.Query{}, err
		}
		matches = append(matches, node)
	}
	create, err := buildCreateRelationshipPattern(matchCtx.CreateRelationshipPattern())
	if err != nil {
		return model.Query{}, err
	}
	return model.Query{Statement: model.MatchCreateStatement{Matches: matches, Create: create}}, nil
}

func buildCreateRelationshipPattern(ctx generated.ICreateRelationshipPatternContext) (model.CreateRelationshipPattern, error) {
	createCtx, ok := ctx.(*generated.CreateRelationshipPatternContext)
	if !ok || len(createCtx.AllVariable()) != 2 {
		return model.CreateRelationshipPattern{}, fmt.Errorf("invalid create relationship pattern")
	}
	fromCtx, ok := createCtx.Variable(0).(*generated.VariableContext)
	if !ok || fromCtx.IDENTIFIER() == nil {
		return model.CreateRelationshipPattern{}, fmt.Errorf("invalid create relationship source")
	}
	toCtx, ok := createCtx.Variable(1).(*generated.VariableContext)
	if !ok || toCtx.IDENTIFIER() == nil {
		return model.CreateRelationshipPattern{}, fmt.Errorf("invalid create relationship target")
	}
	rel := model.RelationshipPattern{Direction: model.RelationshipOutgoing}
	if edge := createCtx.EdgePattern(); edge != nil {
		built, err := buildEdgePattern(edge)
		if err != nil {
			return model.CreateRelationshipPattern{}, err
		}
		built.Direction = model.RelationshipOutgoing
		rel = built
	}
	return model.CreateRelationshipPattern{FromVariable: fromCtx.IDENTIFIER().GetText(), ToVariable: toCtx.IDENTIFIER().GetText(), Relationship: rel}, nil
}

func buildMatchStatement(ctx generated.IMatchStatementContext) (model.Query, error) {
	matchCtx, ok := ctx.(*generated.MatchStatementContext)
	if !ok || matchCtx.MatchPattern() == nil {
		return model.Query{}, fmt.Errorf("expected match statement")
	}
	matchPattern, err := buildMatchPattern(matchCtx.MatchPattern())
	if err != nil {
		return model.Query{}, err
	}
	node := matchPattern.Start
	var where *model.WhereClause
	if whereCtx := matchCtx.WhereClause(); whereCtx != nil {
		built, err := buildWhereClause(whereCtx)
		if err != nil {
			return model.Query{}, err
		}
		where = &built
	}
	var fetchFirst *model.FetchFirstClause
	if fetchCtx := matchCtx.FetchFirstClause(); fetchCtx != nil {
		built, err := buildFetchFirstClause(fetchCtx)
		if err != nil {
			return model.Query{}, err
		}
		fetchFirst = &built
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
	var orderBy []model.OrderItem
	if orderCtx := matchCtx.OrderByClause(); orderCtx != nil {
		built, err := buildOrderByClause(orderCtx)
		if err != nil {
			return model.Query{}, err
		}
		orderBy = built
	}
	stmt := model.MatchStatement{Pattern: node, Where: where, Returns: returns, OrderBy: orderBy, FetchFirst: fetchFirst}
	if matchPattern.Relationship != nil || len(matchPattern.Segments) > 0 {
		stmt.MatchPattern = matchPattern
	}
	return model.Query{Statement: stmt}, nil
}

func buildMatchPattern(ctx generated.IMatchPatternContext) (model.MatchPattern, error) {
	patternCtx, ok := ctx.(*generated.MatchPatternContext)
	if !ok || len(patternCtx.AllNodePattern()) == 0 {
		return model.MatchPattern{}, fmt.Errorf("invalid match pattern")
	}
	start, err := buildNodePattern(patternCtx.NodePattern(0))
	if err != nil {
		return model.MatchPattern{}, err
	}
	out := model.MatchPattern{Start: start}
	rels := patternCtx.AllRelationshipPattern()
	if len(rels) > 0 {
		if len(patternCtx.AllNodePattern()) != len(rels)+1 {
			return model.MatchPattern{}, fmt.Errorf("relationship pattern requires target node")
		}
		for i, relCtx := range rels {
			rel, err := buildRelationshipPattern(relCtx)
			if err != nil {
				return model.MatchPattern{}, err
			}
			node, err := buildNodePattern(patternCtx.NodePattern(i + 1))
			if err != nil {
				return model.MatchPattern{}, err
			}
			if len(rels) == 1 && rel.Quantifier == nil {
				out.Relationship = &rel
				out.End = &node
				continue
			}
			out.Segments = append(out.Segments, model.PathSegment{Relationship: rel, Node: node})
		}
	}
	return out, nil
}

func buildRelationshipPattern(ctx generated.IRelationshipPatternContext) (model.RelationshipPattern, error) {
	relCtx, ok := ctx.(*generated.RelationshipPatternContext)
	if !ok {
		return model.RelationshipPattern{}, fmt.Errorf("invalid relationship pattern")
	}
	rel := model.RelationshipPattern{Direction: model.RelationshipUndirected}
	if relCtx.GT() != nil {
		rel.Direction = model.RelationshipOutgoing
	} else if relCtx.LT() != nil {
		rel.Direction = model.RelationshipIncoming
	}
	if edge := relCtx.EdgePattern(); edge != nil {
		built, err := buildEdgePattern(edge)
		if err != nil {
			return model.RelationshipPattern{}, err
		}
		built.Direction = rel.Direction
		rel = built
	}
	return rel, nil
}

func buildEdgePattern(ctx generated.IEdgePatternContext) (model.RelationshipPattern, error) {
	edgeCtx, ok := ctx.(*generated.EdgePatternContext)
	if !ok {
		return model.RelationshipPattern{}, fmt.Errorf("invalid edge pattern")
	}
	var edge model.RelationshipPattern
	if variable := edgeCtx.Variable(); variable != nil {
		v, ok := variable.(*generated.VariableContext)
		if !ok || v.IDENTIFIER() == nil {
			return model.RelationshipPattern{}, fmt.Errorf("invalid edge variable")
		}
		edge.Variable = v.IDENTIFIER().GetText()
	}
	if labels := edgeCtx.LabelExpression(); labels != nil {
		labelCtx, ok := labels.(*generated.LabelExpressionContext)
		if !ok {
			return model.RelationshipPattern{}, fmt.Errorf("invalid edge label expression")
		}
		for _, label := range labelCtx.AllLabelName() {
			nameCtx, ok := label.(*generated.LabelNameContext)
			if !ok || nameCtx.IDENTIFIER() == nil {
				return model.RelationshipPattern{}, fmt.Errorf("invalid edge label name")
			}
			edge.Labels = append(edge.Labels, nameCtx.IDENTIFIER().GetText())
		}
	}
	if quant := edgeCtx.RelationshipQuantifier(); quant != nil {
		built, err := buildRelationshipQuantifier(quant)
		if err != nil {
			return model.RelationshipPattern{}, err
		}
		edge.Quantifier = &built
	}
	if props := edgeCtx.PropertyMap(); props != nil {
		propCtx, ok := props.(*generated.PropertyMapContext)
		if !ok {
			return model.RelationshipPattern{}, fmt.Errorf("invalid edge property map")
		}
		for _, pair := range propCtx.AllPropertyPair() {
			prop, err := buildPropertyPair(pair)
			if err != nil {
				return model.RelationshipPattern{}, err
			}
			edge.Properties = append(edge.Properties, prop)
		}
	}
	return edge, nil
}

func buildRelationshipQuantifier(ctx generated.IRelationshipQuantifierContext) (model.RelationshipQuantifier, error) {
	quantCtx, ok := ctx.(*generated.RelationshipQuantifierContext)
	if !ok || len(quantCtx.AllINTEGER()) == 0 {
		return model.RelationshipQuantifier{}, fmt.Errorf("invalid relationship quantifier")
	}
	min, err := strconv.Atoi(quantCtx.INTEGER(0).GetText())
	if err != nil {
		return model.RelationshipQuantifier{}, fmt.Errorf("invalid relationship quantifier min: %w", err)
	}
	max := min
	if len(quantCtx.AllINTEGER()) == 2 {
		max, err = strconv.Atoi(quantCtx.INTEGER(1).GetText())
		if err != nil {
			return model.RelationshipQuantifier{}, fmt.Errorf("invalid relationship quantifier max: %w", err)
		}
	}
	return model.RelationshipQuantifier{Min: min, Max: max}, nil
}

func buildFetchFirstClause(ctx generated.IFetchFirstClauseContext) (model.FetchFirstClause, error) {
	fetchCtx, ok := ctx.(*generated.FetchFirstClauseContext)
	if !ok || fetchCtx.INTEGER() == nil {
		return model.FetchFirstClause{}, fmt.Errorf("invalid fetch first clause")
	}
	count, err := strconv.ParseInt(fetchCtx.INTEGER().GetText(), 10, 64)
	if err != nil {
		return model.FetchFirstClause{}, fmt.Errorf("invalid fetch first count: %w", err)
	}
	return model.FetchFirstClause{Count: count}, nil
}

func buildOrderByClause(ctx generated.IOrderByClauseContext) ([]model.OrderItem, error) {
	orderCtx, ok := ctx.(*generated.OrderByClauseContext)
	if !ok {
		return nil, fmt.Errorf("invalid order by clause")
	}
	items := make([]model.OrderItem, 0, len(orderCtx.AllOrderItem()))
	for _, item := range orderCtx.AllOrderItem() {
		itemCtx, ok := item.(*generated.OrderItemContext)
		if !ok || itemCtx.PropertyReference() == nil {
			return nil, fmt.Errorf("invalid order item")
		}
		field, err := buildFieldReference(itemCtx.PropertyReference())
		if err != nil {
			return nil, err
		}
		direction := model.SortAscending
		if sortCtx := itemCtx.SortDirection(); sortCtx != nil {
			ctx, ok := sortCtx.(*generated.SortDirectionContext)
			if !ok {
				return nil, fmt.Errorf("invalid sort direction")
			}
			if ctx.DESC() != nil {
				direction = model.SortDescending
			}
		}
		items = append(items, model.OrderItem{Variable: field.Variable, Namespace: field.Namespace, Property: field.Property, Direction: direction})
	}
	return items, nil
}

func buildReturnItem(ctx *generated.ReturnItemContext) (model.ReturnItem, error) {
	if prop := ctx.PropertyReference(); prop != nil {
		propCtx, ok := prop.(*generated.PropertyReferenceContext)
		if !ok || len(propCtx.AllIDENTIFIER()) < 2 || len(propCtx.AllIDENTIFIER()) > 3 {
			return model.ReturnItem{}, fmt.Errorf("invalid return property")
		}
		item := model.ReturnItem{Kind: model.ReturnProperty, Variable: propCtx.IDENTIFIER(0).GetText(), Property: propCtx.IDENTIFIER(1).GetText()}
		if len(propCtx.AllIDENTIFIER()) == 3 {
			item.Namespace = propCtx.IDENTIFIER(1).GetText()
			item.Property = propCtx.IDENTIFIER(2).GetText()
		}
		return item, nil
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
	where := model.WhereClause{}
	for _, term := range predicateCtx.AllPredicateTerm() {
		termCtx, ok := term.(*generated.PredicateTermContext)
		if !ok {
			return model.WhereClause{}, fmt.Errorf("invalid predicate term")
		}
		if comparison := termCtx.PropertyComparison(); comparison != nil {
			built, err := buildPropertyComparison(comparison)
			if err != nil {
				return model.WhereClause{}, err
			}
			where.Predicates = append(where.Predicates, built)
			continue
		}
		if text := termCtx.TextContainsPredicate(); text != nil {
			built, err := buildTextContainsPredicate(text)
			if err != nil {
				return model.WhereClause{}, err
			}
			where.TextPredicates = append(where.TextPredicates, built)
			continue
		}
		if semantic := termCtx.SemanticSimilarPredicate(); semantic != nil {
			built, err := buildSemanticSimilarPredicate(semantic)
			if err != nil {
				return model.WhereClause{}, err
			}
			where.SemanticPredicates = append(where.SemanticPredicates, built)
			continue
		}
	}
	return where, nil
}

func buildTextContainsPredicate(ctx generated.ITextContainsPredicateContext) (model.TextContainsPredicate, error) {
	textCtx, ok := ctx.(*generated.TextContainsPredicateContext)
	if !ok || textCtx.PropertyReference() == nil || textCtx.STRING() == nil {
		return model.TextContainsPredicate{}, fmt.Errorf("invalid text contains predicate")
	}
	field, err := buildFieldReference(textCtx.PropertyReference())
	if err != nil {
		return model.TextContainsPredicate{}, err
	}
	query, err := unquoteString(textCtx.STRING().GetText())
	if err != nil {
		return model.TextContainsPredicate{}, err
	}
	return model.TextContainsPredicate{Variable: field.Variable, Namespace: field.Namespace, Property: field.Property, Query: query}, nil
}

func buildSemanticSimilarPredicate(ctx generated.ISemanticSimilarPredicateContext) (model.SemanticSimilarPredicate, error) {
	semanticCtx, ok := ctx.(*generated.SemanticSimilarPredicateContext)
	if !ok || semanticCtx.Variable() == nil || semanticCtx.STRING() == nil || semanticCtx.INTEGER() == nil {
		return model.SemanticSimilarPredicate{}, fmt.Errorf("invalid semantic similar predicate")
	}
	variableCtx, ok := semanticCtx.Variable().(*generated.VariableContext)
	if !ok || variableCtx.IDENTIFIER() == nil {
		return model.SemanticSimilarPredicate{}, fmt.Errorf("invalid semantic target")
	}
	query, err := unquoteString(semanticCtx.STRING().GetText())
	if err != nil {
		return model.SemanticSimilarPredicate{}, err
	}
	topK, err := strconv.ParseInt(semanticCtx.INTEGER().GetText(), 10, 64)
	if err != nil {
		return model.SemanticSimilarPredicate{}, fmt.Errorf("invalid semantic top k: %w", err)
	}
	return model.SemanticSimilarPredicate{Variable: variableCtx.IDENTIFIER().GetText(), Query: query, TopK: topK}, nil
}

type fieldReference struct{ Variable, Namespace, Property string }

func buildFieldReference(ctx generated.IPropertyReferenceContext) (fieldReference, error) {
	propCtx, ok := ctx.(*generated.PropertyReferenceContext)
	if !ok || len(propCtx.AllIDENTIFIER()) < 2 || len(propCtx.AllIDENTIFIER()) > 3 {
		return fieldReference{}, fmt.Errorf("invalid field reference")
	}
	field := fieldReference{Variable: propCtx.IDENTIFIER(0).GetText(), Property: propCtx.IDENTIFIER(1).GetText()}
	if len(propCtx.AllIDENTIFIER()) == 3 {
		field.Namespace = propCtx.IDENTIFIER(1).GetText()
		field.Property = propCtx.IDENTIFIER(2).GetText()
	}
	return field, nil
}

func buildPropertyComparison(ctx generated.IPropertyComparisonContext) (model.PropertyComparison, error) {
	comparisonCtx, ok := ctx.(*generated.PropertyComparisonContext)
	if !ok || len(comparisonCtx.AllIDENTIFIER()) != 2 || comparisonCtx.ComparisonOperator() == nil || comparisonCtx.Value() == nil {
		return model.PropertyComparison{}, fmt.Errorf("invalid property comparison")
	}
	value, err := buildValue(comparisonCtx.Value())
	if err != nil {
		return model.PropertyComparison{}, err
	}
	operator, err := buildComparisonOperator(comparisonCtx.ComparisonOperator())
	if err != nil {
		return model.PropertyComparison{}, err
	}
	return model.PropertyComparison{Variable: comparisonCtx.IDENTIFIER(0).GetText(), Property: comparisonCtx.IDENTIFIER(1).GetText(), Operator: operator, Value: value}, nil
}

func buildComparisonOperator(ctx generated.IComparisonOperatorContext) (model.ComparisonOperator, error) {
	opCtx, ok := ctx.(*generated.ComparisonOperatorContext)
	if !ok {
		return "", fmt.Errorf("invalid comparison operator")
	}
	switch {
	case opCtx.EQ() != nil:
		return "", nil
	case opCtx.NEQ() != nil:
		return model.ComparisonNotEqual, nil
	case opCtx.LT() != nil:
		return model.ComparisonLessThan, nil
	case opCtx.LTE() != nil:
		return model.ComparisonLessThanOrEqual, nil
	case opCtx.GT() != nil:
		return model.ComparisonGreaterThan, nil
	case opCtx.GTE() != nil:
		return model.ComparisonGreaterThanOrEqual, nil
	default:
		return "", fmt.Errorf("unsupported comparison operator")
	}
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
