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
	if mergeNode := stmtCtx.MergeNodeStatement(); mergeNode != nil {
		return buildMergeNodeStatement(mergeNode)
	}
	if matchCreate := stmtCtx.MatchCreateStatement(); matchCreate != nil {
		return buildMatchCreateStatement(matchCreate)
	}
	if matchSet := stmtCtx.MatchSetStatement(); matchSet != nil {
		return buildMatchSetStatement(matchSet)
	}
	if matchDelete := stmtCtx.MatchDeleteStatement(); matchDelete != nil {
		return buildMatchDeleteStatement(matchDelete)
	}
	if matchMerge := stmtCtx.MatchMergeRelationshipStatement(); matchMerge != nil {
		return buildMatchMergeRelationshipStatement(matchMerge)
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

func buildMergeNodeStatement(ctx generated.IMergeNodeStatementContext) (model.Query, error) {
	mergeCtx, ok := ctx.(*generated.MergeNodeStatementContext)
	if !ok || mergeCtx.NodePattern() == nil {
		return model.Query{}, fmt.Errorf("expected merge node statement")
	}
	node, err := buildNodePattern(mergeCtx.NodePattern())
	if err != nil {
		return model.Query{}, err
	}
	returns, err := buildReturnProjections(mergeCtx.AllReturnProjection())
	if err != nil {
		return model.Query{}, err
	}
	fetchFirst, err := optionalFetchFirst(mergeCtx.FetchFirstClause())
	if err != nil {
		return model.Query{}, err
	}
	return model.Query{Statement: model.MergeNodeStatement{Pattern: node, Returns: returns, ReturnGraph: mergeCtx.GRAPH() != nil, FetchFirst: fetchFirst}}, nil
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

func buildMatchSetStatement(ctx generated.IMatchSetStatementContext) (model.Query, error) {
	matchCtx, ok := ctx.(*generated.MatchSetStatementContext)
	if !ok || matchCtx.MatchPattern() == nil || len(matchCtx.AllSetAssignment()) == 0 {
		return model.Query{}, fmt.Errorf("expected match set statement")
	}
	matchPattern, err := buildMatchPattern(matchCtx.MatchPattern())
	if err != nil {
		return model.Query{}, err
	}
	assignments := make([]model.SetAssignment, 0, len(matchCtx.AllSetAssignment()))
	for _, assignmentCtx := range matchCtx.AllSetAssignment() {
		assignment, err := buildSetAssignment(assignmentCtx)
		if err != nil {
			return model.Query{}, err
		}
		assignments = append(assignments, assignment)
	}
	returns, err := buildReturnProjections(matchCtx.AllReturnProjection())
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
	var fetchFirst *model.FetchFirstClause
	if fetchCtx := matchCtx.FetchFirstClause(); fetchCtx != nil {
		built, err := buildFetchFirstClause(fetchCtx)
		if err != nil {
			return model.Query{}, err
		}
		fetchFirst = &built
	}
	return model.Query{Statement: model.MatchSetStatement{MatchPattern: matchPattern, Where: where, Assignments: assignments, Returns: returns, ReturnGraph: matchCtx.GRAPH() != nil, FetchFirst: fetchFirst}}, nil
}

func buildMatchDeleteStatement(ctx generated.IMatchDeleteStatementContext) (model.Query, error) {
	deleteCtx, ok := ctx.(*generated.MatchDeleteStatementContext)
	if !ok || deleteCtx.MatchPattern() == nil || len(deleteCtx.AllVariable()) == 0 {
		return model.Query{}, fmt.Errorf("expected match delete statement")
	}
	matchPattern, err := buildMatchPattern(deleteCtx.MatchPattern())
	if err != nil {
		return model.Query{}, err
	}
	targets := make([]string, 0, len(deleteCtx.AllVariable()))
	for _, variable := range deleteCtx.AllVariable() {
		v, ok := variable.(*generated.VariableContext)
		if !ok || v.IDENTIFIER() == nil {
			return model.Query{}, fmt.Errorf("invalid delete variable")
		}
		targets = append(targets, v.IDENTIFIER().GetText())
	}
	returns, err := buildReturnProjections(deleteCtx.AllReturnProjection())
	if err != nil {
		return model.Query{}, err
	}
	where, err := optionalWhere(deleteCtx.WhereClause())
	if err != nil {
		return model.Query{}, err
	}
	fetchFirst, err := optionalFetchFirst(deleteCtx.FetchFirstClause())
	if err != nil {
		return model.Query{}, err
	}
	return model.Query{Statement: model.MatchDeleteStatement{MatchPattern: matchPattern, Where: where, Targets: targets, Returns: returns, ReturnGraph: deleteCtx.GRAPH() != nil, FetchFirst: fetchFirst}}, nil
}

func buildMatchMergeRelationshipStatement(ctx generated.IMatchMergeRelationshipStatementContext) (model.Query, error) {
	mergeCtx, ok := ctx.(*generated.MatchMergeRelationshipStatementContext)
	if !ok || len(mergeCtx.AllNodePattern()) < 2 || mergeCtx.CreateRelationshipPattern() == nil {
		return model.Query{}, fmt.Errorf("expected match merge relationship statement")
	}
	matches := make([]model.NodePattern, 0, len(mergeCtx.AllNodePattern()))
	for _, nodeCtx := range mergeCtx.AllNodePattern() {
		node, err := buildNodePattern(nodeCtx)
		if err != nil {
			return model.Query{}, err
		}
		matches = append(matches, node)
	}
	merge, err := buildCreateRelationshipPattern(mergeCtx.CreateRelationshipPattern())
	if err != nil {
		return model.Query{}, err
	}
	returns, err := buildReturnProjections(mergeCtx.AllReturnProjection())
	if err != nil {
		return model.Query{}, err
	}
	fetchFirst, err := optionalFetchFirst(mergeCtx.FetchFirstClause())
	if err != nil {
		return model.Query{}, err
	}
	return model.Query{Statement: model.MatchMergeRelationshipStatement{Matches: matches, Merge: merge, Returns: returns, ReturnGraph: mergeCtx.GRAPH() != nil, FetchFirst: fetchFirst}}, nil
}

func buildSetAssignment(ctx generated.ISetAssignmentContext) (model.SetAssignment, error) {
	assignmentCtx, ok := ctx.(*generated.SetAssignmentContext)
	if !ok || assignmentCtx.PropertyReference() == nil || assignmentCtx.Value() == nil {
		return model.SetAssignment{}, fmt.Errorf("invalid set assignment")
	}
	field, err := buildFieldReference(assignmentCtx.PropertyReference())
	if err != nil {
		return model.SetAssignment{}, err
	}
	value, err := buildValue(assignmentCtx.Value())
	if err != nil {
		return model.SetAssignment{}, err
	}
	return model.SetAssignment{Variable: field.Variable, Namespace: field.Namespace, Property: field.Property, Value: value}, nil
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
	if pathBinding := matchCtx.PathBinding(); pathBinding != nil {
		pathVariable, err := buildPathBinding(pathBinding)
		if err != nil {
			return model.Query{}, err
		}
		matchPattern.PathVariable = pathVariable
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
	fetchFirst, err := optionalFetchFirst(matchCtx.FetchFirstClause())
	if err != nil {
		return model.Query{}, err
	}
	offset, err := optionalOffset(matchCtx.OffsetClause())
	if err != nil {
		return model.Query{}, err
	}
	returns, err := buildReturnProjections(matchCtx.AllReturnProjection())
	if err != nil {
		return model.Query{}, err
	}
	var orderBy []model.OrderItem
	if orderCtx := matchCtx.OrderByClause(); orderCtx != nil {
		built, err := buildOrderByClause(orderCtx)
		if err != nil {
			return model.Query{}, err
		}
		orderBy = built
	}
	stmt := model.MatchStatement{Pattern: node, Where: where, Returns: returns, ReturnGraph: matchCtx.GRAPH() != nil, Distinct: matchCtx.DISTINCT() != nil, OrderBy: orderBy, Offset: offset, FetchFirst: fetchFirst}
	if matchPattern.Relationship != nil || len(matchPattern.Segments) > 0 {
		stmt.MatchPattern = matchPattern
	}
	return model.Query{Statement: stmt}, nil
}

func buildPathBinding(ctx generated.IPathBindingContext) (string, error) {
	bindingCtx, ok := ctx.(*generated.PathBindingContext)
	if !ok || bindingCtx.Variable() == nil {
		return "", fmt.Errorf("invalid path binding")
	}
	variableCtx, ok := bindingCtx.Variable().(*generated.VariableContext)
	if !ok || variableCtx.IDENTIFIER() == nil {
		return "", fmt.Errorf("invalid path binding variable")
	}
	return variableCtx.IDENTIFIER().GetText(), nil
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
			if !ok || strings.TrimSpace(nameCtx.GetText()) == "" {
				return model.RelationshipPattern{}, fmt.Errorf("invalid edge label name")
			}
			edge.Labels = append(edge.Labels, nameCtx.GetText())
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
	} else if strings.Contains(quantCtx.GetText(), "..") {
		max = -1
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

func optionalWhere(ctx generated.IWhereClauseContext) (*model.WhereClause, error) {
	if ctx == nil {
		return nil, nil
	}
	built, err := buildWhereClause(ctx)
	if err != nil {
		return nil, err
	}
	return &built, nil
}

func optionalFetchFirst(ctx generated.IFetchFirstClauseContext) (*model.FetchFirstClause, error) {
	if ctx == nil {
		return nil, nil
	}
	built, err := buildFetchFirstClause(ctx)
	if err != nil {
		return nil, err
	}
	return &built, nil
}

func optionalOffset(ctx generated.IOffsetClauseContext) (*model.OffsetClause, error) {
	if ctx == nil {
		return nil, nil
	}
	offsetCtx, ok := ctx.(*generated.OffsetClauseContext)
	if !ok || offsetCtx.INTEGER() == nil {
		return nil, fmt.Errorf("invalid offset clause")
	}
	count, err := strconv.ParseInt(offsetCtx.INTEGER().GetText(), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid offset count: %w", err)
	}
	return &model.OffsetClause{Count: count}, nil
}

func buildReturnProjections(items []generated.IReturnProjectionContext) ([]model.ReturnItem, error) {
	returns := make([]model.ReturnItem, 0, len(items))
	for _, item := range items {
		projCtx, ok := item.(*generated.ReturnProjectionContext)
		if !ok || projCtx.ReturnItem() == nil {
			return nil, fmt.Errorf("invalid return projection")
		}
		returnCtx, ok := projCtx.ReturnItem().(*generated.ReturnItemContext)
		if !ok {
			return nil, fmt.Errorf("invalid return item")
		}
		built, err := buildReturnItem(returnCtx)
		if err != nil {
			return nil, err
		}
		if projCtx.AS() != nil {
			if projCtx.IdentifierName() == nil {
				return nil, fmt.Errorf("invalid return alias")
			}
			built.OutputName = projCtx.IdentifierName().GetText()
		}
		returns = append(returns, built)
	}
	return returns, nil
}

func buildReturnItem(ctx *generated.ReturnItemContext) (model.ReturnItem, error) {
	if aggregate := ctx.AggregateFunction(); aggregate != nil {
		aggCtx, ok := aggregate.(*generated.AggregateFunctionContext)
		if !ok {
			return model.ReturnItem{}, fmt.Errorf("invalid aggregate function")
		}
		nameCtx := aggCtx.AggregateName()
		if nameCtx == nil {
			return model.ReturnItem{}, fmt.Errorf("invalid aggregate function")
		}
		item := model.ReturnItem{Kind: model.ReturnAggregate, Aggregate: strings.ToLower(nameCtx.GetText())}
		if aggCtx.STAR() != nil {
			item.AggregateStar = true
			return item, nil
		}
		if prop := aggCtx.PropertyReference(); prop != nil {
			propCtx, ok := prop.(*generated.PropertyReferenceContext)
			if !ok || len(propCtx.AllIDENTIFIER()) < 2 || len(propCtx.AllIDENTIFIER()) > 3 {
				return model.ReturnItem{}, fmt.Errorf("invalid aggregate property")
			}
			item.AggregateAlias = propCtx.IDENTIFIER(0).GetText()
			item.AggregateProperty = propCtx.IDENTIFIER(1).GetText()
			if len(propCtx.AllIDENTIFIER()) == 3 {
				item.AggregateNamespace = propCtx.IDENTIFIER(1).GetText()
				item.AggregateProperty = propCtx.IDENTIFIER(2).GetText()
			}
			return item, nil
		}
		variableCtx, ok := aggCtx.Variable().(*generated.VariableContext)
		if !ok || variableCtx.IDENTIFIER() == nil {
			return model.ReturnItem{}, fmt.Errorf("invalid aggregate variable")
		}
		item.AggregateAlias = variableCtx.IDENTIFIER().GetText()
		return item, nil
	}
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
	expr, err := buildPredicate(whereCtx.Predicate())
	if err != nil {
		return model.WhereClause{}, err
	}
	where := model.WhereClause{}
	if predicateContainsOp(expr, model.PredicateOr) {
		where.Expr = &expr
	} else {
		populateLegacyPredicateSlices(&where, expr)
	}
	return where, nil
}

func predicateContainsOp(expr model.PredicateExpr, op model.PredicateOp) bool {
	if expr.Op == op {
		return true
	}
	for _, term := range expr.Terms {
		if predicateContainsOp(term, op) {
			return true
		}
	}
	return false
}

func populateLegacyPredicateSlices(where *model.WhereClause, expr model.PredicateExpr) {
	if expr.Op == model.PredicateAnd {
		for _, term := range expr.Terms {
			populateLegacyPredicateSlices(where, term)
		}
		return
	}
	if expr.Op != model.PredicateLeaf || expr.Leaf == nil {
		return
	}
	switch expr.Leaf.Kind {
	case model.PredicateLeafComparison:
		if expr.Leaf.Comparison != nil {
			where.Predicates = append(where.Predicates, *expr.Leaf.Comparison)
		}
	case model.PredicateLeafNull:
		if expr.Leaf.Null != nil {
			where.NullPredicates = append(where.NullPredicates, *expr.Leaf.Null)
		}
	case model.PredicateLeafString:
		if expr.Leaf.String != nil {
			where.StringPredicates = append(where.StringPredicates, *expr.Leaf.String)
		}
	case model.PredicateLeafText:
		if expr.Leaf.Text != nil {
			where.TextPredicates = append(where.TextPredicates, *expr.Leaf.Text)
		}
	case model.PredicateLeafSemantic:
		if expr.Leaf.Semantic != nil {
			where.SemanticPredicates = append(where.SemanticPredicates, *expr.Leaf.Semantic)
		}
	}
}

func buildPredicate(ctx generated.IPredicateContext) (model.PredicateExpr, error) {
	predicateCtx, ok := ctx.(*generated.PredicateContext)
	if !ok || predicateCtx.PredicateOr() == nil {
		return model.PredicateExpr{}, fmt.Errorf("invalid predicate")
	}
	return buildPredicateOr(predicateCtx.PredicateOr())
}

func buildPredicateOr(ctx generated.IPredicateOrContext) (model.PredicateExpr, error) {
	orCtx, ok := ctx.(*generated.PredicateOrContext)
	if !ok {
		return model.PredicateExpr{}, fmt.Errorf("invalid OR predicate")
	}
	terms := make([]model.PredicateExpr, 0, len(orCtx.AllPredicateAnd()))
	for _, child := range orCtx.AllPredicateAnd() {
		built, err := buildPredicateAnd(child)
		if err != nil {
			return model.PredicateExpr{}, err
		}
		terms = append(terms, built)
	}
	if len(terms) == 1 {
		return terms[0], nil
	}
	return model.PredicateExpr{Op: model.PredicateOr, Terms: terms}, nil
}

func buildPredicateAnd(ctx generated.IPredicateAndContext) (model.PredicateExpr, error) {
	andCtx, ok := ctx.(*generated.PredicateAndContext)
	if !ok {
		return model.PredicateExpr{}, fmt.Errorf("invalid AND predicate")
	}
	terms := make([]model.PredicateExpr, 0, len(andCtx.AllPredicateFactor()))
	for _, child := range andCtx.AllPredicateFactor() {
		built, err := buildPredicateFactor(child)
		if err != nil {
			return model.PredicateExpr{}, err
		}
		terms = append(terms, built)
	}
	if len(terms) == 1 {
		return terms[0], nil
	}
	return model.PredicateExpr{Op: model.PredicateAnd, Terms: terms}, nil
}

func buildPredicateFactor(ctx generated.IPredicateFactorContext) (model.PredicateExpr, error) {
	factorCtx, ok := ctx.(*generated.PredicateFactorContext)
	if !ok {
		return model.PredicateExpr{}, fmt.Errorf("invalid predicate factor")
	}
	if factorCtx.Predicate() != nil {
		return buildPredicate(factorCtx.Predicate())
	}
	return buildPredicateTermExpr(factorCtx.PredicateTerm())
}

func buildPredicateTermExpr(ctx generated.IPredicateTermContext) (model.PredicateExpr, error) {
	termCtx, ok := ctx.(*generated.PredicateTermContext)
	if !ok {
		return model.PredicateExpr{}, fmt.Errorf("invalid predicate term")
	}
	leaf := model.PredicateLeafExpr{}
	if between := termCtx.PropertyBetween(); between != nil {
		low, high, err := buildPropertyBetween(between)
		if err != nil {
			return model.PredicateExpr{}, err
		}
		return model.PredicateExpr{Op: model.PredicateAnd, Terms: []model.PredicateExpr{{Op: model.PredicateLeaf, Leaf: &model.PredicateLeafExpr{Kind: model.PredicateLeafComparison, Comparison: &low}}, {Op: model.PredicateLeaf, Leaf: &model.PredicateLeafExpr{Kind: model.PredicateLeafComparison, Comparison: &high}}}}, nil
	}
	if comparison := termCtx.PropertyComparison(); comparison != nil {
		built, err := buildPropertyComparison(comparison)
		if err != nil {
			return model.PredicateExpr{}, err
		}
		leaf.Kind = model.PredicateLeafComparison
		leaf.Comparison = &built
		return model.PredicateExpr{Op: model.PredicateLeaf, Leaf: &leaf}, nil
	}
	if nullPred := termCtx.PropertyNullPredicate(); nullPred != nil {
		built, err := buildPropertyNullPredicate(nullPred)
		if err != nil {
			return model.PredicateExpr{}, err
		}
		leaf.Kind = model.PredicateLeafNull
		leaf.Null = &built
		return model.PredicateExpr{Op: model.PredicateLeaf, Leaf: &leaf}, nil
	}
	if stringPred := termCtx.PropertyStringPredicate(); stringPred != nil {
		built, err := buildPropertyStringPredicate(stringPred)
		if err != nil {
			return model.PredicateExpr{}, err
		}
		leaf.Kind = model.PredicateLeafString
		leaf.String = &built
		return model.PredicateExpr{Op: model.PredicateLeaf, Leaf: &leaf}, nil
	}
	if text := termCtx.TextContainsPredicate(); text != nil {
		built, err := buildTextContainsPredicate(text)
		if err != nil {
			return model.PredicateExpr{}, err
		}
		leaf.Kind = model.PredicateLeafText
		leaf.Text = &built
		return model.PredicateExpr{Op: model.PredicateLeaf, Leaf: &leaf}, nil
	}
	if semantic := termCtx.SemanticSimilarPredicate(); semantic != nil {
		built, err := buildSemanticSimilarPredicate(semantic)
		if err != nil {
			return model.PredicateExpr{}, err
		}
		leaf.Kind = model.PredicateLeafSemantic
		leaf.Semantic = &built
		return model.PredicateExpr{Op: model.PredicateLeaf, Leaf: &leaf}, nil
	}
	return model.PredicateExpr{}, fmt.Errorf("unsupported predicate term")
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

func buildPropertyBetween(ctx generated.IPropertyBetweenContext) (model.PropertyComparison, model.PropertyComparison, error) {
	betweenCtx, ok := ctx.(*generated.PropertyBetweenContext)
	if !ok || betweenCtx.PropertyReference() == nil || len(betweenCtx.AllValue()) != 2 {
		return model.PropertyComparison{}, model.PropertyComparison{}, fmt.Errorf("invalid property between")
	}
	field, err := buildFieldReference(betweenCtx.PropertyReference())
	if err != nil {
		return model.PropertyComparison{}, model.PropertyComparison{}, err
	}
	lowValue, err := buildValue(betweenCtx.Value(0))
	if err != nil {
		return model.PropertyComparison{}, model.PropertyComparison{}, err
	}
	highValue, err := buildValue(betweenCtx.Value(1))
	if err != nil {
		return model.PropertyComparison{}, model.PropertyComparison{}, err
	}
	return model.PropertyComparison{Variable: field.Variable, Property: field.Property, Operator: model.ComparisonGreaterThanOrEqual, Value: lowValue}, model.PropertyComparison{Variable: field.Variable, Property: field.Property, Operator: model.ComparisonLessThanOrEqual, Value: highValue}, nil
}

func buildPropertyComparison(ctx generated.IPropertyComparisonContext) (model.PropertyComparison, error) {
	comparisonCtx, ok := ctx.(*generated.PropertyComparisonContext)
	if !ok || comparisonCtx.PropertyReference() == nil || comparisonCtx.ComparisonOperator() == nil || comparisonCtx.Value() == nil {
		return model.PropertyComparison{}, fmt.Errorf("invalid property comparison")
	}
	field, err := buildFieldReference(comparisonCtx.PropertyReference())
	if err != nil {
		return model.PropertyComparison{}, err
	}
	value, err := buildValue(comparisonCtx.Value())
	if err != nil {
		return model.PropertyComparison{}, err
	}
	operator, err := buildComparisonOperator(comparisonCtx.ComparisonOperator())
	if err != nil {
		return model.PropertyComparison{}, err
	}
	return model.PropertyComparison{Variable: field.Variable, Property: field.Property, Operator: operator, Value: value}, nil
}

func buildPropertyNullPredicate(ctx generated.IPropertyNullPredicateContext) (model.NullPredicate, error) {
	nullCtx, ok := ctx.(*generated.PropertyNullPredicateContext)
	if !ok || nullCtx.PropertyReference() == nil {
		return model.NullPredicate{}, fmt.Errorf("invalid null predicate")
	}
	field, err := buildFieldReference(nullCtx.PropertyReference())
	if err != nil {
		return model.NullPredicate{}, err
	}
	return model.NullPredicate{Variable: field.Variable, Namespace: field.Namespace, Property: field.Property, IsNull: nullCtx.NOT() == nil}, nil
}

func buildPropertyStringPredicate(ctx generated.IPropertyStringPredicateContext) (model.StringPredicate, error) {
	stringCtx, ok := ctx.(*generated.PropertyStringPredicateContext)
	if !ok || stringCtx.PropertyReference() == nil || stringCtx.StringPredicateOperator() == nil || stringCtx.STRING() == nil {
		return model.StringPredicate{}, fmt.Errorf("invalid string predicate")
	}
	field, err := buildFieldReference(stringCtx.PropertyReference())
	if err != nil {
		return model.StringPredicate{}, err
	}
	query, err := unquoteString(stringCtx.STRING().GetText())
	if err != nil {
		return model.StringPredicate{}, err
	}
	op, err := buildStringPredicateOperator(stringCtx.StringPredicateOperator())
	if err != nil {
		return model.StringPredicate{}, err
	}
	return model.StringPredicate{Variable: field.Variable, Namespace: field.Namespace, Property: field.Property, Operator: op, Query: query}, nil
}

func buildStringPredicateOperator(ctx generated.IStringPredicateOperatorContext) (model.StringPredicateOperator, error) {
	opCtx, ok := ctx.(*generated.StringPredicateOperatorContext)
	if !ok {
		return "", fmt.Errorf("invalid string predicate operator")
	}
	switch {
	case opCtx.CONTAINS() != nil:
		return model.StringContains, nil
	case opCtx.STARTS() != nil:
		return model.StringStartsWith, nil
	case opCtx.ENDS() != nil:
		return model.StringEndsWith, nil
	default:
		return "", fmt.Errorf("unsupported string predicate operator")
	}
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
			if !ok || strings.TrimSpace(nameCtx.GetText()) == "" {
				return model.NodePattern{}, fmt.Errorf("invalid label name")
			}
			node.Labels = append(node.Labels, nameCtx.GetText())
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
	if !ok || strings.TrimSpace(keyCtx.GetText()) == "" {
		return model.Property{}, fmt.Errorf("invalid property key")
	}
	value, err := buildValue(pairCtx.Value())
	if err != nil {
		return model.Property{}, fmt.Errorf("property %q: %w", keyCtx.GetText(), err)
	}
	return model.Property{Key: keyCtx.GetText(), Value: value}, nil
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
	case valueCtx.PARAMETER() != nil:
		return model.Value{Kind: model.ParameterValue, Value: strings.TrimPrefix(valueCtx.PARAMETER().GetText(), "$")}, nil
	default:
		return model.Value{}, fmt.Errorf("unsupported value %q", valueCtx.GetText())
	}
}
