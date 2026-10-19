package execution

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strings"
	"time"

	graph "github.com/myceldb/mycel/internal/graph/model"
	querylogical "github.com/myceldb/mycel/internal/query/logical"
)

type evaluator struct {
	parameters map[string]querylogical.Value
	now        time.Time
}

func newEvaluator(parameters map[string]querylogical.Value, now time.Time) evaluator {
	if now.IsZero() {
		now = time.Now()
	}
	return evaluator{parameters: parameters, now: now}
}

func (e evaluator) predicate(row BindingRow, predicate *querylogical.Predicate) (bool, error) {
	if predicate == nil {
		return true, nil
	}
	switch predicate.Op {
	case querylogical.PredicateAndOp:
		for i := range predicate.Terms {
			matched, err := e.predicate(row, &predicate.Terms[i])
			if err != nil || !matched {
				return matched, err
			}
		}
		return true, nil
	case querylogical.PredicateOrOp:
		for i := range predicate.Terms {
			matched, err := e.predicate(row, &predicate.Terms[i])
			if err != nil {
				return false, err
			}
			if matched {
				return true, nil
			}
		}
		return false, nil
	case querylogical.PredicateLeafOp:
		if predicate.Leaf == nil {
			return false, executionInvalid("evaluate predicate", "leaf predicate is missing")
		}
		return e.leaf(row, *predicate.Leaf)
	default:
		return false, executionInvalid("evaluate predicate", fmt.Sprintf("unsupported predicate operation %q", predicate.Op))
	}
}

func (e evaluator) leaf(row BindingRow, leaf querylogical.Leaf) (bool, error) {
	switch leaf.Kind {
	case querylogical.LeafComparison:
		left, leftExists, err := e.valueWithExistence(row, dereferenceValue(leaf.Left))
		if err != nil {
			return false, err
		}
		right, rightExists, err := e.valueWithExistence(row, dereferenceValue(leaf.Right))
		if err != nil {
			return false, err
		}
		if !leftExists || !rightExists {
			return false, nil
		}
		comparison := compareValues(left, right)
		switch leaf.Operator {
		case "=":
			return comparison == 0, nil
		case "!=":
			return comparison != 0, nil
		case "<":
			return comparison < 0, nil
		case "<=":
			return comparison <= 0, nil
		case ">":
			return comparison > 0, nil
		case ">=":
			return comparison >= 0, nil
		default:
			return false, executionInvalid("evaluate predicate", fmt.Sprintf("unsupported comparison operator %q", leaf.Operator))
		}
	case querylogical.LeafBetween:
		value, valueExists, err := e.valueWithExistence(row, dereferenceValue(leaf.Left))
		if err != nil || !valueExists {
			return false, err
		}
		low, lowExists, err := e.valueWithExistence(row, dereferenceValue(leaf.Low))
		if err != nil || !lowExists {
			return false, err
		}
		high, highExists, err := e.valueWithExistence(row, dereferenceValue(leaf.High))
		if err != nil || !highExists {
			return false, err
		}
		return compareValues(value, low) >= 0 && compareValues(value, high) <= 0, nil
	case querylogical.LeafNull:
		value, exists, err := e.field(row, dereferenceField(leaf.Field))
		if err != nil {
			return false, err
		}
		isNull := !exists || value.Kind == querylogical.ValueNull
		return isNull == leaf.IsNull, nil
	case querylogical.LeafPropertyExists:
		_, exists, err := e.field(row, dereferenceField(leaf.Field))
		return exists, err
	case querylogical.LeafString:
		value, err := e.value(row, dereferenceValue(leaf.Operand))
		if err != nil {
			return false, err
		}
		actual := strings.ToLower(valueString(value))
		query := strings.ToLower(leaf.Query)
		switch leaf.Operator {
		case "contains":
			return strings.Contains(actual, query), nil
		case "starts_with":
			return strings.HasPrefix(actual, query), nil
		case "ends_with":
			return strings.HasSuffix(actual, query), nil
		default:
			return false, executionInvalid("evaluate predicate", fmt.Sprintf("unsupported string operator %q", leaf.Operator))
		}
	case querylogical.LeafText, querylogical.LeafSemantic:
		value, exists, err := e.field(row, dereferenceField(leaf.Field))
		if err != nil || !exists {
			return false, err
		}
		return strings.Contains(strings.ToLower(valueString(value)), strings.ToLower(leaf.Query)), nil
	case querylogical.LeafHasTag:
		return hasTag(row, dereferenceField(leaf.Field).Alias, leaf.Tag)
	default:
		return false, executionInvalid("evaluate predicate", fmt.Sprintf("unsupported predicate kind %q", leaf.Kind))
	}
}

func (e evaluator) valueWithExistence(row BindingRow, value querylogical.Value) (querylogical.Value, bool, error) {
	switch value.Kind {
	case querylogical.ValueField:
		return e.field(row, dereferenceField(value.Field))
	case querylogical.ValueParameter:
		resolved, ok := e.parameters[value.Parameter]
		if !ok {
			return querylogical.Value{}, false, executionInvalid("evaluate value", fmt.Sprintf("missing parameter %q", value.Parameter))
		}
		return e.valueWithExistence(row, resolved)
	default:
		resolved, err := e.value(row, value)
		return resolved, true, err
	}
}

func (e evaluator) value(row BindingRow, value querylogical.Value) (querylogical.Value, error) {
	switch value.Kind {
	case querylogical.ValueField:
		resolved, _, err := e.field(row, dereferenceField(value.Field))
		return resolved, err
	case querylogical.ValueParameter:
		resolved, ok := e.parameters[value.Parameter]
		if !ok {
			return querylogical.Value{}, executionInvalid("evaluate value", fmt.Sprintf("missing parameter %q", value.Parameter))
		}
		return e.value(row, resolved)
	case querylogical.ValueCurrentDate:
		day := time.Date(e.now.Year(), e.now.Month(), e.now.Day(), 0, 0, 0, 0, e.now.Location()).AddDate(0, 0, int(value.OffsetDays))
		return querylogical.Value{Kind: querylogical.ValueDate, String: day.Format("2006-01-02")}, nil
	case querylogical.ValueDate:
		day, err := time.Parse("2006-01-02", value.String)
		if err != nil {
			return querylogical.Value{}, executionInvalid("evaluate value", err.Error())
		}
		day = day.AddDate(0, 0, int(value.OffsetDays))
		return querylogical.Value{Kind: querylogical.ValueDate, String: day.Format("2006-01-02")}, nil
	case querylogical.ValueBinding:
		if node, ok := row.Nodes[value.Binding]; ok {
			return querylogical.Value{Kind: querylogical.ValueString, String: node.ID.String()}, nil
		}
		if edge, ok := row.Edges[value.Binding]; ok {
			return querylogical.Value{Kind: querylogical.ValueString, String: edge.ID.String()}, nil
		}
		return querylogical.Value{}, executionInvalid("evaluate value", fmt.Sprintf("alias %q is not bound", value.Binding))
	case querylogical.ValueNull, querylogical.ValueBool, querylogical.ValueNumber, querylogical.ValueInteger,
		querylogical.ValueUnsigned, querylogical.ValueString, querylogical.ValueList, querylogical.ValueObject:
		return value, nil
	default:
		return querylogical.Value{}, executionInvalid("evaluate value", fmt.Sprintf("unsupported value kind %q", value.Kind))
	}
}

func (e evaluator) field(row BindingRow, field querylogical.FieldRef) (querylogical.Value, bool, error) {
	if node, ok := row.Nodes[field.Alias]; ok {
		value, exists := nodeField(node, field.Namespace, field.Name)
		return nativeFieldValue(value, exists), exists, nil
	}
	if edge, ok := row.Edges[field.Alias]; ok {
		value, exists := edgeField(edge, field.Namespace, field.Name)
		return nativeFieldValue(value, exists), exists, nil
	}
	return querylogical.Value{}, false, executionInvalid("evaluate field", fmt.Sprintf("alias %q is not bound", field.Alias))
}

func nodeField(node graph.Node, namespace querylogical.FieldNamespace, name string) (any, bool) {
	switch namespace {
	case querylogical.NamespaceProperties, "":
		switch name {
		case "node_id":
			return node.ID.String(), true
		case "content":
			return graph.PayloadText(node), true
		}
		return graph.Property(node, name)
	case querylogical.NamespacePayload:
		value, ok := node.Payload[name]
		if !ok && name == "text" && node.Content != "" {
			return node.Content, true
		}
		return value, ok
	case querylogical.NamespaceMeta:
		value, ok := node.Meta[name]
		return value, ok
	default:
		return nil, false
	}
}

func edgeField(edge graph.Edge, namespace querylogical.FieldNamespace, name string) (any, bool) {
	var values map[string]any
	switch namespace {
	case querylogical.NamespaceProperties, "":
		switch name {
		case "edge_id":
			return edge.ID.String(), true
		case "from_node_id":
			return edge.FromID.String(), true
		case "to_node_id":
			return edge.ToID.String(), true
		}
		values = edge.Properties
	case querylogical.NamespacePayload:
		values = edge.Payload
	case querylogical.NamespaceMeta:
		values = edge.Meta
	default:
		return nil, false
	}
	value, ok := values[name]
	return value, ok
}

func nativeFieldValue(value any, exists bool) querylogical.Value {
	if !exists || value == nil {
		return querylogical.Value{Kind: querylogical.ValueNull}
	}
	return querylogical.NativeValue(value)
}

func hasTag(row BindingRow, alias string, tag string) (bool, error) {
	node, ok := row.Nodes[alias]
	if !ok {
		return false, executionInvalid("evaluate tag", fmt.Sprintf("node alias %q is not bound", alias))
	}
	want, err := graph.NormalizeTag(tag)
	if err != nil {
		return false, executionInvalid("evaluate tag", err.Error())
	}
	value, _ := graph.Property(node, graph.NodePropTags)
	tags, err := graph.NormalizeTagsValue(value)
	if err != nil {
		return false, nil
	}
	for _, candidate := range tags {
		if candidate == want {
			return true, nil
		}
	}
	return false, nil
}

func compareValues(left querylogical.Value, right querylogical.Value) int {
	if left.Kind == querylogical.ValueNull && right.Kind == querylogical.ValueNull {
		return 0
	}
	if left.Kind == querylogical.ValueBool && right.Kind == querylogical.ValueBool {
		switch {
		case !left.Bool && right.Bool:
			return -1
		case left.Bool && !right.Bool:
			return 1
		default:
			return 0
		}
	}
	if leftDate, ok := comparableDate(left); ok {
		if rightDate, rightOK := comparableDate(right); rightOK {
			return strings.Compare(leftDate, rightDate)
		}
	}
	if leftNumber, ok := exactNumericValue(left); ok {
		if rightNumber, rightOK := exactNumericValue(right); rightOK {
			return leftNumber.Cmp(rightNumber)
		}
	}
	return strings.Compare(valueString(left), valueString(right))
}

func comparableDate(value querylogical.Value) (string, bool) {
	if value.Kind != querylogical.ValueDate && value.Kind != querylogical.ValueString {
		return "", false
	}
	parsed, err := time.Parse("2006-01-02", value.String)
	if err != nil {
		return "", false
	}
	return parsed.Format("2006-01-02"), true
}

func exactNumericValue(value querylogical.Value) (*big.Rat, bool) {
	switch value.Kind {
	case querylogical.ValueNumber:
		if math.IsNaN(value.Number) || math.IsInf(value.Number, 0) {
			return nil, false
		}
		out := new(big.Rat)
		out.SetFloat64(value.Number)
		return out, true
	case querylogical.ValueInteger:
		return new(big.Rat).SetInt64(value.Integer), true
	case querylogical.ValueUnsigned:
		integer := new(big.Int).SetUint64(value.Unsigned)
		return new(big.Rat).SetInt(integer), true
	default:
		return nil, false
	}
}

func numericValue(value querylogical.Value) (float64, bool) {
	switch value.Kind {
	case querylogical.ValueNumber:
		return value.Number, true
	case querylogical.ValueInteger:
		return float64(value.Integer), true
	case querylogical.ValueUnsigned:
		return float64(value.Unsigned), true
	default:
		return 0, false
	}
}

func valueString(value querylogical.Value) string {
	switch value.Kind {
	case querylogical.ValueString, querylogical.ValueDate:
		return value.String
	case querylogical.ValueBool:
		return fmt.Sprint(value.Bool)
	case querylogical.ValueNumber:
		return fmt.Sprint(value.Number)
	case querylogical.ValueInteger:
		return fmt.Sprint(value.Integer)
	case querylogical.ValueUnsigned:
		return fmt.Sprint(value.Unsigned)
	case querylogical.ValueNull:
		return "<nil>"
	default:
		return canonicalValue(value)
	}
}

func canonicalValue(value querylogical.Value) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("%s:%v", value.Kind, value)
	}
	return string(encoded)
}

func canonicalRow(row querylogical.Row) string {
	encoded, err := json.Marshal(row)
	if err != nil {
		return fmt.Sprint(row)
	}
	return string(encoded)
}

func estimateBindingRow(row BindingRow) int64 {
	encoded, err := json.Marshal(row)
	if err != nil {
		return 256
	}
	return int64(len(encoded)) + 64
}

func estimateValues(values []querylogical.Value) int64 {
	encoded, err := json.Marshal(values)
	if err != nil {
		return int64(64 * len(values))
	}
	return int64(len(encoded)) + int64(16*len(values))
}

func estimateProjectedRow(row querylogical.Row) int64 {
	encoded, err := json.Marshal(row)
	if err != nil {
		return 128
	}
	return int64(len(encoded)) + 32
}

func dereferenceValue(value *querylogical.Value) querylogical.Value {
	if value == nil {
		return querylogical.Value{Kind: querylogical.ValueNull}
	}
	return *value
}

func dereferenceField(field *querylogical.FieldRef) querylogical.FieldRef {
	if field == nil {
		return querylogical.FieldRef{}
	}
	return *field
}

func executionInvalid(operation string, message string) error {
	return querylogical.NewError(querylogical.ErrorInvalidQuery, operation, message, nil)
}

func projectRow(row BindingRow, projections []querylogical.Projection, eval evaluator) (querylogical.Row, error) {
	fields := make(map[string]querylogical.Cell, len(projections))
	for _, projection := range projections {
		name := projectionOutputName(projection)
		switch projection.Kind {
		case querylogical.ProjectionNode:
			node, ok := row.Nodes[projection.Alias]
			if !ok {
				return querylogical.Row{}, executionInvalid("project query row", fmt.Sprintf("node alias %q is not bound", projection.Alias))
			}
			value := neutralNode(node)
			fields[name] = querylogical.Cell{Kind: querylogical.CellNode, Node: &value}
		case querylogical.ProjectionEdge:
			edge, ok := row.Edges[projection.Alias]
			if !ok {
				return querylogical.Row{}, executionInvalid("project query row", fmt.Sprintf("edge alias %q is not bound", projection.Alias))
			}
			value := neutralEdge(edge)
			fields[name] = querylogical.Cell{Kind: querylogical.CellEdge, Edge: &value}
		case querylogical.ProjectionPath:
			path, ok := row.Paths[projection.Alias]
			if !ok {
				return querylogical.Row{}, executionInvalid("project query row", fmt.Sprintf("path alias %q is not bound", projection.Alias))
			}
			fields[name] = querylogical.Cell{Kind: querylogical.CellPath, Path: &path}
		case querylogical.ProjectionTree:
			tree, ok := row.Trees[projection.Alias]
			if !ok {
				return querylogical.Row{}, executionInvalid("project query row", fmt.Sprintf("tree alias %q is not bound", projection.Alias))
			}
			fields[name] = querylogical.Cell{Kind: querylogical.CellTree, Tree: &tree}
		case querylogical.ProjectionScalar:
			var value querylogical.Value
			var err error
			if projection.Field != nil && projection.Field.Name == "" {
				node, ok := row.Nodes[projection.Alias]
				if !ok {
					return querylogical.Row{}, executionInvalid("project query row", fmt.Sprintf("alias %q is not bound", projection.Alias))
				}
				value = querylogical.Value{Kind: querylogical.ValueString, String: node.ID.String()}
			} else {
				value, err = eval.value(row, querylogical.Value{Kind: querylogical.ValueField, Field: projection.Field})
			}
			if err != nil {
				return querylogical.Row{}, err
			}
			fields[name] = scalarCell(value)
		default:
			return querylogical.Row{}, executionInvalid("project query row", fmt.Sprintf("unsupported projection kind %q", projection.Kind))
		}
	}
	return querylogical.Row{Fields: fields}, nil
}

func projectionOutputName(projection querylogical.Projection) string {
	if projection.OutputName != "" {
		return projection.OutputName
	}
	if projection.Kind == querylogical.ProjectionScalar && projection.Field != nil {
		if projection.Field.Name == "" {
			return projection.Alias
		}
		name := projection.Field.Alias + "." + projection.Field.Name
		if projection.Field.Namespace != "" && projection.Field.Namespace != querylogical.NamespaceProperties {
			name = projection.Field.Alias + "." + string(projection.Field.Namespace) + "." + projection.Field.Name
		}
		return name
	}
	return projection.Alias
}

func aggregateOutputName(aggregate querylogical.Aggregate) string {
	if aggregate.OutputName != "" {
		return aggregate.OutputName
	}
	return string(aggregate.Function)
}

func scalarCell(value querylogical.Value) querylogical.Cell {
	if value.Kind == querylogical.ValueNull {
		return querylogical.Cell{Kind: querylogical.CellNull}
	}
	copyValue := value
	return querylogical.Cell{Kind: querylogical.CellScalar, Scalar: &copyValue}
}

func neutralNode(node graph.Node) querylogical.Node {
	properties := node.Properties
	if properties == nil {
		properties = node.Props
	}
	payload := node.Payload
	if payload == nil {
		payload = map[string]any{}
		if node.Content != "" {
			payload["text"] = node.Content
		}
		if node.BlobRef != nil {
			payload["blob_id"] = string(*node.BlobRef)
		}
	}
	return querylogical.Node{
		ID: node.ID.String(), DomainID: node.DomainID.String(), Labels: append([]string(nil), node.Labels...),
		// Whole-entity query projection preserves the existing API shape, which
		// omits Meta; namespaced scalar meta projections remain available.
		Properties: valueMap(properties), Payload: valueMap(payload),
		CreatedAt: node.CreatedAt, UpdatedAt: node.UpdatedAt,
	}
}

func neutralEdge(edge graph.Edge) querylogical.Edge {
	return querylogical.Edge{
		ID: edge.ID.String(), DomainID: edge.DomainID.String(), FromNodeID: edge.FromID.String(), ToNodeID: edge.ToID.String(),
		Labels: append([]string(nil), edge.Labels...), Properties: valueMap(edge.Properties), Payload: valueMap(edge.Payload),
		CreatedAt: edge.CreatedAt, UpdatedAt: edge.UpdatedAt,
	}
}

func valueMap(values map[string]any) map[string]querylogical.Value {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]querylogical.Value, len(values))
	for name, value := range values {
		out[name] = querylogical.NativeValue(value)
	}
	return out
}

func rowOrderValues(row BindingRow, orders []querylogical.Order, eval evaluator) ([]querylogical.Value, error) {
	values := make([]querylogical.Value, 0, len(orders))
	for _, order := range orders {
		value, err := eval.value(row, order.Value)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func compareOrderValues(left []querylogical.Value, leftSequence int64, right []querylogical.Value, rightSequence int64, orders []querylogical.Order) int {
	for index, order := range orders {
		comparison := compareValues(left[index], right[index])
		if order.Direction == querylogical.SortDescending {
			comparison = -comparison
		}
		if comparison != 0 {
			return comparison
		}
	}
	switch {
	case leftSequence < rightSequence:
		return -1
	case leftSequence > rightSequence:
		return 1
	default:
		return 0
	}
}

func rowOrderLess(left BindingRow, right BindingRow, orders []querylogical.Order, eval evaluator) (bool, error) {
	leftValues, err := rowOrderValues(left, orders, eval)
	if err != nil {
		return false, err
	}
	rightValues, err := rowOrderValues(right, orders, eval)
	if err != nil {
		return false, err
	}
	return compareOrderValues(leftValues, left.Sequence, rightValues, right.Sequence, orders) < 0, nil
}

func sortRows(rows []BindingRow, orders []querylogical.Order, eval evaluator) error {
	var compareErr error
	sort.SliceStable(rows, func(i int, j int) bool {
		if compareErr != nil {
			return false
		}
		less, err := rowOrderLess(rows[i], rows[j], orders, eval)
		if err != nil {
			compareErr = err
			return false
		}
		return less
	})
	return compareErr
}

func finiteNumber(value querylogical.Value) (float64, bool) {
	number, ok := numericValue(value)
	return number, ok && !math.IsNaN(number) && !math.IsInf(number, 0)
}
