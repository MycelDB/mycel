// Package logical defines the internal query shape shared by textual GQL and
// structured protobuf queries before physical planning/execution.
package logical

import "fmt"

// Source identifies the surface that produced a logical query.
type Source string

const (
	SourceStructured Source = "structured"
	SourceGQL        Source = "gql"
)

// Query is a normalized read-query shape that can be compared across query
// surfaces and annotated by planners before choosing physical execution.
type Query struct {
	Source          Source        `json:"source,omitempty"`
	Start           NodePattern   `json:"start"`
	Steps           []Step        `json:"steps,omitempty"`
	PathAlias       string        `json:"path_alias,omitempty"`
	Predicate       *Predicate    `json:"predicate,omitempty"`
	PredicatePlan   PredicatePlan `json:"predicate_plan,omitempty"`
	Returns         []Projection  `json:"returns,omitempty"`
	Aggregates      []Aggregate   `json:"aggregates,omitempty"`
	OrderBy         []Order       `json:"order_by,omitempty"`
	ReturnGraph     bool          `json:"return_graph,omitempty"`
	Distinct        bool          `json:"distinct,omitempty"`
	Offset          int64         `json:"offset,omitempty"`
	Limit           int64         `json:"limit,omitempty"`
	MaxNodes        int32         `json:"max_nodes,omitempty"`
	MaxEdges        int32         `json:"max_edges,omitempty"`
	CursorRequested bool          `json:"cursor_requested,omitempty"`
}

// Comparable returns q with source-specific metadata removed.
func (q Query) Comparable() Query {
	q.Source = ""
	q.CursorRequested = false
	return q
}

type NodePattern struct {
	Alias      string            `json:"alias"`
	NodeIDs    []string          `json:"node_ids,omitempty"`
	Labels     []string          `json:"labels,omitempty"`
	Properties map[string]string `json:"properties,omitempty"`
}

type Step struct {
	Direction string      `json:"direction"`
	EdgeLabel string      `json:"edge_label"`
	EdgeAlias string      `json:"edge_alias,omitempty"`
	MinDepth  int         `json:"min_depth"`
	MaxDepth  int         `json:"max_depth"`
	Target    NodePattern `json:"target"`
}

type ProjectionKind string

const (
	ProjectionNode   ProjectionKind = "node"
	ProjectionEdge   ProjectionKind = "edge"
	ProjectionTree   ProjectionKind = "tree"
	ProjectionScalar ProjectionKind = "scalar"
	ProjectionPath   ProjectionKind = "path"
)

type Projection struct {
	Kind       ProjectionKind `json:"kind"`
	Alias      string         `json:"alias"`
	Namespace  string         `json:"namespace,omitempty"`
	Property   string         `json:"property,omitempty"`
	OutputName string         `json:"output_name,omitempty"`
}

type AggregateFunction string

const (
	AggregateCount AggregateFunction = "count"
)

type Aggregate struct {
	Function   AggregateFunction `json:"function"`
	Star       bool              `json:"star,omitempty"`
	Alias      string            `json:"alias,omitempty"`
	Value      *Value            `json:"value,omitempty"`
	OutputName string            `json:"output_name,omitempty"`
}

type Order struct {
	Value     Value  `json:"value"`
	Direction string `json:"direction"`
}

type PredicateOp string

const (
	PredicateLeafOp PredicateOp = "leaf"
	PredicateAndOp  PredicateOp = "and"
	PredicateOrOp   PredicateOp = "or"
)

type Predicate struct {
	Op    PredicateOp `json:"op"`
	Terms []Predicate `json:"terms,omitempty"`
	Leaf  *Leaf       `json:"leaf,omitempty"`
}

type LeafKind string

const (
	LeafComparison     LeafKind = "comparison"
	LeafBetween        LeafKind = "between"
	LeafNull           LeafKind = "null"
	LeafString         LeafKind = "string"
	LeafText           LeafKind = "text"
	LeafSemantic       LeafKind = "semantic"
	LeafHasTag         LeafKind = "has_tag"
	LeafPropertyExists LeafKind = "property_exists"
)

type Leaf struct {
	Kind       LeafKind `json:"kind"`
	Alias      string   `json:"alias,omitempty"`
	Namespace  string   `json:"namespace,omitempty"`
	Property   string   `json:"property,omitempty"`
	Operator   string   `json:"operator,omitempty"`
	Value      *Value   `json:"value,omitempty"`
	Low        *Value   `json:"low,omitempty"`
	High       *Value   `json:"high,omitempty"`
	Query      string   `json:"query,omitempty"`
	IsNull     bool     `json:"is_null,omitempty"`
	IndexRef   string   `json:"index_ref,omitempty"`
	Limit      int32    `json:"limit,omitempty"`
	Pushdown   string   `json:"pushdown"`
	PushReason string   `json:"push_reason,omitempty"`
}

type ValueKind string

const (
	ValueProperty    ValueKind = "property"
	ValueLiteral     ValueKind = "literal"
	ValueDate        ValueKind = "date"
	ValueCurrentDate ValueKind = "current_date"
	ValueUnknown     ValueKind = "unknown"
)

type Value struct {
	Kind       ValueKind `json:"kind"`
	Alias      string    `json:"alias,omitempty"`
	Namespace  string    `json:"namespace,omitempty"`
	Property   string    `json:"property,omitempty"`
	Literal    string    `json:"literal,omitempty"`
	OffsetDays int32     `json:"offset_days,omitempty"`
}

type PredicatePlacement string

const (
	PredicatePushdownEligible PredicatePlacement = "pushdown_eligible"
	PredicateResidual         PredicatePlacement = "residual"
)

type PredicatePlan struct {
	PushdownEligible []Leaf `json:"pushdown_eligible,omitempty"`
	Residual         []Leaf `json:"residual,omitempty"`
}

func leafPredicate(leaf Leaf) *Predicate {
	return &Predicate{Op: PredicateLeafOp, Leaf: &leaf}
}

func combinePredicate(op PredicateOp, terms []Predicate) *Predicate {
	filtered := make([]Predicate, 0, len(terms))
	for _, term := range terms {
		if term.Op != "" || term.Leaf != nil || len(term.Terms) > 0 {
			filtered = append(filtered, term)
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	if len(filtered) == 1 {
		return &filtered[0]
	}
	return &Predicate{Op: op, Terms: filtered}
}

func propertyParts(name string) (namespace string, property string) {
	namespace = "properties"
	property = name
	for i, r := range name {
		if r == '.' {
			return name[:i], name[i+1:]
		}
	}
	return namespace, property
}

func normalizeLimit(v int64) int64 {
	if v < 0 {
		return 0
	}
	return v
}

func literalString(value any) string {
	return fmt.Sprint(value)
}
