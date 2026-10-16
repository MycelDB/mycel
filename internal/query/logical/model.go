// Package logical defines the transport-neutral query vocabulary shared by
// structured API and textual GQL adapters before binding and physical planning.
package logical

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

// Source identifies the surface that produced a logical query. It is
// provenance only and is removed by Query.Comparable.
type Source string

const (
	SourceStructured Source = "structured"
	SourceGQL        Source = "gql"
)

// AccessMode describes the transaction capability required by a statement.
type AccessMode string

const (
	AccessModeUnspecified AccessMode = ""
	AccessModeReadOnly    AccessMode = "read_only"
	AccessModeReadWrite   AccessMode = "read_write"
)

// Query is the canonical read-query shape. It intentionally contains no
// protobuf, daemon, storage, or service implementation types.
type Query struct {
	Source        Source        `json:"source,omitempty"`
	AccessMode    AccessMode    `json:"access_mode,omitempty"`
	Start         NodePattern   `json:"start"`
	Steps         []Step        `json:"steps,omitempty"`
	PathAlias     string        `json:"path_alias,omitempty"`
	Predicate     *Predicate    `json:"predicate,omitempty"`
	PredicatePlan PredicatePlan `json:"predicate_plan,omitempty"`
	Returns       []Projection  `json:"returns,omitempty"`
	Aggregates    []Aggregate   `json:"aggregates,omitempty"`
	OrderBy       []Order       `json:"order_by,omitempty"`
	ReturnGraph   bool          `json:"return_graph,omitempty"`
	Distinct      bool          `json:"distinct,omitempty"`
	Page          PageSpec      `json:"page,omitempty"`
	Safety        SafetyLimits  `json:"safety,omitempty"`
}

// Comparable removes adapter provenance and opaque continuation state while
// retaining all query semantics.
func (q Query) Comparable() Query {
	q.Source = ""
	q.Page.Cursor = ""
	return q
}

// PageSpec describes logical shaping and response continuation.
type PageSpec struct {
	Offset int64  `json:"offset,omitempty"`
	Limit  int64  `json:"limit,omitempty"`
	Size   int32  `json:"size,omitempty"`
	Cursor string `json:"cursor,omitempty"`
}

// SafetyLimits bound graph expansion independently of response pagination.
type SafetyLimits struct {
	MaxNodes int32 `json:"max_nodes,omitempty"`
	MaxEdges int32 `json:"max_edges,omitempty"`
}

// NodePattern describes one bound graph node.
type NodePattern struct {
	Alias      string           `json:"alias"`
	NodeIDs    []string         `json:"node_ids,omitempty"`
	Labels     []string         `json:"labels,omitempty"`
	Properties map[string]Value `json:"properties,omitempty"`
}

// Direction is the direction of a relationship traversal.
type Direction string

const (
	DirectionUnspecified Direction = "unspecified"
	DirectionOutgoing    Direction = "outgoing"
	DirectionIncoming    Direction = "incoming"
	DirectionUndirected  Direction = "undirected"
)

// EdgePattern describes a bound relationship in a graph pattern.
type EdgePattern struct {
	Alias      string           `json:"alias,omitempty"`
	Labels     []string         `json:"labels,omitempty"`
	Properties map[string]Value `json:"properties,omitempty"`
	Direction  Direction        `json:"direction"`
}

// DepthBounds are inclusive traversal depth bounds.
type DepthBounds struct {
	Min int `json:"min"`
	Max int `json:"max"`
}

// Step connects the previous node pattern to Target through Edge.
type Step struct {
	Edge   EdgePattern `json:"edge"`
	Depth  DepthBounds `json:"depth"`
	Target NodePattern `json:"target"`
}

// FieldNamespace identifies a graph entity value bucket.
type FieldNamespace string

const (
	NamespaceProperties FieldNamespace = "properties"
	NamespacePayload    FieldNamespace = "payload"
	NamespaceMeta       FieldNamespace = "meta"
)

// FieldRef identifies a field on a bound node or edge.
type FieldRef struct {
	Alias     string         `json:"alias"`
	Namespace FieldNamespace `json:"namespace"`
	Name      string         `json:"name"`
}

// ParseFieldRef maps the public dotted field spelling to a typed reference.
// Unqualified names use the properties namespace.
func ParseFieldRef(alias string, name string) FieldRef {
	parts := strings.SplitN(name, ".", 2)
	if len(parts) == 2 {
		switch FieldNamespace(parts[0]) {
		case NamespaceProperties, NamespacePayload, NamespaceMeta:
			return FieldRef{Alias: alias, Namespace: FieldNamespace(parts[0]), Name: parts[1]}
		}
	}
	return FieldRef{Alias: alias, Namespace: NamespaceProperties, Name: name}
}

// ValueKind identifies a typed expression value.
type ValueKind string

const (
	ValueUnknown     ValueKind = "unknown"
	ValueBinding     ValueKind = "binding"
	ValueField       ValueKind = "field"
	ValueNull        ValueKind = "null"
	ValueBool        ValueKind = "bool"
	ValueNumber      ValueKind = "number"
	ValueInteger     ValueKind = "integer"
	ValueUnsigned    ValueKind = "unsigned"
	ValueString      ValueKind = "string"
	ValueList        ValueKind = "list"
	ValueObject      ValueKind = "object"
	ValueDate        ValueKind = "date"
	ValueCurrentDate ValueKind = "current_date"
)

// Value is a typed literal or expression operand. Number uses float64 to
// preserve equivalence with structured API values; Integer and Unsigned retain
// larger GQL integer literals without precision loss.
type Value struct {
	Kind       ValueKind        `json:"kind"`
	Binding    string           `json:"binding,omitempty"`
	Field      *FieldRef        `json:"field,omitempty"`
	Bool       bool             `json:"bool,omitempty"`
	Number     float64          `json:"number,omitempty"`
	Integer    int64            `json:"integer,omitempty"`
	Unsigned   uint64           `json:"unsigned,omitempty"`
	String     string           `json:"string,omitempty"`
	List       []Value          `json:"list,omitempty"`
	Object     map[string]Value `json:"object,omitempty"`
	OffsetDays int32            `json:"offset_days,omitempty"`
}

// BindingValue constructs a whole-binding expression.
func BindingValue(alias string) Value {
	return Value{Kind: ValueBinding, Binding: alias}
}

// FieldValue constructs a field-reference expression.
func FieldValue(alias string, name string) Value {
	field := ParseFieldRef(alias, name)
	return Value{Kind: ValueField, Field: &field}
}

// NativeValue converts JSON-like Go data into a recursively typed Value.
func NativeValue(value any) Value {
	if value == nil {
		return Value{Kind: ValueNull}
	}
	switch v := value.(type) {
	case Value:
		return v
	case bool:
		return Value{Kind: ValueBool, Bool: v}
	case string:
		return Value{Kind: ValueString, String: v}
	case json.Number:
		if !strings.ContainsAny(v.String(), ".eE") {
			if n, err := strconv.ParseInt(v.String(), 10, 64); err == nil {
				return signedNativeValue(n)
			}
			if n, err := strconv.ParseUint(v.String(), 10, 64); err == nil {
				return unsignedNativeValue(n)
			}
		}
		n, err := v.Float64()
		if err != nil {
			return Value{Kind: ValueUnknown, String: v.String()}
		}
		return Value{Kind: ValueNumber, Number: n}
	case float32:
		return Value{Kind: ValueNumber, Number: float64(v)}
	case float64:
		return Value{Kind: ValueNumber, Number: v}
	case int:
		return signedNativeValue(int64(v))
	case int8:
		return signedNativeValue(int64(v))
	case int16:
		return signedNativeValue(int64(v))
	case int32:
		return signedNativeValue(int64(v))
	case int64:
		return signedNativeValue(v)
	case uint:
		return unsignedNativeValue(uint64(v))
	case uint8:
		return unsignedNativeValue(uint64(v))
	case uint16:
		return unsignedNativeValue(uint64(v))
	case uint32:
		return unsignedNativeValue(uint64(v))
	case uint64:
		return unsignedNativeValue(v)
	case []any:
		out := make([]Value, 0, len(v))
		for _, item := range v {
			out = append(out, NativeValue(item))
		}
		return Value{Kind: ValueList, List: out}
	case map[string]any:
		out := make(map[string]Value, len(v))
		for key, item := range v {
			out[key] = NativeValue(item)
		}
		return Value{Kind: ValueObject, Object: out}
	}

	// GQL planning can retain typed slices and string-keyed maps. Reflective
	// handling keeps the neutral model independent of those concrete types.
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		out := make([]Value, 0, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			out = append(out, NativeValue(rv.Index(i).Interface()))
		}
		return Value{Kind: ValueList, List: out}
	case reflect.Map:
		if rv.Type().Key().Kind() == reflect.String {
			out := make(map[string]Value, rv.Len())
			iter := rv.MapRange()
			for iter.Next() {
				out[iter.Key().String()] = NativeValue(iter.Value().Interface())
			}
			return Value{Kind: ValueObject, Object: out}
		}
	}
	return Value{Kind: ValueUnknown, String: fmt.Sprintf("%v", value)}
}

func signedNativeValue(value int64) Value {
	if value < -(1<<53) || value > 1<<53 {
		return Value{Kind: ValueInteger, Integer: value}
	}
	return Value{Kind: ValueNumber, Number: float64(value)}
}

func unsignedNativeValue(value uint64) Value {
	if value > 1<<53 {
		return Value{Kind: ValueUnsigned, Unsigned: value}
	}
	return Value{Kind: ValueNumber, Number: float64(value)}
}

// IsFiniteNumber reports whether a numeric literal is safe for planning.
func (v Value) IsFiniteNumber() bool {
	return v.Kind != ValueNumber || (!math.IsNaN(v.Number) && !math.IsInf(v.Number, 0))
}

type ProjectionKind string

const (
	ProjectionNode   ProjectionKind = "node"
	ProjectionEdge   ProjectionKind = "edge"
	ProjectionTree   ProjectionKind = "tree"
	ProjectionScalar ProjectionKind = "scalar"
	ProjectionPath   ProjectionKind = "path"
)

// Projection describes one output field.
type Projection struct {
	Kind       ProjectionKind `json:"kind"`
	Alias      string         `json:"alias"`
	Field      *FieldRef      `json:"field,omitempty"`
	OutputName string         `json:"output_name,omitempty"`
}

type AggregateFunction string

const (
	AggregateUnspecified AggregateFunction = "unspecified"
	AggregateCount       AggregateFunction = "count"
	AggregateSum         AggregateFunction = "sum"
	AggregateAvg         AggregateFunction = "avg"
	AggregateMin         AggregateFunction = "min"
	AggregateMax         AggregateFunction = "max"
)

// Aggregate describes one aggregate projection.
type Aggregate struct {
	Function   AggregateFunction `json:"function"`
	Star       bool              `json:"star,omitempty"`
	Argument   *Value            `json:"argument,omitempty"`
	OutputName string            `json:"output_name,omitempty"`
}

type SortDirection string

const (
	SortAscending  SortDirection = "asc"
	SortDescending SortDirection = "desc"
)

type Order struct {
	Value     Value         `json:"value"`
	Direction SortDirection `json:"direction"`
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

// Leaf represents one typed predicate. Left/Right are used for comparisons,
// Left/Low/High for ranges, Operand for string predicates, and Field for
// field-specific predicates.
type Leaf struct {
	Kind                LeafKind  `json:"kind"`
	Field               *FieldRef `json:"field,omitempty"`
	Operand             *Value    `json:"operand,omitempty"`
	Left                *Value    `json:"left,omitempty"`
	Right               *Value    `json:"right,omitempty"`
	Low                 *Value    `json:"low,omitempty"`
	High                *Value    `json:"high,omitempty"`
	Operator            string    `json:"operator,omitempty"`
	Query               string    `json:"query,omitempty"`
	IsNull              bool      `json:"is_null,omitempty"`
	Tag                 string    `json:"tag,omitempty"`
	RuleRef             string    `json:"rule_ref,omitempty"`
	EmbeddingBindingKey string    `json:"embedding_binding_key,omitempty"`
	Limit               int32     `json:"limit,omitempty"`
	Pushdown            string    `json:"pushdown"`
	PushReason          string    `json:"push_reason,omitempty"`
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

// LeafPredicate creates a leaf predicate.
func LeafPredicate(leaf Leaf) *Predicate {
	return &Predicate{Op: PredicateLeafOp, Leaf: &leaf}
}

// CombinePredicate creates a normalized boolean predicate.
func CombinePredicate(op PredicateOp, terms []Predicate) *Predicate {
	filtered := make([]Predicate, 0, len(terms))
	var appendTerm func(Predicate)
	appendTerm = func(term Predicate) {
		if term.Op == "" && term.Leaf == nil && len(term.Terms) == 0 {
			return
		}
		if (op == PredicateAndOp || op == PredicateOrOp) && term.Op == op && term.Leaf == nil {
			for _, child := range term.Terms {
				appendTerm(child)
			}
			return
		}
		filtered = append(filtered, term)
	}
	for _, term := range terms {
		appendTerm(term)
	}
	if len(filtered) == 0 {
		return nil
	}
	if len(filtered) == 1 {
		return &filtered[0]
	}
	if op == PredicateAndOp || op == PredicateOrOp {
		sort.SliceStable(filtered, func(i, j int) bool {
			return predicateCanonicalKey(filtered[i]) < predicateCanonicalKey(filtered[j])
		})
		unique := filtered[:0]
		lastKey := ""
		for i, term := range filtered {
			key := predicateCanonicalKey(term)
			if i == 0 || key != lastKey {
				unique = append(unique, term)
				lastKey = key
			}
		}
		filtered = unique
	}
	if len(filtered) == 1 {
		return &filtered[0]
	}
	return &Predicate{Op: op, Terms: filtered}
}

func predicateCanonicalKey(predicate Predicate) string {
	encoded, err := json.Marshal(predicate)
	if err != nil {
		return fmt.Sprintf("%#v", predicate)
	}
	return string(encoded)
}

func normalizeLimit(value int64) int64 {
	if value < 0 {
		return 0
	}
	return value
}
