// Package analysis performs semantic analysis for Mycel GQL ASTs.
package analysis

import (
	"fmt"

	"github.com/myceldb/mycel/internal/query/gql/ast/model"
)

type AccessMode string

const (
	ReadOnly  AccessMode = "read_only"
	ReadWrite AccessMode = "read_write"
)

// Analysis is the semantic description of a valid GQL query.
type Analysis struct {
	Query      model.Query
	AccessMode AccessMode
}

// Analyzer validates a GQL AST and returns semantic information used by later
// planning/execution stages.
type Analyzer interface {
	Analyze(query model.Query) (Analysis, error)
}

type analyzer struct{}

func NewAnalyzer() Analyzer { return analyzer{} }

func Analyze(query model.Query) (Analysis, error) { return NewAnalyzer().Analyze(query) }

func (analyzer) Analyze(query model.Query) (Analysis, error) {
	switch stmt := query.Statement.(type) {
	case model.InsertStatement:
		if err := analyzeInsertStatement(stmt); err != nil {
			return Analysis{}, err
		}
		return Analysis{Query: query, AccessMode: ReadWrite}, nil
	case model.MatchStatement:
		if err := analyzeMatchStatement(stmt); err != nil {
			return Analysis{}, err
		}
		return Analysis{Query: query, AccessMode: ReadOnly}, nil
	case model.MatchCreateStatement:
		if err := analyzeMatchCreateStatement(stmt); err != nil {
			return Analysis{}, err
		}
		return Analysis{Query: query, AccessMode: ReadWrite}, nil
	case nil:
		return Analysis{}, fmt.Errorf("query statement is required")
	default:
		return Analysis{}, fmt.Errorf("unsupported statement %T", query.Statement)
	}
}

func analyzeInsertStatement(stmt model.InsertStatement) error {
	pattern := stmt.Pattern
	if len(pattern.Labels) == 0 {
		return fmt.Errorf("insert node requires at least one label")
	}
	seenLabels := map[string]struct{}{}
	for _, label := range pattern.Labels {
		if label == "" {
			return fmt.Errorf("insert node label cannot be empty")
		}
		if _, exists := seenLabels[label]; exists {
			return fmt.Errorf("duplicate label %q", label)
		}
		seenLabels[label] = struct{}{}
	}
	seenProperties := map[string]struct{}{}
	for _, prop := range pattern.Properties {
		if prop.Key == "" {
			return fmt.Errorf("property key cannot be empty")
		}
		if _, exists := seenProperties[prop.Key]; exists {
			return fmt.Errorf("duplicate property key %q", prop.Key)
		}
		seenProperties[prop.Key] = struct{}{}
		if err := analyzeValue(prop.Value); err != nil {
			return fmt.Errorf("property %q: %w", prop.Key, err)
		}
	}
	return nil
}

func analyzeMatchCreateStatement(stmt model.MatchCreateStatement) error {
	if len(stmt.Matches) < 2 {
		return fmt.Errorf("match create requires at least two node patterns")
	}
	defined := map[string]struct{}{}
	for _, pattern := range stmt.Matches {
		if pattern.Variable == "" {
			return fmt.Errorf("matched node variable is required")
		}
		if _, exists := defined[pattern.Variable]; exists {
			return fmt.Errorf("duplicate variable %q", pattern.Variable)
		}
		defined[pattern.Variable] = struct{}{}
		if err := analyzePatternProperties(pattern.Properties); err != nil {
			return err
		}
	}
	if _, ok := defined[stmt.Create.FromVariable]; !ok {
		return fmt.Errorf("create source variable %q is not defined", stmt.Create.FromVariable)
	}
	if _, ok := defined[stmt.Create.ToVariable]; !ok {
		return fmt.Errorf("create target variable %q is not defined", stmt.Create.ToVariable)
	}
	if len(stmt.Create.Relationship.Labels) == 0 {
		return fmt.Errorf("create relationship requires at least one label")
	}
	if err := analyzePatternProperties(stmt.Create.Relationship.Properties); err != nil {
		return fmt.Errorf("relationship pattern: %w", err)
	}
	return nil
}

func analyzeMatchStatement(stmt model.MatchStatement) error {
	pattern := stmt.MatchPattern
	if pattern.Start.Variable == "" && pattern.Relationship == nil {
		pattern.Start = stmt.Pattern
	}
	if pattern.Start.Variable == "" {
		return fmt.Errorf("match node variable is required")
	}
	defined := map[string]struct{}{pattern.Start.Variable: {}}
	segments := pattern.Segments
	if len(segments) == 0 && pattern.Relationship != nil {
		if pattern.End == nil {
			return fmt.Errorf("relationship pattern requires target node")
		}
		segments = []model.PathSegment{{Relationship: *pattern.Relationship, Node: *pattern.End}}
	}
	if len(segments) > 0 {
		for _, segment := range segments {
			if segment.Node.Variable == "" {
				return fmt.Errorf("relationship target variable is required")
			}
			if _, exists := defined[segment.Node.Variable]; exists {
				return fmt.Errorf("duplicate variable %q", segment.Node.Variable)
			}
			defined[segment.Node.Variable] = struct{}{}
			if segment.Relationship.Quantifier != nil {
				if segment.Relationship.Quantifier.Min < 1 || segment.Relationship.Quantifier.Max < segment.Relationship.Quantifier.Min || segment.Relationship.Quantifier.Max > 5 {
					return fmt.Errorf("relationship quantifier must be within 1..5")
				}
				if segment.Relationship.Variable != "" {
					return fmt.Errorf("relationship variables are not supported on variable-length traversals")
				}
			}
			if segment.Relationship.Variable != "" {
				if _, exists := defined[segment.Relationship.Variable]; exists {
					return fmt.Errorf("duplicate variable %q", segment.Relationship.Variable)
				}
				defined[segment.Relationship.Variable] = struct{}{}
			}
			if err := analyzePatternProperties(segment.Relationship.Properties); err != nil {
				return fmt.Errorf("relationship pattern: %w", err)
			}
			if err := analyzePatternProperties(segment.Node.Properties); err != nil {
				return fmt.Errorf("target node pattern: %w", err)
			}
		}
	}
	if len(stmt.Returns) == 0 {
		return fmt.Errorf("match statement requires at least one return item")
	}
	for _, ret := range stmt.Returns {
		kind := ret.Kind
		if kind == "" {
			kind = model.ReturnVariable
		}
		if ret.Variable == "" {
			return fmt.Errorf("return variable cannot be empty")
		}
		if _, ok := defined[ret.Variable]; !ok {
			return fmt.Errorf("return variable %q is not defined", ret.Variable)
		}
		switch kind {
		case model.ReturnVariable:
		case model.ReturnProperty:
			if ret.Property == "" {
				return fmt.Errorf("return property cannot be empty")
			}
			switch ret.Namespace {
			case "", "properties", "payload", "meta":
			default:
				return fmt.Errorf("unsupported return namespace %q", ret.Namespace)
			}
		default:
			return fmt.Errorf("unsupported return item kind %q", kind)
		}
	}
	if stmt.FetchFirst != nil && stmt.FetchFirst.Count <= 0 {
		return fmt.Errorf("fetch first count must be positive")
	}
	if stmt.Where != nil {
		if len(stmt.Where.Predicates) == 0 && len(stmt.Where.TextPredicates) == 0 && len(stmt.Where.SemanticPredicates) == 0 {
			return fmt.Errorf("where clause requires at least one predicate")
		}
		for _, predicate := range stmt.Where.TextPredicates {
			if _, ok := defined[predicate.Variable]; !ok {
				return fmt.Errorf("where variable %q is not defined", predicate.Variable)
			}
			if predicate.Property == "" || predicate.Query == "" {
				return fmt.Errorf("text predicate requires field and query")
			}
			switch predicate.Namespace {
			case "", "properties", "payload", "meta":
			default:
				return fmt.Errorf("unsupported text predicate namespace %q", predicate.Namespace)
			}
		}
		for _, predicate := range stmt.Where.SemanticPredicates {
			if _, ok := defined[predicate.Variable]; !ok {
				return fmt.Errorf("where variable %q is not defined", predicate.Variable)
			}
			if predicate.Query == "" || predicate.TopK <= 0 {
				return fmt.Errorf("semantic predicate requires query and positive top k")
			}
		}
		for _, predicate := range stmt.Where.Predicates {
			if predicate.Variable == "" {
				return fmt.Errorf("where predicate variable cannot be empty")
			}
			if _, ok := defined[predicate.Variable]; !ok {
				return fmt.Errorf("where variable %q is not defined", predicate.Variable)
			}
			if predicate.Property == "" {
				return fmt.Errorf("where property cannot be empty")
			}
			if err := analyzeValue(predicate.Value); err != nil {
				return fmt.Errorf("where property %q: %w", predicate.Property, err)
			}
		}
	}
	if err := analyzePatternProperties(pattern.Start.Properties); err != nil {
		return err
	}
	return nil
}

func analyzePatternProperties(properties []model.Property) error {
	seenProperties := map[string]struct{}{}
	for _, prop := range properties {
		if prop.Key == "" {
			return fmt.Errorf("property key cannot be empty")
		}
		if _, exists := seenProperties[prop.Key]; exists {
			return fmt.Errorf("duplicate property key %q", prop.Key)
		}
		seenProperties[prop.Key] = struct{}{}
		if err := analyzeValue(prop.Value); err != nil {
			return fmt.Errorf("property %q: %w", prop.Key, err)
		}
	}
	return nil
}

func analyzeValue(value model.Value) error {
	switch value.Kind {
	case model.StringValue:
		if _, ok := value.Value.(string); !ok {
			return fmt.Errorf("string value kind requires string payload")
		}
	case model.IntValue:
		if _, ok := value.Value.(int64); !ok {
			return fmt.Errorf("int value kind requires int64 payload")
		}
	case model.FloatValue:
		if _, ok := value.Value.(float64); !ok {
			return fmt.Errorf("float value kind requires float64 payload")
		}
	case model.BoolValue:
		if _, ok := value.Value.(bool); !ok {
			return fmt.Errorf("bool value kind requires bool payload")
		}
	case model.NullValue:
		if value.Value != nil {
			return fmt.Errorf("null value kind requires nil payload")
		}
	default:
		return fmt.Errorf("unsupported value kind %q", value.Kind)
	}
	return nil
}
