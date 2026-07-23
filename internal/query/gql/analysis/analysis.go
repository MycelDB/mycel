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

func analyzeMatchStatement(stmt model.MatchStatement) error {
	if stmt.Pattern.Variable == "" {
		return fmt.Errorf("match node variable is required")
	}
	if len(stmt.Returns) == 0 {
		return fmt.Errorf("match statement requires at least one return item")
	}
	for _, ret := range stmt.Returns {
		if ret.Variable == "" {
			return fmt.Errorf("return variable cannot be empty")
		}
		if ret.Variable != stmt.Pattern.Variable {
			return fmt.Errorf("return variable %q is not defined", ret.Variable)
		}
	}
	seenProperties := map[string]struct{}{}
	for _, prop := range stmt.Pattern.Properties {
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
