package metadataindex

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/myceldb/mycel/internal/graph/model"
)

type TagMatchMode string

const (
	TagMatchAny TagMatchMode = "any"
	TagMatchAll TagMatchMode = "all"
)

type TagSummary struct {
	Tag   string `json:"tag"`
	Count int    `json:"count"`
}

type PropertyOperator string

const (
	PropertyOperatorExists PropertyOperator = "exists"
	PropertyOperatorEqual  PropertyOperator = "eq"
)

type PropertySummary struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

type FindNodesByPropertyInput struct {
	Name     string
	Operator PropertyOperator
	Value    any
	Limit    int
}

type valueKey struct {
	name      string
	valueType string
	value     string
}

// Index is an in-memory, per-space metadata index derived from graph nodes.
type Index struct {
	nodes      []graph.Node
	nodeByID   map[graph.NodeID]graph.Node
	tags       map[string]map[graph.NodeID]struct{}
	properties map[string]map[graph.NodeID]struct{}
	values     map[valueKey]map[graph.NodeID]struct{}
}

// Build constructs a metadata index from nodes. Invalid legacy metadata shapes
// are ignored for indexing so one malformed node does not make the whole space
// unreadable; write paths should validate canonical metadata before storage.
func Build(nodes []graph.Node) *Index {
	idx := &Index{
		nodes:      append([]graph.Node(nil), nodes...),
		nodeByID:   make(map[graph.NodeID]graph.Node, len(nodes)),
		tags:       map[string]map[graph.NodeID]struct{}{},
		properties: map[string]map[graph.NodeID]struct{}{},
		values:     map[valueKey]map[graph.NodeID]struct{}{},
	}
	for _, node := range nodes {
		idx.nodeByID[node.ID] = node
		idx.indexNode(node)
	}
	return idx
}

func (idx *Index) TagSummaries() []TagSummary {
	out := make([]TagSummary, 0, len(idx.tags))
	for tag, nodes := range idx.tags {
		out = append(out, TagSummary{Tag: tag, Count: len(nodes)})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].Tag < out[j].Tag
	})
	return out
}

func (idx *Index) FindByTags(tags []string, match TagMatchMode, limit int) ([]graph.Node, error) {
	normalized, err := graph.NormalizeTagsValue(tags)
	if err != nil {
		return nil, err
	}
	if len(normalized) == 0 {
		return []graph.Node{}, nil
	}
	if match == "" {
		match = TagMatchAny
	}
	if match != TagMatchAny && match != TagMatchAll {
		return nil, fmt.Errorf("unsupported tag match mode %q", match)
	}

	selected := map[graph.NodeID]struct{}{}
	switch match {
	case TagMatchAny:
		for _, tag := range normalized {
			for id := range idx.tags[tag] {
				selected[id] = struct{}{}
			}
		}
	case TagMatchAll:
		for id := range idx.tags[normalized[0]] {
			selected[id] = struct{}{}
		}
		for _, tag := range normalized[1:] {
			for id := range selected {
				if _, ok := idx.tags[tag][id]; !ok {
					delete(selected, id)
				}
			}
		}
	}
	return idx.nodesInOrder(selected, limit), nil
}

func (idx *Index) PropertySummaries() []PropertySummary {
	out := make([]PropertySummary, 0, len(idx.properties))
	for name, nodes := range idx.properties {
		out = append(out, PropertySummary{Name: name, Count: len(nodes)})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].Name < out[j].Name
	})
	return out
}

func (idx *Index) FindByProperty(in FindNodesByPropertyInput) ([]graph.Node, error) {
	name, err := graph.NormalizePropertyName(in.Name)
	if err != nil {
		return nil, err
	}
	operator := in.Operator
	if operator == "" {
		operator = PropertyOperatorExists
	}
	switch operator {
	case PropertyOperatorExists:
		return idx.nodesInOrder(idx.properties[name], in.Limit), nil
	case PropertyOperatorEqual:
		key, err := propertyValueKey(name, in.Value)
		if err != nil {
			return nil, err
		}
		return idx.nodesInOrder(idx.values[key], in.Limit), nil
	default:
		return nil, fmt.Errorf("unsupported property operator %q", operator)
	}
}

func (idx *Index) indexNode(node graph.Node) {
	for _, tag := range tagsForIndex(node.Props[graph.NodePropTags]) {
		addIndexEntry(idx.tags, tag, node.ID)
	}
	for name, value := range propertiesForIndex(node.Props[graph.NodePropCustomProperties]) {
		addIndexEntry(idx.properties, name, node.ID)
		key, err := propertyValueKey(name, value)
		if err != nil {
			continue
		}
		addValueIndexEntry(idx.values, key, node.ID)
	}
}

func tagsForIndex(value any) []string {
	raw := []string{}
	switch typed := value.(type) {
	case []string:
		raw = append(raw, typed...)
	case []any:
		for _, item := range typed {
			text, ok := item.(string)
			if ok {
				raw = append(raw, text)
			}
		}
	default:
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		tag, err := graph.NormalizeTag(item)
		if err != nil {
			continue
		}
		if _, ok := seen[tag]; ok {
			continue
		}
		seen[tag] = struct{}{}
		out = append(out, tag)
	}
	return out
}

func propertiesForIndex(value any) map[string]any {
	raw := map[string]any{}
	switch typed := value.(type) {
	case map[string]any:
		raw = typed
	case map[string]string:
		for key, val := range typed {
			raw[key] = val
		}
	default:
		return nil
	}
	out := map[string]any{}
	for key, value := range raw {
		normalized, err := graph.NormalizeCustomPropertiesValue(map[string]any{key: value})
		if err != nil {
			continue
		}
		for name, normalizedValue := range normalized {
			if _, exists := out[name]; exists {
				continue
			}
			out[name] = normalizedValue
		}
	}
	return out
}

func (idx *Index) nodesInOrder(selected map[graph.NodeID]struct{}, limit int) []graph.Node {
	if len(selected) == 0 {
		return []graph.Node{}
	}
	out := []graph.Node{}
	for _, node := range idx.nodes {
		if _, ok := selected[node.ID]; !ok {
			continue
		}
		out = append(out, node)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out
}

func propertyValueKey(name string, value any) (valueKey, error) {
	normalized := map[string]any{name: value}
	props, err := graph.NormalizeCustomPropertiesValue(normalized)
	if err != nil {
		return valueKey{}, err
	}
	return normalizedValueKey(name, props[name])
}

func normalizedValueKey(name string, value any) (valueKey, error) {
	switch typed := value.(type) {
	case string:
		return valueKey{name: name, valueType: "string", value: strings.TrimSpace(typed)}, nil
	case bool:
		return valueKey{name: name, valueType: "bool", value: strconv.FormatBool(typed)}, nil
	case json.Number:
		parsed, err := typed.Float64()
		if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
			return valueKey{}, fmt.Errorf("number must be finite")
		}
		return valueKey{name: name, valueType: "number", value: strconv.FormatFloat(parsed, 'g', -1, 64)}, nil
	default:
		if number, ok := numericFloat64(value); ok {
			return valueKey{name: name, valueType: "number", value: strconv.FormatFloat(number, 'g', -1, 64)}, nil
		}
		return valueKey{}, fmt.Errorf("unsupported property value type %T", value)
	}
}

func numericFloat64(value any) (float64, bool) {
	v := reflect.ValueOf(value)
	if !v.IsValid() {
		return 0, false
	}
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(v.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(v.Uint()), true
	case reflect.Float32, reflect.Float64:
		f := v.Convert(reflect.TypeOf(float64(0))).Float()
		return f, !math.IsNaN(f) && !math.IsInf(f, 0)
	default:
		return 0, false
	}
}

func addIndexEntry(index map[string]map[graph.NodeID]struct{}, key string, nodeID graph.NodeID) {
	if index[key] == nil {
		index[key] = map[graph.NodeID]struct{}{}
	}
	index[key][nodeID] = struct{}{}
}

func addValueIndexEntry(index map[valueKey]map[graph.NodeID]struct{}, key valueKey, nodeID graph.NodeID) {
	if index[key] == nil {
		index[key] = map[graph.NodeID]struct{}{}
	}
	index[key][nodeID] = struct{}{}
}
