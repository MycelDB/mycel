package hybrid

import (
	"fmt"
	"strings"

	graph "github.com/myceldb/mycel/internal/graph/model"
)

type Filters struct {
	NodeLabels []string
	NodeIDs    []string
	Properties []PropertyFilter
}

type PropertyFilter struct {
	Path     string
	Operator FilterOperator
	Values   []string
}

type FilterOperator string

const (
	FilterEquals    FilterOperator = "equals"
	FilterNotEquals FilterOperator = "not_equals"
	FilterIn        FilterOperator = "in"
	FilterContains  FilterOperator = "contains"
	FilterExists    FilterOperator = "exists"
)

func MatchesFilters(node graph.Node, filters Filters) (bool, error) {
	if !graph.HasLabels(node, filters.NodeLabels) {
		return false, nil
	}
	if len(filters.NodeIDs) > 0 && !containsString(normalizedSet(filters.NodeIDs), node.ID.String()) {
		return false, nil
	}
	for _, filter := range filters.Properties {
		matched, err := matchesPropertyFilter(node, filter)
		if err != nil || !matched {
			return matched, err
		}
	}
	return true, nil
}

func HasFilters(filters Filters) bool {
	return len(filters.NodeLabels) > 0 || len(filters.NodeIDs) > 0 || len(filters.Properties) > 0
}

func matchesPropertyFilter(node graph.Node, filter PropertyFilter) (bool, error) {
	path := strings.TrimSpace(filter.Path)
	if path == "" {
		return false, fmt.Errorf("property filter path is required")
	}
	value, ok := propertyPath(node, path)
	switch filter.Operator {
	case FilterExists:
		return ok, nil
	case FilterEquals:
		if len(filter.Values) != 1 {
			return false, fmt.Errorf("EQUALS requires exactly one value")
		}
		return ok && valueEquals(value, filter.Values[0]), nil
	case FilterNotEquals:
		if len(filter.Values) != 1 {
			return false, fmt.Errorf("NOT_EQUALS requires exactly one value")
		}
		return ok && !valueEquals(value, filter.Values[0]), nil
	case FilterIn:
		if len(filter.Values) == 0 {
			return false, fmt.Errorf("IN requires at least one value")
		}
		if !ok {
			return false, nil
		}
		for _, candidate := range filter.Values {
			if valueEquals(value, candidate) {
				return true, nil
			}
		}
		return false, nil
	case FilterContains:
		if len(filter.Values) != 1 {
			return false, fmt.Errorf("CONTAINS requires exactly one value")
		}
		return ok && valueContains(value, filter.Values[0]), nil
	case "":
		return false, fmt.Errorf("property filter operator is required")
	default:
		return false, fmt.Errorf("unsupported property filter operator %q", filter.Operator)
	}
}

func propertyPath(node graph.Node, path string) (any, bool) {
	parts := splitPath(path)
	if len(parts) == 0 {
		return nil, false
	}
	roots := []map[string]any{node.Properties, node.Props, node.Payload}
	if node.Props != nil {
		if nested, ok := node.Props["properties"].(map[string]any); ok {
			roots = append(roots, nested)
		}
	}
	for _, root := range roots {
		if root == nil {
			continue
		}
		if value, ok := traverseMap(root, parts); ok {
			return value, true
		}
	}
	if len(parts) == 1 {
		return graph.Property(node, parts[0])
	}
	return nil, false
}

func traverseMap(root map[string]any, parts []string) (any, bool) {
	var current any = root
	for _, part := range parts {
		m, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = m[part]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

func splitPath(path string) []string {
	raw := strings.Split(path, ".")
	parts := make([]string, 0, len(raw))
	for _, part := range raw {
		part = strings.TrimSpace(part)
		if part != "" {
			parts = append(parts, part)
		}
	}
	return parts
}

func valueEquals(value any, want string) bool {
	want = strings.TrimSpace(want)
	switch typed := value.(type) {
	case string:
		return typed == want
	case fmt.Stringer:
		return typed.String() == want
	case []string:
		for _, item := range typed {
			if item == want {
				return true
			}
		}
		return false
	case []any:
		for _, item := range typed {
			if valueEquals(item, want) {
				return true
			}
		}
		return false
	default:
		return fmt.Sprint(value) == want
	}
}

func valueContains(value any, want string) bool {
	switch typed := value.(type) {
	case string:
		return strings.Contains(typed, want)
	case []string:
		for _, item := range typed {
			if item == want {
				return true
			}
		}
		return false
	case []any:
		for _, item := range typed {
			if valueEquals(item, want) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func normalizedSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out[value] = struct{}{}
		}
	}
	return out
}

func containsString(set map[string]struct{}, value string) bool {
	_, ok := set[value]
	return ok
}
