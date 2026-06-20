package graph

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
)

const (
	// NodePropTags is the canonical node props key for block/node-level tags.
	// Values are stored as a deduplicated []string of normalized tag names.
	NodePropTags = "tags"

	// NodePropCustomProperties is the canonical node props key for user-defined
	// name-value metadata. Values are stored as map[string]any with normalized
	// property names and scalar JSON-compatible values.
	NodePropCustomProperties = "properties"
)

// NormalizeNodeMetadataProps returns a copy of props with canonical custom
// metadata fields normalized when present. It intentionally only rewrites the
// standard custom metadata keys, leaving all other application/template props
// untouched.
func NormalizeNodeMetadataProps(props map[string]any) (map[string]any, error) {
	out := copyMetadataProps(props)
	if _, ok := out[NodePropTags]; ok {
		tags, err := NormalizeTagsValue(out[NodePropTags])
		if err != nil {
			return nil, err
		}
		out[NodePropTags] = tags
	}
	if _, ok := out[NodePropCustomProperties]; ok {
		customProps, err := NormalizeCustomPropertiesValue(out[NodePropCustomProperties])
		if err != nil {
			return nil, err
		}
		out[NodePropCustomProperties] = customProps
	}
	return out, nil
}

// NormalizeTag canonicalizes a tag identity. Tags are block/node-level labels,
// not inline markdown tokens; callers should not include leading # characters.
func NormalizeTag(value string) (string, error) {
	normalized := normalizeMetadataToken(value)
	normalized = strings.TrimPrefix(normalized, "#")
	normalized = normalizeMetadataToken(normalized)
	if normalized == "" {
		return "", fmt.Errorf("tag must not be empty")
	}
	return normalized, nil
}

// NormalizePropertyName canonicalizes a user-defined property name.
func NormalizePropertyName(value string) (string, error) {
	normalized := normalizeMetadataToken(value)
	if normalized == "" {
		return "", fmt.Errorf("property name must not be empty")
	}
	return normalized, nil
}

// NormalizeTagsValue accepts the wire/storage forms used by JSON and Go code
// and returns the canonical []string representation.
func NormalizeTagsValue(value any) ([]string, error) {
	if value == nil {
		return []string{}, nil
	}
	var raw []string
	switch typed := value.(type) {
	case []string:
		raw = append(raw, typed...)
	case []any:
		raw = make([]string, 0, len(typed))
		for _, item := range typed {
			text, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("tags must contain strings")
			}
			raw = append(raw, text)
		}
	default:
		return nil, fmt.Errorf("tags must be an array of strings")
	}

	seen := map[string]struct{}{}
	out := make([]string, 0, len(raw))
	for _, tag := range raw {
		normalized, err := NormalizeTag(tag)
		if err != nil {
			return nil, err
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	return out, nil
}

// NormalizeCustomPropertiesValue accepts a map of user-defined property names
// to scalar JSON-compatible values and returns its canonical representation.
func NormalizeCustomPropertiesValue(value any) (map[string]any, error) {
	if value == nil {
		return map[string]any{}, nil
	}

	var raw map[string]any
	switch typed := value.(type) {
	case map[string]any:
		raw = typed
	case map[string]string:
		raw = make(map[string]any, len(typed))
		for key, val := range typed {
			raw[key] = val
		}
	default:
		return nil, fmt.Errorf("properties must be an object")
	}

	out := make(map[string]any, len(raw))
	keys := make([]string, 0, len(raw))
	for key := range raw {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		name, err := NormalizePropertyName(key)
		if err != nil {
			return nil, err
		}
		if _, exists := out[name]; exists {
			return nil, fmt.Errorf("duplicate property name %q after normalization", name)
		}
		val, err := normalizeCustomPropertyValue(raw[key])
		if err != nil {
			return nil, fmt.Errorf("property %q: %w", name, err)
		}
		out[name] = val
	}
	return out, nil
}

func normalizeCustomPropertyValue(value any) (any, error) {
	switch typed := value.(type) {
	case nil:
		return nil, fmt.Errorf("value must not be null")
	case string:
		return strings.TrimSpace(typed), nil
	case bool:
		return typed, nil
	case int:
		return typed, nil
	case int8:
		return typed, nil
	case int16:
		return typed, nil
	case int32:
		return typed, nil
	case int64:
		return typed, nil
	case uint:
		return typed, nil
	case uint8:
		return typed, nil
	case uint16:
		return typed, nil
	case uint32:
		return typed, nil
	case uint64:
		return typed, nil
	case float32:
		if math.IsNaN(float64(typed)) || math.IsInf(float64(typed), 0) {
			return nil, fmt.Errorf("number must be finite")
		}
		return typed, nil
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) {
			return nil, fmt.Errorf("number must be finite")
		}
		return typed, nil
	case json.Number:
		parsed, err := typed.Float64()
		if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
			return nil, fmt.Errorf("number must be finite")
		}
		return typed, nil
	default:
		return nil, fmt.Errorf("value must be a string, number, or boolean")
	}
}

func normalizeMetadataToken(value string) string {
	return strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(value)), " "))
}

func copyMetadataProps(in map[string]any) map[string]any {
	out := map[string]any{}
	for key, value := range in {
		out[key] = value
	}
	return out
}
