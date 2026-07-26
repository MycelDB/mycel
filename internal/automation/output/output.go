package output

import (
	"encoding/json"
	"fmt"
	"strings"
)

type Result struct {
	Mode string
	Text string
	JSON any
}

func Parse(mode string, schema json.RawMessage, raw string) (Result, error) {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" || mode == "text" {
		return Result{Mode: "text", Text: raw}, nil
	}
	if mode != "json" {
		return Result{}, fmt.Errorf("unsupported output mode %q", mode)
	}
	var value any
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		return Result{}, fmt.Errorf("parse json output: %w", err)
	}
	if len(schema) > 0 {
		if err := validateMinimalSchema(value, schema); err != nil {
			return Result{}, err
		}
	}
	return Result{Mode: "json", Text: raw, JSON: value}, nil
}

func Resolve(value Result, expr string, item any) (any, error) {
	expr = strings.TrimSpace(expr)
	switch {
	case expr == "$result.text":
		return value.Text, nil
	case expr == "$item":
		return item, nil
	case strings.HasPrefix(expr, "$result."):
		return path(value.JSON, strings.TrimPrefix(expr, "$result.")), nil
	default:
		return expr, nil
	}
}

func Items(value Result, expr string) ([]any, error) {
	if strings.TrimSpace(expr) == "" {
		return []any{nil}, nil
	}
	resolved, err := Resolve(value, expr, nil)
	if err != nil {
		return nil, err
	}
	if resolved == nil {
		return nil, nil
	}
	if arr, ok := resolved.([]any); ok {
		return arr, nil
	}
	return []any{resolved}, nil
}

func path(root any, p string) any {
	cur := root
	for _, part := range strings.Split(p, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = m[part]
	}
	return cur
}

func validateMinimalSchema(value any, raw json.RawMessage) error {
	var schema map[string]any
	if err := json.Unmarshal(raw, &schema); err != nil {
		return fmt.Errorf("output schema must be valid JSON: %w", err)
	}
	if typ, _ := schema["type"].(string); typ != "" && !matchesType(value, typ) {
		return fmt.Errorf("output does not match schema type %q", typ)
	}
	if required, ok := schema["required"].([]any); ok {
		obj, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("output required fields need object output")
		}
		for _, field := range required {
			name, _ := field.(string)
			if name != "" && obj[name] == nil {
				return fmt.Errorf("output missing required field %q", name)
			}
		}
	}
	return nil
}

func matchesType(value any, typ string) bool {
	switch typ {
	case "object":
		_, ok := value.(map[string]any)
		return ok
	case "array":
		_, ok := value.([]any)
		return ok
	case "string":
		_, ok := value.(string)
		return ok
	case "number":
		_, ok := value.(float64)
		return ok
	case "boolean":
		_, ok := value.(bool)
		return ok
	default:
		return true
	}
}
