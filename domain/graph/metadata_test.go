package graph

import (
	"reflect"
	"testing"
)

func TestNormalizeTagsValue(t *testing.T) {
	tags, err := NormalizeTagsValue([]any{" Project ", "#Urgent", "project", "two  words"})
	if err != nil {
		t.Fatalf("NormalizeTagsValue returned error: %v", err)
	}
	expected := []string{"project", "urgent", "two words"}
	if !reflect.DeepEqual(tags, expected) {
		t.Fatalf("expected %#v, got %#v", expected, tags)
	}
}

func TestNormalizeTagsValueRejectsInvalidInput(t *testing.T) {
	for _, value := range []any{"project", []any{"project", 12}, []any{""}} {
		if _, err := NormalizeTagsValue(value); err == nil {
			t.Fatalf("expected error for %#v", value)
		}
	}
}

func TestNormalizeCustomPropertiesValue(t *testing.T) {
	props, err := NormalizeCustomPropertiesValue(map[string]any{
		" Priority ": " high ",
		"Due Date":   "2026-06-20",
		"Rating":     5.0,
		"Flagged":    true,
	})
	if err != nil {
		t.Fatalf("NormalizeCustomPropertiesValue returned error: %v", err)
	}
	expected := map[string]any{"priority": "high", "due date": "2026-06-20", "rating": 5.0, "flagged": true}
	if !reflect.DeepEqual(props, expected) {
		t.Fatalf("expected %#v, got %#v", expected, props)
	}
}

func TestNormalizeCustomPropertiesValueRejectsInvalidInput(t *testing.T) {
	cases := []any{
		[]any{"not-object"},
		map[string]any{"": "value"},
		map[string]any{"priority": nil},
		map[string]any{"list": []any{"a"}},
		map[string]any{"Priority": "A", "priority": "B"},
	}
	for _, value := range cases {
		if _, err := NormalizeCustomPropertiesValue(value); err == nil {
			t.Fatalf("expected error for %#v", value)
		}
	}
}

func TestNormalizeNodeMetadataPropsOnlyNormalizesCanonicalMetadataKeys(t *testing.T) {
	props, err := NormalizeNodeMetadataProps(map[string]any{
		"tags":       []any{"Project"},
		"properties": map[string]any{"Priority": " A "},
		"status":     "todo",
	})
	if err != nil {
		t.Fatalf("NormalizeNodeMetadataProps returned error: %v", err)
	}
	if !reflect.DeepEqual(props["tags"], []string{"project"}) {
		t.Fatalf("unexpected tags: %#v", props["tags"])
	}
	if !reflect.DeepEqual(props["properties"], map[string]any{"priority": "A"}) {
		t.Fatalf("unexpected properties: %#v", props["properties"])
	}
	if props["status"] != "todo" {
		t.Fatalf("expected non-metadata prop to be preserved, got %#v", props["status"])
	}
}
