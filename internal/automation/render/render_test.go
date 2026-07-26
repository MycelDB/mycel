package render

import (
	"strings"
	"testing"

	automation "github.com/myceldb/mycel/internal/automation/model"
	graph "github.com/myceldb/mycel/internal/graph/model"
)

func TestRenderFields(t *testing.T) {
	res, err := Render(automation.Input{Mode: automation.InputModeFields, Fields: []string{"properties.title", "payload.text"}}, Context{Changed: graph.Node{Properties: map[string]any{"title": "Hello"}, Payload: map[string]any{"text": "World"}}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.Text, "Hello") || !strings.Contains(res.Text, "World") || res.Hash == "" {
		t.Fatalf("unexpected render result: %+v", res)
	}
}

func TestRenderTemplate(t *testing.T) {
	res, err := Render(automation.Input{Mode: automation.InputModeTemplate, Template: "# {{changed.properties.title}}\n{{changed.payload.text}}"}, Context{Changed: graph.Node{Properties: map[string]any{"title": "Hello"}, Payload: map[string]any{"text": "World"}}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Text != "# Hello\nWorld" {
		t.Fatalf("text = %q", res.Text)
	}
}

func TestRenderTemplateCanReferenceOldNode(t *testing.T) {
	old := graph.Node{Properties: map[string]any{"title": "Before"}}
	res, err := Render(automation.Input{Mode: automation.InputModeTemplate, Template: "{{old.properties.title}} -> {{changed.properties.title}}"}, Context{Changed: graph.Node{Properties: map[string]any{"title": "After"}}, Old: &old})
	if err != nil {
		t.Fatal(err)
	}
	if res.Text != "Before -> After" {
		t.Fatalf("text = %q", res.Text)
	}
}
