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

func TestRenderGQLTemplateUsesAliasesAndCollections(t *testing.T) {
	journal := graph.Node{Properties: map[string]any{"date": "2026-08-18"}}
	res, err := Render(automation.Input{Target: "journal", Mode: automation.InputModeGQLTemplate, Template: "# {{journal.properties.date}}\n{{#each entries}}- {{entry.payload.text}}\n{{/each}}"}, Context{
		Aliases: map[string]any{"journal": journal},
		Collections: map[string][]map[string]any{"entries": {
			{"entry": graph.Node{Payload: map[string]any{"text": "first"}}},
			{"entry": graph.Node{Payload: map[string]any{"text": "second"}}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := "# 2026-08-18\n- first\n- second\n"
	if res.Text != want {
		t.Fatalf("text = %q, want %q", res.Text, want)
	}
}

func TestRenderContextMembershipOrderingAndContentChangeFingerprint(t *testing.T) {
	input := automation.Input{Mode: automation.InputModeGQLTemplate, Template: "{{#each entries}}{{entry.payload.text}}\\n{{/each}}"}
	render := func(rows []map[string]any) Result {
		result, err := Render(input, Context{Collections: map[string][]map[string]any{"entries": rows}})
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	base := render([]map[string]any{{"entry": graph.Node{Payload: map[string]any{"text": "one"}}}, {"entry": graph.Node{Payload: map[string]any{"text": "two"}}}})
	if base.Hash == render([]map[string]any{{"entry": graph.Node{Payload: map[string]any{"text": "one"}}}}).Hash {
		t.Fatal("context membership did not change fingerprint")
	}
	if base.Hash == render([]map[string]any{{"entry": graph.Node{Payload: map[string]any{"text": "two"}}}, {"entry": graph.Node{Payload: map[string]any{"text": "one"}}}}).Hash {
		t.Fatal("context ordering did not change fingerprint")
	}
	if base.Hash == render([]map[string]any{{"entry": graph.Node{Payload: map[string]any{"text": "one-modified"}}}, {"entry": graph.Node{Payload: map[string]any{"text": "two"}}}}).Hash {
		t.Fatal("context source content did not change fingerprint")
	}
	// A policy-filtered or truncated result is represented by the bounded rows
	// supplied to rendering and therefore changes the same fingerprint.
	if base.Hash == render([]map[string]any{{"entry": graph.Node{Payload: map[string]any{"text": "two"}}}}).Hash {
		t.Fatal("policy exclusion/truncation did not change fingerprint")
	}
}

func TestRenderFieldsUsesInputTargetAlias(t *testing.T) {
	res, err := Render(automation.Input{Target: "journal", Mode: automation.InputModeFields, Fields: []string{"properties.date"}}, Context{Changed: graph.Node{Properties: map[string]any{"date": "wrong"}}, Aliases: map[string]any{"journal": graph.Node{Properties: map[string]any{"date": "right"}}}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.Text, "right") || strings.Contains(res.Text, "wrong") {
		t.Fatalf("unexpected text = %q", res.Text)
	}
}
