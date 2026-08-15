package service

import "testing"

func TestDecodeDefinitionRejectsLegacyModelFields(t *testing.T) {
	for name, raw := range map[string]string{
		"top-level":     `{"id":"a","model":{"provider":"openai","model":"gpt"}}`,
		"workflow-step": `{"id":"a","workflow":{"steps":[{"id":"llm","kind":"llm","model":{"provider":"openai"}}]}}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeDefinition(raw); err == nil {
				t.Fatalf("expected legacy model field rejection")
			}
		})
	}
}
