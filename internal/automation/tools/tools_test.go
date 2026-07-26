package tools

import (
	"context"
	"testing"
)

func TestRegistryRequiresAllowlist(t *testing.T) {
	r := NewRegistry(EchoTool{})
	if _, err := r.Execute(context.Background(), nil, "debug.echo", map[string]any{"x": 1}); err == nil {
		t.Fatal("expected allowlist error")
	}
	out, err := r.Execute(context.Background(), []string{"debug.echo"}, "debug.echo", map[string]any{"x": 1})
	if err != nil {
		t.Fatal(err)
	}
	if out["input"] == nil {
		t.Fatalf("unexpected output: %+v", out)
	}
}
