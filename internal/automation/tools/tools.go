package tools

import (
	"context"
	"fmt"
	"sort"
)

type Tool interface {
	Name() string
	Execute(context.Context, map[string]any) (map[string]any, error)
}

type Registry struct{ tools map[string]Tool }

func NewRegistry(items ...Tool) *Registry {
	r := &Registry{tools: map[string]Tool{}}
	for _, item := range items {
		r.Register(item)
	}
	return r
}

func (r *Registry) Register(tool Tool) {
	if tool == nil {
		return
	}
	if r.tools == nil {
		r.tools = map[string]Tool{}
	}
	r.tools[tool.Name()] = tool
}

func (r *Registry) Execute(ctx context.Context, allowed []string, name string, input map[string]any) (map[string]any, error) {
	if !allowedTool(allowed, name) {
		return nil, fmt.Errorf("tool %q is not allowed", name)
	}
	tool := r.tools[name]
	if tool == nil {
		return nil, fmt.Errorf("tool %q is not registered", name)
	}
	return tool.Execute(ctx, input)
}

func (r *Registry) Names() []string {
	out := []string{}
	for name := range r.tools {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}
func allowedTool(allowed []string, name string) bool {
	for _, item := range allowed {
		if item == name || item == "*" {
			return true
		}
	}
	return false
}

type EchoTool struct{}

func (EchoTool) Name() string { return "debug.echo" }
func (EchoTool) Execute(_ context.Context, input map[string]any) (map[string]any, error) {
	return map[string]any{"input": input}, nil
}
