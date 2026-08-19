package render

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"

	"github.com/google/uuid"
	automation "github.com/myceldb/mycel/internal/automation/model"
	graph "github.com/myceldb/mycel/internal/graph/model"
	execmodel "github.com/myceldb/mycel/internal/query/gql/execution/model"
)

type Context struct {
	Changed     graph.Node
	Old         *graph.Node
	Aliases     map[string]any
	Collections map[string][]map[string]any
}

type Result struct {
	Text string
	Hash string
}

var templateExpr = regexp.MustCompile(`\{\{\s*([^{}]+?)\s*\}\}`)

func Render(input automation.Input, ctx Context) (Result, error) {
	input.Mode = strings.ToLower(strings.TrimSpace(input.Mode))
	if input.Mode == "" {
		if strings.TrimSpace(input.Template) != "" {
			input.Mode = automation.InputModeTemplate
		} else {
			input.Mode = automation.InputModeFields
		}
	}
	var text string
	switch input.Mode {
	case automation.InputModeFields, automation.InputModeMarkdown:
		target := ctx.Changed
		if alias, ok := nodeAlias(ctx, input.Target); ok {
			target = alias
		}
		text = renderFields(target, input.Fields)
	case automation.InputModeTemplate, automation.InputModeGQLTemplate:
		text = renderTemplate(input.Template, ctx)
	default:
		return Result{}, fmt.Errorf("unsupported input mode %q", input.Mode)
	}
	sum := sha256.Sum256([]byte(text))
	return Result{Text: text, Hash: hex.EncodeToString(sum[:])}, nil
}

func renderFields(node graph.Node, fields []string) string {
	var b strings.Builder
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		b.WriteString("# " + field + "\n")
		b.WriteString(fmt.Sprint(ReadNodePath(node, field)))
		b.WriteString("\n\n")
	}
	return b.String()
}

var eachBlockExpr = regexp.MustCompile(`(?s)\{\{#each\s+([A-Za-z_][A-Za-z0-9_]*)\s*\}\}(.*?)\{\{/each\}\}`)

func renderTemplate(tmpl string, ctx Context) string {
	text := eachBlockExpr.ReplaceAllStringFunc(tmpl, func(match string) string {
		parts := eachBlockExpr.FindStringSubmatch(match)
		if len(parts) != 3 {
			return ""
		}
		name := strings.TrimSpace(parts[1])
		block := parts[2]
		rows := ctx.Collections[name]
		var b strings.Builder
		for _, row := range rows {
			b.WriteString(renderTemplateExpressions(block, ctx, row))
		}
		return b.String()
	})
	return renderTemplateExpressions(text, ctx, nil)
}

func renderTemplateExpressions(tmpl string, ctx Context, row map[string]any) string {
	return templateExpr.ReplaceAllStringFunc(tmpl, func(match string) string {
		parts := templateExpr.FindStringSubmatch(match)
		if len(parts) != 2 {
			return ""
		}
		return fmt.Sprint(resolvePath(ctx, row, strings.TrimSpace(parts[1])))
	})
}

func resolvePath(ctx Context, row map[string]any, path string) any {
	if row != nil {
		if value, ok := resolveAliasPath(row, path); ok {
			return value
		}
	}
	switch {
	case strings.HasPrefix(path, "changed."):
		return ReadNodePath(ctx.Changed, strings.TrimPrefix(path, "changed."))
	case strings.HasPrefix(path, "old.") && ctx.Old != nil:
		return ReadNodePath(*ctx.Old, strings.TrimPrefix(path, "old."))
	}
	if ctx.Aliases != nil {
		if value, ok := resolveAliasPath(ctx.Aliases, path); ok {
			return value
		}
	}
	return ""
}

func nodeAlias(ctx Context, name string) (graph.Node, bool) {
	if strings.TrimSpace(name) == "" || strings.TrimSpace(name) == "changed" {
		return ctx.Changed, true
	}
	if ctx.Aliases == nil {
		return graph.Node{}, false
	}
	value, ok := ctx.Aliases[strings.TrimSpace(name)]
	if !ok {
		return graph.Node{}, false
	}
	switch n := value.(type) {
	case graph.Node:
		return n, true
	case execmodel.Node:
		return graphNodeFromExec(n), true
	default:
		return graph.Node{}, false
	}
}

func resolveAliasPath(aliases map[string]any, path string) (any, bool) {
	if value, ok := aliases[path]; ok {
		return value, true
	}
	alias, rest, ok := strings.Cut(path, ".")
	if !ok {
		return nil, false
	}
	value, ok := aliases[alias]
	if !ok {
		return nil, false
	}
	return readAnyPath(value, rest), true
}

func readAnyPath(value any, path string) any {
	switch v := value.(type) {
	case graph.Node:
		return ReadNodePath(v, path)
	case execmodel.Node:
		return readExecNodePath(v, path)
	case graph.Edge:
		return readGraphEdgePath(v, path)
	case execmodel.Edge:
		return readExecEdgePath(v, path)
	case map[string]any:
		return v[path]
	default:
		return ""
	}
}

func graphNodeFromExec(node execmodel.Node) graph.Node {
	id, _ := uuid.Parse(node.ID)
	domainID, _ := uuid.Parse(node.DomainID)
	return graph.Node{ID: graph.NodeID(id), DomainID: graph.DomainID(domainID), Labels: append([]string(nil), node.Labels...), Properties: copyMap(node.Properties), Payload: copyMap(node.Payload), Meta: copyMap(node.Meta)}
}

func readExecNodePath(node execmodel.Node, path string) any {
	switch {
	case path == "id":
		return node.ID
	case strings.HasPrefix(path, "properties."):
		return node.Properties[strings.TrimPrefix(path, "properties.")]
	case strings.HasPrefix(path, "payload."):
		return node.Payload[strings.TrimPrefix(path, "payload.")]
	case strings.HasPrefix(path, "meta."):
		return node.Meta[strings.TrimPrefix(path, "meta.")]
	default:
		return ""
	}
}

func readGraphEdgePath(edge graph.Edge, path string) any {
	switch {
	case path == "id":
		return edge.ID.String()
	case path == "fromId":
		return edge.FromID.String()
	case path == "toId":
		return edge.ToID.String()
	case strings.HasPrefix(path, "properties."):
		return edge.Properties[strings.TrimPrefix(path, "properties.")]
	case strings.HasPrefix(path, "payload."):
		return edge.Payload[strings.TrimPrefix(path, "payload.")]
	case strings.HasPrefix(path, "meta."):
		return edge.Meta[strings.TrimPrefix(path, "meta.")]
	default:
		return ""
	}
}

func readExecEdgePath(edge execmodel.Edge, path string) any {
	switch {
	case path == "id":
		return edge.ID
	case path == "fromId":
		return edge.FromID
	case path == "toId":
		return edge.ToID
	case strings.HasPrefix(path, "properties."):
		return edge.Properties[strings.TrimPrefix(path, "properties.")]
	case strings.HasPrefix(path, "payload."):
		return edge.Payload[strings.TrimPrefix(path, "payload.")]
	case strings.HasPrefix(path, "meta."):
		return edge.Meta[strings.TrimPrefix(path, "meta.")]
	default:
		return ""
	}
}

func copyMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func ReadNodePath(node graph.Node, path string) any {
	switch {
	case path == "id":
		return node.ID.String()
	case path == "content":
		return node.Content
	case strings.HasPrefix(path, "props."):
		return node.Props[strings.TrimPrefix(path, "props.")]
	case strings.HasPrefix(path, "properties."):
		return node.Properties[strings.TrimPrefix(path, "properties.")]
	case strings.HasPrefix(path, "payload."):
		return node.Payload[strings.TrimPrefix(path, "payload.")]
	default:
		return ""
	}
}
