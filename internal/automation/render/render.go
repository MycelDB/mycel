package render

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"

	automation "github.com/myceldb/mycel/internal/automation/model"
	graph "github.com/myceldb/mycel/internal/graph/model"
)

type Context struct {
	Changed graph.Node
	Old     *graph.Node
	Aliases map[string]any
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
		text = renderFields(ctx.Changed, input.Fields)
	case automation.InputModeTemplate:
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

func renderTemplate(tmpl string, ctx Context) string {
	return templateExpr.ReplaceAllStringFunc(tmpl, func(match string) string {
		parts := templateExpr.FindStringSubmatch(match)
		if len(parts) != 2 {
			return ""
		}
		return fmt.Sprint(resolvePath(ctx, strings.TrimSpace(parts[1])))
	})
}

func resolvePath(ctx Context, path string) any {
	switch {
	case strings.HasPrefix(path, "changed."):
		return ReadNodePath(ctx.Changed, strings.TrimPrefix(path, "changed."))
	case strings.HasPrefix(path, "old.") && ctx.Old != nil:
		return ReadNodePath(*ctx.Old, strings.TrimPrefix(path, "old."))
	}
	if ctx.Aliases != nil {
		if value, ok := ctx.Aliases[path]; ok {
			return value
		}
	}
	return ""
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
