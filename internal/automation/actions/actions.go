package actions

import (
	"context"
	"fmt"
	"strings"

	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/output"
	graph "github.com/myceldb/mycel/internal/graph/model"
	graphservice "github.com/myceldb/mycel/internal/graph/service"
	sessionservice "github.com/myceldb/mycel/internal/session/service"
)

type Engine struct{ Graphs graphservice.Manager }

type Context struct {
	Definition automation.Definition
	RunID      string
	Changed    graph.Node
	Result     output.Result
}

type Summary struct {
	Mutations int
	Changed   bool
}

func (e Engine) Apply(ctx context.Context, tx sessionservice.GraphTransaction, in Context) (Summary, error) {
	if e.Graphs == nil {
		return Summary{}, fmt.Errorf("graph manager is required")
	}
	refs := map[string]string{"changed": in.Changed.ID.String()}
	summary := Summary{}
	for _, action := range in.Definition.Output.Actions {
		s, err := e.applyAction(ctx, tx, in, refs, action)
		if err != nil {
			return summary, err
		}
		summary.Mutations += s.Mutations
		summary.Changed = summary.Changed || s.Changed
	}
	return summary, nil
}

func (e Engine) applyAction(ctx context.Context, tx sessionservice.GraphTransaction, in Context, refs map[string]string, action automation.Action) (Summary, error) {
	switch {
	case action.UpdateNode != nil:
		return e.updateNode(ctx, tx, in, *action.UpdateNode)
	case action.CreateNode != nil:
		return e.createNode(ctx, tx, in, refs, *action.CreateNode)
	case action.CreateEdge != nil:
		return e.createEdge(ctx, tx, in, refs, *action.CreateEdge, false)
	case action.UpsertEdge != nil:
		return e.createEdge(ctx, tx, in, refs, *action.UpsertEdge, true)
	default:
		return Summary{}, fmt.Errorf("missing action kind")
	}
}

func (e Engine) updateNode(ctx context.Context, tx sessionservice.GraphTransaction, in Context, action automation.UpdateNodeAction) (Summary, error) {
	if action.Target != "changed" {
		return Summary{}, fmt.Errorf("update_node target must be changed")
	}
	node := in.Changed
	changed := false
	for path, expr := range action.Set {
		value, err := output.Resolve(in.Result, expr, nil)
		if err != nil {
			return Summary{}, err
		}
		if fmt.Sprint(readNodePath(node, path)) == fmt.Sprint(value) {
			continue
		}
		writeNodePath(&node, path, value)
		changed = true
	}
	if !changed {
		return Summary{Changed: false}, nil
	}
	tagAutomation(&node, in)
	_, err := e.Graphs.UpdateNode(ctx, tx, graphservice.UpdateNodeInput{NodeID: node.ID.String(), Labels: node.Labels, Properties: node.Properties, Payload: node.Payload, Meta: node.Meta, Content: &node.Content, Props: node.Props})
	if err != nil {
		return Summary{}, err
	}
	return Summary{Mutations: 1, Changed: true}, nil
}

func (e Engine) createNode(ctx context.Context, tx sessionservice.GraphTransaction, in Context, refs map[string]string, action automation.CreateNodeAction) (Summary, error) {
	items, err := output.Items(in.Result, action.ForEach)
	if err != nil {
		return Summary{}, err
	}
	summary := Summary{}
	for _, item := range items {
		props, err := resolveMap(in.Result, action.Properties, item)
		if err != nil {
			return summary, err
		}
		payload, err := resolveMap(in.Result, action.Payload, item)
		if err != nil {
			return summary, err
		}
		meta := automationMeta(in)
		created, err := e.Graphs.CreateNode(ctx, tx, graphservice.NodeInput{Labels: append([]string(nil), action.Labels...), Properties: props, Payload: payload, Meta: meta})
		if err != nil {
			return summary, err
		}
		if action.As != "" {
			refs[action.As] = created.ID.String()
		}
		summary.Mutations++
		summary.Changed = true
	}
	return summary, nil
}

func (e Engine) createEdge(ctx context.Context, tx sessionservice.GraphTransaction, in Context, refs map[string]string, action automation.EdgeAction, upsert bool) (Summary, error) {
	items, err := output.Items(in.Result, action.ForEach)
	if err != nil {
		return Summary{}, err
	}
	summary := Summary{}
	for _, item := range items {
		from, err := resolveNodeRef(action.From, refs)
		if err != nil {
			return summary, err
		}
		to, err := resolveNodeRef(action.To, refs)
		if err != nil {
			return summary, err
		}
		props, err := resolveMap(in.Result, action.Properties, item)
		if err != nil {
			return summary, err
		}
		if upsert && e.edgeExists(ctx, tx, from, to, action.Label) {
			continue
		}
		_, err = e.Graphs.CreateEdge(ctx, tx, graphservice.EdgeInput{FromNodeID: from, ToNodeID: to, Labels: []string{action.Label}, Properties: props, Meta: automationMeta(in)})
		if err != nil {
			return summary, err
		}
		summary.Mutations++
		summary.Changed = true
	}
	return summary, nil
}

func (e Engine) edgeExists(ctx context.Context, tx sessionservice.GraphTransaction, from, to, label string) bool {
	token := ""
	for {
		edges, next, err := e.Graphs.ListEdges(ctx, tx, 500, token)
		if err != nil {
			return false
		}
		for _, edge := range edges {
			if edge.FromID.String() == from && edge.ToID.String() == to && hasLabel(edge.Labels, label) {
				return true
			}
		}
		if next == "" {
			return false
		}
		token = next
	}
}

func resolveNodeRef(expr string, refs map[string]string) (string, error) {
	expr = strings.TrimSpace(expr)
	if strings.HasPrefix(expr, "$refs.") {
		expr = strings.TrimPrefix(expr, "$refs.")
	}
	if v, ok := refs[expr]; ok {
		return v, nil
	}
	if strings.HasPrefix(expr, "node:") {
		return strings.TrimPrefix(expr, "node:"), nil
	}
	return "", fmt.Errorf("unknown node ref %q", expr)
}

func resolveMap(result output.Result, values map[string]string, item any) (map[string]any, error) {
	out := map[string]any{}
	for k, expr := range values {
		v, err := output.Resolve(result, expr, item)
		if err != nil {
			return nil, err
		}
		out[k] = v
	}
	return out, nil
}

func tagAutomation(node *graph.Node, in Context) {
	if node.Meta == nil {
		node.Meta = map[string]any{}
	}
	node.Meta["automation"] = automationMeta(in)
}
func automationMeta(in Context) map[string]any {
	return map[string]any{"run_id": in.RunID, "automation_id": in.Definition.ID, "generated": true, "depth": 1}
}
func hasLabel(labels []string, label string) bool {
	for _, l := range labels {
		if l == label {
			return true
		}
	}
	return false
}

func readNodePath(node graph.Node, path string) any {
	switch {
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
func writeNodePath(node *graph.Node, path string, value any) {
	switch {
	case path == "content":
		node.Content = fmt.Sprint(value)
	case strings.HasPrefix(path, "props."):
		if node.Props == nil {
			node.Props = map[string]any{}
		}
		node.Props[strings.TrimPrefix(path, "props.")] = value
	case strings.HasPrefix(path, "properties."):
		if node.Properties == nil {
			node.Properties = map[string]any{}
		}
		node.Properties[strings.TrimPrefix(path, "properties.")] = value
	case strings.HasPrefix(path, "payload."):
		if node.Payload == nil {
			node.Payload = map[string]any{}
		}
		node.Payload[strings.TrimPrefix(path, "payload.")] = value
	}
}
