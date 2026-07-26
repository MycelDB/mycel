package embedding

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	domainembedding "github.com/myceldb/mycel/internal/embedding/domain"
	"github.com/myceldb/mycel/internal/graph/model"
)

type SourceInput struct {
	Root         graph.Node
	Nodes        []graph.Node
	Edges        []graph.Edge
	Mode         domainembedding.SourceMode
	IncludeProps []string
	MaxDepth     *int
}

type SourceResult struct {
	Text string
	Hash string
}

func AssembleSource(in SourceInput) SourceResult {
	mode := in.Mode
	if mode == "" {
		mode = domainembedding.SourceModeSubtree
	}
	byID := map[graph.NodeID]graph.Node{}
	for _, n := range in.Nodes {
		byID[n.ID] = n
	}
	children := map[graph.NodeID][]graph.Edge{}
	for _, e := range in.Edges {
		if graph.EdgeHasLabels(e, []string{"contains"}) {
			children[e.FromID] = append(children[e.FromID], e)
		}
	}
	for parent := range children {
		sort.SliceStable(children[parent], func(i, j int) bool {
			li, lok := edgeOrder(children[parent][i])
			rj, rok := edgeOrder(children[parent][j])
			if lok && rok && li != rj {
				return li < rj
			}
			if lok != rok {
				return lok
			}
			return children[parent][i].ID.String() < children[parent][j].ID.String()
		})
	}
	var b strings.Builder
	writeNode := func(n graph.Node, depth int) {
		text := nodeText(n, in.IncludeProps)
		if text == "" {
			return
		}
		if depth == 0 {
			b.WriteString(text)
			b.WriteString("\n")
			return
		}
		b.WriteString(strings.Repeat("  ", depth-1))
		b.WriteString("- ")
		b.WriteString(strings.ReplaceAll(text, "\n", "\n"+strings.Repeat("  ", depth)+"  "))
		b.WriteString("\n")
	}
	var walk func(graph.Node, int)
	walk = func(n graph.Node, depth int) {
		writeNode(n, depth)
		if mode != domainembedding.SourceModeSubtree {
			return
		}
		if in.MaxDepth != nil && depth >= *in.MaxDepth {
			return
		}
		for _, e := range children[n.ID] {
			child, ok := byID[e.ToID]
			if !ok {
				continue
			}
			walk(child, depth+1)
		}
	}
	walk(in.Root, 0)
	text := strings.TrimSpace(b.String())
	h := sha256.Sum256([]byte(fmt.Sprintf("mode=%s\nprops=%s\nmax_depth=%s\n%s", mode, strings.Join(in.IncludeProps, ","), maxDepthText(in.MaxDepth), text)))
	return SourceResult{Text: text, Hash: hex.EncodeToString(h[:])}
}

func nodeText(n graph.Node, includeProps []string) string {
	parts := []string{}
	if text := strings.TrimSpace(graph.PayloadText(n)); text != "" {
		parts = append(parts, text)
	}
	for _, key := range includeProps {
		if v, ok := graph.Property(n, key); ok {
			s := strings.TrimSpace(fmt.Sprint(v))
			if s != "" {
				parts = append(parts, fmt.Sprintf("%s: %s", key, s))
			}
		}
	}
	return strings.Join(parts, "\n")
}

func maxDepthText(v *int) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%d", *v)
}

func edgeOrder(e graph.Edge) (float64, bool) {
	if e.Properties == nil {
		return 0, false
	}
	v, ok := e.Properties["order"]
	if !ok {
		return 0, false
	}
	switch x := v.(type) {
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case jsonNumber:
		f, err := x.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}

type jsonNumber interface{ Float64() (float64, error) }
