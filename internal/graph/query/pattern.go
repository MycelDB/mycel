package query

// Unbounded is used as a traversal depth maximum to mean no upper limit.
const Unbounded = -1

// DepthSpec defines the inclusive edge-depth range for a traversal step.
type DepthSpec struct {
	Min int
	Max int
}

// Depth creates an inclusive traversal depth specification.
func Depth(min, max int) DepthSpec { return DepthSpec{Min: min, Max: max} }

// NodeOption configures a node pattern.
type NodeOption func(*nodePattern)

// Template restricts a node pattern to nodes whose template key matches key.
func Template(key string) NodeOption {
	return func(n *nodePattern) { n.templateKey = key }
}

type nodePattern struct {
	alias       string
	templateKey string
}

type traversalStep struct {
	direction string
	kind      string
	depth     DepthSpec
	target    nodePattern
}

// GraphPattern describes a single linear graph pattern.
type GraphPattern struct {
	start   *nodePattern
	steps   []traversalStep
	pending *traversalStep
}

// Pattern creates an empty graph pattern.
func Pattern() *GraphPattern { return &GraphPattern{} }

// Node appends a node pattern. The first node is the pattern root; later nodes
// complete the most recent traversal step.
func (p *GraphPattern) Node(alias string, opts ...NodeOption) *GraphPattern {
	n := nodePattern{alias: alias}
	for _, opt := range opts {
		opt(&n)
	}
	if p.start == nil {
		p.start = &n
		return p
	}
	if p.pending != nil {
		p.pending.target = n
		p.steps = append(p.steps, *p.pending)
		p.pending = nil
	}
	return p
}

// Out appends an outgoing traversal step.
func (p *GraphPattern) Out(kind string, depth DepthSpec) *GraphPattern {
	p.pending = &traversalStep{direction: "out", kind: kind, depth: depth}
	return p
}
