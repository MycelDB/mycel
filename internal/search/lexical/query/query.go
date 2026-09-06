package query

import "strings"

type Kind int

const (
	KindUnspecified Kind = iota
	KindTerm
	KindPhrase
	KindAnd
	KindOr
	KindNot
)

type Node struct {
	Kind     Kind
	Term     string
	Terms    []string
	Children []*Node
}

func Term(term string) *Node {
	return &Node{Kind: KindTerm, Term: term}
}

func Phrase(terms []string) *Node {
	return &Node{Kind: KindPhrase, Terms: append([]string(nil), terms...)}
}

func And(children ...*Node) *Node {
	return combine(KindAnd, children...)
}

func Or(children ...*Node) *Node {
	return combine(KindOr, children...)
}

func Not(child *Node) *Node {
	if child == nil {
		return nil
	}
	return &Node{Kind: KindNot, Children: []*Node{child}}
}

func (n *Node) String() string {
	if n == nil {
		return "<nil>"
	}
	switch n.Kind {
	case KindTerm:
		return n.Term
	case KindPhrase:
		return `"` + strings.Join(n.Terms, " ") + `"`
	case KindNot:
		if len(n.Children) == 0 {
			return "NOT(<nil>)"
		}
		return "NOT(" + n.Children[0].String() + ")"
	case KindAnd:
		return joinChildren("AND", n.Children)
	case KindOr:
		return joinChildren("OR", n.Children)
	default:
		return "<unspecified>"
	}
}

func combine(kind Kind, children ...*Node) *Node {
	flat := make([]*Node, 0, len(children))
	for _, child := range children {
		if child == nil {
			continue
		}
		if child.Kind == kind && (kind == KindAnd || kind == KindOr) {
			flat = append(flat, child.Children...)
			continue
		}
		flat = append(flat, child)
	}
	if len(flat) == 0 {
		return nil
	}
	if len(flat) == 1 {
		return flat[0]
	}
	return &Node{Kind: kind, Children: flat}
}

func joinChildren(op string, children []*Node) string {
	parts := make([]string, 0, len(children))
	for _, child := range children {
		parts = append(parts, child.String())
	}
	return "(" + strings.Join(parts, " "+op+" ") + ")"
}
