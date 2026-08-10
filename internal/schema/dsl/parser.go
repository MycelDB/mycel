package dsl

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
	graph "github.com/myceldb/mycel/internal/graph/model"
	schema "github.com/myceldb/mycel/internal/schema/model"
)

// Parse parses Mycel's human-authored schema DSL into the canonical schema model.
//
// Supported syntax is intentionally small and line-oriented:
//
//	schema "Knot PKM" version "1" mode strict domain 00000000-0000-0000-0000-000000000001
//	node Note {
//	  title: string required
//	  tags: string[]
//	}
//	edge contains from Note to Note hierarchy
//	edge references from Note to Note {
//	  confidence: float
//	}
//	index journal_entries_by_date on node JournalEntry field properties.date ordered asc
func Parse(input string) (schema.DomainSchema, error) {
	p := parser{lines: preprocess(input)}
	return p.parse()
}

type parser struct {
	lines []line
	pos   int
	out   schema.DomainSchema
}

type line struct {
	num  int
	text string
}

func preprocess(input string) []line {
	raw := strings.Split(input, "\n")
	out := make([]line, 0, len(raw))
	for i, text := range raw {
		text = stripComment(text)
		text = strings.TrimSpace(text)
		if text == "" {
			continue
		}
		if strings.HasSuffix(text, ";") {
			text = strings.TrimSpace(strings.TrimSuffix(text, ";"))
		}
		out = append(out, line{num: i + 1, text: text})
	}
	return out
}

func stripComment(value string) string {
	inQuote := false
	for i := 0; i < len(value); i++ {
		if value[i] == '"' && (i == 0 || value[i-1] != '\\') {
			inQuote = !inQuote
		}
		if !inQuote && value[i] == '#' {
			return value[:i]
		}
		if !inQuote && value[i] == '/' && i+1 < len(value) && value[i+1] == '/' {
			return value[:i]
		}
	}
	return value
}

func (p *parser) parse() (schema.DomainSchema, error) {
	p.out.Mode = schema.SchemaModePermissive
	for p.pos < len(p.lines) {
		current := p.lines[p.pos]
		lower := strings.ToLower(current.text)
		switch {
		case strings.HasPrefix(lower, "schema "):
			if err := p.parseSchemaHeader(current); err != nil {
				return p.out, err
			}
			p.pos++
		case strings.HasPrefix(lower, "node "):
			nt, err := p.parseNode()
			if err != nil {
				return p.out, err
			}
			p.out.NodeTypes = append(p.out.NodeTypes, nt)
		case strings.HasPrefix(lower, "edge "):
			et, err := p.parseEdge()
			if err != nil {
				return p.out, err
			}
			p.out.EdgeTypes = append(p.out.EdgeTypes, et)
		case strings.HasPrefix(lower, "index "):
			idx, err := p.parseIndex(current)
			if err != nil {
				return p.out, err
			}
			p.out.Indexes = append(p.out.Indexes, idx)
			p.pos++
		default:
			return p.out, p.err(current, "expected schema, node, edge, or index declaration")
		}
	}
	return p.out.Normalize(), nil
}

func (p *parser) parseSchemaHeader(l line) error {
	tokens, err := splitTokens(l.text)
	if err != nil {
		return p.err(l, err.Error())
	}
	for i := 1; i < len(tokens); i++ {
		switch strings.ToLower(tokens[i]) {
		case "version":
			i++
			if i >= len(tokens) {
				return p.err(l, "version requires a value")
			}
			p.out.Version = tokens[i]
		case "mode":
			i++
			if i >= len(tokens) {
				return p.err(l, "mode requires a value")
			}
			p.out.Mode = schema.SchemaMode(strings.ToLower(tokens[i]))
		case "domain", "domain_id", "domainid":
			i++
			if i >= len(tokens) {
				return p.err(l, "domain requires a UUID")
			}
			id, err := uuid.Parse(tokens[i])
			if err != nil {
				return p.err(l, fmt.Sprintf("domain must be a UUID: %v", err))
			}
			p.out.DomainID = graph.DomainID(id)
		default:
			if p.out.Name == "" {
				p.out.Name = tokens[i]
			} else {
				return p.err(l, fmt.Sprintf("unexpected schema option %q", tokens[i]))
			}
		}
	}
	return nil
}

func (p *parser) parseNode() (schema.NodeType, error) {
	l := p.lines[p.pos]
	header := strings.TrimSpace(strings.TrimSuffix(l.text, "{"))
	hasBlock := strings.HasSuffix(l.text, "{")
	tokens, err := splitTokens(header)
	if err != nil {
		return schema.NodeType{}, p.err(l, err.Error())
	}
	if len(tokens) < 2 {
		return schema.NodeType{}, p.err(l, "node requires a name")
	}
	nt := schema.NodeType{Name: tokens[1], Labels: []string{tokens[1]}}
	for i := 2; i < len(tokens); i++ {
		if strings.ToLower(tokens[i]) == "labels" {
			i++
			if i >= len(tokens) {
				return nt, p.err(l, "labels requires a value")
			}
			nt.Labels = splitList(tokens[i])
		} else {
			return nt, p.err(l, fmt.Sprintf("unexpected node option %q", tokens[i]))
		}
	}
	p.pos++
	if hasBlock {
		fields, err := p.parseFieldBlock()
		if err != nil {
			return nt, err
		}
		nt.Properties = fields
	}
	return nt, nil
}

func (p *parser) parseEdge() (schema.EdgeType, error) {
	l := p.lines[p.pos]
	header := strings.TrimSpace(strings.TrimSuffix(l.text, "{"))
	hasBlock := strings.HasSuffix(l.text, "{")
	tokens, err := splitTokens(header)
	if err != nil {
		return schema.EdgeType{}, p.err(l, err.Error())
	}
	if len(tokens) < 6 {
		return schema.EdgeType{}, p.err(l, "edge requires: edge NAME from NODE to NODE")
	}
	et := schema.EdgeType{Name: tokens[1], Labels: []string{tokens[1]}}
	for i := 2; i < len(tokens); i++ {
		switch strings.ToLower(tokens[i]) {
		case "from":
			i++
			if i >= len(tokens) {
				return et, p.err(l, "from requires a node type")
			}
			et.From.NodeTypes = splitList(tokens[i])
		case "to":
			i++
			if i >= len(tokens) {
				return et, p.err(l, "to requires a node type")
			}
			et.To.NodeTypes = splitList(tokens[i])
		case "labels":
			i++
			if i >= len(tokens) {
				return et, p.err(l, "labels requires a value")
			}
			et.Labels = splitList(tokens[i])
		case "hierarchy":
			et.Hierarchy = &schema.HierarchyPolicy{Enabled: true, Acyclic: true, SingleParent: true, SameDomain: true}
		default:
			return et, p.err(l, fmt.Sprintf("unexpected edge option %q", tokens[i]))
		}
	}
	p.pos++
	if hasBlock {
		fields, err := p.parseFieldBlock()
		if err != nil {
			return et, err
		}
		et.Properties = fields
	}
	return et, nil
}

func (p *parser) parseIndex(l line) (schema.IndexDefinition, error) {
	tokens, err := splitTokens(l.text)
	if err != nil {
		return schema.IndexDefinition{}, p.err(l, err.Error())
	}
	if len(tokens) < 7 {
		return schema.IndexDefinition{}, p.err(l, "index requires: index NAME on node|edge TYPE field properties.NAME")
	}
	idx := schema.IndexDefinition{Name: tokens[1], Kind: schema.IndexKindEquality}
	for i := 2; i < len(tokens); i++ {
		switch strings.ToLower(tokens[i]) {
		case "on":
			i++
			if i >= len(tokens) {
				return idx, p.err(l, "on requires node or edge")
			}
			switch strings.ToLower(tokens[i]) {
			case "node":
				idx.TargetKind = schema.IndexTargetNode
			case "edge":
				idx.TargetKind = schema.IndexTargetEdge
			default:
				return idx, p.err(l, "on requires node or edge")
			}
			i++
			if i >= len(tokens) {
				return idx, p.err(l, "index target type is required")
			}
			idx.TargetType = tokens[i]
		case "field":
			i++
			if i >= len(tokens) {
				return idx, p.err(l, "field requires a path")
			}
			field, err := parseFieldPath(tokens[i])
			if err != nil {
				return idx, p.err(l, err.Error())
			}
			idx.Field = field
		case "ordered":
			idx.Kind = schema.IndexKindOrdered
			if i+1 < len(tokens) {
				next := strings.ToLower(tokens[i+1])
				if next == string(schema.IndexSortDirectionAsc) || next == string(schema.IndexSortDirectionDesc) {
					idx.Direction = schema.IndexSortDirection(next)
					i++
				}
			}
		case "unique":
			idx.Unique = true
		case "required":
			idx.Required = true
		default:
			return idx, p.err(l, fmt.Sprintf("unexpected index option %q", tokens[i]))
		}
	}
	return idx, nil
}

func parseFieldPath(value string) (schema.FieldPath, error) {
	parts := strings.Split(value, ".")
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return schema.FieldPath{}, fmt.Errorf("field path must be namespace.name")
	}
	return schema.FieldPath{Namespace: strings.TrimSpace(parts[0]), Name: strings.TrimSpace(parts[1])}, nil
}

func (p *parser) parseFieldBlock() ([]schema.FieldSpec, error) {
	fields := []schema.FieldSpec{}
	for p.pos < len(p.lines) {
		l := p.lines[p.pos]
		if l.text == "}" {
			p.pos++
			return fields, nil
		}
		field, err := parseField(l.text)
		if err != nil {
			return fields, p.err(l, err.Error())
		}
		fields = append(fields, field)
		p.pos++
	}
	return fields, fmt.Errorf("unterminated field block")
}

func parseField(text string) (schema.FieldSpec, error) {
	parts := strings.SplitN(text, ":", 2)
	if len(parts) != 2 {
		return schema.FieldSpec{}, fmt.Errorf("field requires NAME: TYPE")
	}
	name := strings.TrimSpace(parts[0])
	required := true
	if strings.HasSuffix(name, "?") {
		required = false
		name = strings.TrimSuffix(name, "?")
	}
	tokens, err := splitTokens(parts[1])
	if err != nil {
		return schema.FieldSpec{}, err
	}
	if len(tokens) == 0 {
		return schema.FieldSpec{}, fmt.Errorf("field %q requires a type", name)
	}
	ftype := tokens[0]
	repeated := false
	if strings.HasSuffix(ftype, "[]") {
		repeated = true
		ftype = strings.TrimSuffix(ftype, "[]")
	}
	field := schema.FieldSpec{Name: name, Type: schema.FieldType(strings.ToLower(ftype)), Required: required, Repeated: repeated}
	start := 1
	if field.Type == schema.FieldTypeEnum && len(tokens) > 1 && !isFieldKeyword(tokens[1]) {
		field.EnumValues = splitList(tokens[1])
		start = 2
	}
	for i := start; i < len(tokens); i++ {
		switch strings.ToLower(tokens[i]) {
		case "required":
			field.Required = true
		case "optional":
			field.Required = false
		case "enum":
			i++
			if i >= len(tokens) {
				return field, fmt.Errorf("enum requires values")
			}
			field.Type = schema.FieldTypeEnum
			field.EnumValues = splitList(tokens[i])
		default:
			return field, fmt.Errorf("unexpected field option %q", tokens[i])
		}
	}
	return field, nil
}

func isFieldKeyword(value string) bool {
	switch strings.ToLower(value) {
	case "required", "optional", "enum":
		return true
	default:
		return false
	}
}

func splitList(value string) []string {
	value = strings.TrimSpace(value)
	if strings.HasPrefix(value, "[") && strings.HasSuffix(value, "]") {
		value = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(value, "["), "]"))
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func splitTokens(value string) ([]string, error) {
	var out []string
	var b strings.Builder
	inQuote := false
	for i := 0; i < len(value); i++ {
		ch := value[i]
		if ch == '"' && (i == 0 || value[i-1] != '\\') {
			inQuote = !inQuote
			continue
		}
		if !inQuote && (ch == ' ' || ch == '\t') {
			if b.Len() > 0 {
				out = append(out, b.String())
				b.Reset()
			}
			continue
		}
		b.WriteByte(ch)
	}
	if inQuote {
		return nil, fmt.Errorf("unterminated quote")
	}
	if b.Len() > 0 {
		out = append(out, b.String())
	}
	return out, nil
}

func (p *parser) err(l line, msg string) error { return fmt.Errorf("line %d: %s", l.num, msg) }
