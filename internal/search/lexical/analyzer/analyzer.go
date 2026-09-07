package analyzer

import (
	"fmt"
	"sort"
	"strings"
	"unicode"

	graph "github.com/myceldb/mycel/internal/graph/model"
)

const Version = "lexical-analyzer-v1"

type Token struct {
	Term      string
	Position  int
	StartByte int
	EndByte   int
	FieldPath string
}

type Field struct {
	Path string
	Text string
}

type Document struct {
	NodeID   string
	DomainID string
	Fields   []Field
}

type Analyzer struct{}

func New() Analyzer { return Analyzer{} }

func (Analyzer) Version() string { return Version }

func (a Analyzer) Analyze(text string) []Token {
	return a.AnalyzeField("", text, 0)
}

func (Analyzer) AnalyzeField(fieldPath, text string, startPosition int) []Token {
	var tokens []Token
	start := -1
	position := startPosition
	for i, r := range text {
		if isTokenRune(r) {
			if start < 0 {
				start = i
			}
			continue
		}
		if start >= 0 {
			tokens = append(tokens, Token{
				Term:      normalize(text[start:i]),
				Position:  position,
				StartByte: start,
				EndByte:   i,
				FieldPath: fieldPath,
			})
			position++
			start = -1
		}
	}
	if start >= 0 {
		tokens = append(tokens, Token{
			Term:      normalize(text[start:]),
			Position:  position,
			StartByte: start,
			EndByte:   len(text),
			FieldPath: fieldPath,
		})
	}
	return tokens
}

func ExtractNodeDocument(node graph.Node) Document {
	doc := Document{
		NodeID:   node.ID.String(),
		DomainID: node.DomainID.String(),
	}
	if node.Payload != nil {
		doc.Fields = append(doc.Fields, extractValue("payload", node.Payload)...)
	}
	if strings.TrimSpace(node.Content) != "" && !hasStringAtPath(doc.Fields, "payload.text") {
		doc.Fields = append(doc.Fields, Field{Path: "payload.text", Text: node.Content})
	}
	if node.Properties != nil {
		doc.Fields = append(doc.Fields, extractValue("properties", node.Properties)...)
	}
	if node.Props != nil {
		if nested, ok := node.Props[graph.NodePropCustomProperties]; ok {
			doc.Fields = append(doc.Fields, extractValue("properties", nested)...)
		} else {
			doc.Fields = append(doc.Fields, extractValue("properties", node.Props)...)
		}
	}
	sort.SliceStable(doc.Fields, func(i, j int) bool {
		if doc.Fields[i].Path == doc.Fields[j].Path {
			return doc.Fields[i].Text < doc.Fields[j].Text
		}
		return doc.Fields[i].Path < doc.Fields[j].Path
	})
	return doc
}

func AnalyzeDocument(a Analyzer, doc Document) []Token {
	var tokens []Token
	position := 0
	for _, field := range doc.Fields {
		fieldTokens := a.AnalyzeField(field.Path, field.Text, position)
		tokens = append(tokens, fieldTokens...)
		position += len(fieldTokens)
	}
	return tokens
}

func extractValue(path string, value any) []Field {
	switch typed := value.(type) {
	case string:
		if strings.TrimSpace(typed) == "" {
			return nil
		}
		return []Field{{Path: path, Text: typed}}
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		var fields []Field
		for _, key := range keys {
			fields = append(fields, extractValue(joinPath(path, key), typed[key])...)
		}
		return fields
	case []any:
		var fields []Field
		for i, item := range typed {
			fields = append(fields, extractValue(fmt.Sprintf("%s[%d]", path, i), item)...)
		}
		return fields
	default:
		return nil
	}
}

func joinPath(base, key string) string {
	if base == "" {
		return key
	}
	return base + "." + key
}

func hasStringAtPath(fields []Field, path string) bool {
	for _, field := range fields {
		if field.Path == path {
			return true
		}
	}
	return false
}

func isTokenRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r)
}

func normalize(term string) string {
	return strings.ToLower(term)
}
