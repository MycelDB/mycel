package analyzer

import (
	"reflect"
	"testing"

	"github.com/google/uuid"
	graph "github.com/myceldb/mycel/internal/graph/model"
)

func TestAnalyzeUnicodeCasePunctuationAndPositions(t *testing.T) {
	a := New()
	tokens := a.AnalyzeField("payload.text", "Hello, 世界! HELLO café-42", 7)
	got := terms(tokens)
	want := []string{"hello", "世界", "hello", "café", "42"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("terms = %#v, want %#v", got, want)
	}
	for i, token := range tokens {
		if token.Position != 7+i {
			t.Fatalf("token %d position = %d, want %d", i, token.Position, 7+i)
		}
		if token.FieldPath != "payload.text" {
			t.Fatalf("token %d field = %q", i, token.FieldPath)
		}
	}
}

func TestExtractNodeDocumentTraversesUserPayloadAndPropertiesOnly(t *testing.T) {
	node := graph.Node{
		ID:       uuid.NewSHA1(uuid.NameSpaceURL, []byte("node")),
		DomainID: uuid.NewSHA1(uuid.NameSpaceURL, []byte("domain")),
		Payload: map[string]any{
			"text": "body text",
			"nested": map[string]any{
				"z": "last",
				"a": []any{"first", map[string]any{"deep": "inside"}},
			},
			"number": 7,
		},
		Properties: map[string]any{
			"title":  "Title Text",
			"status": true,
		},
		Meta: map[string]any{
			"secret": "must not index",
		},
	}
	doc := ExtractNodeDocument(node)
	got := doc.Fields
	want := []Field{
		{Path: "payload.nested.a[0]", Text: "first"},
		{Path: "payload.nested.a[1].deep", Text: "inside"},
		{Path: "payload.nested.z", Text: "last"},
		{Path: "payload.text", Text: "body text"},
		{Path: "properties.title", Text: "Title Text"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("fields = %#v, want %#v", got, want)
	}
	allTokens := terms(AnalyzeDocument(New(), doc))
	for _, term := range allTokens {
		if term == "secret" || term == "must" || term == "index" {
			t.Fatalf("indexed meta term %q in %#v", term, allTokens)
		}
	}
}

func TestExtractNodeDocumentFallsBackToLegacyContentAndProps(t *testing.T) {
	node := graph.Node{
		ID:       uuid.NewSHA1(uuid.NameSpaceURL, []byte("legacy-node")),
		DomainID: uuid.NewSHA1(uuid.NameSpaceURL, []byte("legacy-domain")),
		Content:  "legacy content",
		Props: map[string]any{
			graph.NodePropCustomProperties: map[string]any{"tag": "legacy prop"},
			"systemish":                    "ignored because custom properties wrapper exists",
		},
	}
	doc := ExtractNodeDocument(node)
	want := []Field{
		{Path: "payload.text", Text: "legacy content"},
		{Path: "properties.tag", Text: "legacy prop"},
	}
	if !reflect.DeepEqual(doc.Fields, want) {
		t.Fatalf("fields = %#v, want %#v", doc.Fields, want)
	}
}

func terms(tokens []Token) []string {
	out := make([]string, len(tokens))
	for i, token := range tokens {
		out[i] = token.Term
	}
	return out
}
