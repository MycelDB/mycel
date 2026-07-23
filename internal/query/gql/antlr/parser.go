package antlr

import (
	"fmt"
	"strings"

	antlr4 "github.com/antlr4-go/antlr/v4"
	"github.com/myceldb/mycel/internal/query/gql/antlr/generated"
)

// Parser parses GQL text into an ANTLR parse tree.
type Parser interface {
	Parse(query string) (antlr4.Tree, error)
}

type parser struct{}

func NewParser() Parser { return parser{} }

func Parse(query string) (antlr4.Tree, error) { return NewParser().Parse(query) }

func (parser) Parse(query string) (antlr4.Tree, error) {
	input := antlr4.NewInputStream(query)
	lexer := generated.NewMycelGQLLexer(input)
	lexerErrors := &syntaxErrorListener{}
	lexer.RemoveErrorListeners()
	lexer.AddErrorListener(lexerErrors)

	tokens := antlr4.NewCommonTokenStream(lexer, antlr4.TokenDefaultChannel)
	parser := generated.NewMycelGQLParser(tokens)
	parserErrors := &syntaxErrorListener{}
	parser.RemoveErrorListeners()
	parser.AddErrorListener(parserErrors)

	tree := parser.Query()
	if err := firstSyntaxError(lexerErrors, parserErrors); err != nil {
		return nil, err
	}
	return tree, nil
}

type syntaxErrorListener struct {
	*antlr4.DefaultErrorListener
	errors []string
}

func (l *syntaxErrorListener) SyntaxError(_ antlr4.Recognizer, _ any, line, column int, msg string, _ antlr4.RecognitionException) {
	l.errors = append(l.errors, fmt.Sprintf("line %d:%d %s", line, column, msg))
}

func firstSyntaxError(listeners ...*syntaxErrorListener) error {
	for _, listener := range listeners {
		if len(listener.errors) > 0 {
			return fmt.Errorf("syntax error: %s", strings.Join(listener.errors, "; "))
		}
	}
	return nil
}
