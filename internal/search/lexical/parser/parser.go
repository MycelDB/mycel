package parser

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/myceldb/mycel/internal/search/lexical/analyzer"
	"github.com/myceldb/mycel/internal/search/lexical/query"
)

type ParseError struct {
	Message           string
	Position          int
	UnsupportedSyntax []string
}

func (e *ParseError) Error() string {
	if e == nil {
		return ""
	}
	if e.Position >= 0 {
		return fmt.Sprintf("lexical query parse error at byte %d: %s", e.Position, e.Message)
	}
	return "lexical query parse error: " + e.Message
}

type Result struct {
	Expr              *query.Node
	NormalizedTerms   []string
	UnsupportedSyntax []string
}

func Parse(input string) (Result, error) {
	p := &parser{lexer: newLexer(input), analyzer: analyzer.New()}
	if err := p.advance(); err != nil {
		return Result{}, err
	}
	if p.current.kind == tokenEOF {
		return Result{}, &ParseError{Message: "query is empty", Position: 0}
	}
	expr, err := p.parseOr()
	if err != nil {
		return Result{}, err
	}
	if p.current.kind != tokenEOF {
		return Result{}, &ParseError{Message: "unexpected token " + p.current.literal, Position: p.current.pos}
	}
	return Result{Expr: expr, NormalizedTerms: uniqueStrings(p.normalized), UnsupportedSyntax: uniqueStrings(p.unsupported)}, nil
}

type parser struct {
	lexer       *lexer
	analyzer    analyzer.Analyzer
	current     token
	normalized  []string
	unsupported []string
}

func (p *parser) advance() error {
	tok, err := p.lexer.next()
	if err != nil {
		return err
	}
	p.current = tok
	return nil
}

func (p *parser) parseOr() (*query.Node, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.current.kind == tokenOR {
		if err := p.advance(); err != nil {
			return nil, err
		}
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = query.Or(left, right)
	}
	return left, nil
}

func (p *parser) parseAnd() (*query.Node, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for {
		switch p.current.kind {
		case tokenAND:
			if err := p.advance(); err != nil {
				return nil, err
			}
			right, err := p.parseUnary()
			if err != nil {
				return nil, err
			}
			left = query.And(left, right)
		case tokenTerm, tokenPhrase, tokenLParen, tokenNOT, tokenMinus:
			right, err := p.parseUnary()
			if err != nil {
				return nil, err
			}
			left = query.And(left, right)
		default:
			return left, nil
		}
	}
}

func (p *parser) parseUnary() (*query.Node, error) {
	switch p.current.kind {
	case tokenNOT, tokenMinus:
		if err := p.advance(); err != nil {
			return nil, err
		}
		child, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return query.Not(child), nil
	default:
		return p.parsePrimary()
	}
}

func (p *parser) parsePrimary() (*query.Node, error) {
	switch p.current.kind {
	case tokenTerm:
		literal := p.current.literal
		pos := p.current.pos
		if err := p.rejectUnsupported(literal, pos); err != nil {
			return nil, err
		}
		if err := p.advance(); err != nil {
			return nil, err
		}
		return p.termExpr(literal, pos)
	case tokenPhrase:
		literal := p.current.literal
		pos := p.current.pos
		if err := p.advance(); err != nil {
			return nil, err
		}
		terms := termsFromTokens(p.analyzer.Analyze(literal))
		if len(terms) == 0 {
			return nil, &ParseError{Message: "phrase contains no searchable terms", Position: pos}
		}
		p.normalized = append(p.normalized, terms...)
		return query.Phrase(terms), nil
	case tokenLParen:
		pos := p.current.pos
		if err := p.advance(); err != nil {
			return nil, err
		}
		expr, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if p.current.kind != tokenRParen {
			return nil, &ParseError{Message: "missing closing parenthesis", Position: pos}
		}
		if err := p.advance(); err != nil {
			return nil, err
		}
		return expr, nil
	default:
		return nil, &ParseError{Message: "expected term, phrase, or group", Position: p.current.pos}
	}
}

func (p *parser) termExpr(literal string, pos int) (*query.Node, error) {
	terms := termsFromTokens(p.analyzer.Analyze(literal))
	if len(terms) == 0 {
		return nil, &ParseError{Message: "term contains no searchable characters", Position: pos}
	}
	p.normalized = append(p.normalized, terms...)
	children := make([]*query.Node, 0, len(terms))
	for _, term := range terms {
		children = append(children, query.Term(term))
	}
	return query.And(children...), nil
}

func (p *parser) rejectUnsupported(literal string, pos int) error {
	unsupported := unsupportedForms(literal)
	if len(unsupported) == 0 {
		return nil
	}
	p.unsupported = append(p.unsupported, unsupported...)
	return &ParseError{Message: "unsupported lexical query syntax: " + strings.Join(unsupported, ", "), Position: pos, UnsupportedSyntax: unsupported}
}

func termsFromTokens(tokens []analyzer.Token) []string {
	terms := make([]string, 0, len(tokens))
	for _, token := range tokens {
		terms = append(terms, token.Term)
	}
	return terms
}

func uniqueStrings(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func unsupportedForms(literal string) []string {
	var out []string
	if strings.ContainsAny(literal, "*?") {
		out = append(out, "wildcard")
	}
	if strings.Contains(literal, "~") {
		out = append(out, "fuzzy")
	}
	if strings.Contains(literal, ":") {
		out = append(out, "fielded")
	}
	if strings.ContainsAny(literal, "[]{}") {
		out = append(out, "range")
	}
	return out
}

type tokenKind int

const (
	tokenEOF tokenKind = iota
	tokenTerm
	tokenPhrase
	tokenAND
	tokenOR
	tokenNOT
	tokenLParen
	tokenRParen
	tokenMinus
)

type token struct {
	kind    tokenKind
	literal string
	pos     int
}

type lexer struct {
	input string
	pos   int
}

func newLexer(input string) *lexer { return &lexer{input: input} }

func (l *lexer) next() (token, error) {
	l.skipSpace()
	if l.pos >= len(l.input) {
		return token{kind: tokenEOF, pos: len(l.input)}, nil
	}
	pos := l.pos
	r, size := utf8.DecodeRuneInString(l.input[l.pos:])
	switch r {
	case '(':
		l.pos += size
		return token{kind: tokenLParen, literal: "(", pos: pos}, nil
	case ')':
		l.pos += size
		return token{kind: tokenRParen, literal: ")", pos: pos}, nil
	case '-':
		l.pos += size
		return token{kind: tokenMinus, literal: "-", pos: pos}, nil
	case '"':
		return l.phrase()
	default:
		return l.term()
	}
}

func (l *lexer) skipSpace() {
	for l.pos < len(l.input) {
		r, size := utf8.DecodeRuneInString(l.input[l.pos:])
		if !unicode.IsSpace(r) {
			return
		}
		l.pos += size
	}
}

func (l *lexer) phrase() (token, error) {
	pos := l.pos
	l.pos++
	var b strings.Builder
	for l.pos < len(l.input) {
		r, size := utf8.DecodeRuneInString(l.input[l.pos:])
		if r == '"' {
			l.pos += size
			return token{kind: tokenPhrase, literal: b.String(), pos: pos}, nil
		}
		if r == '\\' {
			l.pos += size
			if l.pos >= len(l.input) {
				return token{}, &ParseError{Message: "unterminated escape in phrase", Position: pos}
			}
			r, size = utf8.DecodeRuneInString(l.input[l.pos:])
		}
		b.WriteRune(r)
		l.pos += size
	}
	return token{}, &ParseError{Message: "unterminated phrase", Position: pos}
}

func (l *lexer) term() (token, error) {
	pos := l.pos
	for l.pos < len(l.input) {
		r, size := utf8.DecodeRuneInString(l.input[l.pos:])
		if unicode.IsSpace(r) || r == '(' || r == ')' || r == '"' {
			break
		}
		l.pos += size
	}
	literal := l.input[pos:l.pos]
	switch strings.ToUpper(literal) {
	case "AND":
		return token{kind: tokenAND, literal: literal, pos: pos}, nil
	case "OR":
		return token{kind: tokenOR, literal: literal, pos: pos}, nil
	case "NOT":
		return token{kind: tokenNOT, literal: literal, pos: pos}, nil
	default:
		return token{kind: tokenTerm, literal: literal, pos: pos}, nil
	}
}
