package hrql

import (
	"fmt"
	"unicode"
)

// Lexer tokenizes an HRQL input string.
type Lexer struct {
	input []rune
	pos   int
	peeked *Token
}

// NewLexer creates a lexer for the given input.
func NewLexer(input string) *Lexer {
	return &Lexer{input: []rune(input)}
}

// Peek returns the next token without consuming it.
func (l *Lexer) Peek() (Token, error) {
	if l.peeked != nil {
		return *l.peeked, nil
	}
	tok, err := l.next()
	if err != nil {
		return Token{}, err
	}
	l.peeked = &tok
	return tok, nil
}

// Next consumes and returns the next token.
func (l *Lexer) Next() (Token, error) {
	if l.peeked != nil {
		tok := *l.peeked
		l.peeked = nil
		return tok, nil
	}
	return l.next()
}

func (l *Lexer) next() (Token, error) {
	l.skipWhitespace()
	if l.pos >= len(l.input) {
		return Token{Kind: TokEOF, Pos: l.pos}, nil
	}

	ch := l.input[l.pos]
	pos := l.pos

	switch ch {
	case '|':
		l.pos++
		return Token{Kind: TokPipe, Lit: "|", Pos: pos}, nil
	case '.':
		l.pos++
		return Token{Kind: TokDot, Lit: ".", Pos: pos}, nil
	case '(':
		l.pos++
		return Token{Kind: TokLParen, Lit: "(", Pos: pos}, nil
	case ')':
		l.pos++
		return Token{Kind: TokRParen, Lit: ")", Pos: pos}, nil
	case ',':
		l.pos++
		return Token{Kind: TokComma, Lit: ",", Pos: pos}, nil
	case '+':
		l.pos++
		return Token{Kind: TokPlus, Lit: "+", Pos: pos}, nil
	case '-':
		l.pos++
		return Token{Kind: TokMinus, Lit: "-", Pos: pos}, nil
	case '*':
		l.pos++
		return Token{Kind: TokStar, Lit: "*", Pos: pos}, nil
	case '/':
		// Check for // comment
		if l.pos+1 < len(l.input) && l.input[l.pos+1] == '/' {
			l.skipLineComment()
			return l.next()
		}
		l.pos++
		return Token{Kind: TokSlash, Lit: "/", Pos: pos}, nil
	case '=':
		if l.pos+1 < len(l.input) && l.input[l.pos+1] == '=' {
			l.pos += 2
			return Token{Kind: TokEq, Lit: "==", Pos: pos}, nil
		}
		return Token{}, l.errorf(pos, "unexpected '=', did you mean '=='?")
	case '!':
		if l.pos+1 < len(l.input) && l.input[l.pos+1] == '=' {
			l.pos += 2
			return Token{Kind: TokNeq, Lit: "!=", Pos: pos}, nil
		}
		return Token{}, l.errorf(pos, "unexpected '!', did you mean '!='?")
	case '>':
		if l.pos+1 < len(l.input) && l.input[l.pos+1] == '=' {
			l.pos += 2
			return Token{Kind: TokGte, Lit: ">=", Pos: pos}, nil
		}
		l.pos++
		return Token{Kind: TokGt, Lit: ">", Pos: pos}, nil
	case '<':
		if l.pos+1 < len(l.input) && l.input[l.pos+1] == '=' {
			l.pos += 2
			return Token{Kind: TokLte, Lit: "<=", Pos: pos}, nil
		}
		l.pos++
		return Token{Kind: TokLt, Lit: "<", Pos: pos}, nil
	case '"':
		return l.readString(pos)
	default:
		if unicode.IsDigit(ch) {
			return l.readNumber(pos)
		}
		if isIdentStart(ch) {
			return l.readIdent(pos)
		}
		return Token{}, l.errorf(pos, "unexpected character %q", ch)
	}
}

func (l *Lexer) readString(pos int) (Token, error) {
	l.pos++ // skip opening "
	start := l.pos
	for l.pos < len(l.input) {
		if l.input[l.pos] == '\\' && l.pos+1 < len(l.input) {
			l.pos += 2 // skip escaped char
			continue
		}
		if l.input[l.pos] == '"' {
			lit := string(l.input[start:l.pos])
			l.pos++ // skip closing "
			return Token{Kind: TokString, Lit: lit, Pos: pos}, nil
		}
		l.pos++
	}
	return Token{}, l.errorf(pos, "unterminated string literal")
}

func (l *Lexer) readNumber(pos int) (Token, error) {
	start := l.pos
	for l.pos < len(l.input) && unicode.IsDigit(l.input[l.pos]) {
		l.pos++
	}
	if l.pos < len(l.input) && l.input[l.pos] == '.' {
		// Check this isn't a pipe step (e.g., `42 | .field`)
		if l.pos+1 < len(l.input) && unicode.IsDigit(l.input[l.pos+1]) {
			l.pos++ // consume .
			for l.pos < len(l.input) && unicode.IsDigit(l.input[l.pos]) {
				l.pos++
			}
		}
	}
	return Token{Kind: TokNumber, Lit: string(l.input[start:l.pos]), Pos: pos}, nil
}

func (l *Lexer) readIdent(pos int) (Token, error) {
	start := l.pos
	for l.pos < len(l.input) && isIdentCont(l.input[l.pos]) {
		l.pos++
	}
	lit := string(l.input[start:l.pos])
	kind := TokIdent
	if kw, ok := keywords[lit]; ok {
		kind = kw
	}
	return Token{Kind: kind, Lit: lit, Pos: pos}, nil
}

func (l *Lexer) skipWhitespace() {
	for l.pos < len(l.input) && unicode.IsSpace(l.input[l.pos]) {
		l.pos++
	}
}

func (l *Lexer) skipLineComment() {
	for l.pos < len(l.input) && l.input[l.pos] != '\n' {
		l.pos++
	}
}

func (l *Lexer) errorf(pos int, format string, args ...any) error {
	return fmt.Errorf("lexer error at position %d: %s", pos, fmt.Sprintf(format, args...))
}

func isIdentStart(ch rune) bool {
	return unicode.IsLetter(ch) || ch == '_'
}

func isIdentCont(ch rune) bool {
	return unicode.IsLetter(ch) || unicode.IsDigit(ch) || ch == '_'
}
