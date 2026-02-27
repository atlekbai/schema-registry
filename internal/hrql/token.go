package hrql

import "fmt"

// TokenKind classifies a lexical token.
type TokenKind int

const (
	TokEOF    TokenKind = iota
	TokPipe             // |
	TokDot              // .
	TokLParen           // (
	TokRParen           // )
	TokComma            // ,
	TokEq               // ==
	TokNeq              // !=
	TokGt               // >
	TokGte              // >=
	TokLt               // <
	TokLte              // <=
	TokPlus             // +
	TokMinus            // -
	TokStar             // *
	TokSlash            // /
	TokIdent            // identifier
	TokString           // "string literal"
	TokNumber           // 42, 3.14
	TokTrue             // true
	TokFalse            // false
	TokAnd              // and
	TokOr               // or
	TokAsc              // asc
	TokDesc             // desc
)

// Token is a single lexical token produced by the lexer.
type Token struct {
	Kind TokenKind
	Lit  string // raw text of the token
	Pos  int    // byte offset in input
}

func (t Token) String() string {
	if t.Lit != "" {
		return fmt.Sprintf("%s(%q)", t.Kind, t.Lit)
	}
	return t.Kind.String()
}

var kindNames = map[TokenKind]string{
	TokEOF:    "EOF",
	TokPipe:   "|",
	TokDot:    ".",
	TokLParen: "(",
	TokRParen: ")",
	TokComma:  ",",
	TokEq:     "==",
	TokNeq:    "!=",
	TokGt:     ">",
	TokGte:    ">=",
	TokLt:     "<",
	TokLte:    "<=",
	TokPlus:   "+",
	TokMinus:  "-",
	TokStar:   "*",
	TokSlash:  "/",
	TokIdent:  "identifier",
	TokString: "string",
	TokNumber: "number",
	TokTrue:   "true",
	TokFalse:  "false",
	TokAnd:    "and",
	TokOr:     "or",
	TokAsc:    "asc",
	TokDesc:   "desc",
}

func (k TokenKind) String() string {
	if s, ok := kindNames[k]; ok {
		return s
	}
	return fmt.Sprintf("TokenKind(%d)", int(k))
}

var keywords = map[string]TokenKind{
	"true":  TokTrue,
	"false": TokFalse,
	"and":   TokAnd,
	"or":    TokOr,
	"asc":   TokAsc,
	"desc":  TokDesc,
}
