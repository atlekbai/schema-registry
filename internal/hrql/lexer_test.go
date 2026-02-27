package hrql

import (
	"testing"
)

func collectTokens(t *testing.T, input string) []Token {
	t.Helper()
	lex := NewLexer(input)
	var tokens []Token
	for {
		tok, err := lex.Next()
		if err != nil {
			t.Fatalf("lexer error on %q: %v", input, err)
		}
		tokens = append(tokens, tok)
		if tok.Kind == TokEOF {
			break
		}
	}
	return tokens
}

func TestLexerSingleCharTokens(t *testing.T) {
	tests := []struct {
		input string
		kind  TokenKind
	}{
		{"|", TokPipe},
		{".", TokDot},
		{"(", TokLParen},
		{")", TokRParen},
		{",", TokComma},
		{"+", TokPlus},
		{"-", TokMinus},
		{"*", TokStar},
		{"/", TokSlash},
	}
	for _, tt := range tests {
		toks := collectTokens(t, tt.input)
		if len(toks) != 2 { // token + EOF
			t.Errorf("input %q: expected 2 tokens, got %d", tt.input, len(toks))
			continue
		}
		if toks[0].Kind != tt.kind {
			t.Errorf("input %q: expected %v, got %v", tt.input, tt.kind, toks[0].Kind)
		}
	}
}

func TestLexerTwoCharTokens(t *testing.T) {
	tests := []struct {
		input string
		kind  TokenKind
		lit   string
	}{
		{"==", TokEq, "=="},
		{"!=", TokNeq, "!="},
		{">=", TokGte, ">="},
		{"<=", TokLte, "<="},
		{">", TokGt, ">"},
		{"<", TokLt, "<"},
	}
	for _, tt := range tests {
		toks := collectTokens(t, tt.input)
		if toks[0].Kind != tt.kind {
			t.Errorf("input %q: expected %v, got %v", tt.input, tt.kind, toks[0].Kind)
		}
		if toks[0].Lit != tt.lit {
			t.Errorf("input %q: expected lit %q, got %q", tt.input, tt.lit, toks[0].Lit)
		}
	}
}

func TestLexerKeywords(t *testing.T) {
	tests := []struct {
		input string
		kind  TokenKind
	}{
		{"true", TokTrue},
		{"false", TokFalse},
		{"and", TokAnd},
		{"or", TokOr},
		{"asc", TokAsc},
		{"desc", TokDesc},
	}
	for _, tt := range tests {
		toks := collectTokens(t, tt.input)
		if toks[0].Kind != tt.kind {
			t.Errorf("input %q: expected %v, got %v", tt.input, tt.kind, toks[0].Kind)
		}
		if toks[0].Lit != tt.input {
			t.Errorf("input %q: expected lit %q, got %q", tt.input, tt.input, toks[0].Lit)
		}
	}
}

func TestLexerIdentifiers(t *testing.T) {
	tests := []string{"foo", "_bar", "foo_42", "employees", "self", "where", "salary__c"}
	for _, input := range tests {
		toks := collectTokens(t, input)
		if toks[0].Kind != TokIdent {
			t.Errorf("input %q: expected TokIdent, got %v", input, toks[0].Kind)
		}
		if toks[0].Lit != input {
			t.Errorf("input %q: expected lit %q, got %q", input, input, toks[0].Lit)
		}
	}
}

func TestLexerStrings(t *testing.T) {
	toks := collectTokens(t, `"hello"`)
	if toks[0].Kind != TokString {
		t.Fatalf("expected TokString, got %v", toks[0].Kind)
	}
	if toks[0].Lit != "hello" {
		t.Fatalf("expected lit %q, got %q", "hello", toks[0].Lit)
	}

	// Escaped quote
	toks = collectTokens(t, `"a\"b"`)
	if toks[0].Lit != `a\"b` {
		t.Fatalf("expected lit %q, got %q", `a\"b`, toks[0].Lit)
	}

	// Empty string
	toks = collectTokens(t, `""`)
	if toks[0].Kind != TokString || toks[0].Lit != "" {
		t.Fatalf("expected empty TokString, got %v %q", toks[0].Kind, toks[0].Lit)
	}
}

func TestLexerUnterminatedString(t *testing.T) {
	lex := NewLexer(`"hello`)
	_, err := lex.Next()
	if err == nil {
		t.Fatal("expected error for unterminated string")
	}
}

func TestLexerNumbers(t *testing.T) {
	tests := []struct {
		input string
		lit   string
	}{
		{"42", "42"},
		{"3.14", "3.14"},
		{"0", "0"},
		{"100", "100"},
	}
	for _, tt := range tests {
		toks := collectTokens(t, tt.input)
		if toks[0].Kind != TokNumber {
			t.Errorf("input %q: expected TokNumber, got %v", tt.input, toks[0].Kind)
		}
		if toks[0].Lit != tt.lit {
			t.Errorf("input %q: expected lit %q, got %q", tt.input, tt.lit, toks[0].Lit)
		}
	}
}

func TestLexerWhitespace(t *testing.T) {
	toks := collectTokens(t, "  foo  ")
	if len(toks) != 2 { // ident + EOF
		t.Fatalf("expected 2 tokens, got %d", len(toks))
	}
	if toks[0].Kind != TokIdent || toks[0].Lit != "foo" {
		t.Fatalf("expected ident 'foo', got %v %q", toks[0].Kind, toks[0].Lit)
	}
}

func TestLexerLineComment(t *testing.T) {
	toks := collectTokens(t, "// ignored\nfoo")
	if len(toks) != 2 {
		t.Fatalf("expected 2 tokens, got %d", len(toks))
	}
	if toks[0].Kind != TokIdent || toks[0].Lit != "foo" {
		t.Fatalf("expected ident 'foo', got %v %q", toks[0].Kind, toks[0].Lit)
	}
}

func TestLexerErrors(t *testing.T) {
	tests := []struct {
		input   string
		wantErr string
	}{
		{"=", "did you mean '=='"},
		{"!", "did you mean '!='"},
		{"@", "unexpected character"},
	}
	for _, tt := range tests {
		lex := NewLexer(tt.input)
		_, err := lex.Next()
		if err == nil {
			t.Errorf("input %q: expected error containing %q, got nil", tt.input, tt.wantErr)
			continue
		}
		if got := err.Error(); !contains(got, tt.wantErr) {
			t.Errorf("input %q: expected error containing %q, got %q", tt.input, tt.wantErr, got)
		}
	}
}

func TestLexerEOF(t *testing.T) {
	toks := collectTokens(t, "")
	if len(toks) != 1 || toks[0].Kind != TokEOF {
		t.Fatalf("expected single EOF token, got %v", toks)
	}
}

func TestLexerPeekIdempotent(t *testing.T) {
	lex := NewLexer("foo")
	t1, _ := lex.Peek()
	t2, _ := lex.Peek()
	if t1.Kind != t2.Kind || t1.Lit != t2.Lit || t1.Pos != t2.Pos {
		t.Fatalf("Peek not idempotent: %v vs %v", t1, t2)
	}
}

func TestLexerFullExpression(t *testing.T) {
	input := `employees | where(.department == "Engineering" and .level > 3) | count`
	toks := collectTokens(t, input)

	// Verify we get a reasonable number of tokens.
	if len(toks) < 15 {
		t.Fatalf("expected many tokens, got %d", len(toks))
	}

	// Spot-check key tokens.
	if toks[0].Kind != TokIdent || toks[0].Lit != "employees" {
		t.Errorf("token 0: expected ident 'employees', got %v %q", toks[0].Kind, toks[0].Lit)
	}
	if toks[1].Kind != TokPipe {
		t.Errorf("token 1: expected pipe, got %v", toks[1].Kind)
	}

	// Last non-EOF should be "count".
	last := toks[len(toks)-2]
	if last.Kind != TokIdent || last.Lit != "count" {
		t.Errorf("last token: expected ident 'count', got %v %q", last.Kind, last.Lit)
	}
}

func TestLexerPositionTracking(t *testing.T) {
	toks := collectTokens(t, "a | b")
	// a at pos 0, | at pos 2, b at pos 4
	if toks[0].Pos != 0 {
		t.Errorf("'a' pos: expected 0, got %d", toks[0].Pos)
	}
	if toks[1].Pos != 2 {
		t.Errorf("'|' pos: expected 2, got %d", toks[1].Pos)
	}
	if toks[2].Pos != 4 {
		t.Errorf("'b' pos: expected 4, got %d", toks[2].Pos)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && stringContains(s, substr)))
}

func stringContains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
