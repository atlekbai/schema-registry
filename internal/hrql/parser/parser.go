package parser

import (
	"fmt"
	"strconv"
)

// Parse parses an HRQL expression string into an AST.
func Parse(input string) (Node, error) {
	p := &parser{lexer: NewLexer(input), input: input}
	node, err := p.parsePipeExpr()
	if err != nil {
		return nil, err
	}
	// Ensure we consumed everything.
	tok, err := p.peek()
	if err != nil {
		return nil, err
	}
	if tok.Kind != TokEOF {
		return nil, p.errorf(tok.Pos, "unexpected %s, expected end of expression", tok.Kind)
	}
	return node, nil
}

type parser struct {
	lexer *Lexer
	input string
}

// parsePipeExpr: arithExpr { "|" pipeStep }
func (p *parser) parsePipeExpr() (Node, error) {
	first, err := p.parseArithExpr()
	if err != nil {
		return nil, err
	}

	tok, err := p.peek()
	if err != nil {
		return nil, err
	}

	// Handle self.field shorthand: self followed by .field -> PipeExpr{SelfExpr, FieldAccess}
	if _, isSelf := first.(*SelfExpr); isSelf && tok.Kind == TokDot {
		fa, err := p.parseFieldAccessChain()
		if err != nil {
			return nil, err
		}
		first = &PipeExpr{Steps: []Node{first, fa}}
		tok, err = p.peek()
		if err != nil {
			return nil, err
		}
	}

	if tok.Kind != TokPipe {
		return first, nil
	}

	steps := []Node{first}
	for {
		tok, err = p.peek()
		if err != nil {
			return nil, err
		}
		if tok.Kind != TokPipe {
			break
		}
		p.advance() // consume |

		step, err := p.parsePipeStep()
		if err != nil {
			return nil, err
		}
		steps = append(steps, step)
	}

	if len(steps) == 1 {
		return steps[0], nil
	}
	return &PipeExpr{Steps: steps}, nil
}

// parseArithExpr: arithTerm { ("+" | "-") arithTerm }
func (p *parser) parseArithExpr() (Node, error) {
	left, err := p.parseArithTerm()
	if err != nil {
		return nil, err
	}
	for {
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		if tok.Kind != TokPlus && tok.Kind != TokMinus {
			break
		}
		p.advance()
		right, err := p.parseArithTerm()
		if err != nil {
			return nil, err
		}
		left = &BinaryOp{Op: tok.Lit, Left: left, Right: right}
	}
	return left, nil
}

// parseArithTerm: primary { ("*" | "/") primary }
func (p *parser) parseArithTerm() (Node, error) {
	left, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	for {
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		if tok.Kind != TokStar && tok.Kind != TokSlash {
			break
		}
		p.advance()
		right, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		left = &BinaryOp{Op: tok.Lit, Left: left, Right: right}
	}
	return left, nil
}

// parsePipeStep handles the right side of a pipe operator.
func (p *parser) parsePipeStep() (Node, error) {
	tok, err := p.peek()
	if err != nil {
		return nil, err
	}

	switch tok.Kind {
	case TokDot:
		return p.parseFieldAccessChain()

	case TokIdent:
		return p.parsePipeIdent()

	default:
		return nil, p.errorf(tok.Pos, "unexpected %s in pipe, expected field access or function", tok.Kind)
	}
}

// parsePipeIdent handles identifiers in pipe position.
func (p *parser) parsePipeIdent() (Node, error) {
	tok, err := p.peek()
	if err != nil {
		return nil, err
	}
	name := tok.Lit

	switch name {
	case "where":
		return p.parseWhere()
	case "sort_by":
		return p.parseSortBy()
	case "first", "last":
		p.advance()
		return &PickExpr{Op: name}, nil
	case "nth":
		return p.parseNth()
	case "count", "sum", "avg", "min", "max":
		p.advance()
		return &AggExpr{Op: name}, nil
	default:
		// Check if it's a function call: ident(
		return p.parseFuncCallOrIdent()
	}
}

// parsePrimary handles the leftmost element of a pipe or standalone expressions.
func (p *parser) parsePrimary() (Node, error) {
	tok, err := p.peek()
	if err != nil {
		return nil, err
	}

	switch {
	case tok.Kind == TokIdent && tok.Lit == "self":
		p.advance()
		return &SelfExpr{}, nil

	case tok.Kind == TokIdent && tok.Lit == "employees":
		p.advance()
		return &IdentExpr{Name: "employees"}, nil

	case tok.Kind == TokIdent:
		return p.parseFuncCallOrIdent()

	case tok.Kind == TokDot:
		// . alone or .field
		return p.parseDotOrFieldAccess()

	case tok.Kind == TokString || tok.Kind == TokNumber:
		p.advance()
		return &Literal{Kind: tok.Kind, Value: tok.Lit}, nil

	case tok.Kind == TokTrue || tok.Kind == TokFalse:
		p.advance()
		return &Literal{Kind: tok.Kind, Value: tok.Lit}, nil

	case tok.Kind == TokMinus:
		p.advance()
		expr, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		return &UnaryMinus{Expr: expr}, nil

	case tok.Kind == TokLParen:
		p.advance() // consume (
		inner, err := p.parsePipeExpr()
		if err != nil {
			return nil, err
		}
		if err := p.expect(TokRParen); err != nil {
			return nil, err
		}
		return inner, nil

	default:
		return nil, p.errorf(tok.Pos, "unexpected %s, expected expression", tok.Kind)
	}
}

// parseDotOrFieldAccess handles `.` (dot pronoun) or `.field.subfield` (field access).
func (p *parser) parseDotOrFieldAccess() (Node, error) {
	p.advance() // consume .

	tok, err := p.peek()
	if err != nil {
		return nil, err
	}

	if tok.Kind != TokIdent {
		return &DotExpr{}, nil
	}

	// .field.subfield...
	var chain []string
	for {
		tok, err = p.peek()
		if err != nil {
			return nil, err
		}
		if tok.Kind != TokIdent {
			break
		}
		p.advance()
		chain = append(chain, tok.Lit)

		// Check for more dots
		tok, err = p.peek()
		if err != nil {
			return nil, err
		}
		if tok.Kind != TokDot {
			break
		}
		// Look ahead: is the next thing after the dot an identifier?
		// If so, continue the chain. Otherwise, stop (it could be a pipe step).
		// Save state to potentially backtrack.
		p.advance() // consume .
		next, err := p.peek()
		if err != nil {
			return nil, err
		}
		if next.Kind != TokIdent {
			// It was a trailing dot — put it back by creating a synthetic token.
			// Actually, we need to handle this as the dot becoming a pipe's field access.
			// This shouldn't happen in practice since `.field.` without continuation is invalid.
			return nil, p.errorf(next.Pos, "unexpected %s after '.', expected field name", next.Kind)
		}
	}

	return &FieldAccess{Chain: chain}, nil
}

// parseFieldAccessChain handles .field.subfield in pipe position.
func (p *parser) parseFieldAccessChain() (Node, error) {
	tok, err := p.peek()
	if err != nil {
		return nil, err
	}
	if tok.Kind != TokDot {
		return nil, p.errorf(tok.Pos, "expected '.', got %s", tok.Kind)
	}
	p.advance() // consume .

	// Must have at least one field name
	tok, err = p.peek()
	if err != nil {
		return nil, err
	}
	if tok.Kind != TokIdent {
		return nil, p.errorf(tok.Pos, "expected field name after '.', got %s", tok.Kind)
	}

	var chain []string
	for {
		tok, err = p.peek()
		if err != nil {
			return nil, err
		}
		if tok.Kind != TokIdent {
			break
		}
		p.advance()
		chain = append(chain, tok.Lit)

		tok, err = p.peek()
		if err != nil {
			return nil, err
		}
		if tok.Kind != TokDot {
			break
		}
		p.advance() // consume .
		next, err := p.peek()
		if err != nil {
			return nil, err
		}
		if next.Kind != TokIdent {
			return nil, p.errorf(next.Pos, "expected field name after '.', got %s", next.Kind)
		}
	}

	return &FieldAccess{Chain: chain}, nil
}

// parseWhere: where(boolExpr)
func (p *parser) parseWhere() (Node, error) {
	p.advance() // consume "where"
	if err := p.expect(TokLParen); err != nil {
		return nil, err
	}
	cond, err := p.parseBoolExpr()
	if err != nil {
		return nil, err
	}
	if err := p.expect(TokRParen); err != nil {
		return nil, err
	}
	return &WhereExpr{Cond: cond}, nil
}

// parseSortBy: sort_by(.field [, asc|desc])
func (p *parser) parseSortBy() (Node, error) {
	p.advance() // consume "sort_by"
	if err := p.expect(TokLParen); err != nil {
		return nil, err
	}

	fa, err := p.parseFieldAccessChain()
	if err != nil {
		return nil, err
	}
	fieldAccess, ok := fa.(*FieldAccess)
	if !ok {
		return nil, fmt.Errorf("sort_by expects a field access (.field), got %T", fa)
	}

	desc := false
	tok, err := p.peek()
	if err != nil {
		return nil, err
	}
	if tok.Kind == TokComma {
		p.advance() // consume ,
		tok, err = p.peek()
		if err != nil {
			return nil, err
		}
		switch tok.Kind {
		case TokAsc:
			p.advance()
		case TokDesc:
			p.advance()
			desc = true
		default:
			return nil, p.errorf(tok.Pos, "expected 'asc' or 'desc', got %s", tok.Kind)
		}
	}

	if err := p.expect(TokRParen); err != nil {
		return nil, err
	}
	return &SortExpr{Field: fieldAccess, Desc: desc}, nil
}

// parseNth: nth(n)
func (p *parser) parseNth() (Node, error) {
	p.advance() // consume "nth"
	if err := p.expect(TokLParen); err != nil {
		return nil, err
	}
	tok, err := p.peek()
	if err != nil {
		return nil, err
	}
	if tok.Kind != TokNumber {
		return nil, p.errorf(tok.Pos, "nth expects a number, got %s", tok.Kind)
	}
	p.advance()
	n, err := strconv.Atoi(tok.Lit)
	if err != nil || n < 1 {
		return nil, p.errorf(tok.Pos, "nth expects a positive integer, got %q", tok.Lit)
	}
	if err := p.expect(TokRParen); err != nil {
		return nil, err
	}
	return &PickExpr{Op: "nth", N: n}, nil
}

// parseFuncCallOrIdent handles `ident(args...)` or bare `ident`.
// Registered functions are validated for arg count (Prometheus-style).
func (p *parser) parseFuncCallOrIdent() (Node, error) {
	tok, err := p.peek()
	if err != nil {
		return nil, err
	}
	if tok.Kind != TokIdent {
		return nil, p.errorf(tok.Pos, "expected identifier, got %s", tok.Kind)
	}
	p.advance()
	name := tok.Lit
	pos := tok.Pos

	// Check for function call: ident(
	next, err := p.peek()
	if err != nil {
		return nil, err
	}
	if next.Kind != TokLParen {
		// No parens — check for zero-arg registered function.
		if def, ok := GetFunction(name); ok {
			if len(def.ArgTypes) > 0 {
				return nil, p.errorf(pos, "function %q requires arguments", name)
			}
			return &FuncCall{Func: def, Name: name}, nil
		}
		return &IdentExpr{Name: name}, nil
	}

	// Function call with parens — lookup required.
	def, ok := GetFunction(name)
	if !ok {
		return nil, p.errorf(pos, "unknown function %q", name)
	}

	p.advance() // consume (
	var args []Node
	for {
		tok, err = p.peek()
		if err != nil {
			return nil, err
		}
		if tok.Kind == TokRParen {
			break
		}
		if len(args) > 0 {
			if err := p.expect(TokComma); err != nil {
				return nil, err
			}
		}
		arg, err := p.parsePipeExpr()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}
	p.advance() // consume )

	// Validate arg count.
	minArgs := len(def.ArgTypes) - def.Variadic
	maxArgs := len(def.ArgTypes)
	if len(args) < minArgs || len(args) > maxArgs {
		if minArgs == maxArgs {
			return nil, p.errorf(pos, "function %q requires exactly %d argument(s), got %d", name, minArgs, len(args))
		}
		return nil, p.errorf(pos, "function %q requires %d to %d arguments, got %d", name, minArgs, maxArgs, len(args))
	}

	return &FuncCall{Func: def, Name: name, Args: args}, nil
}

// --- Boolean expression parsing (inside where) ---

// parseBoolExpr: boolTerm { "or" boolTerm }
func (p *parser) parseBoolExpr() (Node, error) {
	left, err := p.parseBoolTerm()
	if err != nil {
		return nil, err
	}
	for {
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		if tok.Kind != TokOr {
			break
		}
		p.advance()
		right, err := p.parseBoolTerm()
		if err != nil {
			return nil, err
		}
		left = &BinaryOp{Op: "or", Left: left, Right: right}
	}
	return left, nil
}

// parseBoolTerm: boolFactor { "and" boolFactor }
func (p *parser) parseBoolTerm() (Node, error) {
	left, err := p.parseBoolFactor()
	if err != nil {
		return nil, err
	}
	for {
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		if tok.Kind != TokAnd {
			break
		}
		p.advance()
		right, err := p.parseBoolFactor()
		if err != nil {
			return nil, err
		}
		left = &BinaryOp{Op: "and", Left: left, Right: right}
	}
	return left, nil
}

// parseBoolFactor: comparison | "(" boolExpr ")" | pipeExpr (for subqueries like `reports(., 1) | count > 0`)
func (p *parser) parseBoolFactor() (Node, error) {
	tok, err := p.peek()
	if err != nil {
		return nil, err
	}

	if tok.Kind == TokLParen {
		// Could be grouped boolean or a subexpression.
		// Try parenthesized boolean first.
		p.advance()
		inner, err := p.parseBoolExpr()
		if err != nil {
			return nil, err
		}
		if err := p.expect(TokRParen); err != nil {
			return nil, err
		}
		// Check if followed by a comparison operator
		tok, err = p.peek()
		if err != nil {
			return nil, err
		}
		if isComparisonOp(tok.Kind) {
			return p.finishComparison(inner)
		}
		return inner, nil
	}

	// Parse a value expression, then check for comparison.
	left, err := p.parseValueExpr()
	if err != nil {
		return nil, err
	}

	tok, err = p.peek()
	if err != nil {
		return nil, err
	}
	if isComparisonOp(tok.Kind) {
		return p.finishComparison(left)
	}

	// No comparison operator — this is a boolean subexpression (e.g., a function call that returns bool)
	return left, nil
}

// parseValueExpr parses an expression that may participate in a comparison.
// It handles pipes, field access, function calls, and literals.
func (p *parser) parseValueExpr() (Node, error) {
	return p.parsePipeExpr()
}

// finishComparison: given left side already parsed, parse `op right`.
func (p *parser) finishComparison(left Node) (Node, error) {
	tok, err := p.peek()
	if err != nil {
		return nil, err
	}
	if !isComparisonOp(tok.Kind) {
		return nil, p.errorf(tok.Pos, "expected comparison operator, got %s", tok.Kind)
	}
	p.advance()
	op := tok.Lit

	right, err := p.parseValueExpr()
	if err != nil {
		return nil, err
	}
	return &BinaryOp{Op: op, Left: left, Right: right}, nil
}

func isComparisonOp(k TokenKind) bool {
	switch k {
	case TokEq, TokNeq, TokGt, TokGte, TokLt, TokLte:
		return true
	}
	return false
}

// --- Helpers ---

func (p *parser) peek() (Token, error) {
	return p.lexer.Peek()
}

func (p *parser) advance() {
	p.lexer.Next() //nolint:errcheck
}

func (p *parser) expect(kind TokenKind) error {
	tok, err := p.lexer.Next()
	if err != nil {
		return err
	}
	if tok.Kind != kind {
		return p.errorf(tok.Pos, "expected %s, got %s", kind, tok.Kind)
	}
	return nil
}

func (p *parser) errorf(pos int, format string, args ...any) error {
	return fmt.Errorf("parse error at position %d: %s", pos, fmt.Sprintf(format, args...))
}
