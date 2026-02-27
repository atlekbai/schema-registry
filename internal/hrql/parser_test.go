package hrql

import (
	"strings"
	"testing"
)

// --- Helpers ---

func mustParse(t *testing.T, input string) Node {
	t.Helper()
	node, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse(%q) failed: %v", input, err)
	}
	return node
}

func expectParseError(t *testing.T, input, wantSubstr string) {
	t.Helper()
	_, err := Parse(input)
	if err == nil {
		t.Fatalf("Parse(%q): expected error containing %q, got nil", input, wantSubstr)
	}
	if !strings.Contains(err.Error(), wantSubstr) {
		t.Fatalf("Parse(%q): expected error containing %q, got %q", input, wantSubstr, err.Error())
	}
}

// --- Primary expressions ---

func TestParseEmployees(t *testing.T) {
	node := mustParse(t, "employees")
	ident, ok := node.(*IdentExpr)
	if !ok {
		t.Fatalf("expected *IdentExpr, got %T", node)
	}
	if ident.Name != "employees" {
		t.Fatalf("expected name 'employees', got %q", ident.Name)
	}
}

func TestParseSelf(t *testing.T) {
	node := mustParse(t, "self")
	if _, ok := node.(*SelfExpr); !ok {
		t.Fatalf("expected *SelfExpr, got %T", node)
	}
}

func TestParseSelfField(t *testing.T) {
	node := mustParse(t, "self.name")
	pipe, ok := node.(*PipeExpr)
	if !ok {
		t.Fatalf("expected *PipeExpr, got %T", node)
	}
	if len(pipe.Steps) != 2 {
		t.Fatalf("expected 2 steps, got %d", len(pipe.Steps))
	}
	if _, ok := pipe.Steps[0].(*SelfExpr); !ok {
		t.Fatalf("step 0: expected *SelfExpr, got %T", pipe.Steps[0])
	}
	fa, ok := pipe.Steps[1].(*FieldAccess)
	if !ok {
		t.Fatalf("step 1: expected *FieldAccess, got %T", pipe.Steps[1])
	}
	if len(fa.Chain) != 1 || fa.Chain[0] != "name" {
		t.Fatalf("expected chain [name], got %v", fa.Chain)
	}
}

func TestParseSelfFieldChain(t *testing.T) {
	node := mustParse(t, "self.department.title")
	pipe, ok := node.(*PipeExpr)
	if !ok {
		t.Fatalf("expected *PipeExpr, got %T", node)
	}
	fa := pipe.Steps[1].(*FieldAccess)
	if len(fa.Chain) != 2 || fa.Chain[0] != "department" || fa.Chain[1] != "title" {
		t.Fatalf("expected chain [department, title], got %v", fa.Chain)
	}
}

func TestParseStringLiteral(t *testing.T) {
	node := mustParse(t, `"alice"`)
	lit, ok := node.(*Literal)
	if !ok {
		t.Fatalf("expected *Literal, got %T", node)
	}
	if lit.Kind != TokString || lit.Value != "alice" {
		t.Fatalf("expected string 'alice', got %v %q", lit.Kind, lit.Value)
	}
}

func TestParseNumberLiteral(t *testing.T) {
	node := mustParse(t, "42")
	lit, ok := node.(*Literal)
	if !ok {
		t.Fatalf("expected *Literal, got %T", node)
	}
	if lit.Kind != TokNumber || lit.Value != "42" {
		t.Fatalf("expected number 42, got %v %q", lit.Kind, lit.Value)
	}
}

func TestParseBooleanLiterals(t *testing.T) {
	for _, tt := range []struct {
		input string
		kind  TokenKind
	}{
		{"true", TokTrue},
		{"false", TokFalse},
	} {
		node := mustParse(t, tt.input)
		lit, ok := node.(*Literal)
		if !ok {
			t.Fatalf("input %q: expected *Literal, got %T", tt.input, node)
		}
		if lit.Kind != tt.kind {
			t.Fatalf("input %q: expected kind %v, got %v", tt.input, tt.kind, lit.Kind)
		}
	}
}

func TestParseUnaryMinus(t *testing.T) {
	node := mustParse(t, "-5")
	um, ok := node.(*UnaryMinus)
	if !ok {
		t.Fatalf("expected *UnaryMinus, got %T", node)
	}
	lit, ok := um.Expr.(*Literal)
	if !ok || lit.Value != "5" {
		t.Fatalf("expected inner literal 5, got %T %v", um.Expr, um.Expr)
	}
}

func TestParseParenthesized(t *testing.T) {
	node := mustParse(t, "(employees)")
	ident, ok := node.(*IdentExpr)
	if !ok {
		t.Fatalf("expected *IdentExpr through parens, got %T", node)
	}
	if ident.Name != "employees" {
		t.Fatalf("expected 'employees', got %q", ident.Name)
	}
}

// --- Pipe expressions ---

func TestParsePipeWithWhere(t *testing.T) {
	node := mustParse(t, `employees | where(.active == true)`)
	pipe, ok := node.(*PipeExpr)
	if !ok {
		t.Fatalf("expected *PipeExpr, got %T", node)
	}
	if len(pipe.Steps) != 2 {
		t.Fatalf("expected 2 steps, got %d", len(pipe.Steps))
	}
	if _, ok := pipe.Steps[0].(*IdentExpr); !ok {
		t.Fatalf("step 0: expected *IdentExpr, got %T", pipe.Steps[0])
	}
	w, ok := pipe.Steps[1].(*WhereExpr)
	if !ok {
		t.Fatalf("step 1: expected *WhereExpr, got %T", pipe.Steps[1])
	}
	cmp, ok := w.Cond.(*BinaryOp)
	if !ok || cmp.Op != "==" {
		t.Fatalf("where cond: expected ==, got %T %v", w.Cond, w.Cond)
	}
}

func TestParsePipeFieldAccess(t *testing.T) {
	node := mustParse(t, `employees | .salary`)
	pipe := node.(*PipeExpr)
	fa, ok := pipe.Steps[1].(*FieldAccess)
	if !ok {
		t.Fatalf("step 1: expected *FieldAccess, got %T", pipe.Steps[1])
	}
	if len(fa.Chain) != 1 || fa.Chain[0] != "salary" {
		t.Fatalf("expected chain [salary], got %v", fa.Chain)
	}
}

func TestParsePipeSortBy(t *testing.T) {
	node := mustParse(t, `employees | sort_by(.name)`)
	pipe := node.(*PipeExpr)
	s, ok := pipe.Steps[1].(*SortExpr)
	if !ok {
		t.Fatalf("expected *SortExpr, got %T", pipe.Steps[1])
	}
	if s.Desc {
		t.Fatal("expected asc (default), got desc")
	}
	if s.Field.Chain[0] != "name" {
		t.Fatalf("expected sort field 'name', got %v", s.Field.Chain)
	}
}

func TestParsePipeSortByDesc(t *testing.T) {
	node := mustParse(t, `employees | sort_by(.salary, desc)`)
	pipe := node.(*PipeExpr)
	s := pipe.Steps[1].(*SortExpr)
	if !s.Desc {
		t.Fatal("expected desc, got asc")
	}
}

func TestParsePipeFirst(t *testing.T) {
	node := mustParse(t, `employees | first`)
	pipe := node.(*PipeExpr)
	p, ok := pipe.Steps[1].(*PickExpr)
	if !ok {
		t.Fatalf("expected *PickExpr, got %T", pipe.Steps[1])
	}
	if p.Op != "first" {
		t.Fatalf("expected 'first', got %q", p.Op)
	}
}

func TestParsePipeLast(t *testing.T) {
	node := mustParse(t, `employees | last`)
	pipe := node.(*PipeExpr)
	p := pipe.Steps[1].(*PickExpr)
	if p.Op != "last" {
		t.Fatalf("expected 'last', got %q", p.Op)
	}
}

func TestParsePipeNth(t *testing.T) {
	node := mustParse(t, `employees | nth(3)`)
	pipe := node.(*PipeExpr)
	p := pipe.Steps[1].(*PickExpr)
	if p.Op != "nth" || p.N != 3 {
		t.Fatalf("expected nth(3), got %q(%d)", p.Op, p.N)
	}
}

func TestParsePipeCount(t *testing.T) {
	node := mustParse(t, `employees | count`)
	pipe := node.(*PipeExpr)
	a, ok := pipe.Steps[1].(*AggExpr)
	if !ok {
		t.Fatalf("expected *AggExpr, got %T", pipe.Steps[1])
	}
	if a.Op != "count" {
		t.Fatalf("expected 'count', got %q", a.Op)
	}
}

func TestParseAllAggregations(t *testing.T) {
	for _, op := range []string{"count", "sum", "avg", "min", "max"} {
		node := mustParse(t, "employees | "+op)
		pipe := node.(*PipeExpr)
		a := pipe.Steps[1].(*AggExpr)
		if a.Op != op {
			t.Errorf("expected %q, got %q", op, a.Op)
		}
	}
}

// --- Function calls ---

func TestParseFuncCall(t *testing.T) {
	node := mustParse(t, `chain(self, 2)`)
	fn, ok := node.(*FuncCall)
	if !ok {
		t.Fatalf("expected *FuncCall, got %T", node)
	}
	if fn.Name != "chain" {
		t.Fatalf("expected name 'chain', got %q", fn.Name)
	}
	if len(fn.Args) != 2 {
		t.Fatalf("expected 2 args, got %d", len(fn.Args))
	}
	if _, ok := fn.Args[0].(*SelfExpr); !ok {
		t.Fatalf("arg 0: expected *SelfExpr, got %T", fn.Args[0])
	}
	lit, ok := fn.Args[1].(*Literal)
	if !ok || lit.Value != "2" {
		t.Fatalf("arg 1: expected literal 2, got %T %v", fn.Args[1], fn.Args[1])
	}
}

func TestParseFuncCallNoArgs(t *testing.T) {
	node := mustParse(t, `today()`)
	fn, ok := node.(*FuncCall)
	if !ok {
		t.Fatalf("expected *FuncCall, got %T", node)
	}
	if fn.Name != "today" || len(fn.Args) != 0 {
		t.Fatalf("expected today() with 0 args, got %q(%d)", fn.Name, len(fn.Args))
	}
}

func TestParseFuncCallWithStringArg(t *testing.T) {
	node := mustParse(t, `contains("Director")`)
	fn := node.(*FuncCall)
	if fn.Name != "contains" {
		t.Fatalf("expected 'contains', got %q", fn.Name)
	}
	lit := fn.Args[0].(*Literal)
	if lit.Value != "Director" {
		t.Fatalf("expected 'Director', got %q", lit.Value)
	}
}

func TestParseReportsSelf(t *testing.T) {
	node := mustParse(t, `reports(self, 1)`)
	fn := node.(*FuncCall)
	if fn.Name != "reports" || len(fn.Args) != 2 {
		t.Fatalf("expected reports(self, 1), got %q(%d args)", fn.Name, len(fn.Args))
	}
}

func TestParsePeers(t *testing.T) {
	node := mustParse(t, `peers(self)`)
	fn := node.(*FuncCall)
	if fn.Name != "peers" || len(fn.Args) != 1 {
		t.Fatalf("expected peers(self), got %q(%d args)", fn.Name, len(fn.Args))
	}
}

func TestParseColleagues(t *testing.T) {
	node := mustParse(t, `colleagues(self, .department)`)
	fn := node.(*FuncCall)
	if fn.Name != "colleagues" || len(fn.Args) != 2 {
		t.Fatalf("expected colleagues(self, .department), got %q(%d args)", fn.Name, len(fn.Args))
	}
	fa, ok := fn.Args[1].(*FieldAccess)
	if !ok {
		t.Fatalf("arg 1: expected *FieldAccess, got %T", fn.Args[1])
	}
	if len(fa.Chain) != 1 || fa.Chain[0] != "department" {
		t.Fatalf("expected .department, got %v", fa.Chain)
	}
}

func TestParseReportsTo(t *testing.T) {
	node := mustParse(t, `reports_to(self, "some-uuid")`)
	fn := node.(*FuncCall)
	if fn.Name != "reports_to" || len(fn.Args) != 2 {
		t.Fatalf("expected reports_to(self, uuid), got %q(%d args)", fn.Name, len(fn.Args))
	}
}

// --- Where conditions ---

func TestParseWhereAnd(t *testing.T) {
	node := mustParse(t, `employees | where(.x == 1 and .y != 2)`)
	pipe := node.(*PipeExpr)
	w := pipe.Steps[1].(*WhereExpr)
	op, ok := w.Cond.(*BinaryOp)
	if !ok || op.Op != "and" {
		t.Fatalf("expected and, got %T %v", w.Cond, w.Cond)
	}
	left := op.Left.(*BinaryOp)
	right := op.Right.(*BinaryOp)
	if left.Op != "==" || right.Op != "!=" {
		t.Fatalf("expected == and !=, got %q and %q", left.Op, right.Op)
	}
}

func TestParseWhereOr(t *testing.T) {
	node := mustParse(t, `employees | where(.x > 0 or .y < 10)`)
	pipe := node.(*PipeExpr)
	w := pipe.Steps[1].(*WhereExpr)
	op := w.Cond.(*BinaryOp)
	if op.Op != "or" {
		t.Fatalf("expected or, got %q", op.Op)
	}
}

func TestParseWhereAndOrPrecedence(t *testing.T) {
	// a and b or c should parse as (a and b) or c
	node := mustParse(t, `employees | where(.a == 1 and .b == 2 or .c == 3)`)
	pipe := node.(*PipeExpr)
	w := pipe.Steps[1].(*WhereExpr)
	top := w.Cond.(*BinaryOp)
	if top.Op != "or" {
		t.Fatalf("top-level should be 'or', got %q", top.Op)
	}
	left := top.Left.(*BinaryOp)
	if left.Op != "and" {
		t.Fatalf("left should be 'and', got %q", left.Op)
	}
}

func TestParseWhereParenGrouping(t *testing.T) {
	// a and (b or c) should parse as a and (b or c)
	node := mustParse(t, `employees | where(.a == 1 and (.b == 2 or .c == 3))`)
	pipe := node.(*PipeExpr)
	w := pipe.Steps[1].(*WhereExpr)
	top := w.Cond.(*BinaryOp)
	if top.Op != "and" {
		t.Fatalf("top-level should be 'and', got %q", top.Op)
	}
	right := top.Right.(*BinaryOp)
	if right.Op != "or" {
		t.Fatalf("right should be 'or', got %q", right.Op)
	}
}

func TestParseWhereFieldContains(t *testing.T) {
	// .title | contains("Director") inside where — pipe expression as condition
	node := mustParse(t, `employees | where(.title | contains("Director"))`)
	pipe := node.(*PipeExpr)
	w := pipe.Steps[1].(*WhereExpr)
	innerPipe, ok := w.Cond.(*PipeExpr)
	if !ok {
		t.Fatalf("expected *PipeExpr inside where, got %T", w.Cond)
	}
	if len(innerPipe.Steps) != 2 {
		t.Fatalf("expected 2 steps in inner pipe, got %d", len(innerPipe.Steps))
	}
	_, isFA := innerPipe.Steps[0].(*FieldAccess)
	fn, isFn := innerPipe.Steps[1].(*FuncCall)
	if !isFA || !isFn {
		t.Fatalf("expected FieldAccess | FuncCall, got %T | %T", innerPipe.Steps[0], innerPipe.Steps[1])
	}
	if fn.Name != "contains" {
		t.Fatalf("expected 'contains', got %q", fn.Name)
	}
}

func TestParseWhereSubquery(t *testing.T) {
	// reports(., 1) | count > 0 inside where
	node := mustParse(t, `employees | where(reports(., 1) | count > 0)`)
	pipe := node.(*PipeExpr)
	w := pipe.Steps[1].(*WhereExpr)
	cmp, ok := w.Cond.(*BinaryOp)
	if !ok || cmp.Op != ">" {
		t.Fatalf("expected > comparison, got %T %v", w.Cond, w.Cond)
	}
	// Left should be a pipe: reports(., 1) | count
	innerPipe, ok := cmp.Left.(*PipeExpr)
	if !ok {
		t.Fatalf("left of >: expected *PipeExpr, got %T", cmp.Left)
	}
	if len(innerPipe.Steps) != 2 {
		t.Fatalf("expected 2 steps in subquery pipe, got %d", len(innerPipe.Steps))
	}
}

// --- Complex real-world expressions ---

func TestParseComplexPipe(t *testing.T) {
	input := `reports(self, 1) | where(.employment_type == "FULL_TIME") | sort_by(.start_date, desc) | first`
	node := mustParse(t, input)
	pipe := node.(*PipeExpr)
	if len(pipe.Steps) != 4 {
		t.Fatalf("expected 4 steps, got %d", len(pipe.Steps))
	}

	fn := pipe.Steps[0].(*FuncCall)
	if fn.Name != "reports" {
		t.Fatalf("step 0: expected 'reports', got %q", fn.Name)
	}

	_ = pipe.Steps[1].(*WhereExpr)
	_ = pipe.Steps[2].(*SortExpr)
	_ = pipe.Steps[3].(*PickExpr)
}

func TestParseFieldThenAgg(t *testing.T) {
	node := mustParse(t, `employees | .salary | avg`)
	pipe := node.(*PipeExpr)
	if len(pipe.Steps) != 3 {
		t.Fatalf("expected 3 steps, got %d", len(pipe.Steps))
	}
	_ = pipe.Steps[1].(*FieldAccess)
	a := pipe.Steps[2].(*AggExpr)
	if a.Op != "avg" {
		t.Fatalf("expected 'avg', got %q", a.Op)
	}
}

func TestParseSelfManagerField(t *testing.T) {
	node := mustParse(t, "self.manager.individual.first_name")
	pipe := node.(*PipeExpr)
	fa := pipe.Steps[1].(*FieldAccess)
	if len(fa.Chain) != 3 {
		t.Fatalf("expected 3-element chain, got %v", fa.Chain)
	}
	if fa.Chain[0] != "manager" || fa.Chain[1] != "individual" || fa.Chain[2] != "first_name" {
		t.Fatalf("unexpected chain: %v", fa.Chain)
	}
}

func TestParseSelfPipedFurther(t *testing.T) {
	// self.manager | .individual — self.manager becomes inner PipeExpr, then outer pipe adds .individual
	node := mustParse(t, `self.manager | .individual`)
	pipe, ok := node.(*PipeExpr)
	if !ok {
		t.Fatalf("expected *PipeExpr, got %T", node)
	}
	// Top-level has 2 steps: PipeExpr{self, .manager} and FieldAccess{individual}
	if len(pipe.Steps) != 2 {
		t.Fatalf("expected 2 top-level steps, got %d", len(pipe.Steps))
	}
	// First step is the inner pipe from self.manager
	inner, ok := pipe.Steps[0].(*PipeExpr)
	if !ok {
		t.Fatalf("step 0: expected *PipeExpr, got %T", pipe.Steps[0])
	}
	if len(inner.Steps) != 2 {
		t.Fatalf("inner pipe: expected 2 steps, got %d", len(inner.Steps))
	}
	// Second step is .individual
	fa, ok := pipe.Steps[1].(*FieldAccess)
	if !ok {
		t.Fatalf("step 1: expected *FieldAccess, got %T", pipe.Steps[1])
	}
	if fa.Chain[0] != "individual" {
		t.Fatalf("expected .individual, got %v", fa.Chain)
	}
}

func TestParseAllComparisonOps(t *testing.T) {
	ops := []string{"==", "!=", ">", ">=", "<", "<="}
	for _, op := range ops {
		input := `employees | where(.x ` + op + ` 1)`
		node := mustParse(t, input)
		pipe := node.(*PipeExpr)
		w := pipe.Steps[1].(*WhereExpr)
		cmp := w.Cond.(*BinaryOp)
		if cmp.Op != op {
			t.Errorf("op %q: expected %q, got %q", op, op, cmp.Op)
		}
	}
}

// --- Error cases ---

func TestParseErrorTrailingTokens(t *testing.T) {
	expectParseError(t, "employees foo", "unexpected")
}

func TestParseErrorNthZero(t *testing.T) {
	expectParseError(t, "employees | nth(0)", "positive integer")
}

func TestParseErrorSortByBadOrder(t *testing.T) {
	expectParseError(t, "employees | sort_by(.name, bad)", "expected 'asc' or 'desc'")
}

func TestParseErrorEmptyInput(t *testing.T) {
	expectParseError(t, "", "unexpected EOF")
}

func TestParseErrorUnclosedParen(t *testing.T) {
	expectParseError(t, "chain(self", "EOF")
}

func TestParseErrorWhereNoParen(t *testing.T) {
	expectParseError(t, "employees | where .x == 1", "expected (")
}
