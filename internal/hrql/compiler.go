package hrql

import (
	"fmt"

	"github.com/atlekbai/schema_registry/internal/hrql/parser"
	"github.com/atlekbai/schema_registry/internal/schema"
)

// Compiler compiles an HRQL AST into a Plan.
type Compiler struct {
	cache      *schema.Cache
	selfID     string
	selfObj    *schema.ObjectDef // object type for "self" (nil if selfObject is empty)
	currentObj *schema.ObjectDef // current object flowing through the pipeline
}

// NewCompiler creates a compiler for HRQL expressions.
// selfObj is the object definition for "self"; if nil it defaults to cache.Get("employees").
func NewCompiler(cache *schema.Cache, selfID string, selfObj *schema.ObjectDef) *Compiler {
	if selfObj == nil {
		selfObj = cache.Get("employees")
	}
	return &Compiler{
		cache:   cache,
		selfID:  selfID,
		selfObj: selfObj,
	}
}

// Compile compiles an AST node into a storage-agnostic Plan.
func (c *Compiler) Compile(node parser.Node) (*Plan, error) {
	return c.compileNode(node)
}

func (c *Compiler) compileNode(node parser.Node) (*Plan, error) {
	switch n := node.(type) {
	case *parser.PipeExpr:
		return c.compilePipe(n)
	case *parser.SelfExpr:
		return c.compileSelf()
	case *parser.IdentExpr:
		return c.compileIdent(n)
	case *parser.FuncCall:
		return c.compileFuncCall(n)
	case *parser.BinaryOp, *parser.Literal, *parser.UnaryMinus:
		expr, err := c.compileScalarExpr(node)
		if err != nil {
			return nil, err
		}
		return &Plan{Kind: PlanScalar, ScalarExpr: expr}, nil
	case *parser.FieldAccess:
		return nil, fmt.Errorf("field access requires a source (use self.field or pipe)")
	default:
		return nil, fmt.Errorf("unexpected node type %T at top level", node)
	}
}

// compilePipe walks pipe steps left-to-right, accumulating state.
func (c *Compiler) compilePipe(pipe *parser.PipeExpr) (*Plan, error) {
	if len(pipe.Steps) == 0 {
		return nil, fmt.Errorf("empty pipe expression")
	}

	plan, err := c.compileNode(pipe.Steps[0])
	if err != nil {
		return nil, err
	}

	for _, step := range pipe.Steps[1:] {
		plan, err = c.applyStep(plan, step)
		if err != nil {
			return nil, err
		}
	}

	return plan, nil
}

// applyStep applies a single pipe step to the current plan.
func (c *Compiler) applyStep(plan *Plan, step parser.Node) (*Plan, error) {
	switch s := step.(type) {
	case *parser.FieldAccess:
		return c.applyFieldAccess(plan, s)
	case *parser.WhereExpr:
		return c.applyWhere(plan, s)
	case *parser.SortExpr:
		return c.applySort(plan, s)
	case *parser.PickExpr:
		return c.applyPick(plan, s)
	case *parser.AggExpr:
		return c.applyAgg(plan, s)
	case *parser.FuncCall:
		return c.applyFuncInPipe(plan, s)
	default:
		return nil, fmt.Errorf("unexpected pipe step type %T", step)
	}
}

// compileSelf: the `self` object — filter by ID.
func (c *Compiler) compileSelf() (*Plan, error) {
	if c.selfID == "" {
		return nil, fmt.Errorf("`self` requires self_id in the request")
	}
	if c.selfObj == nil {
		return nil, fmt.Errorf("self object type not found in schema cache")
	}
	c.currentObj = c.selfObj
	return &Plan{
		Kind:          PlanList,
		ObjectAPIName: c.selfObj.APIName,
		Conditions:    []Condition{IdentityFilter{ID: c.selfID}},
		Limit:         1,
	}, nil
}

// compileIdent: `employees`, `departments`, etc. → full scan of any registered object.
func (c *Compiler) compileIdent(n *parser.IdentExpr) (*Plan, error) {
	obj := c.cache.Get(n.Name)
	if obj == nil {
		return nil, fmt.Errorf("unknown object %q", n.Name)
	}
	c.currentObj = obj
	return &Plan{Kind: PlanList, ObjectAPIName: obj.APIName}, nil
}

// requireCurrentObj returns the current object or an error if none is set.
func (c *Compiler) requireCurrentObj() (*schema.ObjectDef, error) {
	if c.currentObj == nil {
		return nil, fmt.Errorf("no source object; start with an object name (e.g. employees) or self")
	}
	return c.currentObj, nil
}

// --- Step application ---

func (c *Compiler) applyFieldAccess(plan *Plan, fa *parser.FieldAccess) (*Plan, error) {
	if plan.Kind != PlanList {
		return nil, fmt.Errorf("field access requires a list, got %v", plan.Kind)
	}
	if len(fa.Chain) == 0 {
		return nil, fmt.Errorf("empty field access")
	}

	obj, err := c.requireCurrentObj()
	if err != nil {
		return nil, err
	}

	fd, ok := obj.FieldsByAPIName[fa.Chain[0]]
	if !ok {
		return nil, fmt.Errorf("unknown field %q on %s", fa.Chain[0], obj.APIName)
	}

	// For LOOKUP fields with deeper chains, tracked for service layer.
	if fd.Type == schema.FieldLookup && len(fa.Chain) > 1 {
	}

	plan.AggField = fd.APIName
	return plan, nil
}

func (c *Compiler) applyWhere(plan *Plan, w *parser.WhereExpr) (*Plan, error) {
	if plan.Kind != PlanList {
		return nil, fmt.Errorf("where requires a list source")
	}

	cond, err := c.compileWhereCond(w.Cond)
	if err != nil {
		return nil, fmt.Errorf("where: %w", err)
	}

	plan.Conditions = append(plan.Conditions, cond)
	return plan, nil
}

func (c *Compiler) applySort(plan *Plan, s *parser.SortExpr) (*Plan, error) {
	if plan.Kind != PlanList {
		return nil, fmt.Errorf("sort_by requires a list source")
	}
	if len(s.Field.Chain) == 0 {
		return nil, fmt.Errorf("sort_by: empty field")
	}

	obj, err := c.requireCurrentObj()
	if err != nil {
		return nil, err
	}

	fieldName := s.Field.Chain[0]
	if _, ok := obj.FieldsByAPIName[fieldName]; !ok {
		return nil, fmt.Errorf("sort_by: unknown field %q", fieldName)
	}

	plan.OrderBy = &OrderBy{Field: fieldName, Desc: s.Desc}
	return plan, nil
}

func (c *Compiler) applyPick(plan *Plan, p *parser.PickExpr) (*Plan, error) {
	if plan.Kind != PlanList {
		return nil, fmt.Errorf("%s requires a list source", p.Op)
	}

	plan.PickOp = p.Op
	plan.PickN = p.N

	switch p.Op {
	case "first":
		plan.Limit = 1
	case "last":
		plan.Limit = 1
		if plan.OrderBy != nil {
			plan.OrderBy.Desc = !plan.OrderBy.Desc
		} else {
			plan.OrderBy = &OrderBy{Field: "id", Desc: true}
		}
	case "nth":
		plan.Limit = 1
	}

	return plan, nil
}

func (c *Compiler) applyAgg(plan *Plan, a *parser.AggExpr) (*Plan, error) {
	if plan.Kind != PlanList {
		return nil, fmt.Errorf("%s requires a list source", a.Op)
	}

	plan.Kind = PlanScalar
	plan.AggFunc = a.Op
	return plan, nil
}

// --- Arithmetic expression compilation ---

func isArithOp(op string) bool {
	return op == "+" || op == "-" || op == "*" || op == "/"
}

// compileScalarExpr compiles a node into a ScalarExpr for arithmetic contexts.
// Handles literals, unary minus, arithmetic BinaryOp, and falls back to compileNode
// for pipe expressions / function calls that produce PlanScalar.
func (c *Compiler) compileScalarExpr(node parser.Node) (ScalarExpr, error) {
	switch n := node.(type) {
	case *parser.Literal:
		if n.Kind == parser.TokNumber {
			return ScalarLiteral{Value: n.Value}, nil
		}
		return nil, fmt.Errorf("expected number in arithmetic, got %s", n.Kind)
	case *parser.UnaryMinus:
		inner, err := c.compileScalarExpr(n.Expr)
		if err != nil {
			return nil, err
		}
		if lit, ok := inner.(ScalarLiteral); ok {
			return ScalarLiteral{Value: "-" + lit.Value}, nil
		}
		return ScalarArith{Op: "-", Left: ScalarLiteral{Value: "0"}, Right: inner}, nil
	case *parser.BinaryOp:
		if isArithOp(n.Op) {
			left, err := c.compileScalarExpr(n.Left)
			if err != nil {
				return nil, err
			}
			right, err := c.compileScalarExpr(n.Right)
			if err != nil {
				return nil, err
			}
			return ScalarArith{Op: n.Op, Left: left, Right: right}, nil
		}
		return nil, fmt.Errorf("unsupported operator %q in arithmetic expression", n.Op)
	default:
		plan, err := c.compileNode(node)
		if err != nil {
			return nil, err
		}
		if plan.Kind != PlanScalar {
			return nil, fmt.Errorf("expected scalar expression, got %v", plan.Kind)
		}
		return ScalarSubquery{Plan: plan}, nil
	}
}

// --- Where condition compilation ---

func (c *Compiler) compileWhereCond(node parser.Node) (Condition, error) {
	switch n := node.(type) {
	case *parser.BinaryOp:
		return c.compileWhereOp(n)
	case *parser.FuncCall:
		return c.compileWhereFuncCall(n)
	case *parser.PipeExpr:
		if cond, ok := c.tryCompileStringOp(n); ok {
			return cond, nil
		}
		return c.compileWhereSubquery(n)
	default:
		return nil, fmt.Errorf("unsupported condition type %T in where", node)
	}
}

func (c *Compiler) compileWhereOp(op *parser.BinaryOp) (Condition, error) {
	switch op.Op {
	case "and":
		left, err := c.compileWhereCond(op.Left)
		if err != nil {
			return nil, err
		}
		right, err := c.compileWhereCond(op.Right)
		if err != nil {
			return nil, err
		}
		return AndCond{Left: left, Right: right}, nil

	case "or":
		left, err := c.compileWhereCond(op.Left)
		if err != nil {
			return nil, err
		}
		right, err := c.compileWhereCond(op.Right)
		if err != nil {
			return nil, err
		}
		return OrCond{Left: left, Right: right}, nil

	case "==", "!=", ">", ">=", "<", "<=":
		return c.compileComparison(op)

	default:
		return nil, fmt.Errorf("unsupported operator %q in where", op.Op)
	}
}

func (c *Compiler) compileComparison(op *parser.BinaryOp) (Condition, error) {
	left, err := c.compileWhereValue(op.Left)
	if err != nil {
		return nil, fmt.Errorf("where left: %w", err)
	}

	right, err := c.compileWhereValue(op.Right)
	if err != nil {
		return nil, fmt.Errorf("where right: %w", err)
	}

	// field == literal or field == field
	if f, ok := left.(fieldRef); ok {
		if lit, ok := right.(literalVal); ok {
			return FieldCmp{Field: f.chain, Op: op.Op, Value: string(lit)}, nil
		}
		if rf, ok := right.(fieldRef); ok {
			return FieldCmp{Field: f.chain, Op: op.Op, Value: "field:" + joinChain(rf.chain)}, nil
		}
		if ref, ok := right.(empRefVal); ok {
			return FieldCmpRef{Field: f.chain, Op: op.Op, Ref: ref.ref}, nil
		}
	}

	if f, ok := right.(fieldRef); ok {
		if lit, ok := left.(literalVal); ok {
			return FieldCmp{Field: f.chain, Op: reverseOp(op.Op), Value: string(lit)}, nil
		}
	}

	// subquery comparison: left is a subquery
	if sub, ok := left.(subqueryVal); ok {
		if lit, ok := right.(literalVal); ok {
			sub.cond.Op = op.Op
			sub.cond.Value = string(lit)
			return sub.cond, nil
		}
	}

	return nil, fmt.Errorf("unsupported comparison operands")
}

// compileWhereValue compiles a value expression inside a where condition.
// Returns a fieldRef, literalVal, empRefVal, or subqueryVal.
func (c *Compiler) compileWhereValue(node parser.Node) (any, error) {
	switch n := node.(type) {
	case *parser.FieldAccess:
		return c.resolveFieldRef(n)
	case *parser.DotExpr:
		return nil, fmt.Errorf("bare '.' in where condition; use '.field' to access a field")
	case *parser.Literal:
		return literalVal(n.Value), nil
	case *parser.SelfExpr:
		return literalVal(c.selfID), nil
	case *parser.PipeExpr:
		return c.compileSelfFieldLookup(n)
	case *parser.FuncCall:
		return c.compileWhereFuncValue(n)
	case *parser.UnaryMinus:
		inner, err := c.compileWhereValue(n.Expr)
		if err != nil {
			return nil, err
		}
		if lit, ok := inner.(literalVal); ok {
			return literalVal("-" + string(lit)), nil
		}
		return nil, fmt.Errorf("unary minus only supported on literals")
	default:
		return nil, fmt.Errorf("unsupported value type %T in where condition", node)
	}
}

// resolveFieldRef validates a field access chain and returns a fieldRef.
func (c *Compiler) resolveFieldRef(fa *parser.FieldAccess) (any, error) {
	if len(fa.Chain) == 0 {
		return nil, fmt.Errorf("empty field access in where")
	}

	obj, err := c.requireCurrentObj()
	if err != nil {
		return nil, err
	}

	fieldName := fa.Chain[0]
	fd, ok := obj.FieldsByAPIName[fieldName]
	if !ok {
		return nil, fmt.Errorf("unknown field %q on %s", fieldName, obj.APIName)
	}

	if len(fa.Chain) == 1 {
		return fieldRef{chain: fa.Chain}, nil
	}

	// Multi-level: .department.title — validate the chain.
	if fd.Type != schema.FieldLookup || fd.LookupObjectID == nil {
		return nil, fmt.Errorf("field %q is not a LOOKUP field, cannot traverse", fieldName)
	}

	currentObj := c.cache.GetByID(*fd.LookupObjectID)
	if currentObj == nil {
		return nil, fmt.Errorf("lookup target for field %q not found", fieldName)
	}

	for i := 1; i < len(fa.Chain); i++ {
		nextFieldName := fa.Chain[i]
		nextFd, ok := currentObj.FieldsByAPIName[nextFieldName]
		if !ok {
			return nil, fmt.Errorf("unknown field %q on %s", nextFieldName, currentObj.APIName)
		}

		if i < len(fa.Chain)-1 {
			if nextFd.Type != schema.FieldLookup || nextFd.LookupObjectID == nil {
				return nil, fmt.Errorf("field %q is not a LOOKUP field, cannot traverse", nextFieldName)
			}
			currentObj = c.cache.GetByID(*nextFd.LookupObjectID)
			if currentObj == nil {
				return nil, fmt.Errorf("lookup target for field %q not found", nextFieldName)
			}
		}
	}

	return fieldRef{chain: fa.Chain}, nil
}

// compileSelfFieldLookup returns an empRefVal for self.field (deferred to SQL).
// Delegates to resolveEmployeeArg for validation (validates all chain fields, not just the first).
func (c *Compiler) compileSelfFieldLookup(pipe *parser.PipeExpr) (any, error) {
	if len(pipe.Steps) == 2 {
		if _, ok := pipe.Steps[0].(*parser.SelfExpr); ok {
			if _, ok := pipe.Steps[1].(*parser.FieldAccess); ok {
				ref, err := c.resolveEmployeeArg(pipe)
				if err != nil {
					return nil, err
				}
				return empRefVal{ref: ref}, nil
			}
		}
	}
	return c.compileWhereSubqueryValue(pipe)
}

// compileWhereSubqueryValue compiles a pipe expression in where value position as a scalar subquery.
func (c *Compiler) compileWhereSubqueryValue(pipe *parser.PipeExpr) (any, error) {
	cond, err := c.compileWhereSubquery(pipe)
	if err != nil {
		return nil, err
	}
	sub, ok := cond.(SubqueryAgg)
	if !ok {
		return nil, fmt.Errorf("expected subquery aggregate in value position")
	}
	return subqueryVal{cond: sub}, nil
}

// compileWhereSubquery compiles a pipe expression as a scalar subquery inside a where condition.
func (c *Compiler) compileWhereSubquery(pipe *parser.PipeExpr) (Condition, error) {
	if len(pipe.Steps) < 2 {
		return nil, fmt.Errorf("subquery in where requires at least 2 pipe steps (source | aggregate)")
	}

	fn, ok := pipe.Steps[0].(*parser.FuncCall)
	if !ok {
		return nil, fmt.Errorf("subquery source must be a function call, got %T", pipe.Steps[0])
	}

	aggOp := ""
	for _, step := range pipe.Steps[1:] {
		switch s := step.(type) {
		case *parser.AggExpr:
			aggOp = s.Op
		case *parser.FieldAccess:
			// Field access before aggregation — ignore for count.
		default:
			return nil, fmt.Errorf("unsupported step %T in where subquery", step)
		}
	}

	if aggOp == "" {
		return nil, fmt.Errorf("where subquery must end with an aggregation (count, sum, avg, min, max)")
	}

	depth := 0
	if len(fn.Args) >= 2 {
		var err error
		depth, err = c.resolveIntArg(fn.Args[1])
		if err != nil {
			return nil, err
		}
	}

	return SubqueryAgg{OrgFunc: fn.Name, Depth: depth, AggFunc: aggOp}, nil
}

// compileWhereFuncCall compiles a function call as a boolean condition.
func (c *Compiler) compileWhereFuncCall(fn *parser.FuncCall) (Condition, error) {
	switch fn.Name {
	case "reports_to":
		if len(fn.Args) != 2 {
			return nil, fmt.Errorf("reports_to() requires 2 arguments")
		}
		if _, ok := fn.Args[0].(*parser.DotExpr); !ok {
			return nil, fmt.Errorf("reports_to() in where expects '.' as first argument")
		}

		targetRef, err := c.resolveEmployeeArg(fn.Args[1])
		if err != nil {
			return nil, fmt.Errorf("reports_to arg 2: %w", err)
		}

		return ReportsTo{Target: targetRef}, nil

	default:
		return nil, fmt.Errorf("function %q is not supported as a where condition", fn.Name)
	}
}

// tryCompileStringOp checks if a PipeExpr is a string operation pattern like `.field | contains("str")`.
func (c *Compiler) tryCompileStringOp(pipe *parser.PipeExpr) (Condition, bool) {
	if len(pipe.Steps) != 2 {
		return nil, false
	}

	fa, isFA := pipe.Steps[0].(*parser.FieldAccess)
	fn, isFn := pipe.Steps[1].(*parser.FuncCall)
	if !isFA || !isFn {
		return nil, false
	}
	if len(fn.Args) != 1 {
		return nil, false
	}
	lit, isLit := fn.Args[0].(*parser.Literal)
	if !isLit || lit.Kind != parser.TokString {
		return nil, false
	}

	if len(fa.Chain) == 0 {
		return nil, false
	}
	if c.currentObj == nil {
		return nil, false
	}
	if _, ok := c.currentObj.FieldsByAPIName[fa.Chain[0]]; !ok {
		return nil, false
	}

	switch fn.Name {
	case "contains", "starts_with", "ends_with":
		return StringMatch{Field: fa.Chain, Op: fn.Name, Pattern: lit.Value}, true
	default:
		return nil, false
	}
}

// compileWhereFuncValue compiles a function in value position inside where.
func (c *Compiler) compileWhereFuncValue(fn *parser.FuncCall) (any, error) {
	switch fn.Name {
	case "contains":
		return nil, fmt.Errorf("contains() should be used with pipe syntax: .field | contains(\"str\")")
	default:
		return nil, fmt.Errorf("function %q is not supported in where value position", fn.Name)
	}
}

// --- Internal value types for where compilation ---

type (
	fieldRef    struct{ chain []string }  // a validated field reference (API names)
	literalVal  string                    // a literal value
	empRefVal   struct{ ref EmployeeRef } // an unresolved employee reference (self.field)
	subqueryVal struct{ cond SubqueryAgg }
)

func reverseOp(op string) string {
	switch op {
	case ">":
		return "<"
	case ">=":
		return "<="
	case "<":
		return ">"
	case "<=":
		return ">="
	default:
		return op
	}
}
