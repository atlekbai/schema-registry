package hrql

import (
	"context"
	"fmt"

	"github.com/atlekbai/schema_registry/internal/schema"
)

// --- Where condition compilation ---

func (c *Compiler) compileWhereCond(ctx context.Context, node Node) (Condition, error) {
	switch n := node.(type) {
	case *BinaryOp:
		return c.compileWhereOp(ctx, n)
	case *FuncCall:
		return c.compileWhereFuncCall(ctx, n)
	case *PipeExpr:
		if cond, ok := c.tryCompileStringOp(n); ok {
			return cond, nil
		}
		return c.compileWhereSubquery(ctx, n)
	default:
		return nil, fmt.Errorf("unsupported condition type %T in where", node)
	}
}

func (c *Compiler) compileWhereOp(ctx context.Context, op *BinaryOp) (Condition, error) {
	switch op.Op {
	case "and":
		left, err := c.compileWhereCond(ctx, op.Left)
		if err != nil {
			return nil, err
		}
		right, err := c.compileWhereCond(ctx, op.Right)
		if err != nil {
			return nil, err
		}
		return AndCond{Left: left, Right: right}, nil

	case "or":
		left, err := c.compileWhereCond(ctx, op.Left)
		if err != nil {
			return nil, err
		}
		right, err := c.compileWhereCond(ctx, op.Right)
		if err != nil {
			return nil, err
		}
		return OrCond{Left: left, Right: right}, nil

	case "==", "!=", ">", ">=", "<", "<=":
		return c.compileComparison(ctx, op)

	default:
		return nil, fmt.Errorf("unsupported operator %q in where", op.Op)
	}
}

func (c *Compiler) compileComparison(ctx context.Context, op *BinaryOp) (Condition, error) {
	left, err := c.compileWhereValue(ctx, op.Left)
	if err != nil {
		return nil, fmt.Errorf("where left: %w", err)
	}

	right, err := c.compileWhereValue(ctx, op.Right)
	if err != nil {
		return nil, fmt.Errorf("where right: %w", err)
	}

	// field == literal or literal == field
	if f, ok := left.(fieldRef); ok {
		if lit, ok := right.(literalVal); ok {
			return FieldCmp{Field: f.chain, Op: op.Op, Value: string(lit)}, nil
		}
		if rf, ok := right.(fieldRef); ok {
			return FieldCmp{Field: f.chain, Op: op.Op, Value: "field:" + joinChain(rf.chain)}, nil
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
// Returns a fieldRef, literalVal, or subqueryVal.
func (c *Compiler) compileWhereValue(ctx context.Context, node Node) (any, error) {
	switch n := node.(type) {
	case *FieldAccess:
		return c.resolveFieldRef(n)
	case *DotExpr:
		return nil, fmt.Errorf("bare '.' in where condition; use '.field' to access a field")
	case *Literal:
		return literalVal(n.Value), nil
	case *SelfExpr:
		return literalVal(c.selfID), nil
	case *PipeExpr:
		return c.compileSelfFieldLookup(ctx, n)
	case *FuncCall:
		return c.compileWhereFuncValue(ctx, n)
	case *UnaryMinus:
		inner, err := c.compileWhereValue(ctx, n.Expr)
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
func (c *Compiler) resolveFieldRef(fa *FieldAccess) (any, error) {
	if len(fa.Chain) == 0 {
		return nil, fmt.Errorf("empty field access in where")
	}

	fieldName := fa.Chain[0]
	fd, ok := c.empObj.FieldsByAPIName[fieldName]
	if !ok {
		return nil, fmt.Errorf("unknown field %q", fieldName)
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

// compileSelfFieldLookup resolves self.field to a literal value at compile time.
func (c *Compiler) compileSelfFieldLookup(ctx context.Context, pipe *PipeExpr) (any, error) {
	if len(pipe.Steps) != 2 {
		return nil, fmt.Errorf("expected self.field, got complex pipe in where value")
	}
	_, isSelf := pipe.Steps[0].(*SelfExpr)
	fa, isFA := pipe.Steps[1].(*FieldAccess)
	if !isSelf || !isFA {
		return c.compileWhereSubqueryValue(ctx, pipe)
	}

	if c.selfID == "" {
		return nil, fmt.Errorf("`self` requires self_id in the request")
	}
	if len(fa.Chain) == 0 {
		return nil, fmt.Errorf("empty field on self")
	}

	fieldName := fa.Chain[0]
	if _, ok := c.empObj.FieldsByAPIName[fieldName]; !ok {
		return nil, fmt.Errorf("unknown field %q on employees", fieldName)
	}

	value, err := c.resolver.LookupFieldValue(ctx, c.selfID, fieldName)
	if err != nil {
		return nil, fmt.Errorf("self.%s: %w", fieldName, err)
	}

	return literalVal(value), nil
}

// compileWhereSubqueryValue compiles a pipe expression in where value position as a scalar subquery.
func (c *Compiler) compileWhereSubqueryValue(ctx context.Context, pipe *PipeExpr) (any, error) {
	cond, err := c.compileWhereSubquery(ctx, pipe)
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
func (c *Compiler) compileWhereSubquery(_ context.Context, pipe *PipeExpr) (Condition, error) {
	if len(pipe.Steps) < 2 {
		return nil, fmt.Errorf("subquery in where requires at least 2 pipe steps (source | aggregate)")
	}

	fn, ok := pipe.Steps[0].(*FuncCall)
	if !ok {
		return nil, fmt.Errorf("subquery source must be a function call, got %T", pipe.Steps[0])
	}

	aggOp := ""
	for _, step := range pipe.Steps[1:] {
		switch s := step.(type) {
		case *AggExpr:
			aggOp = s.Op
		case *FieldAccess:
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
func (c *Compiler) compileWhereFuncCall(ctx context.Context, fn *FuncCall) (Condition, error) {
	switch fn.Name {
	case "reports_to":
		if len(fn.Args) != 2 {
			return nil, fmt.Errorf("reports_to() requires 2 arguments")
		}
		if _, ok := fn.Args[0].(*DotExpr); !ok {
			return nil, fmt.Errorf("reports_to() in where expects '.' as first argument")
		}

		targetID, err := c.resolveEmployeeArg(ctx, fn.Args[1])
		if err != nil {
			return nil, fmt.Errorf("reports_to arg 2: %w", err)
		}

		targetPath, err := c.resolver.LookupPath(ctx, targetID)
		if err != nil {
			return nil, err
		}

		return ReportsTo{TargetPath: targetPath}, nil

	default:
		return nil, fmt.Errorf("function %q is not supported as a where condition", fn.Name)
	}
}

// tryCompileStringOp checks if a PipeExpr is a string operation pattern like `.field | contains("str")`.
func (c *Compiler) tryCompileStringOp(pipe *PipeExpr) (Condition, bool) {
	if len(pipe.Steps) != 2 {
		return nil, false
	}

	fa, isFA := pipe.Steps[0].(*FieldAccess)
	fn, isFn := pipe.Steps[1].(*FuncCall)
	if !isFA || !isFn {
		return nil, false
	}
	if len(fn.Args) != 1 {
		return nil, false
	}
	lit, isLit := fn.Args[0].(*Literal)
	if !isLit || lit.Kind != TokString {
		return nil, false
	}

	if len(fa.Chain) == 0 {
		return nil, false
	}
	if _, ok := c.empObj.FieldsByAPIName[fa.Chain[0]]; !ok {
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
func (c *Compiler) compileWhereFuncValue(_ context.Context, fn *FuncCall) (any, error) {
	switch fn.Name {
	case "contains":
		return nil, fmt.Errorf("contains() should be used with pipe syntax: .field | contains(\"str\")")
	default:
		return nil, fmt.Errorf("function %q is not supported in where value position", fn.Name)
	}
}

// --- Internal value types for where compilation ---

type fieldRef struct{ chain []string } // a validated field reference (API names)
type literalVal string                 // a literal value
type subqueryVal struct{ cond SubqueryAgg }

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
