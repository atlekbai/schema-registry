package hrql

import (
	"context"
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"

	"github.com/atlekbai/schema_registry/internal/query"
	"github.com/atlekbai/schema_registry/internal/schema"
)

// --- Where condition compilation ---

func (c *Compiler) compileWhereCond(ctx context.Context, node Node) (sq.Sqlizer, error) {
	switch n := node.(type) {
	case *BinaryOp:
		return c.compileWhereOp(ctx, n)
	case *FuncCall:
		return c.compileWhereFuncCall(ctx, n)
	case *PipeExpr:
		// Check for string operation pattern: .field | contains("str")
		if cond, ok := c.tryCompileStringOp(n); ok {
			return cond, nil
		}
		// Otherwise it's a subquery: reports(., 1) | count > 0
		return c.compileWhereSubquery(ctx, n)
	default:
		return nil, fmt.Errorf("unsupported condition type %T in where", node)
	}
}

func (c *Compiler) compileWhereOp(ctx context.Context, op *BinaryOp) (sq.Sqlizer, error) {
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
		return sq.And{left, right}, nil

	case "or":
		left, err := c.compileWhereCond(ctx, op.Left)
		if err != nil {
			return nil, err
		}
		right, err := c.compileWhereCond(ctx, op.Right)
		if err != nil {
			return nil, err
		}
		return sq.Or{left, right}, nil

	case "==", "!=", ">", ">=", "<", "<=":
		return c.compileComparison(ctx, op)

	default:
		return nil, fmt.Errorf("unsupported operator %q in where", op.Op)
	}
}

func (c *Compiler) compileComparison(ctx context.Context, op *BinaryOp) (sq.Sqlizer, error) {
	leftSQL, err := c.compileWhereValue(ctx, op.Left)
	if err != nil {
		return nil, fmt.Errorf("where left: %w", err)
	}

	rightSQL, err := c.compileWhereValue(ctx, op.Right)
	if err != nil {
		return nil, fmt.Errorf("where right: %w", err)
	}

	// If left is a column reference and right is a literal, use Squirrel ops.
	if col, ok := leftSQL.(columnRef); ok {
		if lit, ok := rightSQL.(literalVal); ok {
			return comparisonExpr(string(col), op.Op, string(lit)), nil
		}
		// Both are column refs or one is a subquery.
		if rcol, ok := rightSQL.(columnRef); ok {
			return sq.Expr(fmt.Sprintf(`%s %s %s`, string(col), sqlOp(op.Op), string(rcol))), nil
		}
	}

	// If right is a column and left is a literal (reversed comparison).
	if col, ok := rightSQL.(columnRef); ok {
		if lit, ok := leftSQL.(literalVal); ok {
			return comparisonExpr(string(col), reverseOp(op.Op), string(lit)), nil
		}
	}

	// Subquery comparison: left is a subquery expression.
	if sub, ok := leftSQL.(subqueryExpr); ok {
		if lit, ok := rightSQL.(literalVal); ok {
			return sq.Expr(fmt.Sprintf(`(%s) %s ?`, sub.sql, sqlOp(op.Op)), append(sub.args, string(lit))...), nil
		}
	}

	return nil, fmt.Errorf("unsupported comparison operands")
}

// compileWhereValue compiles a value expression inside a where condition.
// Returns a columnRef, literalVal, or subqueryExpr.
func (c *Compiler) compileWhereValue(ctx context.Context, node Node) (any, error) {
	switch n := node.(type) {
	case *FieldAccess:
		return c.resolveFieldToColumn(n)
	case *DotExpr:
		// `.` alone in where doesn't make sense — the user should use `.field`.
		return nil, fmt.Errorf("bare '.' in where condition; use '.field' to access a field")
	case *Literal:
		return literalVal(n.Value), nil
	case *SelfExpr:
		// self in a comparison — unlikely but handle by returning ID.
		return literalVal(c.selfID), nil
	case *PipeExpr:
		// self.field → resolve to literal value at compile time.
		return c.compileSelfFieldLookup(ctx, n)
	case *FuncCall:
		// Function in where value position — e.g., contains("str") as a pipe step.
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

// resolveFieldToColumn resolves a field access chain to a SQL column reference.
func (c *Compiler) resolveFieldToColumn(fa *FieldAccess) (any, error) {
	if len(fa.Chain) == 0 {
		return nil, fmt.Errorf("empty field access in where")
	}

	alias := query.Alias()
	fieldName := fa.Chain[0]
	fd, ok := c.empObj.FieldsByAPIName[fieldName]
	if !ok {
		return nil, fmt.Errorf("unknown field %q", fieldName)
	}

	if len(fa.Chain) == 1 {
		return columnRef(query.FilterExpr(alias, fd)), nil
	}

	// Multi-level: .department.title → need lateral join reference.
	// For where conditions, we use a subquery approach.
	if fd.Type != schema.FieldLookup || fd.LookupObjectID == nil {
		return nil, fmt.Errorf("field %q is not a LOOKUP field, cannot traverse", fieldName)
	}

	targetObj := c.cache.GetByID(*fd.LookupObjectID)
	if targetObj == nil {
		return nil, fmt.Errorf("lookup target for field %q not found", fieldName)
	}

	// Build subquery: (SELECT <final_col> FROM <target_table> WHERE id = <fk_ref>)
	currentAlias := alias
	currentFd := fd
	currentObj := targetObj

	for i := 1; i < len(fa.Chain); i++ {
		nextFieldName := fa.Chain[i]
		nextFd, ok := currentObj.FieldsByAPIName[nextFieldName]
		if !ok {
			return nil, fmt.Errorf("unknown field %q on %s", nextFieldName, currentObj.APIName)
		}

		if i == len(fa.Chain)-1 {
			// Final field — build the subquery.
			fkCol := fkRefExpr(currentAlias, currentFd)
			targetFrom := currentObj.TableName()
			targetCol := query.FilterExpr("_sub", nextFd)
			subSQL := fmt.Sprintf(`(SELECT %s FROM %s "_sub" WHERE "_sub"."id" = %s)`, targetCol, targetFrom, fkCol)
			return columnRef(subSQL), nil
		}

		// Intermediate LOOKUP — chain further.
		if nextFd.Type != schema.FieldLookup || nextFd.LookupObjectID == nil {
			return nil, fmt.Errorf("field %q is not a LOOKUP field, cannot traverse", nextFieldName)
		}
		nextObj := c.cache.GetByID(*nextFd.LookupObjectID)
		if nextObj == nil {
			return nil, fmt.Errorf("lookup target for field %q not found", nextFieldName)
		}

		// Build nested subquery for intermediate join.
		fkCol := fkRefExpr(currentAlias, currentFd)
		innerAlias := fmt.Sprintf("_sub%d", i)
		targetFrom := currentObj.TableName()

		// Replace the alias reference with a subquery that gets the next FK.
		var nextFkCol string
		if nextFd.StorageColumn != nil {
			nextFkCol = fmt.Sprintf(`"%s".%s`, innerAlias, query.QI(*nextFd.StorageColumn))
		} else {
			return nil, fmt.Errorf("custom field LOOKUP chains not yet supported")
		}

		// This gets complex for multi-hop. For now, support 2-level max.
		_ = targetFrom
		_ = fkCol
		_ = innerAlias
		_ = nextFkCol
		currentFd = nextFd
		currentObj = nextObj
		currentAlias = innerAlias
	}

	return nil, fmt.Errorf("LOOKUP chain too deep in where condition")
}

// compileSelfFieldLookup resolves self.field to a literal value at compile time.
func (c *Compiler) compileSelfFieldLookup(ctx context.Context, pipe *PipeExpr) (any, error) {
	if len(pipe.Steps) != 2 {
		return nil, fmt.Errorf("expected self.field, got complex pipe in where value")
	}
	_, isSelf := pipe.Steps[0].(*SelfExpr)
	fa, isFA := pipe.Steps[1].(*FieldAccess)
	if !isSelf || !isFA {
		// Could be a subquery pipe like `reports(., 1) | count`.
		return c.compileWhereSubqueryValue(ctx, pipe)
	}

	if c.selfID == "" {
		return nil, fmt.Errorf("`self` requires self_id in the request")
	}

	if len(fa.Chain) == 0 {
		return nil, fmt.Errorf("empty field on self")
	}

	fieldName := fa.Chain[0]
	fd, ok := c.empObj.FieldsByAPIName[fieldName]
	if !ok {
		return nil, fmt.Errorf("unknown field %q on employees", fieldName)
	}

	// For simple fields, look up the actual value from DB.
	var column string
	if fd.StorageColumn != nil {
		column = *fd.StorageColumn
	} else {
		column = fmt.Sprintf(`"custom_fields"->>%s`, quoteLit(fd.APIName))
	}

	value, err := c.resolver.LookupField(ctx, c.selfID, column)
	if err != nil {
		return nil, fmt.Errorf("self.%s: %w", fieldName, err)
	}

	return literalVal(value), nil
}

// compileWhereSubqueryValue compiles a pipe expression in where value position as a scalar subquery.
func (c *Compiler) compileWhereSubqueryValue(ctx context.Context, pipe *PipeExpr) (any, error) {
	// Compile the pipe as a subquery.
	sub, err := c.compileWhereSubquery(ctx, pipe)
	if err != nil {
		return nil, err
	}
	// Wrap as a subquery expression for comparison.
	sql, args, err := sub.ToSql()
	if err != nil {
		return nil, err
	}
	return subqueryExpr{sql: sql, args: args}, nil
}

// compileWhereSubquery compiles a pipe expression as a scalar subquery inside a where condition.
// e.g., `reports(., 1) | count > 0` → (SELECT count(*) FROM core.employees WHERE ...) > 0
func (c *Compiler) compileWhereSubquery(_ context.Context, pipe *PipeExpr) (sq.Sqlizer, error) {
	// This is a correlated subquery — `.` refers to each row being tested.
	// For now, support the pattern: orgFunc(., args) | [field |] aggFunc
	if len(pipe.Steps) < 2 {
		return nil, fmt.Errorf("subquery in where requires at least 2 pipe steps (source | aggregate)")
	}

	// Parse the source function.
	fn, ok := pipe.Steps[0].(*FuncCall)
	if !ok {
		return nil, fmt.Errorf("subquery source must be a function call, got %T", pipe.Steps[0])
	}

	// Determine the aggregate and optional field.
	aggOp := ""
	for _, step := range pipe.Steps[1:] {
		switch s := step.(type) {
		case *AggExpr:
			aggOp = s.Op
		case *FieldAccess:
			// Field access before aggregation — ignore for count, needed for sum/avg.
		default:
			return nil, fmt.Errorf("unsupported step %T in where subquery", step)
		}
	}

	if aggOp == "" {
		return nil, fmt.Errorf("where subquery must end with an aggregation (count, sum, avg, min, max)")
	}

	// Build the subquery SQL.
	return c.buildCorrelatedSubquery(fn, aggOp)
}

// buildCorrelatedSubquery builds a (SELECT agg FROM ... WHERE ...) expression.
func (c *Compiler) buildCorrelatedSubquery(fn *FuncCall, aggOp string) (sq.Sqlizer, error) {
	// The subquery references the outer row via "_e" alias columns.
	from := c.empObj.TableName() + ` "_sub_e"`
	subCol := `"_sub_e"."manager_path"`

	switch fn.Name {
	case "reports":
		depth := 0
		if len(fn.Args) >= 2 {
			var err error
			depth, err = c.resolveIntArg(fn.Args[1])
			if err != nil {
				return nil, err
			}
		}

		outerPath := fmt.Sprintf(`%s."manager_path"`, query.QI(query.Alias()))

		var whereCond string
		if depth == 0 {
			// Subtree
			whereCond = fmt.Sprintf(`%s <@ %s AND %s != %s`, subCol, outerPath, subCol, outerPath)
		} else {
			// Exact depth
			whereCond = fmt.Sprintf(`%s <@ %s AND nlevel(%s) = nlevel(%s) + %d`,
				subCol, outerPath, subCol, outerPath, depth)
		}

		subSQL := fmt.Sprintf(`(SELECT %s(*) FROM %s WHERE %s)`, aggOp, from, whereCond)
		return sq.Expr(subSQL), nil

	default:
		return nil, fmt.Errorf("correlated subquery not supported for %s()", fn.Name)
	}
}

// compileWhereFuncCall compiles a function call as a boolean condition.
func (c *Compiler) compileWhereFuncCall(ctx context.Context, fn *FuncCall) (sq.Sqlizer, error) {
	switch fn.Name {
	case "reports_to":
		// reports_to(., target) inside where → ltree <@ condition.
		if len(fn.Args) != 2 {
			return nil, fmt.Errorf("reports_to() requires 2 arguments")
		}

		// First arg should be `.` (the current row).
		if _, ok := fn.Args[0].(*DotExpr); !ok {
			return nil, fmt.Errorf("reports_to() in where expects '.' as first argument")
		}

		// Second arg should resolve to an employee ID.
		targetID, err := c.resolveEmployeeArg(ctx, fn.Args[1])
		if err != nil {
			return nil, fmt.Errorf("reports_to arg 2: %w", err)
		}

		targetPath, err := c.resolver.LookupPath(ctx, targetID)
		if err != nil {
			return nil, err
		}

		// WHERE _e.manager_path <@ targetPath AND _e.manager_path != targetPath
		col := fmt.Sprintf(`%s."manager_path"`, query.QI(query.Alias()))
		return sq.Expr(
			fmt.Sprintf(`%s <@ ?::ltree AND %s != ?::ltree`, col, col),
			targetPath, targetPath,
		), nil

	default:
		return nil, fmt.Errorf("function %q is not supported as a where condition", fn.Name)
	}
}

// tryCompileStringOp checks if a PipeExpr is a string operation pattern like `.field | contains("str")`
// and compiles it to an ILIKE condition. Returns (condition, true) if matched, (nil, false) otherwise.
func (c *Compiler) tryCompileStringOp(pipe *PipeExpr) (sq.Sqlizer, bool) {
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

	colRef, err := c.resolveFieldToColumn(fa)
	if err != nil {
		return nil, false
	}
	col, isCol := colRef.(columnRef)
	if !isCol {
		return nil, false
	}

	pattern := lit.Value
	switch fn.Name {
	case "contains":
		return sq.Expr(fmt.Sprintf(`%s ILIKE '%%' || ? || '%%'`, string(col)), pattern), true
	case "starts_with":
		return sq.Expr(fmt.Sprintf(`%s ILIKE ? || '%%'`, string(col)), pattern), true
	case "ends_with":
		return sq.Expr(fmt.Sprintf(`%s ILIKE '%%' || ?`, string(col)), pattern), true
	default:
		return nil, false
	}
}

// compileWhereFuncValue compiles a function in value position inside where.
func (c *Compiler) compileWhereFuncValue(_ context.Context, fn *FuncCall) (any, error) {
	switch fn.Name {
	case "contains":
		// .field | contains("str") → ILIKE pattern.
		// This is handled differently — return a special marker.
		return nil, fmt.Errorf("contains() should be used with pipe syntax: .field | contains(\"str\")")
	default:
		return nil, fmt.Errorf("function %q is not supported in where value position", fn.Name)
	}
}

// --- Internal types for where compilation ---

type columnRef string  // a SQL column expression
type literalVal string // a literal value to be parameterized
type subqueryExpr struct {
	sql  string
	args []any
}

func comparisonExpr(col, op, val string) sq.Sqlizer {
	switch op {
	case "==":
		return sq.Eq{col: val}
	case "!=":
		return sq.NotEq{col: val}
	default:
		return sq.Expr(fmt.Sprintf(`%s %s ?`, col, sqlOp(op)), val)
	}
}

func sqlOp(op string) string {
	switch op {
	case "==":
		return "="
	case "!=":
		return "!="
	default:
		return op
	}
}

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

func fkRefExpr(alias string, fd *schema.FieldDef) string {
	if fd.StorageColumn != nil {
		return fmt.Sprintf(`%s.%s`, query.QI(alias), query.QI(*fd.StorageColumn))
	}
	return fmt.Sprintf(`(%s."data"->>%s)::uuid`, query.QI(alias), quoteLit(fd.APIName))
}

func quoteLit(s string) string {
	return "'" + s + "'"
}

func joinChain(chain []string) string {
	return strings.Join(chain, ".")
}

func nlevelFromPath(path string) int {
	if path == "" {
		return 0
	}
	n := 1
	for _, ch := range path {
		if ch == '.' {
			n++
		}
	}
	return n
}
