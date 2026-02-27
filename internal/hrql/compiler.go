package hrql

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/atlekbai/schema_registry/internal/query"
	"github.com/atlekbai/schema_registry/internal/schema"
)

// ResultKind classifies the output of a compiled HRQL expression.
type ResultKind int

const (
	KindList    ResultKind = iota // produces a list of records
	KindScalar                    // produces a single value (aggregation)
	KindBoolean                   // produces a boolean (reports_to)
)

// Result is the output of compiling an HRQL expression.
type Result struct {
	Kind ResultKind

	// KindList fields
	Conditions  []sq.Sqlizer
	OrderBy     *query.OrderClause
	Limit       int    // 0 = no override
	PickOp      string // "first", "last", "nth"
	PickN       int    // for nth (1-indexed)
	ExpandPlans []query.ExpandPlan

	// KindScalar fields
	AggFunc  string          // "count", "sum", "avg", "min", "max"
	AggField *schema.FieldDef // nil for count(*)

	// KindBoolean fields
	BoolResult *bool
}

// Compiler compiles an HRQL AST into a Result.
type Compiler struct {
	cache  *schema.Cache
	pool   *pgxpool.Pool
	selfID string
	empObj *schema.ObjectDef
}

// NewCompiler creates a compiler for HRQL expressions.
func NewCompiler(cache *schema.Cache, pool *pgxpool.Pool, selfID string) *Compiler {
	return &Compiler{
		cache:  cache,
		pool:   pool,
		selfID: selfID,
		empObj: cache.Get("employees"),
	}
}

// Compile compiles an AST node into a Result.
func (c *Compiler) Compile(ctx context.Context, node Node) (*Result, error) {
	if c.empObj == nil {
		return nil, fmt.Errorf("employees object not found in schema cache")
	}
	return c.compileNode(ctx, node)
}

func (c *Compiler) compileNode(ctx context.Context, node Node) (*Result, error) {
	switch n := node.(type) {
	case *PipeExpr:
		return c.compilePipe(ctx, n)
	case *SelfExpr:
		return c.compileSelf()
	case *IdentExpr:
		return c.compileIdent(n)
	case *FuncCall:
		return c.compileFuncCall(ctx, n)
	case *FieldAccess:
		// Standalone field access (without pipe source) — shouldn't happen at top level.
		return nil, fmt.Errorf("field access requires a source (use self.field or pipe)")
	default:
		return nil, fmt.Errorf("unexpected node type %T at top level", node)
	}
}

// compilePipe walks pipe steps left-to-right, accumulating state.
func (c *Compiler) compilePipe(ctx context.Context, pipe *PipeExpr) (*Result, error) {
	if len(pipe.Steps) == 0 {
		return nil, fmt.Errorf("empty pipe expression")
	}

	// Compile the source (first step).
	result, err := c.compileNode(ctx, pipe.Steps[0])
	if err != nil {
		return nil, err
	}

	// Apply each subsequent step.
	for _, step := range pipe.Steps[1:] {
		result, err = c.applyStep(ctx, result, step)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

// applyStep applies a single pipe step to the current result.
func (c *Compiler) applyStep(ctx context.Context, result *Result, step Node) (*Result, error) {
	switch s := step.(type) {
	case *FieldAccess:
		return c.applyFieldAccess(result, s)
	case *WhereExpr:
		return c.applyWhere(ctx, result, s)
	case *SortExpr:
		return c.applySort(result, s)
	case *PickExpr:
		return c.applyPick(result, s)
	case *AggExpr:
		return c.applyAgg(result, s)
	case *FuncCall:
		return c.applyFuncInPipe(ctx, result, s)
	default:
		return nil, fmt.Errorf("unexpected pipe step type %T", step)
	}
}

// compileSelf: the `self` employee — WHERE id = selfID.
func (c *Compiler) compileSelf() (*Result, error) {
	if c.selfID == "" {
		return nil, fmt.Errorf("`self` requires self_id in the request")
	}
	col := fmt.Sprintf(`%s."id"`, query.QI(query.Alias()))
	return &Result{
		Kind:       KindList,
		Conditions: []sq.Sqlizer{sq.Eq{col: c.selfID}},
		Limit:      1,
	}, nil
}

// compileIdent: `employees` → full table scan.
func (c *Compiler) compileIdent(n *IdentExpr) (*Result, error) {
	switch n.Name {
	case "employees":
		return &Result{Kind: KindList}, nil
	default:
		return nil, fmt.Errorf("unknown identifier %q", n.Name)
	}
}

// compileFuncCall handles org functions at source position.
func (c *Compiler) compileFuncCall(ctx context.Context, fn *FuncCall) (*Result, error) {
	switch fn.Name {
	case "chain":
		return c.compileChain(ctx, fn)
	case "reports":
		return c.compileReports(ctx, fn)
	case "peers":
		return c.compilePeers(ctx, fn)
	case "colleagues":
		return c.compileColleagues(ctx, fn)
	case "reports_to":
		return c.compileReportsTo(ctx, fn)
	default:
		return nil, fmt.Errorf("unknown function %q", fn.Name)
	}
}

func (c *Compiler) compileChain(ctx context.Context, fn *FuncCall) (*Result, error) {
	if len(fn.Args) < 1 || len(fn.Args) > 2 {
		return nil, fmt.Errorf("chain() requires 1-2 arguments: chain(employee [, depth])")
	}

	empID, err := c.resolveEmployeeArg(ctx, fn.Args[0])
	if err != nil {
		return nil, fmt.Errorf("chain arg 1: %w", err)
	}

	depth := 0
	if len(fn.Args) == 2 {
		depth, err = c.resolveIntArg(fn.Args[1])
		if err != nil {
			return nil, fmt.Errorf("chain arg 2: %w", err)
		}
	}

	path, err := c.lookupPath(ctx, empID)
	if err != nil {
		return nil, err
	}

	var conds []sq.Sqlizer
	if depth == 0 {
		conds = append(conds, query.ChainAll(path))
	} else {
		nlevel := nlevelFromPath(path)
		if depth >= nlevel {
			// No ancestors that far up — return empty.
			col := fmt.Sprintf(`%s."id"`, query.QI(query.Alias()))
			conds = append(conds, sq.Eq{col: nil})
		} else {
			conds = append(conds, query.ChainUp(path, depth))
		}
	}

	return &Result{Kind: KindList, Conditions: conds}, nil
}

func (c *Compiler) compileReports(ctx context.Context, fn *FuncCall) (*Result, error) {
	if len(fn.Args) < 1 || len(fn.Args) > 2 {
		return nil, fmt.Errorf("reports() requires 1-2 arguments: reports(employee [, depth])")
	}

	empID, err := c.resolveEmployeeArg(ctx, fn.Args[0])
	if err != nil {
		return nil, fmt.Errorf("reports arg 1: %w", err)
	}

	depth := 0
	if len(fn.Args) == 2 {
		depth, err = c.resolveIntArg(fn.Args[1])
		if err != nil {
			return nil, fmt.Errorf("reports arg 2: %w", err)
		}
	}

	path, err := c.lookupPath(ctx, empID)
	if err != nil {
		return nil, err
	}

	var conds []sq.Sqlizer
	if depth == 0 {
		conds = append(conds, query.Subtree(path))
	} else {
		conds = append(conds, query.ChainDown(path, depth))
	}

	return &Result{Kind: KindList, Conditions: conds}, nil
}

func (c *Compiler) compilePeers(ctx context.Context, fn *FuncCall) (*Result, error) {
	if len(fn.Args) != 1 {
		return nil, fmt.Errorf("peers() requires 1 argument: peers(employee)")
	}

	empID, err := c.resolveEmployeeArg(ctx, fn.Args[0])
	if err != nil {
		return nil, fmt.Errorf("peers arg 1: %w", err)
	}

	managerID, err := c.lookupField(ctx, empID, "manager_id")
	if err != nil {
		return nil, err
	}
	if managerID == "" {
		// Root node — no peers.
		col := fmt.Sprintf(`%s."id"`, query.QI(query.Alias()))
		return &Result{Kind: KindList, Conditions: []sq.Sqlizer{sq.Eq{col: nil}}}, nil
	}

	return &Result{
		Kind:       KindList,
		Conditions: []sq.Sqlizer{query.SameField("manager_id", managerID, empID)},
	}, nil
}

func (c *Compiler) compileColleagues(ctx context.Context, fn *FuncCall) (*Result, error) {
	if len(fn.Args) != 2 {
		return nil, fmt.Errorf("colleagues() requires 2 arguments: colleagues(employee, .field)")
	}

	empID, err := c.resolveEmployeeArg(ctx, fn.Args[0])
	if err != nil {
		return nil, fmt.Errorf("colleagues arg 1: %w", err)
	}

	// Second arg must be a field access like .department
	fa, ok := fn.Args[1].(*FieldAccess)
	if !ok {
		return nil, fmt.Errorf("colleagues arg 2: expected field reference (.field), got %T", fn.Args[1])
	}
	if len(fa.Chain) != 1 {
		return nil, fmt.Errorf("colleagues arg 2: expected single field (.field), got .%s", joinChain(fa.Chain))
	}

	fieldName := fa.Chain[0]
	fd, ok := c.empObj.FieldsByAPIName[fieldName]
	if !ok {
		return nil, fmt.Errorf("colleagues arg 2: unknown field %q", fieldName)
	}

	// Resolve the storage column for the field.
	var column string
	if fd.StorageColumn != nil {
		column = *fd.StorageColumn
	} else {
		return nil, fmt.Errorf("colleagues arg 2: field %q has no storage column", fieldName)
	}

	value, err := c.lookupField(ctx, empID, column)
	if err != nil {
		return nil, err
	}
	if value == "" {
		col := fmt.Sprintf(`%s."id"`, query.QI(query.Alias()))
		return &Result{Kind: KindList, Conditions: []sq.Sqlizer{sq.Eq{col: nil}}}, nil
	}

	return &Result{
		Kind:       KindList,
		Conditions: []sq.Sqlizer{query.SameField(column, value, empID)},
	}, nil
}

func (c *Compiler) compileReportsTo(ctx context.Context, fn *FuncCall) (*Result, error) {
	if len(fn.Args) != 2 {
		return nil, fmt.Errorf("reports_to() requires 2 arguments: reports_to(employee, target)")
	}

	empID, err := c.resolveEmployeeArg(ctx, fn.Args[0])
	if err != nil {
		return nil, fmt.Errorf("reports_to arg 1: %w", err)
	}

	targetID, err := c.resolveEmployeeArg(ctx, fn.Args[1])
	if err != nil {
		return nil, fmt.Errorf("reports_to arg 2: %w", err)
	}

	empPath, err := c.lookupPath(ctx, empID)
	if err != nil {
		return nil, err
	}
	tgtPath, err := c.lookupPath(ctx, targetID)
	if err != nil {
		return nil, err
	}

	var result bool
	err = c.pool.QueryRow(ctx,
		`SELECT $1::ltree <@ $2::ltree AND $1::ltree != $2::ltree`,
		empPath, tgtPath,
	).Scan(&result)
	if err != nil {
		return nil, fmt.Errorf("reports_to query: %w", err)
	}

	return &Result{Kind: KindBoolean, BoolResult: &result}, nil
}

// --- Step application ---

func (c *Compiler) applyFieldAccess(result *Result, fa *FieldAccess) (*Result, error) {
	if result.Kind != KindList {
		return nil, fmt.Errorf("field access requires a list, got %v", result.Kind)
	}

	// Resolve the first field in the chain to determine if it exists.
	if len(fa.Chain) == 0 {
		return nil, fmt.Errorf("empty field access")
	}

	fd, ok := c.empObj.FieldsByAPIName[fa.Chain[0]]
	if !ok {
		return nil, fmt.Errorf("unknown field %q on employees", fa.Chain[0])
	}

	// For LOOKUP fields with deeper chains, we need expand plans.
	if fd.Type == schema.FieldLookup && len(fa.Chain) > 1 {
		// Build expand plans for LOOKUP traversal used in where/filter context.
		// In pipe position, this projects the nested field value.
		// For now, track the field chain for the service layer to handle.
	}

	// Store the aggregation field if it's a numeric field (for sum/avg/min/max later).
	result.AggField = fd
	return result, nil
}

func (c *Compiler) applyWhere(ctx context.Context, result *Result, w *WhereExpr) (*Result, error) {
	if result.Kind != KindList {
		return nil, fmt.Errorf("where requires a list source")
	}

	cond, err := c.compileWhereCond(ctx, w.Cond)
	if err != nil {
		return nil, fmt.Errorf("where: %w", err)
	}

	result.Conditions = append(result.Conditions, cond)
	return result, nil
}

func (c *Compiler) applySort(result *Result, s *SortExpr) (*Result, error) {
	if result.Kind != KindList {
		return nil, fmt.Errorf("sort_by requires a list source")
	}
	if len(s.Field.Chain) == 0 {
		return nil, fmt.Errorf("sort_by: empty field")
	}

	fieldName := s.Field.Chain[0]
	if _, ok := c.empObj.FieldsByAPIName[fieldName]; !ok {
		return nil, fmt.Errorf("sort_by: unknown field %q", fieldName)
	}

	result.OrderBy = &query.OrderClause{FieldAPIName: fieldName, Desc: s.Desc}
	return result, nil
}

func (c *Compiler) applyPick(result *Result, p *PickExpr) (*Result, error) {
	if result.Kind != KindList {
		return nil, fmt.Errorf("%s requires a list source", p.Op)
	}

	result.PickOp = p.Op
	result.PickN = p.N

	switch p.Op {
	case "first":
		result.Limit = 1
	case "last":
		result.Limit = 1
		// Flip sort direction for last.
		if result.OrderBy != nil {
			result.OrderBy.Desc = !result.OrderBy.Desc
		} else {
			// Default: order by id desc to get last.
			result.OrderBy = &query.OrderClause{FieldAPIName: "id", Desc: true}
		}
	case "nth":
		// nth(n) — we need offset. The service layer handles this via LIMIT/OFFSET.
		result.Limit = 1
	}

	return result, nil
}

func (c *Compiler) applyAgg(result *Result, a *AggExpr) (*Result, error) {
	if result.Kind != KindList {
		return nil, fmt.Errorf("%s requires a list source", a.Op)
	}

	result.Kind = KindScalar
	result.AggFunc = a.Op
	// AggField is already set by a preceding FieldAccess step (or nil for count).
	return result, nil
}

func (c *Compiler) applyFuncInPipe(_ context.Context, result *Result, fn *FuncCall) (*Result, error) {
	switch fn.Name {
	case "contains", "starts_with", "ends_with":
		// These are string operations — they make sense in where conditions,
		// but in pipe position they produce a boolean for each item.
		// For now, only support them inside where.
		return nil, fmt.Errorf("%s() is only supported inside where() conditions", fn.Name)
	case "unique", "upper", "lower", "length":
		// These transform the pipe value. Mark as a post-processing hint.
		// For MVP, only `unique` and `length` are meaningful on lists.
		if fn.Name == "length" {
			result.Kind = KindScalar
			result.AggFunc = "count"
			return result, nil
		}
		return result, nil
	default:
		return nil, fmt.Errorf("function %q is not supported in pipe position", fn.Name)
	}
}

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

	value, err := c.lookupField(ctx, c.selfID, column)
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

		targetPath, err := c.lookupPath(ctx, targetID)
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

// --- Argument resolution helpers ---

// resolveEmployeeArg resolves a function argument to an employee UUID string.
func (c *Compiler) resolveEmployeeArg(ctx context.Context, arg Node) (string, error) {
	switch a := arg.(type) {
	case *SelfExpr:
		if c.selfID == "" {
			return "", fmt.Errorf("`self` requires self_id in the request")
		}
		return c.selfID, nil
	case *DotExpr:
		// `.` in function args means the current pipe item — only valid in correlated contexts.
		return "", fmt.Errorf("'.' cannot be resolved to an employee ID outside of where subqueries")
	case *PipeExpr:
		// self.manager → need to resolve.
		if len(a.Steps) == 2 {
			if _, ok := a.Steps[0].(*SelfExpr); ok {
				if fa, ok := a.Steps[1].(*FieldAccess); ok {
					return c.resolveSelfLookup(ctx, fa)
				}
			}
		}
		return "", fmt.Errorf("cannot resolve complex pipe expression to employee ID")
	case *IdentExpr:
		// Could be a UUID passed directly (frontend-resolved).
		return a.Name, nil
	case *Literal:
		if a.Kind == TokString {
			return a.Value, nil
		}
		return "", fmt.Errorf("expected employee reference, got %s", a.Kind)
	default:
		return "", fmt.Errorf("cannot resolve %T to employee ID", arg)
	}
}

// resolveSelfLookup resolves self.field to a value (for LOOKUP fields, returns the FK UUID).
func (c *Compiler) resolveSelfLookup(ctx context.Context, fa *FieldAccess) (string, error) {
	if len(fa.Chain) == 0 {
		return "", fmt.Errorf("empty field access")
	}
	fieldName := fa.Chain[0]
	fd, ok := c.empObj.FieldsByAPIName[fieldName]
	if !ok {
		return "", fmt.Errorf("unknown field %q", fieldName)
	}

	var column string
	if fd.StorageColumn != nil {
		column = *fd.StorageColumn
	} else {
		return "", fmt.Errorf("field %q has no storage column", fieldName)
	}

	value, err := c.lookupField(ctx, c.selfID, column)
	if err != nil {
		return "", err
	}

	// If there are more chain segments (self.manager.manager), resolve recursively.
	if len(fa.Chain) > 1 && value != "" {
		// The value is a FK UUID — look up the next field on that record.
		return c.resolveChainedLookup(ctx, value, fa.Chain[1:])
	}

	return value, nil
}

// resolveChainedLookup resolves a chain of LOOKUP fields from a starting ID.
func (c *Compiler) resolveChainedLookup(ctx context.Context, currentID string, fields []string) (string, error) {
	for _, fieldName := range fields {
		fd, ok := c.empObj.FieldsByAPIName[fieldName]
		if !ok {
			return "", fmt.Errorf("unknown field %q", fieldName)
		}
		var column string
		if fd.StorageColumn != nil {
			column = *fd.StorageColumn
		} else {
			return "", fmt.Errorf("field %q has no storage column", fieldName)
		}

		value, err := c.lookupField(ctx, currentID, column)
		if err != nil {
			return "", err
		}
		if value == "" {
			return "", nil
		}
		currentID = value
	}
	return currentID, nil
}

func (c *Compiler) resolveIntArg(arg Node) (int, error) {
	switch a := arg.(type) {
	case *Literal:
		if a.Kind != TokNumber {
			return 0, fmt.Errorf("expected number, got %s", a.Kind)
		}
		n, err := strconv.Atoi(a.Value)
		if err != nil {
			return 0, fmt.Errorf("invalid integer %q: %w", a.Value, err)
		}
		return n, nil
	case *UnaryMinus:
		inner, err := c.resolveIntArg(a.Expr)
		if err != nil {
			return 0, err
		}
		return -inner, nil
	default:
		return 0, fmt.Errorf("expected integer literal, got %T", arg)
	}
}

// --- DB helpers ---

func (c *Compiler) lookupPath(ctx context.Context, id string) (string, error) {
	var path string
	err := c.pool.QueryRow(ctx,
		`SELECT "manager_path"::text FROM "core"."employees" WHERE "id" = $1`, id,
	).Scan(&path)
	if err == pgx.ErrNoRows {
		return "", fmt.Errorf("employee %s not found", id)
	}
	if err != nil {
		return "", fmt.Errorf("lookup path: %w", err)
	}
	return path, nil
}

func (c *Compiler) lookupField(ctx context.Context, id, column string) (string, error) {
	var value *string
	q := fmt.Sprintf(`SELECT %s::text FROM "core"."employees" WHERE "id" = $1`, schema.QuoteIdent(column))
	err := c.pool.QueryRow(ctx, q, id).Scan(&value)
	if err == pgx.ErrNoRows {
		return "", fmt.Errorf("employee %s not found", id)
	}
	if err != nil {
		return "", fmt.Errorf("lookup field: %w", err)
	}
	if value == nil {
		return "", nil
	}
	return *value, nil
}

// --- Internal types for where compilation ---

type columnRef string   // a SQL column expression
type literalVal string  // a literal value to be parameterized
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
