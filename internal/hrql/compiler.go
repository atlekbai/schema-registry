package hrql

import (
	"context"
	"fmt"

	sq "github.com/Masterminds/squirrel"

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
	AggFunc  string           // "count", "sum", "avg", "min", "max"
	AggField *schema.FieldDef // nil for count(*)

	// KindBoolean fields
	BoolResult *bool
}

// Compiler compiles an HRQL AST into a Result.
type Compiler struct {
	cache    *schema.Cache
	resolver Resolver
	selfID   string
	empObj   *schema.ObjectDef
}

// NewCompiler creates a compiler for HRQL expressions.
func NewCompiler(cache *schema.Cache, resolver Resolver, selfID string) *Compiler {
	return &Compiler{
		cache:    cache,
		resolver: resolver,
		selfID:   selfID,
		empObj:   cache.Get("employees"),
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
